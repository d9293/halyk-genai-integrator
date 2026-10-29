package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/result"
)

// claimResultsSQL takes up to $1 final tasks whose result has not reached
// Kafka yet, are due, and no other worker is publishing, and gives them to
// worker $3 for $2 seconds. Publishing bookkeeping does not touch updated_at:
// BPM sees that field, and the task itself does not change.
const claimResultsSQL = `
	WITH due AS (
		SELECT request_guid FROM integration_queue
		WHERE status IN ('SUCCEEDED', 'FAILED') AND published_at IS NULL
		  AND (publish_retry_at IS NULL OR publish_retry_at <= now())
		  AND (publish_claim_until IS NULL OR publish_claim_until < now())
		ORDER BY completed_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE integration_queue q
	SET publish_claim_token = gen_random_uuid(),
	    publish_claim_until = now() + make_interval(secs => $2),
	    publish_claim_owner = $3
	FROM due
	WHERE q.request_guid = due.request_guid
	RETURNING q.request_guid, q.request_id, q.operation, q.status, q.result_payload,
	          q.error_code, q.error_detail, q.completed_at, q.publish_claim_token, q.publish_claim_until`

// ResultRepo tracks delivery of final task results to BPM through Kafka:
// the published_at / publish_* columns of integration_queue (see docs/database.md).
type ResultRepo struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func NewResultRepo(pool *pgxpool.Pool, queryTimeout time.Duration) *ResultRepo {
	return &ResultRepo{pool: pool, queryTimeout: queryTimeout}
}

// Claim gives workerID up to limit results to publish, each owned for lease.
// The message is built from the task row, so every re-send is identical; the
// caller adds the topic and the envelope fields.
func (r *ResultRepo) Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]result.ClaimedResult, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	rows, err := r.pool.Query(ctx, claimResultsSQL, limit, lease.Seconds(), workerID)
	if err != nil {
		return nil, wrapResult("claim results", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (result.ClaimedResult, error) {
		var (
			c                    result.ClaimedResult
			operation, status    string
			payload              []byte
			errorCode, errorText *string
		)
		err := row.Scan(&c.RequestGUID, &c.Message.RequestID, &operation, &status, &payload,
			&errorCode, &errorText, &c.Message.OccurredAt, &c.ClaimToken, &c.ClaimUntil)
		c.Message.RequestGUID = c.RequestGUID
		c.Message.Operation = domain.Operation(operation)
		c.Message.Status = domain.Status(status)
		c.Message.Result = payload
		if errorCode != nil && errorText != nil {
			c.Message.Error = &domain.Failure{Code: *errorCode, Message: *errorText}
		}
		return c, err
	})
	if err != nil {
		return nil, wrapResult("read claimed results", err)
	}
	return claimed, nil
}

// Published records the broker ACK. Repeating it after a lost response, or
// after another worker already published the same result, returns nil.
func (r *ResultRepo) Published(ctx context.Context, requestGUID, claimToken uuid.UUID) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tag, err := r.pool.Exec(ctx, `
		UPDATE integration_queue
		SET published_at = now(),
		    publish_claim_token = NULL, publish_claim_until = NULL, publish_claim_owner = NULL,
		    publish_error_code = NULL, publish_error_detail = NULL
		WHERE request_guid = $1 AND publish_claim_token = $2 AND publish_claim_until > now()
		  AND published_at IS NULL`,
		requestGUID, claimToken)
	if err != nil {
		return wrapResult("mark published", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	published, err := r.deliveryState(ctx, requestGUID)
	if err != nil {
		return err
	}
	if published {
		return nil // already delivered; BPM deduplicates by requestGuid
	}
	return result.ErrClaimLost
}

// Failed records that Kafka did not accept the result: it is tried again
// after retryBase · 2^(earlier failures), at most retryMax. It is never dropped.
func (r *ResultRepo) Failed(ctx context.Context, requestGUID, claimToken uuid.UUID, failure domain.Failure, retryBase, retryMax time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tag, err := r.pool.Exec(ctx, `
		UPDATE integration_queue
		SET publish_retry_count = publish_retry_count + 1,
		    publish_retry_at = now() + make_interval(secs => least($3 * power(2, publish_retry_count), $4)),
		    publish_claim_token = NULL, publish_claim_until = NULL, publish_claim_owner = NULL,
		    publish_error_code = $5, publish_error_detail = $6
		WHERE request_guid = $1 AND publish_claim_token = $2 AND publish_claim_until > now()
		  AND published_at IS NULL`,
		requestGUID, claimToken, retryBase.Seconds(), retryMax.Seconds(), failure.Code, failure.Message)
	if err != nil {
		return wrapResult("record publish failure", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	if _, err := r.deliveryState(ctx, requestGUID); err != nil {
		return err
	}
	return result.ErrClaimLost
}

// deliveryState reports whether the task's result was already published,
// or ErrResultNotFound if the task has no result to deliver.
func (r *ResultRepo) deliveryState(ctx context.Context, requestGUID uuid.UUID) (published bool, err error) {
	err = r.pool.QueryRow(ctx, `
		SELECT published_at IS NOT NULL FROM integration_queue
		WHERE request_guid = $1 AND status IN ('SUCCEEDED', 'FAILED')`, requestGUID).Scan(&published)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, result.ErrResultNotFound
	}
	if err != nil {
		return false, wrapResult("check result", err)
	}
	return published, nil
}

// wrapResult marks connection problems and timeouts as ErrStorageUnavailable,
// so the API answers 503 and the worker can retry.
func wrapResult(step string, err error) error {
	if IsUnavailable(err) {
		return fmt.Errorf("%s: %w: %w", step, result.ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", step, err)
}
