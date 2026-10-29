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
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

// submissionLockKey identifies the transaction-level advisory lock that makes
// submission claims take turns, so two claims cannot both see a free AI place
// and exceed the limit.
const submissionLockKey = 7_215_001

// Audit actions written by SubmissionRepo.
const (
	auditActionSubmissionStarted  = "SUBMISSION_STARTED"
	auditActionSubmissionAccepted = "SUBMISSION_ACCEPTED"
	auditActionSubmissionUnknown  = "SUBMISSION_UNKNOWN"
	auditActionSubmissionRejected = "SUBMISSION_REJECTED"
)

// Attempt states that occupy an AI place: AI may be working on the task.
const activeAttemptStates = `('SUBMITTING', 'UNKNOWN', 'QUEUED', 'PROCESSING')`

// SubmissionRepo hands out tasks to send to AI and records the outcome:
// tables integration_attempts and integration_queue, plus audit rows
// (see docs/database.md).
type SubmissionRepo struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func NewSubmissionRepo(pool *pgxpool.Pool, queryTimeout time.Duration) *SubmissionRepo {
	return &SubmissionRepo{pool: pool, queryTimeout: queryTimeout}
}

// Claim starts up to limit new attempts for the oldest ready tasks, as long
// as fewer than maxInFlight attempts are active. Each attempt is owned by
// workerID for lease.
func (r *SubmissionRepo) Claim(ctx context.Context, workerID string, limit int, lease time.Duration, maxInFlight int) ([]submission.ClaimedSubmission, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, wrapSubmission("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claims take turns from here until commit (milliseconds).
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, submissionLockKey); err != nil {
		return nil, wrapSubmission("take submission lock", err)
	}

	var inFlight int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM integration_attempts WHERE state IN `+activeAttemptStates).Scan(&inFlight)
	if err != nil {
		return nil, wrapSubmission("count active attempts", err)
	}
	free := min(limit, maxInFlight-inFlight)
	if free <= 0 {
		return []submission.ClaimedSubmission{}, nil
	}

	rows, err := tx.Query(ctx, `
		SELECT request_guid, request_id, operation, request_payload
		FROM integration_queue
		WHERE status = 'READY_TO_SUBMIT' AND (retry_at IS NULL OR retry_at <= now())
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, free)
	if err != nil {
		return nil, wrapSubmission("pick ready tasks", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (submission.ClaimedSubmission, error) {
		var s submission.ClaimedSubmission
		var operation string
		var input []byte
		err := row.Scan(&s.RequestGUID, &s.RequestID, &operation, &input)
		s.Operation = domain.Operation(operation)
		s.Input = input
		return s, err
	})
	if err != nil {
		return nil, wrapSubmission("read ready tasks", err)
	}

	for i := range claimed {
		if err := startAttempt(ctx, tx, &claimed[i], workerID, lease); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, wrapSubmission("commit", err)
	}
	return claimed, nil
}

// startAttempt creates the next attempt of a locked ready task, moves the task
// to SUBMITTING and loads the task's documents in the order AI expects.
func startAttempt(ctx context.Context, tx pgx.Tx, s *submission.ClaimedSubmission, workerID string, lease time.Duration) error {
	s.AttemptID = uuid.New()
	s.ClaimToken = uuid.New()

	err := tx.QueryRow(ctx, `
		INSERT INTO integration_attempts (attempt_id, request_guid, attempt_no, state, claim_token, claim_until, claim_owner)
		VALUES ($1, $2,
		        (SELECT coalesce(max(attempt_no), 0) + 1 FROM integration_attempts WHERE request_guid = $2),
		        'SUBMITTING', $3, now() + make_interval(secs => $4), $5)
		RETURNING attempt_no, claim_until`,
		s.AttemptID, s.RequestGUID, s.ClaimToken, lease.Seconds(), workerID,
	).Scan(&s.AttemptNo, &s.ClaimUntil)
	if err != nil {
		return wrapSubmission("insert attempt", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE integration_queue
		SET status = 'SUBMITTING', active_attempt_id = $2, updated_at = now()
		WHERE request_guid = $1`,
		s.RequestGUID, s.AttemptID)
	if err != nil {
		return wrapSubmission("mark task submitting", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO integration_audit (request_guid, attempt_id, actor, action, old_state, new_state)
		VALUES ($1, $2, $3, $4, 'READY_TO_SUBMIT', 'SUBMITTING')`,
		s.RequestGUID, s.AttemptID, workerID, auditActionSubmissionStarted)
	if err != nil {
		return wrapSubmission("insert audit", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT position, s3_uri FROM integration_documents
		WHERE request_guid = $1
		ORDER BY position`, s.RequestGUID)
	if err != nil {
		return wrapSubmission("read documents", err)
	}
	s.Documents, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (submission.SubmissionDocument, error) {
		var d submission.SubmissionDocument
		err := row.Scan(&d.Position, &d.S3URI)
		return d, err
	})
	if err != nil {
		return wrapSubmission("read documents", err)
	}
	return nil
}

// reportTransition is what one outcome does to the attempt and the task.
type reportTransition struct {
	attemptState string
	taskStatus   domain.Status
	auditAction  string
	// polled: the attempt keeps its AI place and is checked again after the poll delay.
	polled bool
}

func transitionFor(report submission.Report) reportTransition {
	switch report.Outcome {
	case submission.OutcomeAccepted:
		if report.RemoteStatus == "PROCESSING" {
			return reportTransition{"PROCESSING", domain.StatusRemoteRunning, auditActionSubmissionAccepted, true}
		}
		return reportTransition{"QUEUED", domain.StatusRemoteQueued, auditActionSubmissionAccepted, true}
	case submission.OutcomeUnknown:
		return reportTransition{"UNKNOWN", domain.StatusSubmissionUnknown, auditActionSubmissionUnknown, true}
	default: // REJECTED
		return reportTransition{"NOT_ACCEPTED", domain.StatusFailed, auditActionSubmissionRejected, false}
	}
}

// Report applies what the worker learned from its POST to AI. ACCEPTED and
// UNKNOWN keep the AI place and schedule the first status check after
// pollDelay; REJECTED frees the place and fails the task. Repeating the same
// report after a lost response returns nil and writes nothing.
func (r *SubmissionRepo) Report(ctx context.Context, attemptID, claimToken uuid.UUID, report submission.Report, pollDelay time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return wrapSubmission("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock order: attempt, then its task, then writes.
	var (
		requestGUID uuid.UUID
		state       string
		owner       *string
		token       *uuid.UUID
		claimAlive  bool
	)
	err = tx.QueryRow(ctx, `
		SELECT request_guid, state, claim_owner, claim_token, coalesce(claim_until > now(), false)
		FROM integration_attempts
		WHERE attempt_id = $1
		FOR UPDATE`, attemptID,
	).Scan(&requestGUID, &state, &owner, &token, &claimAlive)
	if errors.Is(err, pgx.ErrNoRows) {
		return submission.ErrAttemptNotFound
	}
	if err != nil {
		return wrapSubmission("lock attempt", err)
	}

	next := transitionFor(report)
	if state != "SUBMITTING" {
		// The claim was released by an earlier report. The same report again
		// is a retry after a lost response; anything else is too late.
		if state == next.attemptState {
			return nil
		}
		return submission.ErrClaimLost
	}
	if token == nil || *token != claimToken || !claimAlive {
		return submission.ErrClaimLost
	}

	if _, err := tx.Exec(ctx, `SELECT 1 FROM integration_queue WHERE request_guid = $1 FOR UPDATE`, requestGUID); err != nil {
		return wrapSubmission("lock task", err)
	}

	var errorCode, errorDetail *string
	if report.Failure != nil {
		errorCode, errorDetail = &report.Failure.Code, &report.Failure.Message
	}

	_, err = tx.Exec(ctx, `
		UPDATE integration_attempts
		SET state = $2,
		    poll_at = CASE WHEN $3 THEN now() + make_interval(secs => $4) END,
		    finished_at = CASE WHEN $3 THEN NULL ELSE now() END,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $5, error_detail = $6, updated_at = now()
		WHERE attempt_id = $1`,
		attemptID, next.attemptState, next.polled, pollDelay.Seconds(), errorCode, errorDetail)
	if err != nil {
		return wrapSubmission("update attempt", err)
	}

	if next.polled {
		_, err = tx.Exec(ctx, `
			UPDATE integration_queue SET status = $2, updated_at = now()
			WHERE request_guid = $1`,
			requestGUID, string(next.taskStatus))
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE integration_queue
			SET status = $2, error_code = $3, error_detail = $4, completed_at = now(), updated_at = now()
			WHERE request_guid = $1`,
			requestGUID, string(next.taskStatus), errorCode, errorDetail)
	}
	if err != nil {
		return wrapSubmission("update task", err)
	}

	actor := "unknown"
	if owner != nil {
		actor = *owner
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO integration_audit (request_guid, attempt_id, actor, action, old_state, new_state, detail)
		VALUES ($1, $2, $3, $4, 'SUBMITTING', $5, $6)`,
		requestGUID, attemptID, actor, next.auditAction, string(next.taskStatus), errorCode)
	if err != nil {
		return wrapSubmission("insert audit", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return wrapSubmission("commit", err)
	}
	return nil
}

// Renew extends a live claim on an attempt being sent to now + lease.
func (r *SubmissionRepo) Renew(ctx context.Context, attemptID, claimToken uuid.UUID, lease time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	var claimUntil time.Time
	err := r.pool.QueryRow(ctx, `
		UPDATE integration_attempts
		SET claim_until = now() + make_interval(secs => $3), updated_at = now()
		WHERE attempt_id = $1 AND claim_token = $2 AND state = 'SUBMITTING' AND claim_until > now()
		RETURNING claim_until`,
		attemptID, claimToken, lease.Seconds(),
	).Scan(&claimUntil)
	if err == nil {
		return claimUntil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, wrapSubmission("renew claim", err)
	}

	// Nothing updated: tell a missing attempt apart from a lost claim.
	var exists bool
	err = r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM integration_attempts WHERE attempt_id = $1)`, attemptID).Scan(&exists)
	if err != nil {
		return time.Time{}, wrapSubmission("check attempt", err)
	}
	if !exists {
		return time.Time{}, submission.ErrAttemptNotFound
	}
	return time.Time{}, submission.ErrClaimLost
}

// wrapSubmission marks connection problems and timeouts as ErrStorageUnavailable,
// so the API answers 503 and the worker can retry.
func wrapSubmission(step string, err error) error {
	if IsUnavailable(err) {
		return fmt.Errorf("%s: %w: %w", step, submission.ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", step, err)
}
