package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/d9293/halyk-genai-integrator/internal/usecase/document"
)

// Audit actions written by DocumentRepo.
const (
	auditActionDocumentsPreparing = "DOCUMENTS_PREPARING"
	auditActionDocumentReady      = "DOCUMENT_READY"
	auditActionDocumentCopyFailed = "DOCUMENT_COPY_FAILED"
	auditActionTaskReady          = "TASK_READY"
	auditActionTaskNeedsAttention = "TASK_NEEDS_ATTENTION"
)

// taskErrorDocumentCopyFailed is the task error when one of its documents cannot be copied.
const taskErrorDocumentCopyFailed = "DOCUMENT_COPY_FAILED"

// claimDocumentsSQL takes up to $1 documents that are waiting to be copied, or
// whose previous claim expired, and gives them to the worker $3 for $2 seconds.
// SKIP LOCKED: documents another worker is claiming right now are skipped, so
// parallel workers never get the same document.
const claimDocumentsSQL = `
	WITH candidates AS (
		SELECT d.id
		FROM integration.documents d
		JOIN integration.queue q ON q.request_guid = d.request_guid
		WHERE q.status IN ('WAITING_DOCUMENTS', 'PREPARING_DOCUMENTS')
		  AND (   (d.status = 'PENDING' AND (d.retry_at IS NULL OR d.retry_at <= now()))
		       OR (d.status = 'COPYING' AND d.claim_until < now()))
		ORDER BY d.created_at, d.position
		LIMIT $1
		FOR UPDATE OF d SKIP LOCKED
	)
	UPDATE integration.documents d
	SET status      = 'COPYING',
	    claim_token = gen_random_uuid(),
	    claim_until = now() + make_interval(secs => $2),
	    claim_owner = $3,
	    updated_at  = now()
	FROM candidates c
	WHERE d.id = c.id
	RETURNING d.id, d.request_guid, d.document_id, d.attstore_url, d.position, d.claim_token, d.claim_until`

// startPreparingSQL moves tasks whose first document was just claimed from
// WAITING_DOCUMENTS to PREPARING_DOCUMENTS, with one audit row each. Tasks
// already preparing are left alone. Tasks are locked in request_guid order so
// two claims touching the same tasks cannot deadlock.
const startPreparingSQL = `
	WITH locked AS (
		SELECT request_guid FROM integration.queue
		WHERE request_guid = ANY($1::uuid[]) AND status = 'WAITING_DOCUMENTS'
		ORDER BY request_guid
		FOR UPDATE
	), started AS (
		UPDATE integration.queue q
		SET status = 'PREPARING_DOCUMENTS', updated_at = now()
		FROM locked
		WHERE q.request_guid = locked.request_guid AND q.status = 'WAITING_DOCUMENTS'
		RETURNING q.request_guid
	)
	INSERT INTO integration.audit (request_guid, actor, action, old_state, new_state)
	SELECT request_guid, $2, $3, 'WAITING_DOCUMENTS', 'PREPARING_DOCUMENTS'
	FROM started`

// DocumentRepo tracks copying of task documents: table integration.documents,
// plus the task status and audit rows it affects (see docs/database.md).
type DocumentRepo struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func NewDocumentRepo(pool *pgxpool.Pool, queryTimeout time.Duration) *DocumentRepo {
	return &DocumentRepo{pool: pool, queryTimeout: queryTimeout}
}

// Claim gives workerID up to limit documents to copy, each owned for lease.
func (r *DocumentRepo) Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]document.ClaimedDocument, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, wrapDocument("begin transaction", err)
	}
	// After Commit this is a no-op.
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, claimDocumentsSQL, limit, lease.Seconds(), workerID)
	if err != nil {
		return nil, wrapDocument("claim documents", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (document.ClaimedDocument, error) {
		var d document.ClaimedDocument
		err := row.Scan(&d.ID, &d.RequestGUID, &d.DocumentID, &d.AttstoreURL, &d.Position, &d.ClaimToken, &d.ClaimUntil)
		return d, err
	})
	if err != nil {
		return nil, wrapDocument("read claimed documents", err)
	}
	if len(claimed) == 0 {
		return claimed, nil
	}

	_, err = tx.Exec(ctx, startPreparingSQL, taskGUIDs(claimed), workerID, auditActionDocumentsPreparing)
	if err != nil {
		return nil, wrapDocument("start preparing tasks", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, wrapDocument("commit", err)
	}
	return claimed, nil
}

// lockedDocument is a document row locked for update by Complete or Fail.
type lockedDocument struct {
	requestGUID    uuid.UUID
	documentID     uuid.UUID
	status         string
	claimToken     *uuid.UUID
	claimOwner     *string
	claimAlive     bool // claim_until is still in the future
	s3URI          *string
	sizeBytes      *int64
	checksumSHA256 *string
	retryCount     int
}

// ownedBy reports whether claimToken is the live claim on a document being copied.
func (d lockedDocument) ownedBy(claimToken uuid.UUID) bool {
	return d.status == "COPYING" && d.claimToken != nil && *d.claimToken == claimToken && d.claimAlive
}

// actor is the worker that held the claim, for audit rows.
func (d lockedDocument) actor() string {
	if d.claimOwner == nil {
		return "unknown"
	}
	return *d.claimOwner
}

// lockDocument locks one document row. Lock order everywhere is: document,
// then its task (lockTaskStatus), then any writes. The task must be locked
// before writing audit rows: their foreign key takes a shared lock on the task,
// and two transactions holding that shared lock while both waiting for the
// exclusive one would deadlock.
func lockDocument(ctx context.Context, tx pgx.Tx, id uuid.UUID) (lockedDocument, error) {
	var d lockedDocument
	err := tx.QueryRow(ctx, `
		SELECT request_guid, document_id, status, claim_token, claim_owner,
		       coalesce(claim_until > now(), false), s3_uri, size_bytes, checksum_sha256, retry_count
		FROM integration.documents
		WHERE id = $1
		FOR UPDATE`, id,
	).Scan(&d.requestGUID, &d.documentID, &d.status, &d.claimToken, &d.claimOwner,
		&d.claimAlive, &d.s3URI, &d.sizeBytes, &d.checksumSHA256, &d.retryCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedDocument{}, document.ErrDocumentNotFound
	}
	if err != nil {
		return lockedDocument{}, wrapDocument("lock document", err)
	}
	return d, nil
}

// lockTaskStatus locks the task row and returns its status.
func lockTaskStatus(ctx context.Context, tx pgx.Tx, requestGUID uuid.UUID) (string, error) {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM integration.queue WHERE request_guid = $1 FOR UPDATE`, requestGUID).
		Scan(&status)
	if err != nil {
		return "", wrapDocument("lock task", err)
	}
	return status, nil
}

func insertAudit(ctx context.Context, tx pgx.Tx, requestGUID uuid.UUID, documentID *uuid.UUID,
	actor, action, oldState, newState string, detail *string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO integration.audit (request_guid, document_id, actor, action, old_state, new_state, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		requestGUID, documentID, actor, action, oldState, newState, detail)
	if err != nil {
		return wrapDocument("insert audit", err)
	}
	return nil
}

// Complete marks the document READY with its S3 copy. When it is the task's
// last document, the task becomes READY_TO_SUBMIT. Parallel completions of one
// task's documents wait for each other on the task row, so exactly one of them
// sees all documents READY and moves the task.
func (r *DocumentRepo) Complete(ctx context.Context, id, claimToken uuid.UUID, result document.CopyResult) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return wrapDocument("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	d, err := lockDocument(ctx, tx, id)
	if err != nil {
		return err
	}

	// A repeated call after a lost response: already READY with the same copy.
	if d.status == "READY" {
		if d.s3URI != nil && *d.s3URI == result.S3URI &&
			d.sizeBytes != nil && *d.sizeBytes == result.SizeBytes &&
			d.checksumSHA256 != nil && *d.checksumSHA256 == result.ChecksumSHA256 {
			return nil
		}
		return document.ErrClaimLost
	}
	if !d.ownedBy(claimToken) {
		return document.ErrClaimLost
	}
	taskStatus, err := lockTaskStatus(ctx, tx, d.requestGUID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE integration.documents
		SET status = 'READY', s3_uri = $2, size_bytes = $3, checksum_sha256 = $4,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = NULL, error_detail = NULL, updated_at = now()
		WHERE id = $1`,
		id, result.S3URI, result.SizeBytes, result.ChecksumSHA256)
	if err != nil {
		return wrapDocument("mark document ready", err)
	}
	if err := insertAudit(ctx, tx, d.requestGUID, &id, d.actor(), auditActionDocumentReady, "COPYING", "READY", nil); err != nil {
		return err
	}

	if taskStatus == "PREPARING_DOCUMENTS" {
		// Runs while holding the task lock: sees every document completed before us.
		tag, err := tx.Exec(ctx, `
			UPDATE integration.queue
			SET status = 'READY_TO_SUBMIT', retry_at = NULL, updated_at = now()
			WHERE request_guid = $1
			  AND NOT EXISTS (SELECT 1 FROM integration.documents
			                  WHERE request_guid = $1 AND status <> 'READY')`,
			d.requestGUID)
		if err != nil {
			return wrapDocument("mark task ready", err)
		}
		if tag.RowsAffected() == 1 {
			err := insertAudit(ctx, tx, d.requestGUID, nil, d.actor(), auditActionTaskReady,
				"PREPARING_DOCUMENTS", "READY_TO_SUBMIT", nil)
			if err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return wrapDocument("commit", err)
	}
	return nil
}

// Fail records a failed copy. A retryable failure within the policy's budget
// returns the document to PENDING with a delayed retry_at; otherwise the
// document and its task go to NEEDS_ATTENTION. A repeated call after a lost
// response finds the claim already released and gets ErrClaimLost.
func (r *DocumentRepo) Fail(ctx context.Context, id, claimToken uuid.UUID, failure document.CopyFailure, policy document.RetryPolicy) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return wrapDocument("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	d, err := lockDocument(ctx, tx, id)
	if err != nil {
		return err
	}
	if !d.ownedBy(claimToken) {
		return document.ErrClaimLost
	}
	taskStatus, err := lockTaskStatus(ctx, tx, d.requestGUID)
	if err != nil {
		return err
	}

	failures := d.retryCount + 1
	if failure.Retryable && !policy.Exhausted(failures) {
		_, err = tx.Exec(ctx, `
			UPDATE integration.documents
			SET status = 'PENDING', retry_count = $2, retry_at = now() + make_interval(secs => $3),
			    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
			    error_code = $4, error_detail = $5, updated_at = now()
			WHERE id = $1`,
			id, failures, policy.Delay(failures).Seconds(), failure.Code, failure.Message)
		if err != nil {
			return wrapDocument("schedule document retry", err)
		}
		err = insertAudit(ctx, tx, d.requestGUID, &id, d.actor(), auditActionDocumentCopyFailed, "COPYING", "PENDING", &failure.Code)
		if err != nil {
			return err
		}
		return commitDocument(ctx, tx)
	}

	// No more retries: the task cannot be sent to AI without this document.
	_, err = tx.Exec(ctx, `
		UPDATE integration.documents
		SET status = 'NEEDS_ATTENTION', retry_count = $2, retry_at = NULL,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $3, error_detail = $4, updated_at = now()
		WHERE id = $1`,
		id, failures, failure.Code, failure.Message)
	if err != nil {
		return wrapDocument("mark document needs attention", err)
	}
	err = insertAudit(ctx, tx, d.requestGUID, &id, d.actor(), auditActionDocumentCopyFailed, "COPYING", "NEEDS_ATTENTION", &failure.Code)
	if err != nil {
		return err
	}

	if taskStatus == "WAITING_DOCUMENTS" || taskStatus == "PREPARING_DOCUMENTS" {
		detail := fmt.Sprintf("Document %s could not be copied after %d attempt(s): %s", d.documentID, failures, failure.Code)
		_, err = tx.Exec(ctx, `
			UPDATE integration.queue
			SET status = 'NEEDS_ATTENTION', error_code = $2, error_detail = $3, updated_at = now()
			WHERE request_guid = $1`,
			d.requestGUID, taskErrorDocumentCopyFailed, detail)
		if err != nil {
			return wrapDocument("mark task needs attention", err)
		}
		err = insertAudit(ctx, tx, d.requestGUID, &id, d.actor(), auditActionTaskNeedsAttention, taskStatus, "NEEDS_ATTENTION", &failure.Code)
		if err != nil {
			return err
		}
	}
	return commitDocument(ctx, tx)
}

// Renew extends a live claim to now + lease. An expired claim cannot be
// revived: another worker may already own the document.
func (r *DocumentRepo) Renew(ctx context.Context, id, claimToken uuid.UUID, lease time.Duration) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	var claimUntil time.Time
	err := r.pool.QueryRow(ctx, `
		UPDATE integration.documents
		SET claim_until = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND claim_token = $2 AND status = 'COPYING' AND claim_until > now()
		RETURNING claim_until`,
		id, claimToken, lease.Seconds(),
	).Scan(&claimUntil)
	if err == nil {
		return claimUntil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, wrapDocument("renew claim", err)
	}

	// Nothing updated: tell a missing document apart from a lost claim.
	var exists bool
	err = r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM integration.documents WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return time.Time{}, wrapDocument("check document", err)
	}
	if !exists {
		return time.Time{}, document.ErrDocumentNotFound
	}
	return time.Time{}, document.ErrClaimLost
}

func commitDocument(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return wrapDocument("commit", err)
	}
	return nil
}

// taskGUIDs lists the distinct tasks of the claimed documents.
func taskGUIDs(claimed []document.ClaimedDocument) []string {
	seen := make(map[string]bool)
	var guids []string
	for _, d := range claimed {
		guid := d.RequestGUID.String()
		if !seen[guid] {
			seen[guid] = true
			guids = append(guids, guid)
		}
	}
	return guids
}

// wrapDocument marks connection problems and timeouts as ErrStorageUnavailable,
// so the API answers 503 and the worker can retry.
func wrapDocument(step string, err error) error {
	if IsUnavailable(err) {
		return fmt.Errorf("%s: %w: %w", step, document.ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", step, err)
}
