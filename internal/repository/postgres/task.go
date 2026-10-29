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
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

const auditActionRegistered = "TASK_REGISTERED"

const selectTaskColumns = `
	SELECT request_guid, request_id, caller_id, operation, request_payload, request_hash, status,
	       result_payload, error_code, error_detail, created_at, updated_at, completed_at
	FROM integration_queue`

// TaskRepo stores BPM tasks: tables integration_queue, integration_documents
// and integration_audit (see docs/database.md).
type TaskRepo struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func NewTaskRepo(pool *pgxpool.Pool, queryTimeout time.Duration) *TaskRepo {
	return &TaskRepo{pool: pool, queryTimeout: queryTimeout}
}

// CreateIfAbsent registers t in one transaction: the task, its documents in
// BPM order, and one audit row. If the requestGuid already exists it writes
// nothing and returns the stored task.
func (r *TaskRepo) CreateIfAbsent(ctx context.Context, t domain.Task) (domain.Task, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Task{}, false, wrapTask("begin transaction", err)
	}
	// After Commit this is a no-op.
	defer func() { _ = tx.Rollback(ctx) }()

	// ON CONFLICT instead of SELECT-then-INSERT: if two requests race, the
	// loser waits for the winner's commit and then inserts nothing.
	err = tx.QueryRow(ctx, `
		INSERT INTO integration_queue (request_guid, request_id, caller_id, operation, request_payload, request_hash, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (request_guid) DO NOTHING
		RETURNING created_at, updated_at`,
		t.RequestGUID, t.RequestID, t.CallerID, string(t.Operation), string(t.Input), t.RequestHash, string(t.Status),
	).Scan(&t.CreatedAt, &t.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		existing, err := scanTask(tx.QueryRow(ctx, selectTaskColumns+` WHERE request_guid = $1`, t.RequestGUID))
		if err != nil {
			return domain.Task{}, false, wrapTask("read existing task", err)
		}
		return existing, false, nil
	}
	if err != nil {
		return domain.Task{}, false, wrapTask("insert task", err)
	}

	for position, d := range t.Documents {
		_, err := tx.Exec(ctx, `
			INSERT INTO integration_documents (id, request_guid, document_id, position, attstore_url)
			VALUES ($1, $2, $3, $4, $5)`,
			uuid.New(), t.RequestGUID, d.DocumentID, position, d.AttstoreURL)
		if err != nil {
			return domain.Task{}, false, wrapTask("insert document", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO integration_audit (request_guid, actor, action, new_state)
		VALUES ($1, $2, $3, $4)`,
		t.RequestGUID, t.CallerID, auditActionRegistered, string(t.Status))
	if err != nil {
		return domain.Task{}, false, wrapTask("insert audit", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Task{}, false, wrapTask("commit", err)
	}
	return t, true, nil
}

// Get returns the task without its documents; the API response does not include them.
func (r *TaskRepo) Get(ctx context.Context, callerID string, requestGUID uuid.UUID) (domain.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	t, err := scanTask(r.pool.QueryRow(ctx, selectTaskColumns+` WHERE request_guid = $1 AND caller_id = $2`,
		requestGUID, callerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, task.ErrTaskNotFound
	}
	if err != nil {
		return domain.Task{}, wrapTask("select task", err)
	}
	return t, nil
}

func scanTask(row pgx.Row) (domain.Task, error) {
	var (
		t                    domain.Task
		operation, status    string
		input, result        []byte
		errorCode, errorText *string
	)
	err := row.Scan(&t.RequestGUID, &t.RequestID, &t.CallerID, &operation, &input, &t.RequestHash, &status,
		&result, &errorCode, &errorText, &t.CreatedAt, &t.UpdatedAt, &t.CompletedAt)
	if err != nil {
		return domain.Task{}, err
	}

	t.Operation = domain.Operation(operation)
	t.Status = domain.Status(status)
	t.Input = input
	t.Result = result // nil when NULL
	if errorCode != nil && errorText != nil {
		t.Error = &domain.Failure{Code: *errorCode, Message: *errorText}
	}
	return t, nil
}

// wrapTask marks connection problems and timeouts as ErrStorageUnavailable, so the
// API answers 503 and the client can retry.
func wrapTask(step string, err error) error {
	if IsUnavailable(err) {
		return fmt.Errorf("%s: %w: %w", step, task.ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", step, err)
}
