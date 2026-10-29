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
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
)

// Audit actions written by PollRepo.
const (
	auditActionSubmissionRecovered = "SUBMISSION_RECOVERED"
	auditActionRemoteStatus        = "REMOTE_STATUS"
	auditActionTaskSucceeded       = "TASK_SUCCEEDED"
	auditActionTaskFailed          = "TASK_FAILED"
	auditActionResultRejected      = "RESULT_REJECTED"
	auditActionAITaskLostResubmit  = "AI_TASK_LOST_RESUBMIT"
	auditActionAITaskLostFailed    = "AI_TASK_LOST_RETRIES_EXHAUSTED"
	auditActionAINoProgress        = "AI_NO_PROGRESS"
)

// Error codes written by PollRepo.
const (
	errorSubmissionAbandoned = "SUBMISSION_ABANDONED"
	errorAITaskLost          = "AI_TASK_LOST"
	errorRetriesExhausted    = "AI_TASK_LOST_RETRIES_EXHAUSTED"
	errorResultTooLarge      = "RESULT_TOO_LARGE"
	errorAINoProgress        = "AI_NO_PROGRESS"
)

// maxRecoveredPerClaim bounds the recovery work done by one claim call.
const maxRecoveredPerClaim = 100

// recoverSubmissionsSQL turns attempts whose sender died (claim expired while
// still SUBMITTING) into UNKNOWN, due for a status check right away. They are
// never sent again blindly: AI may already have them. They keep their AI place.
const recoverSubmissionsSQL = `
	WITH expired AS (
		SELECT attempt_id FROM integration_attempts
		WHERE state = 'SUBMITTING' AND claim_until < now()
		ORDER BY claim_until
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	), recovered AS (
		UPDATE integration_attempts a
		SET state = 'UNKNOWN', poll_at = now(),
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $2, error_detail = 'The worker did not report the outcome before its claim expired',
		    updated_at = now()
		FROM expired
		WHERE a.attempt_id = expired.attempt_id
		RETURNING a.attempt_id, a.request_guid
	), tasks AS (
		UPDATE integration_queue q
		SET status = 'SUBMISSION_UNKNOWN', updated_at = now()
		FROM recovered
		WHERE q.request_guid = recovered.request_guid AND q.status = 'SUBMITTING'
		RETURNING q.request_guid
	)
	INSERT INTO integration_audit (request_guid, attempt_id, actor, action, old_state, new_state, detail)
	SELECT request_guid, attempt_id, 'system', $3, 'SUBMITTING', 'SUBMISSION_UNKNOWN', $2
	FROM recovered`

// claimPollsSQL takes up to $1 attempts whose status check is due and that no
// other worker is checking, and gives them to worker $3 for $2 seconds.
const claimPollsSQL = `
	WITH due AS (
		SELECT attempt_id FROM integration_attempts
		WHERE state IN ('UNKNOWN', 'QUEUED', 'PROCESSING')
		  AND poll_at <= now()
		  AND (claim_until IS NULL OR claim_until < now())
		ORDER BY poll_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE integration_attempts a
	SET claim_token = gen_random_uuid(),
	    claim_until = now() + make_interval(secs => $2),
	    claim_owner = $3,
	    updated_at  = now()
	FROM due, integration_queue q
	WHERE a.attempt_id = due.attempt_id AND q.request_guid = a.request_guid
	RETURNING a.attempt_id, a.attempt_no, a.request_guid, q.request_id, q.operation, a.claim_token, a.claim_until`

// PollRepo hands out status checks of tasks at AI and applies the answers:
// tables integration_attempts and integration_queue, plus audit rows
// (see docs/database.md).
type PollRepo struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

func NewPollRepo(pool *pgxpool.Pool, queryTimeout time.Duration) *PollRepo {
	return &PollRepo{pool: pool, queryTimeout: queryTimeout}
}

// Claim first recovers abandoned submissions, then gives workerID up to limit
// due status checks, each owned for lease.
func (r *PollRepo) Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]poll.ClaimedPoll, error) {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, wrapPoll("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, recoverSubmissionsSQL, maxRecoveredPerClaim, errorSubmissionAbandoned, auditActionSubmissionRecovered)
	if err != nil {
		return nil, wrapPoll("recover abandoned submissions", err)
	}

	rows, err := tx.Query(ctx, claimPollsSQL, limit, lease.Seconds(), workerID)
	if err != nil {
		return nil, wrapPoll("claim polls", err)
	}
	claimed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (poll.ClaimedPoll, error) {
		var p poll.ClaimedPoll
		var operation string
		err := row.Scan(&p.AttemptID, &p.AttemptNo, &p.RequestGUID, &p.RequestID, &operation, &p.ClaimToken, &p.ClaimUntil)
		p.Operation = domain.Operation(operation)
		return p, err
	})
	if err != nil {
		return nil, wrapPoll("read claimed polls", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, wrapPoll("commit", err)
	}
	return claimed, nil
}

// polledAttempt is an attempt row locked for update by Observe.
type polledAttempt struct {
	requestGUID     uuid.UUID
	state           string
	claimToken      *uuid.UUID
	claimOwner      *string
	claimAlive      bool
	notFoundCount   int
	notFoundSeconds float64 // since the first NOT_FOUND of the current streak; 0 if none
	ageSeconds      float64 // since the attempt was sent
	taskStatus      string
	taskRetryCount  int
}

// finalState is the attempt state an outcome leads to, used to recognize a
// repeated observation. NOT_FOUND and TECHNICAL_ERROR have none: repeating
// them must not count twice.
func finalState(outcome poll.Outcome) string {
	switch outcome {
	case poll.OutcomeQueued:
		return "QUEUED"
	case poll.OutcomeProcessing:
		return "PROCESSING"
	case poll.OutcomeSucceeded:
		return "DONE"
	case poll.OutcomeFailed:
		return "ERROR"
	}
	return ""
}

// Observe applies what AI answered to one status check. Lock order: attempt,
// then its task, then writes. A repeated QUEUED/PROCESSING/SUCCEEDED/FAILED
// after a lost response returns nil and writes nothing; a repeated NOT_FOUND
// or TECHNICAL_ERROR gets ErrClaimLost.
func (r *PollRepo) Observe(ctx context.Context, attemptID, claimToken uuid.UUID, obs poll.Observation, rules poll.Rules) error {
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return wrapPoll("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var a polledAttempt
	err = tx.QueryRow(ctx, `
		SELECT request_guid, state, claim_token, claim_owner, coalesce(claim_until > now(), false),
		       not_found_count, coalesce(extract(epoch FROM now() - first_not_found_at), 0),
		       extract(epoch FROM now() - created_at)
		FROM integration_attempts
		WHERE attempt_id = $1
		FOR UPDATE`, attemptID,
	).Scan(&a.requestGUID, &a.state, &a.claimToken, &a.claimOwner, &a.claimAlive, &a.notFoundCount, &a.notFoundSeconds, &a.ageSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return poll.ErrAttemptNotFound
	}
	if err != nil {
		return wrapPoll("lock attempt", err)
	}

	owned := a.claimToken != nil && *a.claimToken == claimToken && a.claimAlive &&
		(a.state == "UNKNOWN" || a.state == "QUEUED" || a.state == "PROCESSING")
	if !owned {
		if a.claimToken == nil && finalState(obs.Outcome) != "" && a.state == finalState(obs.Outcome) {
			return nil // repeated after a lost response
		}
		return poll.ErrClaimLost
	}

	err = tx.QueryRow(ctx, `SELECT status, retry_count FROM integration_queue WHERE request_guid = $1 FOR UPDATE`, a.requestGUID).
		Scan(&a.taskStatus, &a.taskRetryCount)
	if err != nil {
		return wrapPoll("lock task", err)
	}

	o := &observer{tx: tx, attemptID: attemptID, attempt: a, rules: rules}
	var stillAtAI bool // the observation did not end the attempt
	switch obs.Outcome {
	case poll.OutcomeQueued, poll.OutcomeProcessing:
		err, stillAtAI = o.progress(ctx, obs.Outcome), true
	case poll.OutcomeSucceeded:
		err = o.succeeded(ctx, obs.Result)
	case poll.OutcomeFailed:
		err = o.failed(ctx, obs.Failure)
	case poll.OutcomeNotFound:
		stillAtAI, err = o.notFound(ctx)
	case poll.OutcomeTechnicalError:
		err, stillAtAI = o.technicalError(ctx, obs.Failure), true
	default:
		err = fmt.Errorf("unknown outcome %q", obs.Outcome)
	}
	if err == nil && stillAtAI {
		err = o.flagNoProgress(ctx)
	}
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return wrapPoll("commit", err)
	}
	return nil
}

// observer applies one observation to a locked attempt and its locked task.
type observer struct {
	tx        pgx.Tx
	attemptID uuid.UUID
	attempt   polledAttempt
	rules     poll.Rules
}

func (o *observer) actor() string {
	if o.attempt.claimOwner == nil {
		return "unknown"
	}
	return *o.attempt.claimOwner
}

// progress: AI has the task and is not done. Any valid answer ends a NOT_FOUND streak.
func (o *observer) progress(ctx context.Context, outcome poll.Outcome) error {
	state, status := "QUEUED", domain.StatusRemoteQueued
	// A late QUEUED after PROCESSING does not move the task backwards.
	if outcome == poll.OutcomeProcessing || o.attempt.state == "PROCESSING" {
		state, status = "PROCESSING", domain.StatusRemoteRunning
	}

	_, err := o.tx.Exec(ctx, `
		UPDATE integration_attempts
		SET state = $2, poll_at = now() + make_interval(secs => $3), poll_count = poll_count + 1,
		    not_found_count = 0, first_not_found_at = NULL,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = NULL, error_detail = NULL, updated_at = now()
		WHERE attempt_id = $1`,
		o.attemptID, state, o.rules.NextPoll.Seconds())
	if err != nil {
		return wrapPoll("update attempt", err)
	}
	// A task flagged for no progress stays flagged until AI gives a final answer.
	if o.attempt.taskStatus == string(status) || o.attempt.taskStatus == string(domain.StatusNeedsAttention) {
		return nil
	}
	return o.setTask(ctx, status, nil, auditActionRemoteStatus, nil)
}

// succeeded: AI finished. The result is stored as AI returned it, unless it is
// too big; then a person must look. Either way the AI place is freed.
func (o *observer) succeeded(ctx context.Context, result []byte) error {
	if err := o.finishAttempt(ctx, "DONE", nil); err != nil {
		return err
	}

	if len(result) > o.rules.ResultMaxBytes {
		failure := &domain.Failure{
			Code:    errorResultTooLarge,
			Message: fmt.Sprintf("AI result is %d bytes, the limit is %d", len(result), o.rules.ResultMaxBytes),
		}
		return o.setTask(ctx, domain.StatusNeedsAttention, failure, auditActionResultRejected, nil)
	}

	_, err := o.tx.Exec(ctx, `
		UPDATE integration_queue
		SET status = 'SUCCEEDED', result_payload = $2, error_code = NULL, error_detail = NULL,
		    completed_at = now(), updated_at = now()
		WHERE request_guid = $1`,
		o.attempt.requestGUID, string(result))
	if err != nil {
		return wrapPoll("store result", err)
	}
	return o.audit(ctx, auditActionTaskSucceeded, string(domain.StatusSucceeded), nil)
}

// failed: AI finished with a business error. The AI place is freed.
func (o *observer) failed(ctx context.Context, failure *domain.Failure) error {
	if err := o.finishAttempt(ctx, "ERROR", failure); err != nil {
		return err
	}
	return o.setTask(ctx, domain.StatusFailed, failure, auditActionTaskFailed, &failure.Code)
}

// notFound counts AI's "task not found" answers. Enough of them in a row, over
// a long enough window, mean AI lost the task: the attempt is LOST, its AI
// place is freed, and the task is sent again or, after too many re-sends, fails.
// stillAtAI reports whether the attempt is still active afterwards.
func (o *observer) notFound(ctx context.Context) (stillAtAI bool, err error) {
	count := o.attempt.notFoundCount + 1
	window := time.Duration(o.attempt.notFoundSeconds * float64(time.Second))
	lost := count >= o.rules.LostThreshold && window >= o.rules.LostMinWindow

	if !lost {
		_, err := o.tx.Exec(ctx, `
			UPDATE integration_attempts
			SET not_found_count = $2, first_not_found_at = coalesce(first_not_found_at, now()),
			    poll_at = now() + make_interval(secs => $3), poll_count = poll_count + 1,
			    claim_token = NULL, claim_until = NULL, claim_owner = NULL, updated_at = now()
			WHERE attempt_id = $1`,
			o.attemptID, count, o.rules.NotFoundDelay.Seconds())
		if err != nil {
			return false, wrapPoll("count not found", err)
		}
		return true, nil
	}

	lostFailure := &domain.Failure{
		Code:    errorAITaskLost,
		Message: fmt.Sprintf("AI did not find the task %d times in a row", count),
	}
	_, err = o.tx.Exec(ctx, `
		UPDATE integration_attempts
		SET state = 'LOST', not_found_count = $2, poll_at = NULL, poll_count = poll_count + 1, finished_at = now(),
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $3, error_detail = $4, updated_at = now()
		WHERE attempt_id = $1`,
		o.attemptID, count, lostFailure.Code, lostFailure.Message)
	if err != nil {
		return false, wrapPoll("mark attempt lost", err)
	}

	if o.attempt.taskRetryCount >= o.rules.MaxResubmissions {
		failure := &domain.Failure{
			Code:    errorRetriesExhausted,
			Message: fmt.Sprintf("AI lost the task %d times", o.attempt.taskRetryCount+1),
		}
		return false, o.setTask(ctx, domain.StatusFailed, failure, auditActionAITaskLostFailed, &lostFailure.Code)
	}

	// Sent again by the next submission claim, as a new attempt.
	_, err = o.tx.Exec(ctx, `
		UPDATE integration_queue
		SET status = 'READY_TO_SUBMIT', retry_count = retry_count + 1, retry_at = NULL,
		    error_code = NULL, error_detail = NULL, updated_at = now()
		WHERE request_guid = $1`,
		o.attempt.requestGUID)
	if err != nil {
		return false, wrapPoll("resubmit task", err)
	}
	return false, o.audit(ctx, auditActionAITaskLostResubmit, string(domain.StatusReadyToSubmit), &lostFailure.Code)
}

// technicalError: no usable answer. Nothing about the task changes; it is
// checked again later. The NOT_FOUND streak is neither extended nor reset.
func (o *observer) technicalError(ctx context.Context, failure *domain.Failure) error {
	_, err := o.tx.Exec(ctx, `
		UPDATE integration_attempts
		SET poll_at = now() + make_interval(secs => $2), poll_count = poll_count + 1,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $3, error_detail = $4, updated_at = now()
		WHERE attempt_id = $1`,
		o.attemptID, o.rules.ErrorDelay.Seconds(), failure.Code, failure.Message)
	if err != nil {
		return wrapPoll("record technical error", err)
	}
	return nil
}

// flagNoProgress puts a task that is still at AI long after it was sent into
// NEEDS_ATTENTION, once. It keeps being polled and keeps its AI place; a final
// answer from AI later still finishes it normally.
func (o *observer) flagNoProgress(ctx context.Context) error {
	timeout := o.rules.NoProgressTimeout
	age := time.Duration(o.attempt.ageSeconds * float64(time.Second))
	if timeout <= 0 || age < timeout || o.attempt.taskStatus == string(domain.StatusNeedsAttention) {
		return nil
	}
	failure := &domain.Failure{
		Code:    errorAINoProgress,
		Message: fmt.Sprintf("No final answer from AI %s after sending; still checking", age.Round(time.Minute)),
	}
	return o.setTask(ctx, domain.StatusNeedsAttention, failure, auditActionAINoProgress, nil)
}

// finishAttempt ends the attempt in a final state, which frees its AI place.
func (o *observer) finishAttempt(ctx context.Context, state string, failure *domain.Failure) error {
	var code, detail *string
	if failure != nil {
		code, detail = &failure.Code, &failure.Message
	}
	_, err := o.tx.Exec(ctx, `
		UPDATE integration_attempts
		SET state = $2, poll_at = NULL, poll_count = poll_count + 1, finished_at = now(),
		    not_found_count = 0, first_not_found_at = NULL,
		    claim_token = NULL, claim_until = NULL, claim_owner = NULL,
		    error_code = $3, error_detail = $4, updated_at = now()
		WHERE attempt_id = $1`,
		o.attemptID, state, code, detail)
	if err != nil {
		return wrapPoll("finish attempt", err)
	}
	return nil
}

// setTask moves the task to status with an optional error, and writes the audit row.
func (o *observer) setTask(ctx context.Context, status domain.Status, failure *domain.Failure, action string, detail *string) error {
	var code, message *string
	if failure != nil {
		code, message = &failure.Code, &failure.Message
	}
	terminal := status == domain.StatusSucceeded || status == domain.StatusFailed
	_, err := o.tx.Exec(ctx, `
		UPDATE integration_queue
		SET status = $2, error_code = $3, error_detail = $4,
		    completed_at = CASE WHEN $5 THEN now() END, updated_at = now()
		WHERE request_guid = $1`,
		o.attempt.requestGUID, string(status), code, message, terminal)
	if err != nil {
		return wrapPoll("update task", err)
	}
	if err := o.audit(ctx, action, string(status), detail); err != nil {
		return err
	}
	o.attempt.taskStatus = string(status)
	return nil
}

func (o *observer) audit(ctx context.Context, action, newState string, detail *string) error {
	_, err := o.tx.Exec(ctx, `
		INSERT INTO integration_audit (request_guid, attempt_id, actor, action, old_state, new_state, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		o.attempt.requestGUID, o.attemptID, o.actor(), action, o.attempt.taskStatus, newState, detail)
	if err != nil {
		return wrapPoll("insert audit", err)
	}
	return nil
}

// wrapPoll marks connection problems and timeouts as ErrStorageUnavailable,
// so the API answers 503 and the worker can retry.
func wrapPoll(step string, err error) error {
	if IsUnavailable(err) {
		return fmt.Errorf("%s: %w: %w", step, poll.ErrStorageUnavailable, err)
	}
	return fmt.Errorf("%s: %w", step, err)
}
