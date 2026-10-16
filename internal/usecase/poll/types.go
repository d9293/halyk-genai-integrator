// Package poll hands out tasks at AI whose status must be checked, and
// applies what AI answered: progress, the final result, or that AI lost the
// task. The API never calls AI itself; worker.Response does.
package poll

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	ErrAttemptNotFound = errors.New("attempt not found")
	// ErrClaimLost: the claim token is wrong or expired, or the observation came too late.
	ErrClaimLost = errors.New("claim lost")
	// ErrStorageUnavailable: the database cannot be reached; the request may be retried.
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ClaimedPoll is one status check a worker must make now: a single GET to AI.
type ClaimedPoll struct {
	// AttemptID is the task_id AI knows the task by.
	AttemptID   uuid.UUID
	AttemptNo   int
	RequestGUID uuid.UUID
	RequestID   uuid.UUID
	Operation   domain.Operation
	ClaimToken  uuid.UUID
	ClaimUntil  time.Time
}

// Outcome is what AI answered to the status check.
type Outcome string

const (
	// OutcomeQueued: AI has the task, not started yet.
	OutcomeQueued Outcome = "QUEUED"
	// OutcomeProcessing: AI is working on the task.
	OutcomeProcessing Outcome = "PROCESSING"
	// OutcomeSucceeded: AI finished; the result is attached. Frees the AI place.
	OutcomeSucceeded Outcome = "SUCCEEDED"
	// OutcomeFailed: AI finished with a business error. Frees the AI place.
	OutcomeFailed Outcome = "FAILED"
	// OutcomeNotFound: AI confirmed it does not know the task (e.g. after its restart).
	// Several in a row mean the task is lost and is sent again.
	OutcomeNotFound Outcome = "NOT_FOUND"
	// OutcomeTechnicalError: no usable answer (timeout, 5xx, proxy error). Checked again later.
	OutcomeTechnicalError Outcome = "TECHNICAL_ERROR"
)

// Observation is the worker's account of one status check.
type Observation struct {
	Outcome Outcome
	// Result is AI's result JSON, as AI returned it. Only for SUCCEEDED.
	Result json.RawMessage
	// Failure explains FAILED and TECHNICAL_ERROR.
	Failure *domain.Failure
}

// Rules decide what an observation does beyond moving the attempt along.
type Rules struct {
	// NextPoll is when to check again after QUEUED/PROCESSING.
	NextPoll time.Duration
	// NotFoundDelay is when to check again after NOT_FOUND.
	NotFoundDelay time.Duration
	// ErrorDelay is when to check again after TECHNICAL_ERROR.
	ErrorDelay time.Duration
	// The task is lost after LostThreshold NOT_FOUND in a row, the first at
	// least LostMinWindow ago. It is then sent again unless it already was
	// MaxResubmissions times.
	LostThreshold    int
	LostMinWindow    time.Duration
	MaxResubmissions int
	// ResultMaxBytes caps the stored result; a bigger one needs attention.
	ResultMaxBytes int
}
