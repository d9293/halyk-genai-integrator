// Package submission hands out tasks that are ready to be sent to AI, at most
// a fixed number at a time, and records what happened when worker.Request
// sent them. The API never calls AI itself.
package submission

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	ErrAttemptNotFound = errors.New("attempt not found")
	// ErrClaimLost: the claim token is wrong or expired.
	ErrClaimLost = errors.New("claim lost")
	// ErrStorageUnavailable: the database cannot be reached; the request may be retried.
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ClaimedSubmission is one POST to AI that a worker must make now. It already
// holds one of the AI places; reporting the outcome decides whether it keeps it.
type ClaimedSubmission struct {
	// AttemptID is sent to AI as its task_id.
	AttemptID   uuid.UUID
	AttemptNo   int
	RequestGUID uuid.UUID
	RequestID   uuid.UUID
	Operation   domain.Operation
	// Input is the normalized operation input stored at registration.
	Input json.RawMessage
	// Documents are the S3 copies in the order AI must receive them; empty for operations without files.
	Documents  []SubmissionDocument
	ClaimToken uuid.UUID
	ClaimUntil time.Time
}

type SubmissionDocument struct {
	Position int
	S3URI    string
}

// Outcome is what the worker learned from its POST to AI.
type Outcome string

const (
	// OutcomeAccepted: AI confirmed it took the task. It keeps its AI place.
	OutcomeAccepted Outcome = "ACCEPTED"
	// OutcomeUnknown: no reliable answer (timeout, lost response, unexpected
	// reply). AI may have the task, so it keeps its place until polling finds out.
	OutcomeUnknown Outcome = "UNKNOWN"
	// OutcomeRejected: AI clearly refused the task. The AI place is freed.
	OutcomeRejected Outcome = "REJECTED"
)

// Report is the worker's account of one POST to AI.
type Report struct {
	Outcome Outcome
	// RemoteStatus is AI's status for an accepted task: QUEUED or PROCESSING.
	RemoteStatus string
	// Failure explains UNKNOWN (optional) and REJECTED (required).
	Failure *domain.Failure
}
