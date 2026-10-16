// Package result hands out final task results that still have to reach BPM,
// and records whether Kafka accepted them. The API never talks to Kafka
// itself; worker.Response publishes and reports back.
package result

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	ErrResultNotFound = errors.New("result not found")
	// ErrClaimLost: the claim token is wrong or expired.
	ErrClaimLost = errors.New("claim lost")
	// ErrStorageUnavailable: the database cannot be reached; the request may be retried.
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ClaimedResult is one Kafka message a worker must publish now.
type ClaimedResult struct {
	RequestGUID uuid.UUID
	Topic       string
	// MessageKey is the Kafka message key: the Camunda requestId.
	MessageKey string
	Message    Message
	ClaimToken uuid.UUID
	ClaimUntil time.Time
}

const (
	MessageSchemaVersion = 1
	EventTaskCompleted   = "TASK_COMPLETED"
)

// Message is the Kafka message BPM receives when a task ends. It is built
// from the task row, so every re-send of the same task is identical.
type Message struct {
	SchemaVersion int
	EventType     string
	// OccurredAt is when the task finished, not when the message was sent.
	OccurredAt  time.Time
	RequestID   uuid.UUID
	RequestGUID uuid.UUID
	Operation   domain.Operation
	// Status is SUCCEEDED or FAILED.
	Status domain.Status
	// Result is AI's result JSON for SUCCEEDED; nil for FAILED.
	Result json.RawMessage
	// Error explains FAILED; nil for SUCCEEDED.
	Error *domain.Failure
}
