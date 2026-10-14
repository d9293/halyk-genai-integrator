package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Operation string

const (
	OperationSocialMedia     Operation = "SOCIAL_MEDIA"
	OperationRegisterCompare Operation = "REGISTER_COMPARE"
)

type Status string

const (
	StatusWaitingDocuments   Status = "WAITING_DOCUMENTS"
	StatusPreparingDocuments Status = "PREPARING_DOCUMENTS"
	StatusReadyToSubmit      Status = "READY_TO_SUBMIT"
	StatusSubmitting         Status = "SUBMITTING"
	StatusSubmissionUnknown  Status = "SUBMISSION_UNKNOWN"
	StatusRemoteQueued       Status = "REMOTE_QUEUED"
	StatusRemoteRunning      Status = "REMOTE_RUNNING"
	StatusNeedsAttention     Status = "NEEDS_ATTENTION"
	StatusSucceeded          Status = "SUCCEEDED"
	StatusFailed             Status = "FAILED"
)

// Task is one operation BPM asked for, identified by its requestGuid.
type Task struct {
	// RequestGUID identifies this task; BPM generates it and reuses it on retries.
	RequestGUID uuid.UUID
	// RequestID is the Camunda request the task belongs to; many tasks can share it.
	RequestID uuid.UUID
	CallerID  string
	Operation Operation
	// Input is the operation-specific JSON object, already normalized.
	Input json.RawMessage
	// Documents keep the order BPM sent them in; AI results refer to files by position.
	Documents []Document
	// RequestHash fingerprints the normalized registration for idempotency checks.
	RequestHash string
	Status      Status
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
	// Result is the AI result JSON as AI returned it.
	Result json.RawMessage
	// Error explains a FAILED or NEEDS_ATTENTION task.
	Error *Failure
}

// Document is a source file in Attstore that must be copied to S3 before submission.
type Document struct {
	DocumentID  uuid.UUID
	AttstoreURL string
}

type Failure struct {
	Code    string
	Message string
}
