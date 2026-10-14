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

// Task is one BPM business task, identified by the caller's requestId.
type Task struct {
	RequestID        uuid.UUID
	CallerID         string
	Operation        Operation
	OperationVersion int
	// Input is the operation-specific JSON object, already normalized.
	Input     json.RawMessage
	Documents []Document
	// RequestHash fingerprints the normalized registration for idempotency checks.
	RequestHash  string
	Status       Status
	StateVersion int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CompletedAt  *time.Time
	Result       json.RawMessage
	Reports      []ReportLink
	Error        *Failure
	Attention    *Failure
}

// Document is a source file in Attstore that must be copied to S3 before submission.
type Document struct {
	DocumentID    uuid.UUID
	Role          string
	Ordinal       int
	AttstoreURL   string
	SourceVersion string
}

type ReportLink struct {
	Type string
	URL  string
	// ExpiresAt nil means the expiry is unknown, not that the link never expires.
	ExpiresAt *time.Time
}

type Failure struct {
	Code    string
	Message string
}
