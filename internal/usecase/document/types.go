// Package document hands out task documents to worker.Request for copying
// Attstore -> S3, and records the outcome. The API itself never touches the
// files: the worker copies them and reports back.
package document

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrDocumentNotFound = errors.New("document not found")
	// ErrClaimLost: the claim token is wrong or expired; another worker may own the document now.
	ErrClaimLost = errors.New("claim lost")
	// ErrStorageUnavailable: the database cannot be reached; the request may be retried.
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ClaimedDocument is a document handed to a worker, owned by it until ClaimUntil.
type ClaimedDocument struct {
	// ID is our documents.id; the worker uses it in complete/failure/renew.
	ID          uuid.UUID
	RequestGUID uuid.UUID
	// DocumentID is BPM's ID of the file.
	DocumentID  uuid.UUID
	AttstoreURL string
	// Position is the file's index in the task; AI receives files in this order.
	Position   int
	ClaimToken uuid.UUID
	ClaimUntil time.Time
}

// CopyResult describes the file copy the worker stored in S3.
type CopyResult struct {
	S3URI          string
	SizeBytes      int64
	ChecksumSHA256 string
}

// CopyFailure describes why the worker could not copy the file.
type CopyFailure struct {
	Code    string
	Message string
	// Retryable is the worker's opinion; the API decides with its own retry budget.
	Retryable bool
}
