package task

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	ErrTaskNotFound = errors.New("task not found")
	// ErrIdempotencyConflict: the requestGuid is already registered by this caller with different data.
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	// ErrRequestGUIDUnavailable: the requestGuid belongs to another caller.
	ErrRequestGUIDUnavailable = errors.New("request guid unavailable")
	ErrOperationNotEnabled    = errors.New("operation not enabled")
	// ErrStorageUnavailable: the database cannot be reached; the request may be retried.
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// ValidationError reports semantically invalid input for an operation.
type ValidationError struct {
	Fields []FieldError
}

type FieldError struct {
	Path string
	Code string
}

func (e *ValidationError) Error() string {
	return "validation failed"
}

// Field-level error codes returned in ValidationError.
const (
	CodeRequired      = "REQUIRED"
	CodeInvalidFormat = "INVALID_FORMAT"
	CodeInvalidType   = "INVALID_TYPE"
	CodeUnknownField  = "UNKNOWN_FIELD"
	CodeTooLong       = "TOO_LONG"
	CodeDuplicate     = "DUPLICATE"
	CodeNotAllowed    = "NOT_ALLOWED"
	CodeInvalidCount  = "INVALID_COUNT"
)

type RegisterInput struct {
	CallerID    string
	RequestGUID uuid.UUID
	RequestID   uuid.UUID
	Operation   domain.Operation
	Input       json.RawMessage
	Documents   []domain.Document
}
