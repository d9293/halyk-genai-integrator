package task

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	ErrTaskNotFound = errors.New("task not found")
	// ErrIdempotencyConflict: the requestId is already registered by this caller with different input.
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	// ErrRequestIDUnavailable: the requestId belongs to another caller.
	ErrRequestIDUnavailable = errors.New("request id unavailable")
	ErrOperationNotEnabled  = errors.New("operation not enabled")
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
	CodeRequired           = "REQUIRED"
	CodeInvalidFormat      = "INVALID_FORMAT"
	CodeInvalidType        = "INVALID_TYPE"
	CodeInvalidValue       = "INVALID_VALUE"
	CodeUnknownField       = "UNKNOWN_FIELD"
	CodeTooLong            = "TOO_LONG"
	CodeOutOfRange         = "OUT_OF_RANGE"
	CodeDuplicate          = "DUPLICATE"
	CodeNotAllowed         = "NOT_ALLOWED"
	CodeInvalidCount       = "INVALID_COUNT"
	CodeUnsupportedVersion = "UNSUPPORTED_VERSION"
)

type RegisterInput struct {
	CallerID         string
	RequestID        uuid.UUID
	Operation        domain.Operation
	OperationVersion int
	Input            json.RawMessage
	Documents        []domain.Document
}
