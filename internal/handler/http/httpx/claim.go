package httpx

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

// Request and response bodies shared by the worker APIs (documents,
// submissions), which all follow the same claim / renew / report pattern.

const (
	maxClaimLimit  = 10
	maxWorkerIDLen = 200
)

// Fields are pointers so a missing field can be told apart from a zero value.
type ClaimRequest struct {
	// Name of the worker pod, e.g. its HOSTNAME. For diagnostics only.
	WorkerID *string `json:"workerId" maxLength:"200" example:"worker-request-7d9f8c6b5-x2kqp" validate:"required"`
	// How many items to take at most.
	Limit *int `json:"limit" minimum:"1" maximum:"10" example:"1" validate:"required"`
}

func (r ClaimRequest) Validate() (workerID string, limit int, fields []FieldError) {
	switch {
	case r.WorkerID == nil || strings.TrimSpace(*r.WorkerID) == "":
		fields = append(fields, FieldError{Path: "workerId", Code: FieldRequired})
	case len(*r.WorkerID) > maxWorkerIDLen:
		fields = append(fields, FieldError{Path: "workerId", Code: FieldTooLong})
	default:
		workerID = *r.WorkerID
	}

	switch {
	case r.Limit == nil:
		fields = append(fields, FieldError{Path: "limit", Code: FieldRequired})
	case *r.Limit < 1 || *r.Limit > maxClaimLimit:
		fields = append(fields, FieldError{Path: "limit", Code: FieldOutOfRange})
	default:
		limit = *r.Limit
	}
	return workerID, limit, fields
}

type RenewRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
}

func (r RenewRequest) Validate() (claimToken uuid.UUID, fields []FieldError) {
	claimToken, fe := ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}
	return claimToken, fields
}

type RenewResponse struct {
	ClaimUntil time.Time `json:"claimUntil" example:"2026-10-15T08:02:00Z"`
}

type AppliedResponse struct {
	Applied bool `json:"applied" example:"true"`
}

const (
	maxFailureCodeLen = 100
	maxMessageLen     = 2000
)

// FailureInput is a worker's description of an error.
type FailureInput struct {
	// Short machine-readable reason.
	Code *string `json:"code" maxLength:"100" example:"AI_TIMEOUT" validate:"required"`
	// Human-readable details. No personal data, raw HTML or signed URLs.
	Message *string `json:"message" maxLength:"2000" example:"No response from AI within 30s" validate:"required"`
}

// ToDomain validates the failure, reporting problems under path (e.g. "failure").
func (f FailureInput) ToDomain(path string) (*domain.Failure, []FieldError) {
	var fields []FieldError
	var failure domain.Failure

	switch {
	case f.Code == nil || strings.TrimSpace(*f.Code) == "":
		fields = append(fields, FieldError{Path: path + ".code", Code: FieldRequired})
	case len(*f.Code) > maxFailureCodeLen:
		fields = append(fields, FieldError{Path: path + ".code", Code: FieldTooLong})
	default:
		failure.Code = *f.Code
	}

	switch {
	case f.Message == nil || strings.TrimSpace(*f.Message) == "":
		fields = append(fields, FieldError{Path: path + ".message", Code: FieldRequired})
	case len([]rune(*f.Message)) > maxMessageLen:
		fields = append(fields, FieldError{Path: path + ".message", Code: FieldTooLong})
	default:
		failure.Message = *f.Message
	}
	return &failure, fields
}
