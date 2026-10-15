package httpx

import (
	"strings"
	"time"

	"github.com/google/uuid"
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
