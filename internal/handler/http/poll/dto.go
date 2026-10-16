package poll

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
)

type ClaimResponse struct {
	// Empty when no status check is due.
	Items []ClaimedPoll `json:"items"`
}

type ClaimedPoll struct {
	// Use in /polls/{attemptId}/observation. It is the task_id AI knows the task by.
	AttemptID uuid.UUID `json:"attemptId" format:"uuid" example:"a77e0000-0000-4000-8000-000000000002"`
	// 1 for the first send of the task, 2 for a re-send after AI lost it, ...
	AttemptNo   int       `json:"attemptNo" example:"1"`
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"`
	RequestID   uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	// Tells which AI status endpoint to call.
	Operation string `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"REGISTER_COMPARE"`
	// Proof of ownership; send it back with the observation.
	ClaimToken uuid.UUID `json:"claimToken" format:"uuid" example:"7b1e2c3d-0000-4000-8000-000000000001"`
	// Report the observation before this time.
	ClaimUntil time.Time `json:"claimUntil" example:"2026-10-16T08:00:30Z"`
}

// Fields are pointers so a missing field can be told apart from a zero value.
type ObservationRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
	// What AI answered:
	// QUEUED / PROCESSING: AI has the task and is not done.
	// SUCCEEDED: AI finished; put AI's result JSON in result.
	// FAILED: AI finished with a business error; describe it in failure.
	// NOT_FOUND: AI confirmed it does not know this task.
	// TECHNICAL_ERROR: no usable answer (timeout, 5xx, proxy 404); describe it in failure.
	Outcome *string `json:"outcome" enums:"QUEUED,PROCESSING,SUCCEEDED,FAILED,NOT_FOUND,TECHNICAL_ERROR" example:"SUCCEEDED" validate:"required"`
	// AI's result JSON as AI returned it. Required for SUCCEEDED, not allowed otherwise.
	Result json.RawMessage `json:"result" swaggertype:"object"`
	// Required for FAILED and TECHNICAL_ERROR, not allowed otherwise.
	Failure *httpx.FailureInput `json:"failure"`
}

func (r ObservationRequest) validate() (claimToken uuid.UUID, observation poll.Observation, fields []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}

	if r.Outcome == nil || *r.Outcome == "" {
		fields = append(fields, httpx.FieldError{Path: "outcome", Code: httpx.FieldRequired})
		return claimToken, observation, fields
	}
	observation.Outcome = poll.Outcome(*r.Outcome)

	var needsResult, needsFailure bool
	switch observation.Outcome {
	case poll.OutcomeQueued, poll.OutcomeProcessing, poll.OutcomeNotFound:
	case poll.OutcomeSucceeded:
		needsResult = true
	case poll.OutcomeFailed, poll.OutcomeTechnicalError:
		needsFailure = true
	default:
		fields = append(fields, httpx.FieldError{Path: "outcome", Code: httpx.FieldInvalidFormat})
		return claimToken, observation, fields
	}

	result := bytes.TrimSpace(r.Result)
	hasResult := len(result) > 0 && !bytes.Equal(result, []byte("null"))
	switch {
	case needsResult && !hasResult:
		fields = append(fields, httpx.FieldError{Path: "result", Code: httpx.FieldRequired})
	case needsResult && result[0] != '{':
		fields = append(fields, httpx.FieldError{Path: "result", Code: httpx.FieldInvalidType})
	case needsResult:
		observation.Result = result
	case hasResult:
		fields = append(fields, httpx.FieldError{Path: "result", Code: httpx.FieldNotAllowed})
	}

	switch {
	case needsFailure && r.Failure == nil:
		fields = append(fields, httpx.FieldError{Path: "failure", Code: httpx.FieldRequired})
	case needsFailure:
		failure, failureFields := r.Failure.ToDomain("failure")
		fields = append(fields, failureFields...)
		observation.Failure = failure
	case r.Failure != nil:
		fields = append(fields, httpx.FieldError{Path: "failure", Code: httpx.FieldNotAllowed})
	}
	return claimToken, observation, fields
}

func toClaimedPoll(p poll.ClaimedPoll) ClaimedPoll {
	return ClaimedPoll{
		AttemptID:   p.AttemptID,
		AttemptNo:   p.AttemptNo,
		RequestGUID: p.RequestGUID,
		RequestID:   p.RequestID,
		Operation:   string(p.Operation),
		ClaimToken:  p.ClaimToken,
		ClaimUntil:  p.ClaimUntil.UTC(),
	}
}
