package submission

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

type ClaimResponse struct {
	// Empty when nothing is ready or all AI places are taken.
	Items []ClaimedSubmission `json:"items"`
}

type ClaimedSubmission struct {
	// Use in /submissions/{attemptId}/outcome and /renew. Send it to AI as task_id.
	AttemptID uuid.UUID `json:"attemptId" format:"uuid" example:"a77e0000-0000-4000-8000-000000000001"`
	// 1 for the first send of the task, 2 for a re-send after AI lost it, ...
	AttemptNo   int       `json:"attemptNo" example:"1"`
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"`
	RequestID   uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	Operation   string    `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"REGISTER_COMPARE"`
	// Operation input as registered by BPM, already normalized.
	Input json.RawMessage `json:"input" swaggertype:"object"`
	// S3 copies of the task's files, in the order AI must receive them. Empty for operations without files.
	Documents []SubmissionDocument `json:"documents"`
	// Proof of ownership; send it back with outcome and renew.
	ClaimToken uuid.UUID `json:"claimToken" format:"uuid" example:"7b1e2c3d-0000-4000-8000-000000000001"`
	// Report the outcome before this time, or renew.
	ClaimUntil time.Time `json:"claimUntil" example:"2026-10-15T08:01:00Z"`
}

type SubmissionDocument struct {
	Position int    `json:"position" example:"0"`
	S3URI    string `json:"s3Uri" example:"s3://ai-input/tasks/bbbbbbbb/0"`
}

// Fields are pointers so a missing field can be told apart from a zero value.
type OutcomeRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
	// ACCEPTED: AI confirmed it took the task.
	// UNKNOWN: no reliable answer (timeout, lost response, unexpected reply); AI may have it.
	// REJECTED: AI clearly refused the task.
	Outcome *string `json:"outcome" enums:"ACCEPTED,UNKNOWN,REJECTED" example:"ACCEPTED" validate:"required"`
	// Only for ACCEPTED: AI's status of the task. Defaults to QUEUED.
	RemoteStatus *string `json:"remoteStatus" enums:"QUEUED,PROCESSING" example:"QUEUED"`
	// Required for REJECTED, optional for UNKNOWN, not allowed for ACCEPTED.
	Failure *httpx.FailureInput `json:"failure"`
}

func (r OutcomeRequest) validate() (claimToken uuid.UUID, report submission.Report, fields []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}

	if r.Outcome == nil || *r.Outcome == "" {
		fields = append(fields, httpx.FieldError{Path: "outcome", Code: httpx.FieldRequired})
		return claimToken, report, fields
	}
	report.Outcome = submission.Outcome(*r.Outcome)

	switch report.Outcome {
	case submission.OutcomeAccepted:
		report.RemoteStatus = "QUEUED"
		if r.RemoteStatus != nil {
			if *r.RemoteStatus != "QUEUED" && *r.RemoteStatus != "PROCESSING" {
				fields = append(fields, httpx.FieldError{Path: "remoteStatus", Code: httpx.FieldInvalidFormat})
			}
			report.RemoteStatus = *r.RemoteStatus
		}
		if r.Failure != nil {
			fields = append(fields, httpx.FieldError{Path: "failure", Code: httpx.FieldNotAllowed})
		}
	case submission.OutcomeUnknown, submission.OutcomeRejected:
		if r.RemoteStatus != nil {
			fields = append(fields, httpx.FieldError{Path: "remoteStatus", Code: httpx.FieldNotAllowed})
		}
		switch {
		case r.Failure != nil:
			failure, failureFields := r.Failure.ToDomain("failure")
			fields = append(fields, failureFields...)
			report.Failure = failure
		case report.Outcome == submission.OutcomeRejected:
			fields = append(fields, httpx.FieldError{Path: "failure", Code: httpx.FieldRequired})
		}
	default:
		fields = append(fields, httpx.FieldError{Path: "outcome", Code: httpx.FieldInvalidFormat})
	}
	return claimToken, report, fields
}

func toClaimedSubmission(s submission.ClaimedSubmission) ClaimedSubmission {
	documents := make([]SubmissionDocument, 0, len(s.Documents))
	for _, d := range s.Documents {
		documents = append(documents, SubmissionDocument{Position: d.Position, S3URI: d.S3URI})
	}
	return ClaimedSubmission{
		AttemptID:   s.AttemptID,
		AttemptNo:   s.AttemptNo,
		RequestGUID: s.RequestGUID,
		RequestID:   s.RequestID,
		Operation:   string(s.Operation),
		Input:       s.Input,
		Documents:   documents,
		ClaimToken:  s.ClaimToken,
		ClaimUntil:  s.ClaimUntil.UTC(),
	}
}
