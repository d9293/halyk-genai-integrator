package result

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/result"
)

type ClaimResponse struct {
	// Empty when no result is waiting to be delivered.
	Items []ClaimedResult `json:"items"`
}

type ClaimedResult struct {
	// Use in /results/{requestGuid}/published and /failure.
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"`
	// Kafka topic to publish to.
	Topic string `json:"topic" example:"integrator.task-results.v1"`
	// Kafka message key: the Camunda requestId.
	MessageKey string `json:"messageKey" example:"11111111-1111-4111-8111-111111111111"`
	// Publish exactly this as the message value, unchanged.
	Message Message `json:"message"`
	// Proof of ownership; send it back with published or failure.
	ClaimToken uuid.UUID `json:"claimToken" format:"uuid" example:"7b1e2c3d-0000-4000-8000-000000000001"`
	// Report the outcome before this time.
	ClaimUntil time.Time `json:"claimUntil" example:"2026-10-16T10:00:30Z"`
}

// Message is the Kafka message BPM receives when a task ends. BPM may receive
// it more than once and must ignore repeats by requestGuid.
type Message struct {
	SchemaVersion int    `json:"schemaVersion" example:"1"`
	EventType     string `json:"eventType" enums:"TASK_COMPLETED" example:"TASK_COMPLETED"`
	// When the task finished, not when the message was sent.
	OccurredAt  time.Time `json:"occurredAt" example:"2026-10-16T10:00:00Z"`
	RequestID   uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"`
	Operation   string    `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"REGISTER_COMPARE"`
	Status      string    `json:"status" enums:"SUCCEEDED,FAILED" example:"SUCCEEDED"`
	// AI's result JSON as AI returned it; null for FAILED.
	Result json.RawMessage `json:"result" swaggertype:"object" extensions:"x-nullable"`
	// Why the task failed; null for SUCCEEDED.
	Error *Failure `json:"error" extensions:"x-nullable"`
}

type Failure struct {
	Code    string `json:"code" example:"AI_TASK_LOST_RETRIES_EXHAUSTED"`
	Message string `json:"message" example:"AI lost the task 4 times"`
}

type PublishedRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
}

func (r PublishedRequest) validate() (uuid.UUID, []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		return uuid.Nil, []httpx.FieldError{*fe}
	}
	return claimToken, nil
}

type FailureRequest struct {
	ClaimToken *string `json:"claimToken" format:"uuid" example:"claimToken-from-the-claim-response" validate:"required"`
	// Why Kafka did not accept the message.
	Failure *httpx.FailureInput `json:"failure" validate:"required"`
}

func (r FailureRequest) validate() (claimToken uuid.UUID, failure domain.Failure, fields []httpx.FieldError) {
	claimToken, fe := httpx.ParseUUID("claimToken", r.ClaimToken)
	if fe != nil {
		fields = append(fields, *fe)
	}
	if r.Failure == nil {
		fields = append(fields, httpx.FieldError{Path: "failure", Code: httpx.FieldRequired})
		return claimToken, failure, fields
	}
	f, failureFields := r.Failure.ToDomain("failure")
	return claimToken, *f, append(fields, failureFields...)
}

func toClaimedResult(r result.ClaimedResult) ClaimedResult {
	msg := r.Message
	resultJSON := msg.Result
	if len(resultJSON) == 0 {
		resultJSON = json.RawMessage("null")
	}
	var failure *Failure
	if msg.Error != nil {
		failure = &Failure{Code: msg.Error.Code, Message: msg.Error.Message}
	}

	return ClaimedResult{
		RequestGUID: r.RequestGUID,
		Topic:       r.Topic,
		MessageKey:  r.MessageKey,
		Message: Message{
			SchemaVersion: msg.SchemaVersion,
			EventType:     msg.EventType,
			OccurredAt:    msg.OccurredAt.UTC(),
			RequestID:     msg.RequestID,
			RequestGUID:   msg.RequestGUID,
			Operation:     string(msg.Operation),
			Status:        string(msg.Status),
			Result:        resultJSON,
			Error:         failure,
		},
		ClaimToken: r.ClaimToken,
		ClaimUntil: r.ClaimUntil.UTC(),
	}
}
