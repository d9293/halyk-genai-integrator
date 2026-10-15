package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

const maxURLLen = 8192

// Fields are pointers so a missing field can be told apart from a zero value.
type CreateTaskRequest struct {
	// Camunda request the task belongs to; several tasks can share it.
	RequestID *string `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111" validate:"required"`
	// Unique ID of this task. Reuse it when retrying the same task.
	RequestGUID *string `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" validate:"required"`
	Operation   *string `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"SOCIAL_MEDIA" validate:"required"`
	// Operation-specific JSON object.
	// SOCIAL_MEDIA: {"iin": "<12 digits>", "fullName": "<1..500 chars>"}.
	// REGISTER_COMPARE: {"adjudicate": true}.
	Input json.RawMessage `json:"input" swaggertype:"object" validate:"required"`
	// Empty array for operations without files. REGISTER_COMPARE: exactly two.
	// Order matters: AI results refer to files by position (only_a = first, only_b = second).
	Documents *[]DocumentInput `json:"documents" validate:"required"`
}

type DocumentInput struct {
	// Unique within one task.
	DocumentID  *string `json:"documentId" format:"uuid" example:"22222222-2222-4222-8222-222222222222" validate:"required"`
	AttstoreURL *string `json:"attstoreUrl" maxLength:"8192" example:"https://attstore.example/documents/1" validate:"required"`
}

// toInput checks the request shape. Operation-specific rules are the usecase's job.
func (r CreateTaskRequest) toInput(callerID string) (task.RegisterInput, []httpx.FieldError) {
	var fields []httpx.FieldError
	in := task.RegisterInput{CallerID: callerID}

	if id, fe := httpx.ParseUUID("requestId", r.RequestID); fe != nil {
		fields = append(fields, *fe)
	} else {
		in.RequestID = id
	}

	if id, fe := httpx.ParseUUID("requestGuid", r.RequestGUID); fe != nil {
		fields = append(fields, *fe)
	} else {
		in.RequestGUID = id
	}

	if r.Operation == nil || *r.Operation == "" {
		fields = append(fields, httpx.FieldError{Path: "operation", Code: httpx.FieldRequired})
	} else {
		in.Operation = domain.Operation(*r.Operation)
	}

	trimmed := bytes.TrimSpace(r.Input)
	switch {
	case len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")):
		fields = append(fields, httpx.FieldError{Path: "input", Code: httpx.FieldRequired})
	case trimmed[0] != '{':
		fields = append(fields, httpx.FieldError{Path: "input", Code: httpx.FieldInvalidType})
	default:
		in.Input = trimmed
	}

	if r.Documents == nil {
		fields = append(fields, httpx.FieldError{Path: "documents", Code: httpx.FieldRequired})
	} else {
		in.Documents = make([]domain.Document, 0, len(*r.Documents))
		for i, d := range *r.Documents {
			doc, docFields := d.toDomain(fmt.Sprintf("documents[%d]", i))
			fields = append(fields, docFields...)
			in.Documents = append(in.Documents, doc)
		}
	}

	return in, fields
}

func (d DocumentInput) toDomain(path string) (domain.Document, []httpx.FieldError) {
	var fields []httpx.FieldError
	var doc domain.Document

	if id, fe := httpx.ParseUUID(path+".documentId", d.DocumentID); fe != nil {
		fields = append(fields, *fe)
	} else {
		doc.DocumentID = id
	}

	switch {
	case d.AttstoreURL == nil || *d.AttstoreURL == "":
		fields = append(fields, httpx.FieldError{Path: path + ".attstoreUrl", Code: httpx.FieldRequired})
	case len(*d.AttstoreURL) > maxURLLen:
		fields = append(fields, httpx.FieldError{Path: path + ".attstoreUrl", Code: httpx.FieldTooLong})
	default:
		doc.AttstoreURL = *d.AttstoreURL
	}

	return doc, fields
}

type CreateTaskResponse struct {
	RequestID   uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	RequestGUID uuid.UUID `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"`
	Operation   string    `json:"operation" example:"SOCIAL_MEDIA"`
	Status      string    `json:"status" enums:"WAITING_DOCUMENTS,READY_TO_SUBMIT" example:"READY_TO_SUBMIT"`
	CreatedAt   time.Time `json:"createdAt" example:"2026-10-13T08:00:00Z"`
}

type TaskView struct {
	RequestID   uuid.UUID  `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	RequestGUID uuid.UUID  `json:"requestGuid" format:"uuid" example:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"`
	Operation   string     `json:"operation" example:"SOCIAL_MEDIA"`
	Status      string     `json:"status" enums:"WAITING_DOCUMENTS,PREPARING_DOCUMENTS,READY_TO_SUBMIT,SUBMITTING,SUBMISSION_UNKNOWN,REMOTE_QUEUED,REMOTE_RUNNING,NEEDS_ATTENTION,SUCCEEDED,FAILED" example:"READY_TO_SUBMIT"`
	CreatedAt   time.Time  `json:"createdAt" example:"2026-10-13T08:00:00Z"`
	UpdatedAt   time.Time  `json:"updatedAt" example:"2026-10-13T08:00:00Z"`
	CompletedAt *time.Time `json:"completedAt" extensions:"x-nullable"`
	// AI result JSON as AI returned it; null until the task succeeds.
	Result json.RawMessage `json:"result" swaggertype:"object" extensions:"x-nullable"`
	// Set when the task is FAILED or NEEDS_ATTENTION.
	Error *Failure `json:"error" extensions:"x-nullable"`
}

type Failure struct {
	Code    string `json:"code" example:"AI_TASK_LOST_RETRIES_EXHAUSTED"`
	Message string `json:"message"`
}

func toCreateResponse(t domain.Task) CreateTaskResponse {
	return CreateTaskResponse{
		RequestID:   t.RequestID,
		RequestGUID: t.RequestGUID,
		Operation:   string(t.Operation),
		Status:      string(t.Status),
		CreatedAt:   t.CreatedAt.UTC(),
	}
}

func toTaskView(t domain.Task) TaskView {
	result := t.Result
	if len(result) == 0 {
		result = json.RawMessage("null")
	}

	var failure *Failure
	if t.Error != nil {
		failure = &Failure{Code: t.Error.Code, Message: t.Error.Message}
	}

	var completedAt *time.Time
	if t.CompletedAt != nil {
		utc := t.CompletedAt.UTC()
		completedAt = &utc
	}

	return TaskView{
		RequestID:   t.RequestID,
		RequestGUID: t.RequestGUID,
		Operation:   string(t.Operation),
		Status:      string(t.Status),
		CreatedAt:   t.CreatedAt.UTC(),
		UpdatedAt:   t.UpdatedAt.UTC(),
		CompletedAt: completedAt,
		Result:      result,
		Error:       failure,
	}
}
