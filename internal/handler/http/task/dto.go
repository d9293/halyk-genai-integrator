package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

const maxURLLen = 8192

// canonicalUUID accepts only the 8-4-4-4-12 form; uuid.Parse alone also
// accepts braces, urn: prefixes and undashed hex.
var canonicalUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Fields are pointers so a missing field can be told apart from a zero value.
type CreateTaskRequest struct {
	RequestID        *string `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111" validate:"required"`
	Operation        *string `json:"operation" enums:"SOCIAL_MEDIA,REGISTER_COMPARE" example:"SOCIAL_MEDIA" validate:"required"`
	OperationVersion *int    `json:"operationVersion" example:"1" validate:"required"`
	// Operation-specific JSON object.
	// SOCIAL_MEDIA: {"iin": "<12 digits>", "fullName": "<1..500 chars>"}.
	// REGISTER_COMPARE: {"adjudicate": true}.
	Input json.RawMessage `json:"input" swaggertype:"object" validate:"required"`
	// Empty array for operations without files. REGISTER_COMPARE: exactly two, roles A and B, ordinal 1.
	Documents *[]DocumentInput `json:"documents" validate:"required"`
}

type DocumentInput struct {
	DocumentID  *string `json:"documentId" format:"uuid" example:"22222222-2222-4222-8222-222222222222" validate:"required"`
	Role        *string `json:"role" example:"A" validate:"required"`
	Ordinal     *int    `json:"ordinal" minimum:"1" example:"1" validate:"required"`
	AttstoreURL *string `json:"attstoreUrl" maxLength:"8192" example:"https://attstore.example/documents/1" validate:"required"`
	// Required for document operations.
	SourceVersion *string `json:"sourceVersion" example:"3"`
}

// toInput checks the request shape. Operation-specific rules are the usecase's job.
func (r CreateTaskRequest) toInput(callerID string) (task.RegisterInput, []httpx.FieldError) {
	var fields []httpx.FieldError
	in := task.RegisterInput{CallerID: callerID}

	if id, fe := parseUUID("requestId", r.RequestID); fe != nil {
		fields = append(fields, *fe)
	} else {
		in.RequestID = id
	}

	if r.Operation == nil || *r.Operation == "" {
		fields = append(fields, httpx.FieldError{Path: "operation", Code: httpx.FieldRequired})
	} else {
		in.Operation = domain.Operation(*r.Operation)
	}

	if r.OperationVersion == nil {
		fields = append(fields, httpx.FieldError{Path: "operationVersion", Code: httpx.FieldRequired})
	} else {
		in.OperationVersion = *r.OperationVersion
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

	if id, fe := parseUUID(path+".documentId", d.DocumentID); fe != nil {
		fields = append(fields, *fe)
	} else {
		doc.DocumentID = id
	}

	if d.Role == nil || *d.Role == "" {
		fields = append(fields, httpx.FieldError{Path: path + ".role", Code: httpx.FieldRequired})
	} else {
		doc.Role = *d.Role
	}

	switch {
	case d.Ordinal == nil:
		fields = append(fields, httpx.FieldError{Path: path + ".ordinal", Code: httpx.FieldRequired})
	case *d.Ordinal < 1:
		fields = append(fields, httpx.FieldError{Path: path + ".ordinal", Code: httpx.FieldOutOfRange})
	default:
		doc.Ordinal = *d.Ordinal
	}

	switch {
	case d.AttstoreURL == nil || *d.AttstoreURL == "":
		fields = append(fields, httpx.FieldError{Path: path + ".attstoreUrl", Code: httpx.FieldRequired})
	case len(*d.AttstoreURL) > maxURLLen:
		fields = append(fields, httpx.FieldError{Path: path + ".attstoreUrl", Code: httpx.FieldTooLong})
	default:
		doc.AttstoreURL = *d.AttstoreURL
	}

	if d.SourceVersion != nil {
		doc.SourceVersion = *d.SourceVersion
	}

	return doc, fields
}

func parseUUID(path string, s *string) (uuid.UUID, *httpx.FieldError) {
	if s == nil || *s == "" {
		return uuid.Nil, &httpx.FieldError{Path: path, Code: httpx.FieldRequired}
	}
	if !canonicalUUID.MatchString(*s) {
		return uuid.Nil, &httpx.FieldError{Path: path, Code: httpx.FieldInvalidFormat}
	}
	return uuid.MustParse(*s), nil
}

type CreateTaskResponse struct {
	RequestID uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	Operation string    `json:"operation" example:"SOCIAL_MEDIA"`
	Status    string    `json:"status" enums:"WAITING_DOCUMENTS,READY_TO_SUBMIT" example:"READY_TO_SUBMIT"`
	CreatedAt time.Time `json:"createdAt" example:"2026-10-13T08:00:00Z"`
}

type TaskView struct {
	RequestID        uuid.UUID `json:"requestId" format:"uuid" example:"11111111-1111-4111-8111-111111111111"`
	Operation        string    `json:"operation" example:"SOCIAL_MEDIA"`
	OperationVersion int       `json:"operationVersion" example:"1"`
	Status           string    `json:"status" enums:"WAITING_DOCUMENTS,PREPARING_DOCUMENTS,READY_TO_SUBMIT,SUBMITTING,SUBMISSION_UNKNOWN,REMOTE_QUEUED,REMOTE_RUNNING,NEEDS_ATTENTION,SUCCEEDED,FAILED" example:"READY_TO_SUBMIT"`
	// Increments on every state change; repeated polls of the same AI state do not change it.
	StateVersion int64      `json:"stateVersion" example:"1"`
	CreatedAt    time.Time  `json:"createdAt" example:"2026-10-13T08:00:00Z"`
	UpdatedAt    time.Time  `json:"updatedAt" example:"2026-10-13T08:00:00Z"`
	CompletedAt  *time.Time `json:"completedAt" extensions:"x-nullable"`
	// Operation-specific result JSON; null until the task succeeds.
	Result    json.RawMessage `json:"result" swaggertype:"object" extensions:"x-nullable"`
	Reports   []ReportLink    `json:"reports"`
	Error     *Failure        `json:"error" extensions:"x-nullable"`
	Attention *Failure        `json:"attention" extensions:"x-nullable"`
}

type ReportLink struct {
	Type string `json:"type" example:"WORD"`
	URL  string `json:"url" example:"https://s3.example/reports/1.docx"`
	// Null means the expiry is unknown, not that the link never expires.
	ExpiresAt *time.Time `json:"expiresAt" extensions:"x-nullable"`
}

type Failure struct {
	Code    string `json:"code" example:"AI_TASK_LOST_RETRIES_EXHAUSTED"`
	Message string `json:"message"`
}

func toCreateResponse(t domain.Task) CreateTaskResponse {
	return CreateTaskResponse{
		RequestID: t.RequestID,
		Operation: string(t.Operation),
		Status:    string(t.Status),
		CreatedAt: t.CreatedAt.UTC(),
	}
}

func toTaskView(t domain.Task) TaskView {
	reports := make([]ReportLink, 0, len(t.Reports))
	for _, r := range t.Reports {
		reports = append(reports, ReportLink{Type: r.Type, URL: r.URL, ExpiresAt: utcPtr(r.ExpiresAt)})
	}

	result := t.Result
	if len(result) == 0 {
		result = json.RawMessage("null")
	}

	return TaskView{
		RequestID:        t.RequestID,
		Operation:        string(t.Operation),
		OperationVersion: t.OperationVersion,
		Status:           string(t.Status),
		StateVersion:     t.StateVersion,
		CreatedAt:        t.CreatedAt.UTC(),
		UpdatedAt:        t.UpdatedAt.UTC(),
		CompletedAt:      utcPtr(t.CompletedAt),
		Result:           result,
		Reports:          reports,
		Error:            toFailure(t.Error),
		Attention:        toFailure(t.Attention),
	}
}

func toFailure(f *domain.Failure) *Failure {
	if f == nil {
		return nil
	}
	return &Failure{Code: f.Code, Message: f.Message}
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
