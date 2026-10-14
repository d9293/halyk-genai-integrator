package task

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

const (
	testCaller    = "bpm-test"
	testRequestID = "11111111-1111-4111-8111-111111111111"
	socialMedia   = `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"SOCIAL_MEDIA","operationVersion":1,"input":{"iin":"000000000000","fullName":"Тестовый клиент"},"documents":[]}`
)

var createdAt = time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)

func newEngine(svc taskService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewHandler(svc).Routes(r.Group("/api/v1", httpx.FixedCaller(testCaller)))
	return r
}

func do(r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func decodeError(t *testing.T, w *httptest.ResponseRecorder) httpx.ErrorResponse {
	t.Helper()
	var resp httpx.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error body %q: %v", w.Body.String(), err)
	}
	return resp
}

func registerReturning(created bool, err error) *taskServiceMock {
	return &taskServiceMock{
		RegisterFunc: func(_ context.Context, in task.RegisterInput) (domain.Task, bool, error) {
			if err != nil {
				return domain.Task{}, false, err
			}
			return domain.Task{
				RequestID: in.RequestID,
				Operation: in.Operation,
				Status:    domain.StatusReadyToSubmit,
				CreatedAt: createdAt,
			}, created, nil
		},
	}
}

func TestCreate_NewTask(t *testing.T) {
	svc := registerReturning(true, nil)
	w := do(newEngine(svc), http.MethodPost, "/api/v1/tasks", socialMedia)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	if got := w.Header().Get("Location"); got != "/api/v1/tasks/"+testRequestID {
		t.Errorf("Location = %q", got)
	}
	want := `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"SOCIAL_MEDIA","status":"READY_TO_SUBMIT","createdAt":"2026-10-13T08:00:00Z"}`
	if w.Body.String() != want {
		t.Errorf("body = %s", w.Body)
	}

	in := svc.RegisterCalls()[0].In
	if in.CallerID != testCaller || in.RequestID.String() != testRequestID || in.OperationVersion != 1 {
		t.Errorf("unexpected input %+v", in)
	}
	if string(in.Input) != `{"iin":"000000000000","fullName":"Тестовый клиент"}` {
		t.Errorf("input = %s", in.Input)
	}
	if in.Documents == nil || len(in.Documents) != 0 {
		t.Errorf("documents = %#v, want empty non-nil", in.Documents)
	}
}

func TestCreate_Replay(t *testing.T) {
	w := do(newEngine(registerReturning(false, nil)), http.MethodPost, "/api/v1/tasks", socialMedia)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	if w.Header().Get("Location") != "" {
		t.Error("replay must not set Location")
	}
}

func TestCreate_Documents(t *testing.T) {
	svc := registerReturning(true, nil)
	body := `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"REGISTER_COMPARE","operationVersion":1,"input":{"adjudicate":true},"documents":[
		{"documentId":"22222222-2222-4222-8222-222222222222","role":"A","ordinal":1,"attstoreUrl":"https://attstore.example/documents/1","sourceVersion":"3"},
		{"documentId":"33333333-3333-4333-8333-333333333333","role":"B","ordinal":1,"attstoreUrl":"https://attstore.example/documents/2","sourceVersion":"1"}]}`
	w := do(newEngine(svc), http.MethodPost, "/api/v1/tasks", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	docs := svc.RegisterCalls()[0].In.Documents
	want := domain.Document{
		DocumentID:    uuid.MustParse("33333333-3333-4333-8333-333333333333"),
		Role:          "B",
		Ordinal:       1,
		AttstoreURL:   "https://attstore.example/documents/2",
		SourceVersion: "1",
	}
	if len(docs) != 2 || docs[1] != want {
		t.Errorf("documents = %+v", docs)
	}
}

func TestCreate_BadRequest(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantCode   string
		wantFields []httpx.FieldError
	}{
		{
			name:     "empty body",
			body:     ``,
			wantCode: httpx.CodeMalformedJSON,
		},
		{
			name:     "syntax error",
			body:     `{"requestId":`,
			wantCode: httpx.CodeMalformedJSON,
		},
		{
			name:     "trailing value",
			body:     socialMedia + `{}`,
			wantCode: httpx.CodeMalformedJSON,
		},
		{
			name:       "duplicate top-level key",
			body:       `{"requestId":"a","requestId":"b"}`,
			wantCode:   httpx.CodeMalformedJSON,
			wantFields: []httpx.FieldError{{Path: "requestId", Code: httpx.FieldDuplicateKey}},
		},
		{
			name:       "duplicate key inside input",
			body:       `{"input":{"iin":"1","iin":"2"}}`,
			wantCode:   httpx.CodeMalformedJSON,
			wantFields: []httpx.FieldError{{Path: "input.iin", Code: httpx.FieldDuplicateKey}},
		},
		{
			name:       "unknown field",
			body:       `{"requestId":"11111111-1111-4111-8111-111111111111","extra":1}`,
			wantCode:   httpx.CodeValidationError,
			wantFields: []httpx.FieldError{{Path: "extra", Code: httpx.FieldUnknown}},
		},
		{
			name:       "wrong type",
			body:       `{"operationVersion":"1"}`,
			wantCode:   httpx.CodeValidationError,
			wantFields: []httpx.FieldError{{Path: "operationVersion", Code: httpx.FieldInvalidType}},
		},
		{
			name:     "all fields missing",
			body:     `{}`,
			wantCode: httpx.CodeValidationError,
			wantFields: []httpx.FieldError{
				{Path: "requestId", Code: httpx.FieldRequired},
				{Path: "operation", Code: httpx.FieldRequired},
				{Path: "operationVersion", Code: httpx.FieldRequired},
				{Path: "input", Code: httpx.FieldRequired},
				{Path: "documents", Code: httpx.FieldRequired},
			},
		},
		{
			name:     "non-canonical uuid and input not an object",
			body:     `{"requestId":"{11111111-1111-4111-8111-111111111111}","operation":"SOCIAL_MEDIA","operationVersion":1,"input":"{}","documents":[]}`,
			wantCode: httpx.CodeValidationError,
			wantFields: []httpx.FieldError{
				{Path: "requestId", Code: httpx.FieldInvalidFormat},
				{Path: "input", Code: httpx.FieldInvalidType},
			},
		},
		{
			name:       "null input",
			body:       `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"SOCIAL_MEDIA","operationVersion":1,"input":null,"documents":[]}`,
			wantCode:   httpx.CodeValidationError,
			wantFields: []httpx.FieldError{{Path: "input", Code: httpx.FieldRequired}},
		},
		{
			name:     "invalid document",
			body:     `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"REGISTER_COMPARE","operationVersion":1,"input":{},"documents":[{"documentId":"x","ordinal":0}]}`,
			wantCode: httpx.CodeValidationError,
			wantFields: []httpx.FieldError{
				{Path: "documents[0].documentId", Code: httpx.FieldInvalidFormat},
				{Path: "documents[0].role", Code: httpx.FieldRequired},
				{Path: "documents[0].ordinal", Code: httpx.FieldOutOfRange},
				{Path: "documents[0].attstoreUrl", Code: httpx.FieldRequired},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := registerReturning(true, nil)
			w := do(newEngine(svc), http.MethodPost, "/api/v1/tasks", tt.body)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body %s", w.Code, w.Body)
			}
			resp := decodeError(t, w)
			if resp.Error.Code != tt.wantCode {
				t.Errorf("code = %s, want %s", resp.Error.Code, tt.wantCode)
			}
			if fmt.Sprint(resp.Error.Fields) != fmt.Sprint(tt.wantFields) {
				t.Errorf("fields = %v, want %v", resp.Error.Fields, tt.wantFields)
			}
			if len(svc.RegisterCalls()) != 0 {
				t.Error("usecase must not be called for a bad request")
			}
		})
	}
}

func TestCreate_BodyTooLarge(t *testing.T) {
	body := `{"input":{"x":"` + strings.Repeat("a", httpx.MaxBodyBytes) + `"}}`
	w := do(newEngine(registerReturning(true, nil)), http.MethodPost, "/api/v1/tasks", body)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", w.Code)
	}
	if code := decodeError(t, w).Error.Code; code != httpx.CodeBodyTooLarge {
		t.Errorf("code = %s", code)
	}
}

func TestCreate_UsecaseErrors(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{task.ErrIdempotencyConflict, http.StatusConflict, "IDEMPOTENCY_CONFLICT"},
		{task.ErrRequestIDUnavailable, http.StatusConflict, "REQUEST_ID_UNAVAILABLE"},
		{task.ErrOperationNotEnabled, http.StatusUnprocessableEntity, "OPERATION_NOT_ENABLED"},
		{&task.ValidationError{Fields: []task.FieldError{{Path: "input.iin", Code: "INVALID_FORMAT"}}}, http.StatusUnprocessableEntity, httpx.CodeValidationError},
		{fmt.Errorf("wrapped: %w", task.ErrIdempotencyConflict), http.StatusConflict, "IDEMPOTENCY_CONFLICT"},
		{fmt.Errorf("storage down"), http.StatusInternalServerError, httpx.CodeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			w := do(newEngine(registerReturning(false, tt.err)), http.MethodPost, "/api/v1/tasks", socialMedia)
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			resp := decodeError(t, w)
			if resp.Error.Code != tt.wantCode {
				t.Errorf("code = %s", resp.Error.Code)
			}
			if strings.Contains(w.Body.String(), "storage down") {
				t.Error("internal error details leaked")
			}
		})
	}
}

func TestGet(t *testing.T) {
	completed := createdAt.Add(time.Hour)
	svc := &taskServiceMock{
		GetFunc: func(_ context.Context, callerID string, requestID uuid.UUID) (domain.Task, error) {
			return domain.Task{
				RequestID:        requestID,
				CallerID:         callerID,
				Operation:        domain.OperationSocialMedia,
				OperationVersion: 1,
				Status:           domain.StatusSucceeded,
				StateVersion:     4,
				CreatedAt:        createdAt,
				UpdatedAt:        completed,
				CompletedAt:      &completed,
				Result:           json.RawMessage(`{"found":true}`),
			}, nil
		},
	}
	w := do(newEngine(svc), http.MethodGet, "/api/v1/tasks/"+testRequestID, "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	want := `{"requestId":"11111111-1111-4111-8111-111111111111","operation":"SOCIAL_MEDIA","operationVersion":1,"status":"SUCCEEDED","stateVersion":4,` +
		`"createdAt":"2026-10-13T08:00:00Z","updatedAt":"2026-10-13T09:00:00Z","completedAt":"2026-10-13T09:00:00Z",` +
		`"result":{"found":true},"reports":[],"error":null,"attention":null}`
	if w.Body.String() != want {
		t.Errorf("body =\n%s\nwant\n%s", w.Body, want)
	}
	if got := svc.GetCalls()[0].CallerID; got != testCaller {
		t.Errorf("caller = %s", got)
	}
}

func TestGet_NullResult(t *testing.T) {
	svc := &taskServiceMock{
		GetFunc: func(_ context.Context, _ string, requestID uuid.UUID) (domain.Task, error) {
			return domain.Task{RequestID: requestID, Status: domain.StatusReadyToSubmit}, nil
		},
	}
	w := do(newEngine(svc), http.MethodGet, "/api/v1/tasks/"+testRequestID, "")
	if !strings.Contains(w.Body.String(), `"completedAt":null,"result":null,"reports":[]`) {
		t.Errorf("body = %s", w.Body)
	}
}

func TestGet_Errors(t *testing.T) {
	notFound := &taskServiceMock{
		GetFunc: func(context.Context, string, uuid.UUID) (domain.Task, error) {
			return domain.Task{}, task.ErrTaskNotFound
		},
	}

	w := do(newEngine(notFound), http.MethodGet, "/api/v1/tasks/"+testRequestID, "")
	if w.Code != http.StatusNotFound || decodeError(t, w).Error.Code != "TASK_NOT_FOUND" {
		t.Errorf("not found: status = %d, body %s", w.Code, w.Body)
	}

	w = do(newEngine(notFound), http.MethodGet, "/api/v1/tasks/not-a-uuid", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad uuid: status = %d", w.Code)
	}
	if len(notFound.GetCalls()) != 1 {
		t.Error("usecase must not be called for a bad uuid")
	}
}
