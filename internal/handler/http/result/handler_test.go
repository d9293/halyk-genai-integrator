package result

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
	"github.com/d9293/halyk-genai-integrator/internal/usecase/result"
)

const (
	requestGUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	token       = "7b1e2c3d-0000-4000-8000-000000000001"
)

// okService accepts everything.
func okService() *resultServiceMock {
	return &resultServiceMock{
		ClaimFunc:     func(context.Context, string, int) ([]result.ClaimedResult, error) { return nil, nil },
		PublishedFunc: func(context.Context, uuid.UUID, uuid.UUID) error { return nil },
		FailedFunc:    func(context.Context, uuid.UUID, uuid.UUID, domain.Failure) error { return nil },
	}
}

func post(svc resultService, path, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewHandler(svc).Routes(r.Group("/internal/v1"))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return w
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) httpx.ErrorResponse {
	t.Helper()
	var resp httpx.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return resp
}

func TestClaim(t *testing.T) {
	svc := okService()
	svc.ClaimFunc = func(context.Context, string, int) ([]result.ClaimedResult, error) {
		return []result.ClaimedResult{
			{
				RequestGUID: uuid.MustParse(requestGUID),
				Topic:       "integrator.task-results.v1",
				MessageKey:  "11111111-1111-4111-8111-111111111111",
				Message: result.Message{
					SchemaVersion: 1,
					EventType:     result.EventTaskCompleted,
					OccurredAt:    time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC),
					RequestID:     uuid.MustParse("11111111-1111-4111-8111-111111111111"),
					RequestGUID:   uuid.MustParse(requestGUID),
					Operation:     domain.OperationRegisterCompare,
					Status:        domain.StatusSucceeded,
					Result:        json.RawMessage(`{"found":true}`),
				},
				ClaimToken: uuid.MustParse(token),
				ClaimUntil: time.Date(2026, 10, 16, 10, 0, 30, 0, time.UTC),
			},
			{
				RequestGUID: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
				Message: result.Message{
					Status: domain.StatusFailed,
					Error:  &domain.Failure{Code: "X", Message: "y"},
				},
			},
		}, nil
	}

	w := post(svc, "/internal/v1/results/claim", `{"workerId":"pod-1","limit":5}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	succeeded := `{"requestGuid":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","topic":"integrator.task-results.v1",` +
		`"messageKey":"11111111-1111-4111-8111-111111111111","message":{"schemaVersion":1,"eventType":"TASK_COMPLETED",` +
		`"occurredAt":"2026-10-16T10:00:00Z","requestId":"11111111-1111-4111-8111-111111111111",` +
		`"requestGuid":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","operation":"REGISTER_COMPARE","status":"SUCCEEDED",` +
		`"result":{"found":true},"error":null},"claimToken":"7b1e2c3d-0000-4000-8000-000000000001","claimUntil":"2026-10-16T10:00:30Z"}`
	if !strings.Contains(w.Body.String(), succeeded) {
		t.Errorf("body =\n%s\nwant item\n%s", w.Body, succeeded)
	}
	if !strings.Contains(w.Body.String(), `"status":"FAILED","result":null,"error":{"code":"X","message":"y"}`) {
		t.Errorf("FAILED message must have result null and an error: %s", w.Body)
	}
}

func TestClaim_Empty(t *testing.T) {
	w := post(okService(), "/internal/v1/results/claim", `{"workerId":"pod-1","limit":1}`)
	if w.Code != http.StatusOK || w.Body.String() != `{"items":[]}` {
		t.Errorf("status = %d, body %s", w.Code, w.Body)
	}
}

func TestPublishedAndFailure(t *testing.T) {
	svc := okService()
	w := post(svc, "/internal/v1/results/"+requestGUID+"/published", fmt.Sprintf(`{"claimToken":%q}`, token))
	if w.Code != http.StatusOK || w.Body.String() != `{"applied":true}` {
		t.Fatalf("published: status = %d, body %s", w.Code, w.Body)
	}
	if call := svc.PublishedCalls()[0]; call.RequestGUID.String() != requestGUID || call.ClaimToken.String() != token {
		t.Errorf("published call = %+v", call)
	}

	body := fmt.Sprintf(`{"claimToken":%q,"failure":{"code":"KAFKA_UNAVAILABLE","message":"no broker ACK"}}`, token)
	w = post(svc, "/internal/v1/results/"+requestGUID+"/failure", body)
	if w.Code != http.StatusOK {
		t.Fatalf("failure: status = %d, body %s", w.Code, w.Body)
	}
	if got := svc.FailedCalls()[0].Failure; got != (domain.Failure{Code: "KAFKA_UNAVAILABLE", Message: "no broker ACK"}) {
		t.Errorf("failure = %+v", got)
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		want string
	}{
		{"claim: missing fields", "/internal/v1/results/claim", `{}`,
			"[{workerId REQUIRED} {limit REQUIRED}]"},
		{"bad requestGuid", "/internal/v1/results/x/published", `{}`,
			"[{requestGuid INVALID_FORMAT}]"},
		{"published: missing token", "/internal/v1/results/" + requestGUID + "/published", `{}`,
			"[{claimToken REQUIRED}]"},
		{"failure: missing fields", "/internal/v1/results/" + requestGUID + "/failure", `{}`,
			"[{claimToken REQUIRED} {failure REQUIRED}]"},
		{"failure: empty failure", "/internal/v1/results/" + requestGUID + "/failure",
			fmt.Sprintf(`{"claimToken":%q,"failure":{}}`, token),
			"[{failure.code REQUIRED} {failure.message REQUIRED}]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := okService()
			w := post(svc, tt.path, tt.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body %s", w.Code, w.Body)
			}
			if got := fmt.Sprint(errorOf(t, w).Error.Fields); got != tt.want {
				t.Errorf("fields = %s\nwant     %s", got, tt.want)
			}
			if len(svc.ClaimCalls())+len(svc.PublishedCalls())+len(svc.FailedCalls()) != 0 {
				t.Error("service must not be called for a bad request")
			}
		})
	}
}

func TestUsecaseErrors(t *testing.T) {
	tests := []struct {
		err        error
		wantStatus int
		wantCode   string
	}{
		{result.ErrResultNotFound, http.StatusNotFound, "RESULT_NOT_FOUND"},
		{fmt.Errorf("wrapped: %w", result.ErrClaimLost), http.StatusConflict, "CLAIM_LOST"},
		{result.ErrStorageUnavailable, http.StatusServiceUnavailable, httpx.CodeUnavailable},
		{fmt.Errorf("boom"), http.StatusInternalServerError, httpx.CodeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			svc := okService()
			svc.PublishedFunc = func(context.Context, uuid.UUID, uuid.UUID) error { return tt.err }
			w := post(svc, "/internal/v1/results/"+requestGUID+"/published", fmt.Sprintf(`{"claimToken":%q}`, token))
			if w.Code != tt.wantStatus || errorOf(t, w).Error.Code != tt.wantCode {
				t.Errorf("status = %d, body %s", w.Code, w.Body)
			}
		})
	}
}
