package poll

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
)

const (
	attemptID = "a77e0000-0000-4000-8000-000000000002"
	token     = "7b1e2c3d-0000-4000-8000-000000000001"
)

// okService accepts everything.
func okService() *pollServiceMock {
	return &pollServiceMock{
		ClaimFunc:   func(context.Context, string, int) ([]poll.ClaimedPoll, error) { return nil, nil },
		ObserveFunc: func(context.Context, uuid.UUID, uuid.UUID, poll.Observation) error { return nil },
	}
}

func post(svc pollService, path, body string) *httptest.ResponseRecorder {
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

func observationPath() string { return "/internal/v1/polls/" + attemptID + "/observation" }

func TestClaim(t *testing.T) {
	svc := okService()
	svc.ClaimFunc = func(context.Context, string, int) ([]poll.ClaimedPoll, error) {
		return []poll.ClaimedPoll{{
			AttemptID:   uuid.MustParse(attemptID),
			AttemptNo:   2,
			RequestGUID: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
			RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
			Operation:   domain.OperationRegisterCompare,
			ClaimToken:  uuid.MustParse(token),
			ClaimUntil:  time.Date(2026, 10, 16, 8, 0, 30, 0, time.UTC),
		}}, nil
	}

	w := post(svc, "/internal/v1/polls/claim", `{"workerId":"pod-1","limit":5}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	want := `{"items":[{"attemptId":"a77e0000-0000-4000-8000-000000000002","attemptNo":2,` +
		`"requestGuid":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","requestId":"11111111-1111-4111-8111-111111111111",` +
		`"operation":"REGISTER_COMPARE","claimToken":"7b1e2c3d-0000-4000-8000-000000000001","claimUntil":"2026-10-16T08:00:30Z"}]}`
	if w.Body.String() != want {
		t.Errorf("body =\n%s\nwant\n%s", w.Body, want)
	}
}

func TestClaim_Empty(t *testing.T) {
	w := post(okService(), "/internal/v1/polls/claim", `{"workerId":"pod-1","limit":1}`)
	if w.Code != http.StatusOK || w.Body.String() != `{"items":[]}` {
		t.Errorf("status = %d, body %s", w.Code, w.Body)
	}
}

func TestObservation(t *testing.T) {
	failure := &domain.Failure{Code: "AI_TIMEOUT", Message: "30s"}
	tests := []struct {
		outcome string
		extra   string
		want    poll.Observation
	}{
		{"QUEUED", ``, poll.Observation{Outcome: poll.OutcomeQueued}},
		{"PROCESSING", ``, poll.Observation{Outcome: poll.OutcomeProcessing}},
		{"NOT_FOUND", ``, poll.Observation{Outcome: poll.OutcomeNotFound}},
		{"SUCCEEDED", `,"result":{"summary":{"both":12},"report_url":"https://s3/r.docx"}`,
			poll.Observation{Outcome: poll.OutcomeSucceeded, Result: json.RawMessage(`{"summary":{"both":12},"report_url":"https://s3/r.docx"}`)}},
		{"FAILED", `,"failure":{"code":"AI_TIMEOUT","message":"30s"}`,
			poll.Observation{Outcome: poll.OutcomeFailed, Failure: failure}},
		{"TECHNICAL_ERROR", `,"failure":{"code":"AI_TIMEOUT","message":"30s"}`,
			poll.Observation{Outcome: poll.OutcomeTechnicalError, Failure: failure}},
	}
	for _, tt := range tests {
		t.Run(tt.outcome, func(t *testing.T) {
			svc := okService()
			body := fmt.Sprintf(`{"claimToken":%q,"outcome":%q%s}`, token, tt.outcome, tt.extra)
			w := post(svc, observationPath(), body)
			if w.Code != http.StatusOK || w.Body.String() != `{"applied":true}` {
				t.Fatalf("status = %d, body %s", w.Code, w.Body)
			}
			if got := svc.ObserveCalls()[0].Observation; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("observation = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		want string
	}{
		{"claim: missing fields", "/internal/v1/polls/claim", `{}`,
			"[{workerId REQUIRED} {limit REQUIRED}]"},
		{"bad attempt id", "/internal/v1/polls/x/observation", `{}`,
			"[{attemptId INVALID_FORMAT}]"},
		{"missing fields", observationPath(), `{}`,
			"[{claimToken REQUIRED} {outcome REQUIRED}]"},
		{"unknown outcome", observationPath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"DONE"}`, token),
			"[{outcome INVALID_FORMAT}]"},
		{"succeeded without result", observationPath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"SUCCEEDED"}`, token),
			"[{result REQUIRED}]"},
		{"succeeded with non-object result", observationPath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"SUCCEEDED","result":[1]}`, token),
			"[{result INVALID_TYPE}]"},
		{"failed without failure", observationPath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"FAILED"}`, token),
			"[{failure REQUIRED}]"},
		{"queued with result and failure", observationPath(),
			fmt.Sprintf(`{"claimToken":%q,"outcome":"QUEUED","result":{},"failure":{"code":"x","message":"y"}}`, token),
			"[{result NOT_ALLOWED} {failure NOT_ALLOWED}]"},
		{"technical error with empty failure", observationPath(),
			fmt.Sprintf(`{"claimToken":%q,"outcome":"TECHNICAL_ERROR","failure":{}}`, token),
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
			if len(svc.ClaimCalls())+len(svc.ObserveCalls()) != 0 {
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
		{poll.ErrAttemptNotFound, http.StatusNotFound, "ATTEMPT_NOT_FOUND"},
		{fmt.Errorf("wrapped: %w", poll.ErrClaimLost), http.StatusConflict, "CLAIM_LOST"},
		{poll.ErrStorageUnavailable, http.StatusServiceUnavailable, httpx.CodeUnavailable},
		{fmt.Errorf("boom"), http.StatusInternalServerError, httpx.CodeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			svc := okService()
			svc.ObserveFunc = func(context.Context, uuid.UUID, uuid.UUID, poll.Observation) error { return tt.err }
			w := post(svc, observationPath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"QUEUED"}`, token))
			if w.Code != tt.wantStatus || errorOf(t, w).Error.Code != tt.wantCode {
				t.Errorf("status = %d, body %s", w.Code, w.Body)
			}
		})
	}
}
