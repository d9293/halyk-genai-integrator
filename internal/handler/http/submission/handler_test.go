package submission

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
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

const (
	attemptID = "a77e0000-0000-4000-8000-000000000002"
	token     = "7b1e2c3d-0000-4000-8000-000000000001"
)

var claimUntil = time.Date(2026, 10, 15, 8, 1, 0, 0, time.UTC)

// okService accepts everything.
func okService() *submissionServiceMock {
	return &submissionServiceMock{
		ClaimFunc: func(context.Context, string, int) ([]submission.ClaimedSubmission, error) {
			return nil, nil
		},
		ReportFunc: func(context.Context, uuid.UUID, uuid.UUID, submission.Report) error { return nil },
		RenewFunc: func(context.Context, uuid.UUID, uuid.UUID) (time.Time, error) {
			return claimUntil, nil
		},
	}
}

func post(svc submissionService, path, body string) *httptest.ResponseRecorder {
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

func outcomePath() string { return "/internal/v1/submissions/" + attemptID + "/outcome" }

func TestClaim(t *testing.T) {
	svc := okService()
	svc.ClaimFunc = func(context.Context, string, int) ([]submission.ClaimedSubmission, error) {
		return []submission.ClaimedSubmission{{
			AttemptID:   uuid.MustParse(attemptID),
			AttemptNo:   1,
			RequestGUID: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
			RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
			Operation:   domain.OperationRegisterCompare,
			Input:       json.RawMessage(`{"adjudicate":true}`),
			Documents:   []submission.SubmissionDocument{{Position: 0, S3URI: "s3://b/0"}, {Position: 1, S3URI: "s3://b/1"}},
			ClaimToken:  uuid.MustParse(token),
			ClaimUntil:  claimUntil,
		}}, nil
	}

	w := post(svc, "/internal/v1/submissions/claim", `{"workerId":"pod-1","limit":1}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	want := `{"items":[{"attemptId":"a77e0000-0000-4000-8000-000000000002","attemptNo":1,` +
		`"requestGuid":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","requestId":"11111111-1111-4111-8111-111111111111",` +
		`"operation":"REGISTER_COMPARE","input":{"adjudicate":true},` +
		`"documents":[{"position":0,"s3Uri":"s3://b/0"},{"position":1,"s3Uri":"s3://b/1"}],` +
		`"claimToken":"7b1e2c3d-0000-4000-8000-000000000001","claimUntil":"2026-10-15T08:01:00Z"}]}`
	if w.Body.String() != want {
		t.Errorf("body =\n%s\nwant\n%s", w.Body, want)
	}
}

func TestClaim_EmptyAndNoDocuments(t *testing.T) {
	w := post(okService(), "/internal/v1/submissions/claim", `{"workerId":"pod-1","limit":1}`)
	if w.Code != http.StatusOK || w.Body.String() != `{"items":[]}` {
		t.Errorf("status = %d, body %s", w.Code, w.Body)
	}

	svc := okService()
	svc.ClaimFunc = func(context.Context, string, int) ([]submission.ClaimedSubmission, error) {
		return []submission.ClaimedSubmission{{Operation: domain.OperationSocialMedia, Input: json.RawMessage(`{}`)}}, nil
	}
	w = post(svc, "/internal/v1/submissions/claim", `{"workerId":"pod-1","limit":1}`)
	if !strings.Contains(w.Body.String(), `"documents":[]`) {
		t.Errorf("operation without files must return documents [], body %s", w.Body)
	}
}

func TestOutcome(t *testing.T) {
	tests := []struct {
		name string
		body string
		want submission.Report
	}{
		{"accepted defaults to QUEUED",
			fmt.Sprintf(`{"claimToken":%q,"outcome":"ACCEPTED"}`, token),
			submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}},
		{"accepted processing",
			fmt.Sprintf(`{"claimToken":%q,"outcome":"ACCEPTED","remoteStatus":"PROCESSING"}`, token),
			submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "PROCESSING"}},
		{"unknown without failure",
			fmt.Sprintf(`{"claimToken":%q,"outcome":"UNKNOWN"}`, token),
			submission.Report{Outcome: submission.OutcomeUnknown}},
		{"rejected",
			fmt.Sprintf(`{"claimToken":%q,"outcome":"REJECTED","failure":{"code":"INVALID_INPUT","message":"bad files"}}`, token),
			submission.Report{Outcome: submission.OutcomeRejected, Failure: &domain.Failure{Code: "INVALID_INPUT", Message: "bad files"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := okService()
			w := post(svc, outcomePath(), tt.body)
			if w.Code != http.StatusOK || w.Body.String() != `{"applied":true}` {
				t.Fatalf("status = %d, body %s", w.Code, w.Body)
			}
			if got := svc.ReportCalls()[0].Report; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("report = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestRenew(t *testing.T) {
	w := post(okService(), "/internal/v1/submissions/"+attemptID+"/renew", fmt.Sprintf(`{"claimToken":%q}`, token))
	if w.Code != http.StatusOK || w.Body.String() != `{"claimUntil":"2026-10-15T08:01:00Z"}` {
		t.Errorf("status = %d, body %s", w.Code, w.Body)
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		want string
	}{
		{"claim: missing fields", "/internal/v1/submissions/claim", `{}`,
			"[{workerId REQUIRED} {limit REQUIRED}]"},
		{"outcome: bad attempt id", "/internal/v1/submissions/x/outcome", `{}`,
			"[{attemptId INVALID_FORMAT}]"},
		{"outcome: missing fields", outcomePath(), `{}`,
			"[{claimToken REQUIRED} {outcome REQUIRED}]"},
		{"outcome: unknown value", outcomePath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"MAYBE"}`, token),
			"[{outcome INVALID_FORMAT}]"},
		{"accepted: failure not allowed, bad status", outcomePath(),
			fmt.Sprintf(`{"claimToken":%q,"outcome":"ACCEPTED","remoteStatus":"DONE","failure":{"code":"x","message":"y"}}`, token),
			"[{remoteStatus INVALID_FORMAT} {failure NOT_ALLOWED}]"},
		{"rejected: failure required", outcomePath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"REJECTED"}`, token),
			"[{failure REQUIRED}]"},
		{"unknown: remoteStatus not allowed, empty failure", outcomePath(),
			fmt.Sprintf(`{"claimToken":%q,"outcome":"UNKNOWN","remoteStatus":"QUEUED","failure":{}}`, token),
			"[{remoteStatus NOT_ALLOWED} {failure.code REQUIRED} {failure.message REQUIRED}]"},
		{"renew: missing token", "/internal/v1/submissions/" + attemptID + "/renew", `{}`,
			"[{claimToken REQUIRED}]"},
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
			if len(svc.ClaimCalls())+len(svc.ReportCalls())+len(svc.RenewCalls()) != 0 {
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
		{submission.ErrAttemptNotFound, http.StatusNotFound, "ATTEMPT_NOT_FOUND"},
		{fmt.Errorf("wrapped: %w", submission.ErrClaimLost), http.StatusConflict, "CLAIM_LOST"},
		{submission.ErrStorageUnavailable, http.StatusServiceUnavailable, httpx.CodeUnavailable},
		{fmt.Errorf("boom"), http.StatusInternalServerError, httpx.CodeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			svc := okService()
			svc.ReportFunc = func(context.Context, uuid.UUID, uuid.UUID, submission.Report) error { return tt.err }
			w := post(svc, outcomePath(), fmt.Sprintf(`{"claimToken":%q,"outcome":"UNKNOWN"}`, token))
			if w.Code != tt.wantStatus || errorOf(t, w).Error.Code != tt.wantCode {
				t.Errorf("status = %d, body %s", w.Code, w.Body)
			}
		})
	}
}
