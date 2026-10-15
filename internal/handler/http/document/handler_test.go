package document

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

	"github.com/d9293/halyk-genai-integrator/internal/handler/http/httpx"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/document"
)

const (
	docID    = "d0c00000-0000-4000-8000-000000000001"
	token    = "7b1e2c3d-0000-4000-8000-000000000001"
	checksum = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
)

var claimUntil = time.Date(2026, 10, 15, 8, 1, 0, 0, time.UTC)

// okService accepts everything.
func okService() *documentServiceMock {
	return &documentServiceMock{
		ClaimFunc: func(context.Context, string, int) ([]document.ClaimedDocument, error) {
			return nil, nil
		},
		CompleteFunc: func(context.Context, uuid.UUID, uuid.UUID, document.CopyResult) error { return nil },
		FailFunc:     func(context.Context, uuid.UUID, uuid.UUID, document.CopyFailure) error { return nil },
		RenewFunc: func(context.Context, uuid.UUID, uuid.UUID) (time.Time, error) {
			return claimUntil, nil
		},
	}
}

func post(svc documentService, path, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	NewHandler(svc).Routes(r.Group("/internal/v1"))

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
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
	svc.ClaimFunc = func(context.Context, string, int) ([]document.ClaimedDocument, error) {
		return []document.ClaimedDocument{{
			ID:          uuid.MustParse(docID),
			RequestGUID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
			DocumentID:  uuid.MustParse("22222222-2222-4222-8222-222222222222"),
			AttstoreURL: "https://attstore.example/1",
			Position:    1,
			ClaimToken:  uuid.MustParse(token),
			ClaimUntil:  claimUntil,
		}}, nil
	}

	w := post(svc, "/internal/v1/documents/claim", `{"workerId":"pod-1","limit":3}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	want := `{"items":[{"id":"d0c00000-0000-4000-8000-000000000001","requestGuid":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",` +
		`"documentId":"22222222-2222-4222-8222-222222222222","attstoreUrl":"https://attstore.example/1","position":1,` +
		`"claimToken":"7b1e2c3d-0000-4000-8000-000000000001","claimUntil":"2026-10-15T08:01:00Z"}]}`
	if w.Body.String() != want {
		t.Errorf("body =\n%s\nwant\n%s", w.Body, want)
	}
	if call := svc.ClaimCalls()[0]; call.WorkerID != "pod-1" || call.Limit != 3 {
		t.Errorf("call = %+v", call)
	}
}

func TestClaim_Empty(t *testing.T) {
	w := post(okService(), "/internal/v1/documents/claim", `{"workerId":"pod-1","limit":1}`)
	if w.Code != http.StatusOK || w.Body.String() != `{"items":[]}` {
		t.Errorf("status = %d, body %s", w.Code, w.Body)
	}
}

func TestComplete(t *testing.T) {
	svc := okService()
	body := fmt.Sprintf(`{"claimToken":%q,"s3Uri":"s3://ai-input/a","sizeBytes":10,"checksumSha256":%q}`, token, strings.ToUpper(checksum))

	w := post(svc, "/internal/v1/documents/"+docID+"/complete", body)
	if w.Code != http.StatusOK || w.Body.String() != `{"applied":true}` {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	call := svc.CompleteCalls()[0]
	if call.ID.String() != docID || call.ClaimToken.String() != token {
		t.Errorf("call = %+v", call)
	}
	want := document.CopyResult{S3URI: "s3://ai-input/a", SizeBytes: 10, ChecksumSHA256: checksum}
	if call.Result != want {
		t.Errorf("result = %+v, want %+v (checksum lower-cased)", call.Result, want)
	}
}

func TestFailure(t *testing.T) {
	svc := okService()
	body := fmt.Sprintf(`{"claimToken":%q,"code":"SOURCE_UNAVAILABLE","message":"Attstore returned 503","retryable":true}`, token)

	w := post(svc, "/internal/v1/documents/"+docID+"/failure", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	want := document.CopyFailure{Code: "SOURCE_UNAVAILABLE", Message: "Attstore returned 503", Retryable: true}
	if got := svc.FailCalls()[0].Failure; got != want {
		t.Errorf("failure = %+v", got)
	}
}

func TestRenew(t *testing.T) {
	body := fmt.Sprintf(`{"claimToken":%q}`, token)
	w := post(okService(), "/internal/v1/documents/"+docID+"/renew", body)
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
		{"claim: missing fields", "/internal/v1/documents/claim", `{}`,
			"[{workerId REQUIRED} {limit REQUIRED}]"},
		{"claim: limit out of range", "/internal/v1/documents/claim", `{"workerId":"pod-1","limit":11}`,
			"[{limit OUT_OF_RANGE}]"},
		{"complete: bad id in path", "/internal/v1/documents/not-a-uuid/complete", `{}`,
			"[{id INVALID_FORMAT}]"},
		{"complete: missing fields", "/internal/v1/documents/" + docID + "/complete", `{}`,
			"[{claimToken REQUIRED} {s3Uri REQUIRED} {sizeBytes REQUIRED} {checksumSha256 REQUIRED}]"},
		{"complete: bad values", "/internal/v1/documents/" + docID + "/complete",
			`{"claimToken":"x","s3Uri":"https://bucket/a","sizeBytes":-1,"checksumSha256":"abc"}`,
			"[{claimToken INVALID_FORMAT} {s3Uri INVALID_FORMAT} {sizeBytes OUT_OF_RANGE} {checksumSha256 INVALID_FORMAT}]"},
		{"failure: missing fields", "/internal/v1/documents/" + docID + "/failure", `{}`,
			"[{claimToken REQUIRED} {code REQUIRED} {message REQUIRED} {retryable REQUIRED}]"},
		{"renew: missing token", "/internal/v1/documents/" + docID + "/renew", `{}`,
			"[{claimToken REQUIRED}]"},
		{"unknown field", "/internal/v1/documents/claim", `{"workerId":"pod-1","limit":1,"extra":1}`,
			"[{extra UNKNOWN_FIELD}]"},
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
			if len(svc.ClaimCalls())+len(svc.CompleteCalls())+len(svc.FailCalls())+len(svc.RenewCalls()) != 0 {
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
		{document.ErrDocumentNotFound, http.StatusNotFound, "DOCUMENT_NOT_FOUND"},
		{fmt.Errorf("wrapped: %w", document.ErrClaimLost), http.StatusConflict, "CLAIM_LOST"},
		{fmt.Errorf("boom"), http.StatusInternalServerError, httpx.CodeInternalError},
	}
	for _, tt := range tests {
		t.Run(tt.wantCode, func(t *testing.T) {
			svc := okService()
			svc.RenewFunc = func(context.Context, uuid.UUID, uuid.UUID) (time.Time, error) {
				return time.Time{}, tt.err
			}
			body := fmt.Sprintf(`{"claimToken":%q}`, token)
			w := post(svc, "/internal/v1/documents/"+docID+"/renew", body)
			if w.Code != tt.wantStatus || errorOf(t, w).Error.Code != tt.wantCode {
				t.Errorf("status = %d, body %s", w.Code, w.Body)
			}
		})
	}
}
