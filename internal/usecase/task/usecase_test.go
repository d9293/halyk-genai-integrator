package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	requestID   = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	requestGUID = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	doc1        = domain.Document{
		DocumentID:  uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		AttstoreURL: "https://attstore.example/documents/1",
	}
	doc2 = domain.Document{
		DocumentID:  uuid.MustParse("33333333-3333-4333-8333-333333333333"),
		AttstoreURL: "https://attstore.example/documents/2",
	}
)

// insertingRepo stores nothing and reports every task as newly inserted.
func insertingRepo() *taskRepoMock {
	return &taskRepoMock{
		CreateIfAbsentFunc: func(_ context.Context, t domain.Task) (domain.Task, bool, error) {
			return t, true, nil
		},
	}
}

func newTestUseCase(t *testing.T, repo taskRepo) *UseCase {
	t.Helper()
	uc, err := NewUseCase(repo, Config{
		EnabledOperations: []domain.Operation{domain.OperationSocialMedia, domain.OperationRegisterCompare},
	})
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func socialMediaRequest(input string) RegisterInput {
	return RegisterInput{
		CallerID:    "bpm",
		RequestGUID: requestGUID,
		RequestID:   requestID,
		Operation:   domain.OperationSocialMedia,
		Input:       json.RawMessage(input),
		Documents:   []domain.Document{},
	}
}

func registerCompareRequest(input string, docs ...domain.Document) RegisterInput {
	return RegisterInput{
		CallerID:    "bpm",
		RequestGUID: requestGUID,
		RequestID:   requestID,
		Operation:   domain.OperationRegisterCompare,
		Input:       json.RawMessage(input),
		Documents:   docs,
	}
}

func TestNewUseCase_UnknownOperation(t *testing.T) {
	_, err := NewUseCase(insertingRepo(), Config{EnabledOperations: []domain.Operation{"ANALYZE_DECISIONS"}})
	if err == nil {
		t.Fatal("expected error for an unimplemented operation")
	}
}

func TestRegister_SocialMedia(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())

	got, created, err := uc.Register(context.Background(), socialMediaRequest(`{"fullName":"Тестовый клиент","iin":"000000000000"}`))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if got.Status != domain.StatusReadyToSubmit {
		t.Errorf("status = %s", got.Status)
	}
	// Normalized: fixed key order regardless of the request's order.
	if string(got.Input) != `{"iin":"000000000000","fullName":"Тестовый клиент"}` {
		t.Errorf("input = %s", got.Input)
	}
	if got.RequestHash == "" {
		t.Error("hash not set")
	}
}

func TestRegister_RegisterCompare(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())

	got, _, err := uc.Register(context.Background(), registerCompareRequest(`{}`, doc2, doc1))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusWaitingDocuments {
		t.Errorf("status = %s", got.Status)
	}
	if string(got.Input) != `{"adjudicate":true}` {
		t.Errorf("default not materialized: %s", got.Input)
	}
	if got.Documents[0] != doc2 || got.Documents[1] != doc1 {
		t.Errorf("documents must keep BPM order: %+v", got.Documents)
	}
}

func TestRegister_Hash(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())
	ctx := context.Background()

	base, _, _ := uc.Register(ctx, registerCompareRequest(`{}`, doc1, doc2))
	sameWithDefault, _, _ := uc.Register(ctx, registerCompareRequest(`{"adjudicate":true}`, doc1, doc2))
	otherInput, _, _ := uc.Register(ctx, registerCompareRequest(`{"adjudicate":false}`, doc1, doc2))
	swappedDocs, _, _ := uc.Register(ctx, registerCompareRequest(`{}`, doc2, doc1))
	otherCamundaRequest := registerCompareRequest(`{}`, doc1, doc2)
	otherCamundaRequest.RequestID = uuid.New()
	otherRequestID, _, _ := uc.Register(ctx, otherCamundaRequest)

	if base.RequestHash != sameWithDefault.RequestHash {
		t.Error("equivalent requests must hash equally")
	}
	if base.RequestHash == otherInput.RequestHash {
		t.Error("different input must hash differently")
	}
	if base.RequestHash == swappedDocs.RequestHash {
		t.Error("swapped documents must hash differently: AI results depend on the order")
	}
	if base.RequestHash == otherRequestID.RequestHash {
		t.Error("a different requestId must hash differently")
	}
}

func TestRegister_Idempotency(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())
	original, _, err := uc.Register(context.Background(), socialMediaRequest(`{"iin":"000000000000","fullName":"Тест"}`))
	if err != nil {
		t.Fatal(err)
	}
	// The stored task has moved on since registration; replay must still match.
	original.Status = domain.StatusRemoteRunning

	existingRepo := &taskRepoMock{
		CreateIfAbsentFunc: func(context.Context, domain.Task) (domain.Task, bool, error) {
			return original, false, nil
		},
	}
	uc = newTestUseCase(t, existingRepo)

	otherCaller := socialMediaRequest(`{"iin":"000000000000","fullName":"Тест"}`)
	otherCaller.CallerID = "someone-else"

	tests := []struct {
		name    string
		in      RegisterInput
		wantErr error
	}{
		{"same request", socialMediaRequest(`{"fullName":"Тест","iin":"000000000000"}`), nil},
		{"different input", socialMediaRequest(`{"iin":"000000000001","fullName":"Тест"}`), ErrIdempotencyConflict},
		{"other caller", otherCaller, ErrRequestGUIDUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, created, err := uc.Register(context.Background(), tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if created {
				t.Error("created must be false for an existing task")
			}
			if tt.wantErr == nil && got.Status != domain.StatusRemoteRunning {
				t.Errorf("replay must return current state, got %s", got.Status)
			}
		})
	}
}

func TestRegister_OperationNotEnabled(t *testing.T) {
	uc, err := NewUseCase(insertingRepo(), Config{EnabledOperations: []domain.Operation{domain.OperationSocialMedia}})
	if err != nil {
		t.Fatal(err)
	}

	for _, op := range []domain.Operation{domain.OperationRegisterCompare, "UNKNOWN"} {
		in := registerCompareRequest(`{}`, doc1, doc2)
		in.Operation = op
		if _, _, err := uc.Register(context.Background(), in); !errors.Is(err, ErrOperationNotEnabled) {
			t.Errorf("%s: err = %v", op, err)
		}
	}
}

func TestRegister_ValidationErrors(t *testing.T) {
	longName := strings.Repeat("я", maxFullNameLen+1)
	withURL := func(url string) domain.Document {
		d := doc2
		d.AttstoreURL = url
		return d
	}
	socialMediaWithDocs := socialMediaRequest(`{"iin":"000000000000","fullName":"x"}`)
	socialMediaWithDocs.Documents = []domain.Document{doc1}

	tests := []struct {
		name string
		in   RegisterInput
		want string
	}{
		{"social media missing fields", socialMediaRequest(`{}`),
			"[{input.iin REQUIRED} {input.fullName REQUIRED}]"},
		{"iin format and blank name", socialMediaRequest(`{"iin":"12345","fullName":"  "}`),
			"[{input.iin INVALID_FORMAT} {input.fullName REQUIRED}]"},
		{"iin as number", socialMediaRequest(`{"iin":123456789012,"fullName":"x"}`),
			"[{input.iin INVALID_TYPE}]"},
		{"name too long", socialMediaRequest(`{"iin":"000000000000","fullName":"` + longName + `"}`),
			"[{input.fullName TOO_LONG}]"},
		{"unknown input field", socialMediaRequest(`{"iin":"000000000000","fullName":"x","extra":1}`),
			"[{input.extra UNKNOWN_FIELD}]"},
		{"social media with documents", socialMediaWithDocs,
			"[{documents NOT_ALLOWED}]"},
		{"compare with one document", registerCompareRequest(`{}`, doc1),
			"[{documents INVALID_COUNT}]"},
		{"compare adjudicate wrong type", registerCompareRequest(`{"adjudicate":"yes"}`, doc1, doc2),
			"[{input.adjudicate INVALID_TYPE}]"},
		{"duplicate document id", registerCompareRequest(`{}`, doc1, doc1),
			"[{documents[1].documentId DUPLICATE}]"},
		{"not an http(s) link", registerCompareRequest(`{}`, doc1, withURL("file:///etc/passwd")),
			"[{documents[1].attstoreUrl INVALID_FORMAT}]"},
		{"no host", registerCompareRequest(`{}`, doc1, withURL("https:///a.pdf")),
			"[{documents[1].attstoreUrl INVALID_FORMAT}]"},
		{"credentials in url", registerCompareRequest(`{}`, doc1, withURL("https://user:pass@attstore.example/2")),
			"[{documents[1].attstoreUrl INVALID_FORMAT}]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := insertingRepo()
			_, _, err := newTestUseCase(t, repo).Register(context.Background(), tt.in)

			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("err = %v, want ValidationError", err)
			}
			if got := fmt.Sprint(ve.Fields); got != tt.want {
				t.Errorf("fields = %s\nwant     %s", got, tt.want)
			}
			if len(repo.CreateIfAbsentCalls()) != 0 {
				t.Error("invalid task must not be stored")
			}
		})
	}
}

func TestRegister_RepoErrorsPassThrough(t *testing.T) {
	for _, repoErr := range []error{ErrStorageUnavailable} {
		repo := &taskRepoMock{
			CreateIfAbsentFunc: func(context.Context, domain.Task) (domain.Task, bool, error) {
				return domain.Task{}, false, repoErr
			},
		}
		_, _, err := newTestUseCase(t, repo).Register(context.Background(), socialMediaRequest(`{"iin":"000000000000","fullName":"x"}`))
		if !errors.Is(err, repoErr) {
			t.Errorf("err = %v, want %v", err, repoErr)
		}
	}
}

func TestGet_PassesCallerAndWrapsNotFound(t *testing.T) {
	repo := &taskRepoMock{
		GetFunc: func(context.Context, string, uuid.UUID) (domain.Task, error) {
			return domain.Task{}, ErrTaskNotFound
		},
	}
	_, err := newTestUseCase(t, repo).Get(context.Background(), "bpm", requestGUID)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v", err)
	}
	if call := repo.GetCalls()[0]; call.CallerID != "bpm" || call.RequestGUID != requestGUID {
		t.Errorf("call = %+v", call)
	}
}
