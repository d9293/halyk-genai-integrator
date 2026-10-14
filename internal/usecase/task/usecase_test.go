package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var (
	requestID = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	docA      = domain.Document{
		DocumentID:    uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		Role:          "A",
		Ordinal:       1,
		AttstoreURL:   "https://attstore.example/documents/1",
		SourceVersion: "3",
	}
	docB = domain.Document{
		DocumentID:    uuid.MustParse("33333333-3333-4333-8333-333333333333"),
		Role:          "B",
		Ordinal:       1,
		AttstoreURL:   "https://attstore.example/documents/2",
		SourceVersion: "1",
	}
	fixedNow = time.Date(2026, 10, 13, 8, 0, 0, 0, time.UTC)
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
		AttstoreHosts:     []string{"attstore.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	uc.now = func() time.Time { return fixedNow }
	return uc
}

func socialMedia(input string) RegisterInput {
	return RegisterInput{
		CallerID:         "bpm",
		RequestID:        requestID,
		Operation:        domain.OperationSocialMedia,
		OperationVersion: 1,
		Input:            json.RawMessage(input),
		Documents:        []domain.Document{},
	}
}

func registerCompare(input string, docs ...domain.Document) RegisterInput {
	return RegisterInput{
		CallerID:         "bpm",
		RequestID:        requestID,
		Operation:        domain.OperationRegisterCompare,
		OperationVersion: 1,
		Input:            json.RawMessage(input),
		Documents:        docs,
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

	got, created, err := uc.Register(context.Background(), socialMedia(`{"fullName":"Тестовый клиент","iin":"000000000000"}`))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if got.Status != domain.StatusReadyToSubmit || got.StateVersion != 1 {
		t.Errorf("status=%s version=%d", got.Status, got.StateVersion)
	}
	if !got.CreatedAt.Equal(fixedNow) || !got.UpdatedAt.Equal(fixedNow) {
		t.Errorf("times %v %v", got.CreatedAt, got.UpdatedAt)
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

	got, _, err := uc.Register(context.Background(), registerCompare(`{}`, docB, docA))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusWaitingDocuments {
		t.Errorf("status = %s", got.Status)
	}
	if string(got.Input) != `{"adjudicate":true}` {
		t.Errorf("default not materialized: %s", got.Input)
	}
	if got.Documents[0].Role != "A" || got.Documents[1].Role != "B" {
		t.Errorf("documents not sorted: %+v", got.Documents)
	}
}

func TestRegister_HashIgnoresKeyAndDocumentOrder(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())
	ctx := context.Background()

	a, _, _ := uc.Register(ctx, registerCompare(`{}`, docA, docB))
	b, _, _ := uc.Register(ctx, registerCompare(`{"adjudicate":true}`, docB, docA))
	c, _, _ := uc.Register(ctx, registerCompare(`{"adjudicate":false}`, docA, docB))

	if a.RequestHash != b.RequestHash {
		t.Error("equivalent requests must hash equally")
	}
	if a.RequestHash == c.RequestHash {
		t.Error("different input must hash differently")
	}
}

func TestRegister_Idempotency(t *testing.T) {
	uc := newTestUseCase(t, insertingRepo())
	original, _, err := uc.Register(context.Background(), socialMedia(`{"iin":"000000000000","fullName":"Тест"}`))
	if err != nil {
		t.Fatal(err)
	}
	// The stored task has moved on since registration; replay must still match.
	original.Status = domain.StatusRemoteRunning
	original.StateVersion = 3

	existingRepo := &taskRepoMock{
		CreateIfAbsentFunc: func(context.Context, domain.Task) (domain.Task, bool, error) {
			return original, false, nil
		},
	}
	uc = newTestUseCase(t, existingRepo)

	tests := []struct {
		name    string
		in      RegisterInput
		wantErr error
	}{
		{"same request", socialMedia(`{"fullName":"Тест","iin":"000000000000"}`), nil},
		{"different input", socialMedia(`{"iin":"000000000001","fullName":"Тест"}`), ErrIdempotencyConflict},
		{"other caller", func() RegisterInput {
			in := socialMedia(`{"iin":"000000000000","fullName":"Тест"}`)
			in.CallerID = "someone-else"
			return in
		}(), ErrRequestIDUnavailable},
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
		in := registerCompare(`{}`, docA, docB)
		in.Operation = op
		if _, _, err := uc.Register(context.Background(), in); !errors.Is(err, ErrOperationNotEnabled) {
			t.Errorf("%s: err = %v", op, err)
		}
	}
}

func TestRegister_ValidationErrors(t *testing.T) {
	longName := strings.Repeat("я", maxFullNameLen+1)
	docBadURL := docB
	docBadURL.AttstoreURL = "http://attstore.example/2"
	docOtherHost := docB
	docOtherHost.AttstoreURL = "https://evil.example/2"
	docWithCreds := docB
	docWithCreds.AttstoreURL = "https://user:pass@attstore.example/2"
	docDupID := docB
	docDupID.DocumentID = docA.DocumentID
	docRoleC := docB
	docRoleC.Role = "C"
	docNoVersion := docB
	docNoVersion.SourceVersion = ""
	docOrdinal2 := docB
	docOrdinal2.Ordinal = 2

	tests := []struct {
		name string
		in   RegisterInput
		want string
	}{
		{"unsupported version", func() RegisterInput {
			in := socialMedia(`{}`)
			in.OperationVersion = 2
			return in
		}(), "[{operationVersion UNSUPPORTED_VERSION}]"},
		{"social media missing fields", socialMedia(`{}`),
			"[{input.iin REQUIRED} {input.fullName REQUIRED}]"},
		{"iin format and blank name", socialMedia(`{"iin":"12345","fullName":"  "}`),
			"[{input.iin INVALID_FORMAT} {input.fullName REQUIRED}]"},
		{"iin as number", socialMedia(`{"iin":123456789012,"fullName":"x"}`),
			"[{input.iin INVALID_TYPE}]"},
		{"name too long", socialMedia(`{"iin":"000000000000","fullName":"` + longName + `"}`),
			"[{input.fullName TOO_LONG}]"},
		{"unknown input field", socialMedia(`{"iin":"000000000000","fullName":"x","extra":1}`),
			"[{input.extra UNKNOWN_FIELD}]"},
		{"social media with documents", func() RegisterInput {
			in := socialMedia(`{"iin":"000000000000","fullName":"x"}`)
			in.Documents = []domain.Document{docA}
			return in
		}(), "[{documents NOT_ALLOWED}]"},
		{"compare with one document", registerCompare(`{}`, docA),
			"[{documents INVALID_COUNT}]"},
		{"compare adjudicate wrong type", registerCompare(`{"adjudicate":"yes"}`, docA, docB),
			"[{input.adjudicate INVALID_TYPE}]"},
		{"two A documents", registerCompare(`{}`, docA, func() domain.Document { d := docA; d.DocumentID = docB.DocumentID; return d }()),
			"[{documents[1].role DUPLICATE}]"},
		{"duplicate document id", registerCompare(`{}`, docA, docDupID),
			"[{documents[1].documentId DUPLICATE}]"},
		{"role C, no source version, ordinal 2", registerCompare(`{}`, docA, func() domain.Document {
			d := docRoleC
			d.SourceVersion = ""
			d.Ordinal = 2
			return d
		}()), "[{documents[1].role INVALID_VALUE} {documents[1].ordinal OUT_OF_RANGE} {documents[1].sourceVersion REQUIRED}]"},
		{"missing source version", registerCompare(`{}`, docA, docNoVersion),
			"[{documents[1].sourceVersion REQUIRED}]"},
		{"ordinal 2", registerCompare(`{}`, docA, docOrdinal2),
			"[{documents[1].ordinal OUT_OF_RANGE}]"},
		{"http url", registerCompare(`{}`, docA, docBadURL),
			"[{documents[1].attstoreUrl INVALID_FORMAT}]"},
		{"host not allowed", registerCompare(`{}`, docA, docOtherHost),
			"[{documents[1].attstoreUrl NOT_ALLOWED}]"},
		{"credentials in url", registerCompare(`{}`, docA, docWithCreds),
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

func TestRegister_RepoError(t *testing.T) {
	repo := &taskRepoMock{
		CreateIfAbsentFunc: func(context.Context, domain.Task) (domain.Task, bool, error) {
			return domain.Task{}, false, errors.New("db down")
		},
	}
	_, _, err := newTestUseCase(t, repo).Register(context.Background(), socialMedia(`{"iin":"000000000000","fullName":"x"}`))
	if err == nil || errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestGet_PassesCallerAndWrapsNotFound(t *testing.T) {
	repo := &taskRepoMock{
		GetFunc: func(context.Context, string, uuid.UUID) (domain.Task, error) {
			return domain.Task{}, ErrTaskNotFound
		},
	}
	_, err := newTestUseCase(t, repo).Get(context.Background(), "bpm", requestID)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v", err)
	}
	if call := repo.GetCalls()[0]; call.CallerID != "bpm" || call.RequestID != requestID {
		t.Errorf("call = %+v", call)
	}
}
