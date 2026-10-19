package task

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out task_repo_mock_test.go . taskRepo

type taskRepo interface {
	// CreateIfAbsent stores t unless its requestGuid already exists. It returns the
	// stored task and whether this call inserted it. Must be atomic: of two
	// concurrent calls with the same requestGuid exactly one inserts. The store
	// sets CreatedAt and UpdatedAt.
	CreateIfAbsent(ctx context.Context, t domain.Task) (domain.Task, bool, error)
	// Get returns ErrTaskNotFound when the task is missing or owned by another caller.
	Get(ctx context.Context, callerID string, requestGUID uuid.UUID) (domain.Task, error)
}

type Config struct {
	// EnabledOperations accept new tasks; unconfirmed operations stay disabled.
	EnabledOperations []domain.Operation
}

type UseCase struct {
	repo    taskRepo
	enabled map[domain.Operation]bool
}

func NewUseCase(repo taskRepo, cfg Config) (*UseCase, error) {
	known := make(map[domain.Operation]bool)
	for op := range knownOperations {
		known[op] = true
	}

	enabled := make(map[domain.Operation]bool)
	for _, op := range cfg.EnabledOperations {
		if !known[op] {
			return nil, fmt.Errorf("enabled operation %q is not implemented", op)
		}
		enabled[op] = true
	}

	return &UseCase{
		repo:    repo,
		enabled: enabled,
	}, nil
}

// Register stores a new task, or returns the existing one when the same caller
// repeats the same registration. created reports whether this call stored it.
func (uc *UseCase) Register(ctx context.Context, in RegisterInput) (domain.Task, bool, error) {
	if !uc.enabled[in.Operation] {
		return domain.Task{}, false, ErrOperationNotEnabled
	}
	spec := knownOperations[in.Operation]

	input, fields := spec.NormalizeInput(in.Input)
	fields = append(fields, uc.validateDocuments(in.Documents)...)
	fields = append(fields, spec.ValidateDocuments(in.Documents)...)
	if len(fields) > 0 {
		return domain.Task{}, false, &ValidationError{Fields: fields}
	}

	status := domain.StatusReadyToSubmit
	if spec.RequiresDocuments() {
		status = domain.StatusWaitingDocuments
	}

	t := domain.Task{
		RequestGUID: in.RequestGUID,
		RequestID:   in.RequestID,
		CallerID:    in.CallerID,
		Operation:   in.Operation,
		Input:       input,
		Documents:   in.Documents,
		RequestHash: requestHash(in.RequestID, in.Operation, input, in.Documents),
		Status:      status,
	}

	stored, created, err := uc.repo.CreateIfAbsent(ctx, t)
	if err != nil {
		return domain.Task{}, false, fmt.Errorf("create task: %w", err)
	}
	if created {
		return stored, true, nil
	}

	// Compare against the original registration, never the current operational state.
	switch {
	case stored.CallerID != t.CallerID:
		return domain.Task{}, false, ErrRequestGUIDUnavailable
	case stored.RequestHash != t.RequestHash:
		return domain.Task{}, false, ErrIdempotencyConflict
	}
	return stored, false, nil
}

func (uc *UseCase) Get(ctx context.Context, callerID string, requestGUID uuid.UUID) (domain.Task, error) {
	t, err := uc.repo.Get(ctx, callerID, requestGUID)
	if err != nil {
		return domain.Task{}, fmt.Errorf("get task: %w", err)
	}
	return t, nil
}

// validateDocuments applies the rules shared by every operation.
func (uc *UseCase) validateDocuments(docs []domain.Document) []FieldError {
	var fields []FieldError
	seenIDs := make(map[uuid.UUID]bool)

	for i, d := range docs {
		path := documentPath(i)
		if seenIDs[d.DocumentID] {
			fields = append(fields, FieldError{Path: path + ".documentId", Code: CodeDuplicate})
		}
		seenIDs[d.DocumentID] = true

		// Any http(s) link is accepted: BPM is a trusted caller and its links
		// are not restricted to known hosts (decision 2026-10-18). Credentials
		// in the link are refused so they never land in our database and logs.
		u, err := url.Parse(d.AttstoreURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			fields = append(fields, FieldError{Path: path + ".attstoreUrl", Code: CodeInvalidFormat})
		}
	}
	return fields
}

func documentPath(i int) string {
	return "documents[" + strconv.Itoa(i) + "]"
}

// requestHash fingerprints the normalized registration. Document order is
// part of it: swapped files give a swapped AI result.
func requestHash(requestID uuid.UUID, op domain.Operation, input json.RawMessage, docs []domain.Document) string {
	type hashedDocument struct {
		DocumentID  uuid.UUID `json:"documentId"`
		AttstoreURL string    `json:"attstoreUrl"`
	}
	hashed := make([]hashedDocument, 0, len(docs))
	for _, d := range docs {
		hashed = append(hashed, hashedDocument(d))
	}

	payload := mustMarshal(struct {
		RequestID uuid.UUID        `json:"requestId"`
		Operation domain.Operation `json:"operation"`
		Input     json.RawMessage  `json:"input"`
		Documents []hashedDocument `json:"documents"`
	}{requestID, op, input, hashed})

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
