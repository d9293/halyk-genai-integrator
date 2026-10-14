package task

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

//go:generate moq -out task_repo_mock_test.go . taskRepo

type taskRepo interface {
	// CreateIfAbsent stores t unless its requestId already exists. It returns the
	// stored task and whether this call inserted it. Must be atomic: of two
	// concurrent calls with the same requestId exactly one inserts.
	CreateIfAbsent(ctx context.Context, t domain.Task) (domain.Task, bool, error)
	// Get returns ErrTaskNotFound when the task is missing or owned by another caller.
	Get(ctx context.Context, callerID string, requestID uuid.UUID) (domain.Task, error)
}

type Config struct {
	// EnabledOperations accept new tasks; unconfirmed operations stay disabled.
	EnabledOperations []domain.Operation
	// AttstoreHosts is the allowlist of hosts for document attstoreUrl.
	AttstoreHosts []string
}

type UseCase struct {
	repo          taskRepo
	enabled       map[domain.Operation]bool
	attstoreHosts map[string]bool
	now           func() time.Time
}

func NewUseCase(repo taskRepo, cfg Config) (*UseCase, error) {
	known := make(map[domain.Operation]bool)
	for key := range knownOperations {
		known[key.operation] = true
	}

	enabled := make(map[domain.Operation]bool)
	for _, op := range cfg.EnabledOperations {
		if !known[op] {
			return nil, fmt.Errorf("enabled operation %q is not implemented", op)
		}
		enabled[op] = true
	}

	hosts := make(map[string]bool)
	for _, h := range cfg.AttstoreHosts {
		hosts[h] = true
	}

	return &UseCase{
		repo:          repo,
		enabled:       enabled,
		attstoreHosts: hosts,
		now:           time.Now,
	}, nil
}

// Register stores a new task, or returns the existing one when the same caller
// repeats the same registration. created reports whether this call stored it.
func (uc *UseCase) Register(ctx context.Context, in RegisterInput) (domain.Task, bool, error) {
	if !uc.enabled[in.Operation] {
		return domain.Task{}, false, ErrOperationNotEnabled
	}
	spec, ok := knownOperations[operationKey{in.Operation, in.OperationVersion}]
	if !ok {
		return domain.Task{}, false, &ValidationError{Fields: []FieldError{
			{Path: "operationVersion", Code: CodeUnsupportedVersion},
		}}
	}

	input, fields := spec.NormalizeInput(in.Input)
	fields = append(fields, uc.validateDocuments(in.Documents)...)
	fields = append(fields, spec.ValidateDocuments(in.Documents)...)
	if len(fields) > 0 {
		return domain.Task{}, false, &ValidationError{Fields: fields}
	}

	// Order is not part of the request's identity; role/ordinal is.
	docs := slices.Clone(in.Documents)
	slices.SortFunc(docs, func(a, b domain.Document) int {
		return cmp.Or(cmp.Compare(a.Role, b.Role), cmp.Compare(a.Ordinal, b.Ordinal))
	})

	status := domain.StatusReadyToSubmit
	if spec.RequiresDocuments() {
		status = domain.StatusWaitingDocuments
	}

	now := uc.now().UTC()
	t := domain.Task{
		RequestID:        in.RequestID,
		CallerID:         in.CallerID,
		Operation:        in.Operation,
		OperationVersion: in.OperationVersion,
		Input:            input,
		Documents:        docs,
		RequestHash:      requestHash(in.Operation, in.OperationVersion, input, docs),
		Status:           status,
		StateVersion:     1,
		CreatedAt:        now,
		UpdatedAt:        now,
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
		return domain.Task{}, false, ErrRequestIDUnavailable
	case stored.RequestHash != t.RequestHash:
		return domain.Task{}, false, ErrIdempotencyConflict
	}
	return stored, false, nil
}

func (uc *UseCase) Get(ctx context.Context, callerID string, requestID uuid.UUID) (domain.Task, error) {
	t, err := uc.repo.Get(ctx, callerID, requestID)
	if err != nil {
		return domain.Task{}, fmt.Errorf("get task: %w", err)
	}
	return t, nil
}

// validateDocuments applies the rules shared by every operation.
func (uc *UseCase) validateDocuments(docs []domain.Document) []FieldError {
	var fields []FieldError
	seenIDs := make(map[uuid.UUID]bool)
	type slot struct {
		role    string
		ordinal int
	}
	seenSlots := make(map[slot]bool)

	for i, d := range docs {
		path := documentPath(i)
		if seenIDs[d.DocumentID] {
			fields = append(fields, FieldError{Path: path + ".documentId", Code: CodeDuplicate})
		}
		seenIDs[d.DocumentID] = true

		s := slot{d.Role, d.Ordinal}
		if seenSlots[s] {
			fields = append(fields, FieldError{Path: path + ".role", Code: CodeDuplicate})
		}
		seenSlots[s] = true

		u, err := url.Parse(d.AttstoreURL)
		switch {
		case err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil:
			fields = append(fields, FieldError{Path: path + ".attstoreUrl", Code: CodeInvalidFormat})
		case !uc.attstoreHosts[u.Hostname()]:
			fields = append(fields, FieldError{Path: path + ".attstoreUrl", Code: CodeNotAllowed})
		}
	}
	return fields
}

func documentPath(i int) string {
	return "documents[" + strconv.Itoa(i) + "]"
}

// requestHash fingerprints the normalized registration. Documents must already be sorted.
func requestHash(op domain.Operation, version int, input json.RawMessage, docs []domain.Document) string {
	type hashedDocument struct {
		DocumentID    uuid.UUID `json:"documentId"`
		Role          string    `json:"role"`
		Ordinal       int       `json:"ordinal"`
		AttstoreURL   string    `json:"attstoreUrl"`
		SourceVersion string    `json:"sourceVersion"`
	}
	hashed := make([]hashedDocument, 0, len(docs))
	for _, d := range docs {
		hashed = append(hashed, hashedDocument(d))
	}

	payload := mustMarshal(struct {
		Operation        domain.Operation `json:"operation"`
		OperationVersion int              `json:"operationVersion"`
		Input            json.RawMessage  `json:"input"`
		Documents        []hashedDocument `json:"documents"`
	}{op, version, input, hashed})

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
