package task

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

var requestID = uuid.MustParse("11111111-1111-4111-8111-111111111111")

func newTask(callerID string) domain.Task {
	return domain.Task{
		RequestID: requestID,
		CallerID:  callerID,
		Input:     json.RawMessage(`{"a":1}`),
		Documents: []domain.Document{{Role: "A"}},
		Status:    domain.StatusReadyToSubmit,
	}
}

func TestCreateIfAbsent(t *testing.T) {
	repo := NewRepo()
	ctx := context.Background()

	if _, inserted, _ := repo.CreateIfAbsent(ctx, newTask("bpm")); !inserted {
		t.Fatal("first insert must succeed")
	}

	second := newTask("other")
	got, inserted, _ := repo.CreateIfAbsent(ctx, second)
	if inserted {
		t.Fatal("second insert must not overwrite")
	}
	if got.CallerID != "bpm" {
		t.Errorf("must return the stored task, got caller %s", got.CallerID)
	}
}

func TestGet_FiltersByCaller(t *testing.T) {
	repo := NewRepo()
	ctx := context.Background()
	_, _, _ = repo.CreateIfAbsent(ctx, newTask("bpm"))

	if _, err := repo.Get(ctx, "bpm", requestID); err != nil {
		t.Fatalf("owner: %v", err)
	}
	if _, err := repo.Get(ctx, "other", requestID); !errors.Is(err, task.ErrTaskNotFound) {
		t.Errorf("other caller: err = %v", err)
	}
	if _, err := repo.Get(ctx, "bpm", uuid.New()); !errors.Is(err, task.ErrTaskNotFound) {
		t.Errorf("missing: err = %v", err)
	}
}

func TestReturnedTasksAreCopies(t *testing.T) {
	repo := NewRepo()
	ctx := context.Background()
	in := newTask("bpm")
	_, _, _ = repo.CreateIfAbsent(ctx, in)
	in.Input[0] = 'X'
	in.Documents[0].Role = "X"

	got, _ := repo.Get(ctx, "bpm", requestID)
	got.Documents[0].Role = "Y"

	again, _ := repo.Get(ctx, "bpm", requestID)
	if string(again.Input) != `{"a":1}` || again.Documents[0].Role != "A" {
		t.Errorf("stored task was mutated: %s %+v", again.Input, again.Documents)
	}
}

// Many concurrent identical registrations through the real usecase: exactly
// one creates the task, the rest get a replay of it.
func TestConcurrentRegister(t *testing.T) {
	uc, err := task.NewUseCase(NewRepo(), task.Config{
		EnabledOperations: []domain.Operation{domain.OperationSocialMedia},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := task.RegisterInput{
		CallerID:         "bpm",
		RequestID:        requestID,
		Operation:        domain.OperationSocialMedia,
		OperationVersion: 1,
		Input:            json.RawMessage(`{"iin":"000000000000","fullName":"x"}`),
		Documents:        []domain.Document{},
	}

	const n = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		errs    []error
	)
	for range n {
		wg.Go(func() {
			_, c, err := uc.Register(context.Background(), in)
			mu.Lock()
			defer mu.Unlock()
			if c {
				created++
			}
			if err != nil {
				errs = append(errs, err)
			}
		})
	}
	wg.Wait()

	if created != 1 || len(errs) != 0 {
		t.Fatalf("created = %d, errors = %v", created, errs)
	}
}
