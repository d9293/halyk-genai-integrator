// Package task is an in-memory task store for running without PostgreSQL.
// State is lost on restart, and the mutex only coordinates one process; it is
// not a substitute for database coordination between instances.
package task

import (
	"context"
	"slices"
	"sync"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

type Repo struct {
	mu    sync.RWMutex
	tasks map[uuid.UUID]domain.Task
}

func NewRepo() *Repo {
	return &Repo{tasks: make(map[uuid.UUID]domain.Task)}
}

func (r *Repo) CreateIfAbsent(_ context.Context, t domain.Task) (domain.Task, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.tasks[t.RequestID]; ok {
		return clone(existing), false, nil
	}
	r.tasks[t.RequestID] = clone(t)
	return clone(t), true, nil
}

func (r *Repo) Get(_ context.Context, callerID string, requestID uuid.UUID) (domain.Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.tasks[requestID]
	if !ok || t.CallerID != callerID {
		return domain.Task{}, task.ErrTaskNotFound
	}
	return clone(t), nil
}

// clone deep-copies t so callers can never mutate stored state.
func clone(t domain.Task) domain.Task {
	t.Input = slices.Clone(t.Input)
	t.Result = slices.Clone(t.Result)
	t.Documents = slices.Clone(t.Documents)
	t.Reports = slices.Clone(t.Reports)
	for i, r := range t.Reports {
		t.Reports[i].ExpiresAt = clonePtr(r.ExpiresAt)
	}
	t.CompletedAt = clonePtr(t.CompletedAt)
	t.Error = clonePtr(t.Error)
	t.Attention = clonePtr(t.Attention)
	return t
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
