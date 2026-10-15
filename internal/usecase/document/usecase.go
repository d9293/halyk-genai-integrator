package document

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

//go:generate moq -out document_repo_mock_test.go . documentRepo

type documentRepo interface {
	// Claim gives workerID up to limit documents to copy, each owned for lease.
	// Tasks whose first document is claimed move to PREPARING_DOCUMENTS.
	Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]ClaimedDocument, error)
	// Complete marks the document READY. The task's last READY document moves
	// the task to READY_TO_SUBMIT. Repeating it with the same result is a no-op.
	Complete(ctx context.Context, id, claimToken uuid.UUID, result CopyResult) error
	// Fail schedules a retry, or puts the document and its task into
	// NEEDS_ATTENTION, as policy decides.
	Fail(ctx context.Context, id, claimToken uuid.UUID, failure CopyFailure, policy RetryPolicy) error
	// Renew extends a live claim to now + lease and returns the new expiry.
	Renew(ctx context.Context, id, claimToken uuid.UUID, lease time.Duration) (time.Time, error)
}

// RetryPolicy decides what happens after a failed copy.
type RetryPolicy struct {
	// MaxAttempts is how many failed copies a document may have before it needs attention.
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// Exhausted reports whether a document with this many failures gets no more retries.
func (p RetryPolicy) Exhausted(failures int) bool {
	return failures >= p.MaxAttempts
}

// Delay is the wait before the next copy after the given number of failures:
// BaseDelay, then doubling (5s, 10s, 20s, ...), never more than MaxDelay.
func (p RetryPolicy) Delay(failures int) time.Duration {
	delay := p.BaseDelay
	for range failures - 1 {
		delay *= 2
		if delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	return min(delay, p.MaxDelay)
}

type UseCase struct {
	repo documentRepo
	// lease is how long a worker owns a claimed document without renewing.
	lease  time.Duration
	policy RetryPolicy
}

func NewUseCase(repo documentRepo, lease time.Duration, policy RetryPolicy) *UseCase {
	return &UseCase{repo: repo, lease: lease, policy: policy}
}

func (uc *UseCase) Claim(ctx context.Context, workerID string, limit int) ([]ClaimedDocument, error) {
	claimed, err := uc.repo.Claim(ctx, workerID, limit, uc.lease)
	if err != nil {
		return nil, fmt.Errorf("claim documents: %w", err)
	}
	return claimed, nil
}

func (uc *UseCase) Complete(ctx context.Context, id, claimToken uuid.UUID, result CopyResult) error {
	if err := uc.repo.Complete(ctx, id, claimToken, result); err != nil {
		return fmt.Errorf("complete document: %w", err)
	}
	return nil
}

func (uc *UseCase) Fail(ctx context.Context, id, claimToken uuid.UUID, failure CopyFailure) error {
	if err := uc.repo.Fail(ctx, id, claimToken, failure, uc.policy); err != nil {
		return fmt.Errorf("fail document: %w", err)
	}
	return nil
}

func (uc *UseCase) Renew(ctx context.Context, id, claimToken uuid.UUID) (time.Time, error) {
	claimUntil, err := uc.repo.Renew(ctx, id, claimToken, uc.lease)
	if err != nil {
		return time.Time{}, fmt.Errorf("renew document claim: %w", err)
	}
	return claimUntil, nil
}
