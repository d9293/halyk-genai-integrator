package submission

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

//go:generate go run github.com/matryer/moq@v0.8.1 -out submission_repo_mock_test.go . submissionRepo

type submissionRepo interface {
	// Claim starts up to limit new attempts for the oldest ready tasks, as long
	// as fewer than maxInFlight attempts are active, each owned for lease.
	Claim(ctx context.Context, workerID string, limit int, lease time.Duration, maxInFlight int) ([]ClaimedSubmission, error)
	// Report applies the outcome of a POST to AI. ACCEPTED and UNKNOWN schedule
	// the first status check after pollDelay. Repeating a report is a no-op.
	Report(ctx context.Context, attemptID, claimToken uuid.UUID, report Report, pollDelay time.Duration) error
	// Renew extends a live claim to now + lease.
	Renew(ctx context.Context, attemptID, claimToken uuid.UUID, lease time.Duration) (time.Time, error)
}

type Config struct {
	// Lease is how long a worker owns a POST to AI in progress without renewing.
	Lease time.Duration
	// MaxInFlight is how many tasks may be at AI at once.
	MaxInFlight int
	// PollDelay is when the first status check happens after a task was sent.
	PollDelay time.Duration
}

type UseCase struct {
	repo submissionRepo
	cfg  Config
}

func NewUseCase(repo submissionRepo, cfg Config) *UseCase {
	return &UseCase{repo: repo, cfg: cfg}
}

func (uc *UseCase) Claim(ctx context.Context, workerID string, limit int) ([]ClaimedSubmission, error) {
	claimed, err := uc.repo.Claim(ctx, workerID, limit, uc.cfg.Lease, uc.cfg.MaxInFlight)
	if err != nil {
		return nil, fmt.Errorf("claim submissions: %w", err)
	}
	return claimed, nil
}

func (uc *UseCase) Report(ctx context.Context, attemptID, claimToken uuid.UUID, report Report) error {
	if err := uc.repo.Report(ctx, attemptID, claimToken, report, uc.cfg.PollDelay); err != nil {
		return fmt.Errorf("report submission: %w", err)
	}
	return nil
}

func (uc *UseCase) Renew(ctx context.Context, attemptID, claimToken uuid.UUID) (time.Time, error) {
	claimUntil, err := uc.repo.Renew(ctx, attemptID, claimToken, uc.cfg.Lease)
	if err != nil {
		return time.Time{}, fmt.Errorf("renew submission claim: %w", err)
	}
	return claimUntil, nil
}
