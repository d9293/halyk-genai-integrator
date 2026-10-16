package poll

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

//go:generate moq -out poll_repo_mock_test.go . pollRepo

type pollRepo interface {
	// Claim recovers abandoned submissions, then gives workerID up to limit
	// due status checks, each owned for lease.
	Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]ClaimedPoll, error)
	// Observe applies AI's answer to one status check according to rules.
	Observe(ctx context.Context, attemptID, claimToken uuid.UUID, observation Observation, rules Rules) error
}

type Config struct {
	// Lease is how long a worker owns one status check.
	Lease time.Duration
	// Interval ± Jitter is when to check again after QUEUED/PROCESSING.
	Interval time.Duration
	Jitter   time.Duration
	// Rules except NextPoll, which is computed per observation.
	Rules Rules
}

type UseCase struct {
	repo pollRepo
	cfg  Config
}

func NewUseCase(repo pollRepo, cfg Config) *UseCase {
	return &UseCase{repo: repo, cfg: cfg}
}

func (uc *UseCase) Claim(ctx context.Context, workerID string, limit int) ([]ClaimedPoll, error) {
	claimed, err := uc.repo.Claim(ctx, workerID, limit, uc.cfg.Lease)
	if err != nil {
		return nil, fmt.Errorf("claim polls: %w", err)
	}
	return claimed, nil
}

func (uc *UseCase) Observe(ctx context.Context, attemptID, claimToken uuid.UUID, observation Observation) error {
	rules := uc.cfg.Rules
	rules.NextPoll = nextPoll(uc.cfg.Interval, uc.cfg.Jitter)
	if err := uc.repo.Observe(ctx, attemptID, claimToken, observation, rules); err != nil {
		return fmt.Errorf("observe: %w", err)
	}
	return nil
}

// nextPoll spreads status checks out (interval ± jitter), so many tasks sent
// at once are not all checked in the same second.
func nextPoll(interval, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return interval
	}
	return interval - jitter + rand.N(2*jitter+1)
}
