package result

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

//go:generate go run github.com/matryer/moq@v0.5.3 -out result_repo_mock_test.go . resultRepo

type resultRepo interface {
	// Claim gives workerID up to limit results to publish, each owned for
	// lease, with the message fields read from the task.
	Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]ClaimedResult, error)
	// Published records the broker ACK; repeating it is a no-op.
	Published(ctx context.Context, requestGUID, claimToken uuid.UUID) error
	// Failed schedules the next try after retryBase · 2^(earlier failures), at most retryMax.
	Failed(ctx context.Context, requestGUID, claimToken uuid.UUID, failure domain.Failure, retryBase, retryMax time.Duration) error
}

type Config struct {
	// Topic is where final task results are published for BPM.
	Topic string
	// Lease is how long a worker owns one result while publishing it.
	Lease     time.Duration
	RetryBase time.Duration
	RetryMax  time.Duration
}

type UseCase struct {
	repo resultRepo
	cfg  Config
}

func NewUseCase(repo resultRepo, cfg Config) *UseCase {
	return &UseCase{repo: repo, cfg: cfg}
}

func (uc *UseCase) Claim(ctx context.Context, workerID string, limit int) ([]ClaimedResult, error) {
	claimed, err := uc.repo.Claim(ctx, workerID, limit, uc.cfg.Lease)
	if err != nil {
		return nil, fmt.Errorf("claim results: %w", err)
	}
	for i := range claimed {
		c := &claimed[i]
		c.Topic = uc.cfg.Topic
		c.MessageKey = c.Message.RequestID.String()
		c.Message.SchemaVersion = MessageSchemaVersion
		c.Message.EventType = EventTaskCompleted
	}
	return claimed, nil
}

func (uc *UseCase) Published(ctx context.Context, requestGUID, claimToken uuid.UUID) error {
	if err := uc.repo.Published(ctx, requestGUID, claimToken); err != nil {
		return fmt.Errorf("mark result published: %w", err)
	}
	return nil
}

func (uc *UseCase) Failed(ctx context.Context, requestGUID, claimToken uuid.UUID, failure domain.Failure) error {
	if err := uc.repo.Failed(ctx, requestGUID, claimToken, failure, uc.cfg.RetryBase, uc.cfg.RetryMax); err != nil {
		return fmt.Errorf("record result failure: %w", err)
	}
	return nil
}
