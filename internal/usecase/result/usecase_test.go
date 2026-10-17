package result

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

var testConfig = Config{Topic: "results-topic", Lease: 30 * time.Second, RetryBase: 5 * time.Second, RetryMax: 5 * time.Minute}

func TestClaim_AddsTopicKeyAndEnvelope(t *testing.T) {
	requestID := uuid.New()
	repo := &resultRepoMock{
		ClaimFunc: func(context.Context, string, int, time.Duration) ([]ClaimedResult, error) {
			return []ClaimedResult{{Message: Message{RequestID: requestID, Status: domain.StatusSucceeded}}}, nil
		},
	}

	got, err := NewUseCase(repo, testConfig).Claim(context.Background(), "pod-1", 5)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v, err %v", got, err)
	}
	c := got[0]
	if c.Topic != "results-topic" || c.MessageKey != requestID.String() ||
		c.Message.SchemaVersion != 1 || c.Message.EventType != "TASK_COMPLETED" {
		t.Errorf("claimed = %+v", c)
	}
	if call := repo.ClaimCalls()[0]; call.Lease != 30*time.Second || call.Limit != 5 {
		t.Errorf("claim call = %+v", call)
	}
}

func TestFailed_PassesRetryDelays(t *testing.T) {
	repo := &resultRepoMock{
		FailedFunc: func(context.Context, uuid.UUID, uuid.UUID, domain.Failure, time.Duration, time.Duration) error {
			return nil
		},
	}
	if err := NewUseCase(repo, testConfig).Failed(context.Background(), uuid.New(), uuid.New(), domain.Failure{Code: "X"}); err != nil {
		t.Fatal(err)
	}
	if call := repo.FailedCalls()[0]; call.RetryBase != 5*time.Second || call.RetryMax != 5*time.Minute {
		t.Errorf("failed call = %+v", call)
	}
}

func TestUseCase_KeepsRepoErrors(t *testing.T) {
	repo := &resultRepoMock{
		PublishedFunc: func(context.Context, uuid.UUID, uuid.UUID) error { return ErrClaimLost },
	}
	err := NewUseCase(repo, testConfig).Published(context.Background(), uuid.New(), uuid.New())
	if !errors.Is(err, ErrClaimLost) {
		t.Errorf("err = %v", err)
	}
}
