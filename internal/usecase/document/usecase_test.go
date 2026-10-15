package document

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testPolicy = RetryPolicy{MaxAttempts: 5, BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Minute}

func TestRetryPolicy(t *testing.T) {
	tests := []struct {
		failures  int
		delay     time.Duration
		exhausted bool
	}{
		{1, 5 * time.Second, false},
		{2, 10 * time.Second, false},
		{3, 20 * time.Second, false},
		{4, 40 * time.Second, false},
		{5, 80 * time.Second, true},
	}
	for _, tt := range tests {
		if got := testPolicy.Delay(tt.failures); got != tt.delay {
			t.Errorf("Delay(%d) = %v, want %v", tt.failures, got, tt.delay)
		}
		if got := testPolicy.Exhausted(tt.failures); got != tt.exhausted {
			t.Errorf("Exhausted(%d) = %v, want %v", tt.failures, got, tt.exhausted)
		}
	}

	capped := RetryPolicy{MaxAttempts: 100, BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Minute}
	if got := capped.Delay(50); got != 5*time.Minute {
		t.Errorf("Delay(50) = %v, want the 5m cap", got)
	}
}

func TestUseCase_PassesConfigToRepo(t *testing.T) {
	ctx := context.Background()
	id, token := uuid.New(), uuid.New()
	claimUntil := time.Now()
	repo := &documentRepoMock{
		ClaimFunc: func(context.Context, string, int, time.Duration) ([]ClaimedDocument, error) {
			return []ClaimedDocument{{ID: id}}, nil
		},
		CompleteFunc: func(context.Context, uuid.UUID, uuid.UUID, CopyResult) error { return nil },
		FailFunc:     func(context.Context, uuid.UUID, uuid.UUID, CopyFailure, RetryPolicy) error { return nil },
		RenewFunc: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (time.Time, error) {
			return claimUntil, nil
		},
	}
	uc := NewUseCase(repo, time.Minute, testPolicy)

	if got, err := uc.Claim(ctx, "pod-1", 3); err != nil || len(got) != 1 {
		t.Fatalf("claim: %v, err %v", got, err)
	}
	if call := repo.ClaimCalls()[0]; call.WorkerID != "pod-1" || call.Limit != 3 || call.Lease != time.Minute {
		t.Errorf("claim call = %+v", call)
	}

	result := CopyResult{S3URI: "s3://b/k", SizeBytes: 1, ChecksumSHA256: "c"}
	if err := uc.Complete(ctx, id, token, result); err != nil {
		t.Fatal(err)
	}
	if call := repo.CompleteCalls()[0]; call.ID != id || call.ClaimToken != token || call.Result != result {
		t.Errorf("complete call = %+v", call)
	}

	failure := CopyFailure{Code: "X", Message: "y", Retryable: true}
	if err := uc.Fail(ctx, id, token, failure); err != nil {
		t.Fatal(err)
	}
	if call := repo.FailCalls()[0]; call.Failure != failure || call.Policy != testPolicy {
		t.Errorf("fail call = %+v", call)
	}

	if got, err := uc.Renew(ctx, id, token); err != nil || !got.Equal(claimUntil) {
		t.Fatalf("renew: %v, err %v", got, err)
	}
	if call := repo.RenewCalls()[0]; call.Lease != time.Minute {
		t.Errorf("renew call = %+v", call)
	}
}

func TestUseCase_KeepsRepoErrors(t *testing.T) {
	repo := &documentRepoMock{
		CompleteFunc: func(context.Context, uuid.UUID, uuid.UUID, CopyResult) error { return ErrClaimLost },
		RenewFunc: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (time.Time, error) {
			return time.Time{}, ErrDocumentNotFound
		},
	}
	uc := NewUseCase(repo, time.Minute, testPolicy)

	if err := uc.Complete(context.Background(), uuid.New(), uuid.New(), CopyResult{}); !errors.Is(err, ErrClaimLost) {
		t.Errorf("complete: err = %v", err)
	}
	if _, err := uc.Renew(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrDocumentNotFound) {
		t.Errorf("renew: err = %v", err)
	}
}
