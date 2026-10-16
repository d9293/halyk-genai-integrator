package poll

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testConfig = Config{
	Lease:    30 * time.Second,
	Interval: 15 * time.Second,
	Jitter:   5 * time.Second,
	Rules: Rules{
		NotFoundDelay: 30 * time.Second, ErrorDelay: time.Minute,
		LostThreshold: 3, LostMinWindow: time.Minute, MaxResubmissions: 3, ResultMaxBytes: 512 << 10,
	},
}

func TestNextPoll_StaysWithinJitter(t *testing.T) {
	for range 1000 {
		got := nextPoll(15*time.Second, 5*time.Second)
		if got < 10*time.Second || got > 20*time.Second {
			t.Fatalf("nextPoll = %v, want 10s..20s", got)
		}
	}
	if got := nextPoll(15*time.Second, 0); got != 15*time.Second {
		t.Errorf("without jitter: %v", got)
	}
}

func TestUseCase_PassesConfigToRepo(t *testing.T) {
	ctx := context.Background()
	repo := &pollRepoMock{
		ClaimFunc: func(context.Context, string, int, time.Duration) ([]ClaimedPoll, error) {
			return []ClaimedPoll{{AttemptID: uuid.New()}}, nil
		},
		ObserveFunc: func(context.Context, uuid.UUID, uuid.UUID, Observation, Rules) error { return nil },
	}
	uc := NewUseCase(repo, testConfig)

	if got, err := uc.Claim(ctx, "pod-1", 5); err != nil || len(got) != 1 {
		t.Fatalf("claim: %v, err %v", got, err)
	}
	if call := repo.ClaimCalls()[0]; call.Lease != 30*time.Second || call.Limit != 5 {
		t.Errorf("claim call = %+v", call)
	}

	if err := uc.Observe(ctx, uuid.New(), uuid.New(), Observation{Outcome: OutcomeQueued}); err != nil {
		t.Fatal(err)
	}
	rules := repo.ObserveCalls()[0].Rules
	if rules.NextPoll < 10*time.Second || rules.NextPoll > 20*time.Second ||
		rules.LostThreshold != 3 || rules.MaxResubmissions != 3 || rules.ErrorDelay != time.Minute {
		t.Errorf("rules = %+v", rules)
	}
}

func TestUseCase_KeepsRepoErrors(t *testing.T) {
	repo := &pollRepoMock{
		ObserveFunc: func(context.Context, uuid.UUID, uuid.UUID, Observation, Rules) error { return ErrClaimLost },
	}
	err := NewUseCase(repo, testConfig).Observe(context.Background(), uuid.New(), uuid.New(), Observation{})
	if !errors.Is(err, ErrClaimLost) {
		t.Errorf("err = %v", err)
	}
}
