package submission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testConfig = Config{Lease: time.Minute, MaxInFlight: 10, PollDelay: 15 * time.Second}

func TestUseCase_PassesConfigToRepo(t *testing.T) {
	ctx := context.Background()
	id, token := uuid.New(), uuid.New()
	repo := &submissionRepoMock{
		ClaimFunc: func(context.Context, string, int, time.Duration, int) ([]ClaimedSubmission, error) {
			return []ClaimedSubmission{{AttemptID: id}}, nil
		},
		ReportFunc: func(context.Context, uuid.UUID, uuid.UUID, Report, time.Duration) error { return nil },
		RenewFunc: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) (time.Time, error) {
			return time.Now(), nil
		},
	}
	uc := NewUseCase(repo, testConfig)

	if got, err := uc.Claim(ctx, "pod-1", 1); err != nil || len(got) != 1 {
		t.Fatalf("claim: %v, err %v", got, err)
	}
	if call := repo.ClaimCalls()[0]; call.Lease != time.Minute || call.MaxInFlight != 10 || call.Limit != 1 {
		t.Errorf("claim call = %+v", call)
	}

	report := Report{Outcome: OutcomeAccepted, RemoteStatus: "QUEUED"}
	if err := uc.Report(ctx, id, token, report); err != nil {
		t.Fatal(err)
	}
	if call := repo.ReportCalls()[0]; call.PollDelay != 15*time.Second || call.Report.Outcome != OutcomeAccepted {
		t.Errorf("report call = %+v", call)
	}

	if _, err := uc.Renew(ctx, id, token); err != nil {
		t.Fatal(err)
	}
	if call := repo.RenewCalls()[0]; call.Lease != time.Minute {
		t.Errorf("renew call = %+v", call)
	}
}

func TestUseCase_KeepsRepoErrors(t *testing.T) {
	repo := &submissionRepoMock{
		ReportFunc: func(context.Context, uuid.UUID, uuid.UUID, Report, time.Duration) error { return ErrClaimLost },
	}
	err := NewUseCase(repo, testConfig).Report(context.Background(), uuid.New(), uuid.New(), Report{})
	if !errors.Is(err, ErrClaimLost) {
		t.Errorf("err = %v", err)
	}
}
