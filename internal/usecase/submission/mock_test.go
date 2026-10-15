package submission

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMockService(t *testing.T) {
	ctx := context.Background()
	m := NewMockService(time.Minute)

	first, _ := m.Claim(ctx, "pod-1", 10)
	if len(first) != 2 {
		t.Fatalf("claimed %d submissions, want both demo submissions", len(first))
	}
	if first[0].Documents == nil || len(first[0].Documents) != 0 || len(first[1].Documents) != 2 {
		t.Errorf("SOCIAL_MEDIA has no files, REGISTER_COMPARE has two: %+v", first)
	}

	// Claiming again replaces the first submission's token.
	if again, _ := m.Claim(ctx, "pod-1", 1); len(again) != 1 {
		t.Fatalf("limit 1 returned %d", len(again))
	}
	s := first[0]
	if err := m.Report(ctx, s.AttemptID, s.ClaimToken, Report{Outcome: OutcomeAccepted}); !errors.Is(err, ErrClaimLost) {
		t.Errorf("old token: err = %v, want ErrClaimLost", err)
	}

	other := first[1]
	if err := m.Report(ctx, other.AttemptID, other.ClaimToken, Report{Outcome: OutcomeUnknown}); err != nil {
		t.Errorf("current token: err = %v", err)
	}
	if until, err := m.Renew(ctx, other.AttemptID, other.ClaimToken); err != nil || until.Before(time.Now()) {
		t.Errorf("renew: until=%v err=%v", until, err)
	}
	if err := m.Report(ctx, uuid.New(), other.ClaimToken, Report{}); !errors.Is(err, ErrAttemptNotFound) {
		t.Errorf("unknown attempt: err = %v", err)
	}
}
