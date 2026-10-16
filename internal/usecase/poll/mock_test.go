package poll

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMockService(t *testing.T) {
	ctx := context.Background()
	m := NewMockService(30 * time.Second)

	first, _ := m.Claim(ctx, "pod-1", 10)
	if len(first) != 2 {
		t.Fatalf("claimed %d checks, want both demo checks", len(first))
	}

	// Claiming again replaces the first check's token.
	if again, _ := m.Claim(ctx, "pod-1", 1); len(again) != 1 {
		t.Fatalf("limit 1 returned %d", len(again))
	}
	p := first[0]
	if err := m.Observe(ctx, p.AttemptID, p.ClaimToken, Observation{Outcome: OutcomeQueued}); !errors.Is(err, ErrClaimLost) {
		t.Errorf("old token: err = %v, want ErrClaimLost", err)
	}

	other := first[1]
	if err := m.Observe(ctx, other.AttemptID, other.ClaimToken, Observation{Outcome: OutcomeProcessing}); err != nil {
		t.Errorf("current token: err = %v", err)
	}
	if err := m.Observe(ctx, uuid.New(), other.ClaimToken, Observation{}); !errors.Is(err, ErrAttemptNotFound) {
		t.Errorf("unknown attempt: err = %v", err)
	}
}
