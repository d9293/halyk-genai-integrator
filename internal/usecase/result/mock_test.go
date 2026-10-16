package result

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

func TestMockService(t *testing.T) {
	ctx := context.Background()
	m := NewMockService(30*time.Second, "test-topic")

	first, _ := m.Claim(ctx, "pod-1", 10)
	if len(first) != 2 {
		t.Fatalf("claimed %d results, want both demo results", len(first))
	}
	r := first[0]
	if r.Topic != "test-topic" || r.MessageKey != r.Message.RequestID.String() || r.Message.RequestGUID != r.RequestGUID {
		t.Errorf("unexpected result %+v", r)
	}

	// Claiming again replaces the first result's token.
	if again, _ := m.Claim(ctx, "pod-1", 1); len(again) != 1 {
		t.Fatalf("limit 1 returned %d", len(again))
	}
	if err := m.Published(ctx, r.RequestGUID, r.ClaimToken); !errors.Is(err, ErrClaimLost) {
		t.Errorf("old token: err = %v, want ErrClaimLost", err)
	}

	other := first[1]
	if err := m.Failed(ctx, other.RequestGUID, other.ClaimToken, domain.Failure{Code: "X", Message: "y"}); err != nil {
		t.Errorf("current token: err = %v", err)
	}
	if err := m.Published(ctx, uuid.New(), other.ClaimToken); !errors.Is(err, ErrResultNotFound) {
		t.Errorf("unknown result: err = %v", err)
	}
}
