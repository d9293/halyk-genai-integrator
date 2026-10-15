package document

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
		t.Fatalf("claimed %d documents, want both demo documents", len(first))
	}
	if one, _ := m.Claim(ctx, "pod-1", 1); len(one) != 1 {
		t.Fatalf("limit 1 returned %d documents", len(one))
	}

	// The second claim replaced the first document's token.
	doc := first[0]
	if err := m.Complete(ctx, doc.ID, doc.ClaimToken, CopyResult{}); !errors.Is(err, ErrClaimLost) {
		t.Errorf("old token: err = %v, want ErrClaimLost", err)
	}

	// The second document still has its token from the first claim.
	other := first[1]
	if err := m.Fail(ctx, other.ID, other.ClaimToken, CopyFailure{}); err != nil {
		t.Errorf("current token: err = %v", err)
	}
	if until, err := m.Renew(ctx, other.ID, other.ClaimToken); err != nil || until.Before(time.Now()) {
		t.Errorf("renew: until=%v err=%v", until, err)
	}

	if err := m.Complete(ctx, uuid.New(), other.ClaimToken, CopyResult{}); !errors.Is(err, ErrDocumentNotFound) {
		t.Errorf("unknown document: err = %v", err)
	}
}
