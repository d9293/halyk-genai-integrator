package poll

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

// MockService stands in for the real poll usecase until the database logic
// exists. It always offers the same two demo status checks and only checks
// that the claim token is the latest one it issued. It changes no task and
// frees no AI place; state is lost on restart.
type MockService struct {
	lease time.Duration

	mu     sync.Mutex
	tokens map[uuid.UUID]uuid.UUID // attempt ID -> latest claim token
}

var mockPolls = []ClaimedPoll{
	{
		AttemptID:   uuid.MustParse("a77e0000-0000-4000-8000-000000000001"),
		AttemptNo:   1,
		RequestGUID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		Operation:   domain.OperationSocialMedia,
	},
	{
		AttemptID:   uuid.MustParse("a77e0000-0000-4000-8000-000000000002"),
		AttemptNo:   1,
		RequestGUID: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
		RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		Operation:   domain.OperationRegisterCompare,
	},
}

func NewMockService(lease time.Duration) *MockService {
	return &MockService{lease: lease, tokens: make(map[uuid.UUID]uuid.UUID)}
}

// Claim returns up to limit demo status checks, each with a new claim token.
func (m *MockService) Claim(_ context.Context, _ string, limit int) ([]ClaimedPoll, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	claimed := make([]ClaimedPoll, 0, limit)
	for _, p := range mockPolls[:min(limit, len(mockPolls))] {
		p.ClaimToken = uuid.New()
		p.ClaimUntil = time.Now().Add(m.lease).UTC()
		m.tokens[p.AttemptID] = p.ClaimToken
		claimed = append(claimed, p)
	}
	return claimed, nil
}

func (m *MockService) Observe(_ context.Context, attemptID, claimToken uuid.UUID, _ Observation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	token, ok := m.tokens[attemptID]
	switch {
	case !isMockPoll(attemptID):
		return ErrAttemptNotFound
	case !ok || token != claimToken:
		return ErrClaimLost
	}
	return nil
}

func isMockPoll(attemptID uuid.UUID) bool {
	for _, p := range mockPolls {
		if p.AttemptID == attemptID {
			return true
		}
	}
	return false
}
