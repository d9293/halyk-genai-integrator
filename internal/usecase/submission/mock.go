package submission

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

// MockService stands in for the real submission usecase until the database
// logic exists. It always offers the same two demo submissions and only checks
// that the claim token is the latest one it issued. It does not count AI
// places and keeps no queue; state is lost on restart.
type MockService struct {
	lease time.Duration

	mu     sync.Mutex
	tokens map[uuid.UUID]uuid.UUID // attempt ID -> latest claim token
}

var mockSubmissions = []ClaimedSubmission{
	{
		AttemptID:   uuid.MustParse("a77e0000-0000-4000-8000-000000000001"),
		AttemptNo:   1,
		RequestGUID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		Operation:   domain.OperationSocialMedia,
		Input:       json.RawMessage(`{"iin":"000000000000","fullName":"Тестовый клиент"}`),
		Documents:   []SubmissionDocument{},
	},
	{
		AttemptID:   uuid.MustParse("a77e0000-0000-4000-8000-000000000002"),
		AttemptNo:   1,
		RequestGUID: uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
		RequestID:   uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		Operation:   domain.OperationRegisterCompare,
		Input:       json.RawMessage(`{"adjudicate":true}`),
		Documents: []SubmissionDocument{
			{Position: 0, S3URI: "s3://ai-input/tasks/bbbbbbbb/0"},
			{Position: 1, S3URI: "s3://ai-input/tasks/bbbbbbbb/1"},
		},
	},
}

func NewMockService(lease time.Duration) *MockService {
	return &MockService{lease: lease, tokens: make(map[uuid.UUID]uuid.UUID)}
}

// Claim returns up to limit demo submissions, each with a new claim token.
func (m *MockService) Claim(_ context.Context, _ string, limit int) ([]ClaimedSubmission, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	claimed := make([]ClaimedSubmission, 0, limit)
	for _, s := range mockSubmissions[:min(limit, len(mockSubmissions))] {
		s.ClaimToken = uuid.New()
		s.ClaimUntil = time.Now().Add(m.lease).UTC()
		m.tokens[s.AttemptID] = s.ClaimToken
		claimed = append(claimed, s)
	}
	return claimed, nil
}

func (m *MockService) Report(_ context.Context, attemptID, claimToken uuid.UUID, _ Report) error {
	return m.checkToken(attemptID, claimToken)
}

func (m *MockService) Renew(_ context.Context, attemptID, claimToken uuid.UUID) (time.Time, error) {
	if err := m.checkToken(attemptID, claimToken); err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(m.lease).UTC(), nil
}

func (m *MockService) checkToken(attemptID, claimToken uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	token, ok := m.tokens[attemptID]
	switch {
	case !isMockSubmission(attemptID):
		return ErrAttemptNotFound
	case !ok || token != claimToken:
		return ErrClaimLost
	}
	return nil
}

func isMockSubmission(attemptID uuid.UUID) bool {
	for _, s := range mockSubmissions {
		if s.AttemptID == attemptID {
			return true
		}
	}
	return false
}
