package result

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
)

// MockService stands in for the real result usecase until the database logic
// exists. It always offers the same two demo results and only checks that the
// claim token is the latest one it issued. Nothing is ever marked published;
// state is lost on restart.
type MockService struct {
	lease time.Duration
	topic string

	mu     sync.Mutex
	tokens map[uuid.UUID]uuid.UUID // requestGuid -> latest claim token
}

var mockMessages = []Message{
	{
		SchemaVersion: MessageSchemaVersion,
		EventType:     EventTaskCompleted,
		OccurredAt:    time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC),
		RequestID:     uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		RequestGUID:   uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		Operation:     domain.OperationRegisterCompare,
		Status:        domain.StatusSucceeded,
		Result:        json.RawMessage(`{"summary":{"counts":{"both":12,"review":1}},"report_url":"https://s3.example/reports/r.docx"}`),
	},
	{
		SchemaVersion: MessageSchemaVersion,
		EventType:     EventTaskCompleted,
		OccurredAt:    time.Date(2026, 10, 16, 10, 5, 0, 0, time.UTC),
		RequestID:     uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		RequestGUID:   uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
		Operation:     domain.OperationSocialMedia,
		Status:        domain.StatusFailed,
		Error:         &domain.Failure{Code: "AI_TASK_LOST_RETRIES_EXHAUSTED", Message: "AI lost the task 4 times"},
	},
}

func NewMockService(lease time.Duration, topic string) *MockService {
	return &MockService{lease: lease, topic: topic, tokens: make(map[uuid.UUID]uuid.UUID)}
}

// Claim returns up to limit demo results, each with a new claim token.
func (m *MockService) Claim(_ context.Context, _ string, limit int) ([]ClaimedResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	claimed := make([]ClaimedResult, 0, limit)
	for _, msg := range mockMessages[:min(limit, len(mockMessages))] {
		r := ClaimedResult{
			RequestGUID: msg.RequestGUID,
			Topic:       m.topic,
			MessageKey:  msg.RequestID.String(),
			Message:     msg,
			ClaimToken:  uuid.New(),
			ClaimUntil:  time.Now().Add(m.lease).UTC(),
		}
		m.tokens[r.RequestGUID] = r.ClaimToken
		claimed = append(claimed, r)
	}
	return claimed, nil
}

func (m *MockService) Published(_ context.Context, requestGUID, claimToken uuid.UUID) error {
	return m.checkToken(requestGUID, claimToken)
}

func (m *MockService) Failed(_ context.Context, requestGUID, claimToken uuid.UUID, _ domain.Failure) error {
	return m.checkToken(requestGUID, claimToken)
}

func (m *MockService) checkToken(requestGUID, claimToken uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	token, ok := m.tokens[requestGUID]
	switch {
	case !isMockResult(requestGUID):
		return ErrResultNotFound
	case !ok || token != claimToken:
		return ErrClaimLost
	}
	return nil
}

func isMockResult(requestGUID uuid.UUID) bool {
	for _, msg := range mockMessages {
		if msg.RequestGUID == requestGUID {
			return true
		}
	}
	return false
}
