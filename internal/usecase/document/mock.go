package document

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MockService stands in for the real document usecase until the database
// logic exists. It always offers the same two demo documents and only checks
// that the claim token is the latest one it issued. State is kept in memory
// and lost on restart; it is not a work queue.
type MockService struct {
	lease time.Duration

	mu     sync.Mutex
	tokens map[uuid.UUID]uuid.UUID // document ID -> latest claim token
}

var mockDocuments = []ClaimedDocument{
	{
		ID:          uuid.MustParse("d0c00000-0000-4000-8000-000000000001"),
		RequestGUID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		DocumentID:  uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		AttstoreURL: "https://attstore.example/documents/1",
		Position:    0,
	},
	{
		ID:          uuid.MustParse("d0c00000-0000-4000-8000-000000000002"),
		RequestGUID: uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
		DocumentID:  uuid.MustParse("33333333-3333-4333-8333-333333333333"),
		AttstoreURL: "https://attstore.example/documents/2",
		Position:    1,
	},
}

func NewMockService(lease time.Duration) *MockService {
	return &MockService{lease: lease, tokens: make(map[uuid.UUID]uuid.UUID)}
}

// Claim returns up to limit demo documents, each with a new claim token.
func (m *MockService) Claim(_ context.Context, _ string, limit int) ([]ClaimedDocument, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	claimed := make([]ClaimedDocument, 0, limit)
	for _, d := range mockDocuments[:min(limit, len(mockDocuments))] {
		d.ClaimToken = uuid.New()
		d.ClaimUntil = time.Now().Add(m.lease).UTC()
		m.tokens[d.ID] = d.ClaimToken
		claimed = append(claimed, d)
	}
	return claimed, nil
}

func (m *MockService) Complete(_ context.Context, id, claimToken uuid.UUID, _ CopyResult) error {
	return m.checkToken(id, claimToken)
}

func (m *MockService) Fail(_ context.Context, id, claimToken uuid.UUID, _ CopyFailure) error {
	return m.checkToken(id, claimToken)
}

func (m *MockService) Renew(_ context.Context, id, claimToken uuid.UUID) (time.Time, error) {
	if err := m.checkToken(id, claimToken); err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(m.lease).UTC(), nil
}

func (m *MockService) checkToken(id, claimToken uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !isMockDocument(id) {
		return ErrDocumentNotFound
	}
	if m.tokens[id] != claimToken {
		return ErrClaimLost
	}
	return nil
}

func isMockDocument(id uuid.UUID) bool {
	for _, d := range mockDocuments {
		if d.ID == id {
			return true
		}
	}
	return false
}
