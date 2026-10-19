package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/repository/postgres"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

const (
	maxInFlight = 10
	pollDelay   = 15 * time.Second
)

// readySocialMedia registers n SOCIAL_MEDIA tasks; they start READY_TO_SUBMIT.
func readySocialMedia(t *testing.T, pool *pgxpool.Pool, n int) []uuid.UUID {
	t.Helper()
	uc := newUseCase(t, pool)
	guids := make([]uuid.UUID, 0, n)
	for range n {
		guid := uuid.New()
		if _, _, err := uc.Register(context.Background(), socialMedia(guid)); err != nil {
			t.Fatal(err)
		}
		guids = append(guids, guid)
	}
	return guids
}

// readyRegisterCompare registers one REGISTER_COMPARE task and copies both documents.
func readyRegisterCompare(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	documents := postgres.NewDocumentRepo(pool, queryTimeout)
	guid, claimed := claimTask(t, pool, documents)
	for _, d := range claimed {
		if err := documents.Complete(context.Background(), d.ID, d.ClaimToken, copyResult(d.Position)); err != nil {
			t.Fatal(err)
		}
	}
	return guid
}

func activeAttempts(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM integration.attempts
		WHERE state IN ('SUBMITTING', 'UNKNOWN', 'QUEUED', 'PROCESSING')`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func attemptState(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) string {
	t.Helper()
	var state string
	err := pool.QueryRow(context.Background(), `SELECT state FROM integration.attempts WHERE attempt_id = $1`, attemptID).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func claimSubmission(t *testing.T, repo *postgres.SubmissionRepo) submission.ClaimedSubmission {
	t.Helper()
	claimed, err := repo.Claim(context.Background(), "pod-1", 1, lease, maxInFlight)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	return claimed[0]
}

func TestSubmissionClaim_SocialMedia(t *testing.T) {
	pool := newPool(t)
	guid := readySocialMedia(t, pool, 1)[0]
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)

	s := claimSubmission(t, repo)

	if s.RequestGUID != guid || s.RequestID != camundaRequest || s.Operation != domain.OperationSocialMedia || s.AttemptNo != 1 {
		t.Errorf("unexpected submission %+v", s)
	}
	var input map[string]string
	if err := json.Unmarshal(s.Input, &input); err != nil || input["iin"] != "000000000000" || input["fullName"] != "Тест" {
		t.Errorf("input = %s, err %v", s.Input, err)
	}
	if s.Documents == nil || len(s.Documents) != 0 {
		t.Errorf("documents = %#v, want empty", s.Documents)
	}
	if got := attemptState(t, pool, s.AttemptID); got != "SUBMITTING" {
		t.Errorf("attempt state = %s", got)
	}

	var status string
	var active uuid.UUID
	err := pool.QueryRow(context.Background(), `SELECT status, active_attempt_id FROM integration.queue WHERE request_guid = $1`, guid).
		Scan(&status, &active)
	if err != nil {
		t.Fatal(err)
	}
	if status != "SUBMITTING" || active != s.AttemptID {
		t.Errorf("task: status=%s active_attempt_id=%s", status, active)
	}
	if n := auditCount(t, pool, guid, "SUBMISSION_STARTED"); n != 1 {
		t.Errorf("SUBMISSION_STARTED audit rows = %d", n)
	}
}

func TestSubmissionClaim_RegisterCompareGetsFilesInOrder(t *testing.T) {
	pool := newPool(t)
	guid := readyRegisterCompare(t, pool)

	s := claimSubmission(t, postgres.NewSubmissionRepo(pool, queryTimeout))

	if s.RequestGUID != guid || len(s.Documents) != 2 {
		t.Fatalf("unexpected submission %+v", s)
	}
	for i, d := range s.Documents {
		if d.Position != i || d.S3URI != copyResult(i).S3URI {
			t.Errorf("document %d = %+v", i, d)
		}
	}
}

func TestSubmissionClaim_RespectsLimit(t *testing.T) {
	pool := newPool(t)
	readySocialMedia(t, pool, 5)
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)
	ctx := context.Background()

	claimed, err := repo.Claim(ctx, "pod-1", 10, lease, 3)
	if err != nil || len(claimed) != 3 {
		t.Fatalf("claimed %d with max 3 in flight, err %v", len(claimed), err)
	}
	if more, _ := repo.Claim(ctx, "pod-1", 10, lease, 3); len(more) != 0 {
		t.Fatalf("claimed %d more while full", len(more))
	}

	// Rejecting one frees its place.
	rejected := submission.Report{Outcome: submission.OutcomeRejected, Failure: &domain.Failure{Code: "INVALID_INPUT", Message: "bad"}}
	if err := repo.Report(ctx, claimed[0].AttemptID, claimed[0].ClaimToken, rejected, pollDelay); err != nil {
		t.Fatal(err)
	}
	if more, _ := repo.Claim(ctx, "pod-1", 10, lease, 3); len(more) != 1 {
		t.Errorf("after one rejection claimed %d, want 1", len(more))
	}

	// An accepted task keeps its place: AI is working on it.
	accepted := submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}
	if err := repo.Report(ctx, claimed[1].AttemptID, claimed[1].ClaimToken, accepted, pollDelay); err != nil {
		t.Fatal(err)
	}
	if more, _ := repo.Claim(ctx, "pod-1", 10, lease, 3); len(more) != 0 {
		t.Errorf("claimed %d although accepted tasks still hold their places", len(more))
	}
}

// Like many worker pods asking for work at once, through two API instances.
func TestSubmissionClaim_ConcurrentNeverExceedsLimit(t *testing.T) {
	pool := newPool(t)
	otherPool, err := pgxpool.New(context.Background(), os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherPool.Close)

	readySocialMedia(t, pool, 30)
	repos := []*postgres.SubmissionRepo{
		postgres.NewSubmissionRepo(pool, queryTimeout),
		postgres.NewSubmissionRepo(otherPool, queryTimeout),
	}

	const workers = 30
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed int
		seen    = make(map[uuid.UUID]bool)
		errs    []error
	)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := repos[i%2].Claim(context.Background(), "pod", 1, lease, maxInFlight)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			for _, s := range got {
				claimed++
				if seen[s.RequestGUID] {
					t.Errorf("task %s claimed twice", s.RequestGUID)
				}
				seen[s.RequestGUID] = true
			}
		}()
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if claimed != maxInFlight || activeAttempts(t, pool) != maxInFlight {
		t.Errorf("claimed %d, active %d; want exactly %d", claimed, activeAttempts(t, pool), maxInFlight)
	}
}

func TestSubmissionClaim_SkipsTasksNotReady(t *testing.T) {
	pool := newPool(t)
	registerCompareTasks(t, pool, 1) // still waiting for documents
	guid := readySocialMedia(t, pool, 1)[0]
	_, err := pool.Exec(context.Background(),
		`UPDATE integration.queue SET retry_at = now() + interval '1 hour' WHERE request_guid = $1`, guid)
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := postgres.NewSubmissionRepo(pool, queryTimeout).Claim(context.Background(), "pod-1", 10, lease, maxInFlight)
	if err != nil || len(claimed) != 0 {
		t.Errorf("claimed %d, err %v; want none", len(claimed), err)
	}
}

func TestSubmissionReport_Outcomes(t *testing.T) {
	tests := []struct {
		name         string
		report       submission.Report
		attemptState string
		taskStatus   string
		polled       bool
	}{
		{"accepted queued", submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"},
			"QUEUED", "REMOTE_QUEUED", true},
		{"accepted processing", submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "PROCESSING"},
			"PROCESSING", "REMOTE_RUNNING", true},
		{"unknown", submission.Report{Outcome: submission.OutcomeUnknown, Failure: &domain.Failure{Code: "AI_TIMEOUT", Message: "30s"}},
			"UNKNOWN", "SUBMISSION_UNKNOWN", true},
		{"rejected", submission.Report{Outcome: submission.OutcomeRejected, Failure: &domain.Failure{Code: "INVALID_INPUT", Message: "bad files"}},
			"NOT_ACCEPTED", "FAILED", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newPool(t)
			guid := readySocialMedia(t, pool, 1)[0]
			repo := postgres.NewSubmissionRepo(pool, queryTimeout)
			ctx := context.Background()
			s := claimSubmission(t, repo)

			if err := repo.Report(ctx, s.AttemptID, s.ClaimToken, tt.report, pollDelay); err != nil {
				t.Fatal(err)
			}

			var state string
			var pollIn *float64
			var claimToken *uuid.UUID
			var finished bool
			err := pool.QueryRow(ctx, `
				SELECT state, extract(epoch FROM poll_at - now()), claim_token, finished_at IS NOT NULL
				FROM integration.attempts WHERE attempt_id = $1`, s.AttemptID).Scan(&state, &pollIn, &claimToken, &finished)
			if err != nil {
				t.Fatal(err)
			}
			if state != tt.attemptState || claimToken != nil {
				t.Errorf("attempt: state=%s claim=%v", state, claimToken)
			}
			if tt.polled && (pollIn == nil || *pollIn < 10 || *pollIn > 20 || finished) {
				t.Errorf("attempt must be polled in about %v: poll in %v, finished %v", pollDelay, pollIn, finished)
			}
			if !tt.polled && (pollIn != nil || !finished) {
				t.Errorf("finished attempt: poll in %v, finished %v", pollIn, finished)
			}

			var status string
			var errorCode *string
			var completed bool
			err = pool.QueryRow(ctx, `
				SELECT status, error_code, completed_at IS NOT NULL
				FROM integration.queue WHERE request_guid = $1`, guid).Scan(&status, &errorCode, &completed)
			if err != nil {
				t.Fatal(err)
			}
			if status != tt.taskStatus {
				t.Errorf("task status = %s, want %s", status, tt.taskStatus)
			}
			if !tt.polled && (errorCode == nil || *errorCode != tt.report.Failure.Code || !completed) {
				t.Errorf("failed task: error=%v completed=%v", errorCode, completed)
			}

			// Repeating the same report after a lost response: OK, nothing new.
			if err := repo.Report(ctx, s.AttemptID, s.ClaimToken, tt.report, pollDelay); err != nil {
				t.Errorf("repeated report: err = %v", err)
			}
			var audits int
			err = pool.QueryRow(ctx, `SELECT count(*) FROM integration.audit WHERE attempt_id = $1`, s.AttemptID).Scan(&audits)
			if err != nil || audits != 2 { // started + this outcome
				t.Errorf("audit rows for attempt = %d, err %v; want 2", audits, err)
			}
		})
	}
}

func TestSubmissionReport_WrongCalls(t *testing.T) {
	pool := newPool(t)
	readySocialMedia(t, pool, 1)
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)
	ctx := context.Background()
	s := claimSubmission(t, repo)
	accepted := submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}

	if err := repo.Report(ctx, s.AttemptID, uuid.New(), accepted, pollDelay); !errors.Is(err, submission.ErrClaimLost) {
		t.Errorf("wrong token: err = %v", err)
	}
	if err := repo.Report(ctx, uuid.New(), s.ClaimToken, accepted, pollDelay); !errors.Is(err, submission.ErrAttemptNotFound) {
		t.Errorf("unknown attempt: err = %v", err)
	}

	if err := repo.Report(ctx, s.AttemptID, s.ClaimToken, accepted, pollDelay); err != nil {
		t.Fatal(err)
	}
	// A different report after the first one: too late.
	unknown := submission.Report{Outcome: submission.OutcomeUnknown}
	if err := repo.Report(ctx, s.AttemptID, s.ClaimToken, unknown, pollDelay); !errors.Is(err, submission.ErrClaimLost) {
		t.Errorf("different second report: err = %v", err)
	}
}

func TestSubmissionReport_ExpiredClaim(t *testing.T) {
	pool := newPool(t)
	readySocialMedia(t, pool, 1)
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)
	ctx := context.Background()

	claimed, err := repo.Claim(ctx, "pod-1", 1, 50*time.Millisecond, maxInFlight)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)

	report := submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}
	if err := repo.Report(ctx, claimed[0].AttemptID, claimed[0].ClaimToken, report, pollDelay); !errors.Is(err, submission.ErrClaimLost) {
		t.Errorf("err = %v, want ErrClaimLost", err)
	}
	if _, err := repo.Renew(ctx, claimed[0].AttemptID, claimed[0].ClaimToken, lease); !errors.Is(err, submission.ErrClaimLost) {
		t.Errorf("renew: err = %v, want ErrClaimLost", err)
	}
	// Until recovery exists, the stuck attempt keeps its place.
	if n := activeAttempts(t, pool); n != 1 {
		t.Errorf("active attempts = %d, want 1", n)
	}
}

func TestSubmissionRenew(t *testing.T) {
	pool := newPool(t)
	readySocialMedia(t, pool, 1)
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)
	ctx := context.Background()
	s := claimSubmission(t, repo)

	until, err := repo.Renew(ctx, s.AttemptID, s.ClaimToken, 2*lease)
	if err != nil || !until.After(s.ClaimUntil) {
		t.Fatalf("renew: until=%v (was %v), err %v", until, s.ClaimUntil, err)
	}
	if _, err := repo.Renew(ctx, s.AttemptID, uuid.New(), lease); !errors.Is(err, submission.ErrClaimLost) {
		t.Errorf("wrong token: err = %v", err)
	}
	if _, err := repo.Renew(ctx, uuid.New(), s.ClaimToken, lease); !errors.Is(err, submission.ErrAttemptNotFound) {
		t.Errorf("unknown attempt: err = %v", err)
	}
}

// After AI lost a task (step 5 will do this), the next send gets attempt number 2.
func TestSubmissionClaim_NextAttemptNumber(t *testing.T) {
	pool := newPool(t)
	guid := readySocialMedia(t, pool, 1)[0]
	repo := postgres.NewSubmissionRepo(pool, queryTimeout)
	ctx := context.Background()
	first := claimSubmission(t, repo)

	_, err := pool.Exec(ctx, `
		UPDATE integration.attempts SET state = 'LOST', claim_token = NULL, claim_until = NULL, claim_owner = NULL
		WHERE attempt_id = $1;`, first.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `UPDATE integration.queue SET status = 'READY_TO_SUBMIT' WHERE request_guid = $1`, guid)
	if err != nil {
		t.Fatal(err)
	}

	second := claimSubmission(t, repo)
	if second.AttemptNo != 2 || second.AttemptID == first.AttemptID {
		t.Errorf("second attempt = %+v", second)
	}
}
