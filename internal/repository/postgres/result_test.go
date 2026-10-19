package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/repository/postgres"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/document"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/result"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

const (
	resultLease = 30 * time.Second
	retryBase   = 5 * time.Second
	retryMax    = 5 * time.Minute
)

// succeededTask takes a SOCIAL_MEDIA task all the way to SUCCEEDED.
func succeededTask(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	guid, attempt := atAI(t, pool)
	obs := poll.Observation{Outcome: poll.OutcomeSucceeded, Result: json.RawMessage(`{"found":true}`)}
	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, obs, testRules)
	return guid
}

func claimResults(t *testing.T, repo *postgres.ResultRepo) []result.ClaimedResult {
	t.Helper()
	claimed, err := repo.Claim(context.Background(), "response-pod-1", 10, resultLease)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

func claimResult(t *testing.T, repo *postgres.ResultRepo, guid uuid.UUID) result.ClaimedResult {
	t.Helper()
	for _, c := range claimResults(t, repo) {
		if c.RequestGUID == guid {
			return c
		}
	}
	t.Fatalf("result of %s was not claimed", guid)
	return result.ClaimedResult{}
}

// Every way a task can end must leave exactly one result to deliver; tasks
// that are not final, or need attention, leave none.
func TestResultClaim_EveryEndingProducesOneResult(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	submissions := postgres.NewSubmissionRepo(pool, queryTimeout)
	polls := postgres.NewPollRepo(pool, queryTimeout)

	// 1. AI finished: SUCCEEDED.
	succeeded := succeededTask(t, pool)

	// 2. AI finished with a business error: FAILED.
	failedGUID, attempt := atAI(t, pool)
	observe(t, pool, polls, attempt, poll.Observation{Outcome: poll.OutcomeFailed, Failure: &domain.Failure{Code: "EXTRACTION_FAILED", Message: "x"}}, testRules)

	// 3. AI rejected the task when it was sent: FAILED.
	rejectedGUID := readySocialMedia(t, pool, 1)[0]
	s := claimSubmission(t, submissions)
	rejected := submission.Report{Outcome: submission.OutcomeRejected, Failure: &domain.Failure{Code: "INVALID_INPUT", Message: "x"}}
	if err := submissions.Report(ctx, s.AttemptID, s.ClaimToken, rejected, pollDelay); err != nil {
		t.Fatal(err)
	}

	// 4. AI lost the task too often: FAILED.
	lostGUID, lostAttempt := atAI(t, pool)
	if _, err := pool.Exec(ctx, `UPDATE integration.queue SET retry_count = 3 WHERE request_guid = $1`, lostGUID); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		observe(t, pool, polls, lostAttempt, poll.Observation{Outcome: poll.OutcomeNotFound}, testRules)
	}

	// Not final, or needing attention: no result.
	readySocialMedia(t, pool, 1)               // READY_TO_SUBMIT
	atAI(t, pool)                              // REMOTE_QUEUED
	tooBigGUID, tooBigAttempt := atAI(t, pool) // NEEDS_ATTENTION
	big := json.RawMessage(`{"x":"` + strings.Repeat("a", testRules.ResultMaxBytes) + `"}`)
	observe(t, pool, polls, tooBigAttempt, poll.Observation{Outcome: poll.OutcomeSucceeded, Result: big}, testRules)

	claimed := claimResults(t, postgres.NewResultRepo(pool, queryTimeout))

	got := make(map[uuid.UUID]result.ClaimedResult)
	for _, c := range claimed {
		got[c.RequestGUID] = c
	}
	want := map[uuid.UUID]string{succeeded: "SUCCEEDED", failedGUID: "FAILED", rejectedGUID: "FAILED", lostGUID: "FAILED"}
	if len(claimed) != len(want) {
		t.Errorf("claimed %d results, want %d", len(claimed), len(want))
	}
	for guid, status := range want {
		if got[guid].Message.Status != domain.Status(status) {
			t.Errorf("task %s: result %+v, want status %s", guid, got[guid].Message, status)
		}
	}
	if _, ok := got[tooBigGUID]; ok {
		t.Error("a NEEDS_ATTENTION task must not produce a result")
	}
}

func TestResultClaim_MessageContent(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewResultRepo(pool, queryTimeout)
	guid := succeededTask(t, pool)

	ok := claimResult(t, repo, guid)
	m := ok.Message
	if m.RequestGUID != guid || m.RequestID != camundaRequest || m.Operation != domain.OperationSocialMedia ||
		m.Status != domain.StatusSucceeded || m.Error != nil || m.OccurredAt.IsZero() || ok.ClaimToken == uuid.Nil {
		t.Errorf("SUCCEEDED message = %+v", m)
	}
	var res map[string]any
	if err := json.Unmarshal(m.Result, &res); err != nil || res["found"] != true {
		t.Errorf("result = %s, err %v", m.Result, err)
	}

	var completedAt time.Time
	if err := pool.QueryRow(context.Background(), `SELECT completed_at FROM integration.queue WHERE request_guid = $1`, guid).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if !m.OccurredAt.Equal(completedAt) {
		t.Errorf("occurredAt %v must be completed_at %v", m.OccurredAt, completedAt)
	}
}

func TestResultClaim_FailedMessage(t *testing.T) {
	pool := newPool(t)
	failedGUID, attempt := atAI(t, pool)
	failure := &domain.Failure{Code: "EXTRACTION_FAILED", Message: "Could not read document 2"}
	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, poll.Observation{Outcome: poll.OutcomeFailed, Failure: failure}, testRules)

	m := claimResult(t, postgres.NewResultRepo(pool, queryTimeout), failedGUID).Message
	if m.Status != domain.StatusFailed || m.Result != nil || m.Error == nil || *m.Error != *failure {
		t.Errorf("FAILED message = %+v", m)
	}
}

func TestResultPublished(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewResultRepo(pool, queryTimeout)
	ctx := context.Background()
	guid := succeededTask(t, pool)

	var updatedBefore time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM integration.queue WHERE request_guid = $1`, guid).Scan(&updatedBefore); err != nil {
		t.Fatal(err)
	}

	c := claimResult(t, repo, guid)
	if again := claimResults(t, repo); len(again) != 0 {
		t.Errorf("a result being published was handed out again: %+v", again)
	}
	if err := repo.Published(ctx, guid, c.ClaimToken); err != nil {
		t.Fatal(err)
	}

	var published bool
	var updatedAfter time.Time
	err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL, updated_at FROM integration.queue WHERE request_guid = $1`, guid).
		Scan(&published, &updatedAfter)
	if err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Error("published_at not set")
	}
	if !updatedAfter.Equal(updatedBefore) {
		t.Error("publishing must not change updated_at, which BPM sees")
	}

	// Never handed out again; repeating the confirmation is fine.
	if again := claimResults(t, repo); len(again) != 0 {
		t.Errorf("a published result was handed out again: %+v", again)
	}
	if err := repo.Published(ctx, guid, c.ClaimToken); err != nil {
		t.Errorf("repeated published: err = %v", err)
	}
	if err := repo.Failed(ctx, guid, c.ClaimToken, domain.Failure{Code: "X", Message: "y"}, retryBase, retryMax); !errors.Is(err, result.ErrClaimLost) {
		t.Errorf("failure after published: err = %v, want ErrClaimLost", err)
	}
}

func TestResultFailed_RetriesWithBackoff(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewResultRepo(pool, queryTimeout)
	ctx := context.Background()
	guid := succeededTask(t, pool)
	kafkaDown := domain.Failure{Code: "KAFKA_UNAVAILABLE", Message: "no broker ACK"}

	retryIn := func() (seconds float64, count int, code string) {
		t.Helper()
		err := pool.QueryRow(ctx, `
			SELECT extract(epoch FROM publish_retry_at - now()), publish_retry_count, publish_error_code
			FROM integration.queue WHERE request_guid = $1`, guid).Scan(&seconds, &count, &code)
		if err != nil {
			t.Fatal(err)
		}
		return seconds, count, code
	}

	c := claimResult(t, repo, guid)
	if err := repo.Failed(ctx, guid, c.ClaimToken, kafkaDown, retryBase, retryMax); err != nil {
		t.Fatal(err)
	}
	if s, n, code := retryIn(); n != 1 || s < 3 || s > 6 || code != "KAFKA_UNAVAILABLE" {
		t.Errorf("after 1st failure: retry in %.1fs, count %d, code %s; want ~5s, 1", s, n, code)
	}
	if again := claimResults(t, repo); len(again) != 0 {
		t.Error("handed out again before its retry time")
	}
	// Repeating the failure call: the claim is already released.
	if err := repo.Failed(ctx, guid, c.ClaimToken, kafkaDown, retryBase, retryMax); !errors.Is(err, result.ErrClaimLost) {
		t.Errorf("repeated failure: err = %v, want ErrClaimLost", err)
	}

	// Second failure: the delay doubles.
	if _, err := pool.Exec(ctx, `UPDATE integration.queue SET publish_retry_at = now() WHERE request_guid = $1`, guid); err != nil {
		t.Fatal(err)
	}
	c = claimResult(t, repo, guid)
	if err := repo.Failed(ctx, guid, c.ClaimToken, kafkaDown, retryBase, retryMax); err != nil {
		t.Fatal(err)
	}
	if s, n, _ := retryIn(); n != 2 || s < 8 || s > 11 {
		t.Errorf("after 2nd failure: retry in %.1fs, count %d; want ~10s, 2", s, n)
	}

	// Many failures: capped, never dropped.
	if _, err := pool.Exec(ctx, `UPDATE integration.queue SET publish_retry_at = now(), publish_retry_count = 20 WHERE request_guid = $1`, guid); err != nil {
		t.Fatal(err)
	}
	c = claimResult(t, repo, guid)
	if err := repo.Failed(ctx, guid, c.ClaimToken, kafkaDown, retryBase, retryMax); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := retryIn(); s < 295 || s > 301 {
		t.Errorf("after many failures: retry in %.1fs, want the 5m cap", s)
	}
}

func TestResult_WrongCalls(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewResultRepo(pool, queryTimeout)
	ctx := context.Background()
	guid := succeededTask(t, pool)
	notFinal := readySocialMedia(t, pool, 1)[0]

	if err := repo.Published(ctx, guid, uuid.New()); !errors.Is(err, result.ErrClaimLost) {
		t.Errorf("not claimed: err = %v", err)
	}
	if err := repo.Published(ctx, uuid.New(), uuid.New()); !errors.Is(err, result.ErrResultNotFound) {
		t.Errorf("unknown task: err = %v", err)
	}
	if err := repo.Published(ctx, notFinal, uuid.New()); !errors.Is(err, result.ErrResultNotFound) {
		t.Errorf("task without a result: err = %v", err)
	}

	claimed, err := repo.Claim(ctx, "response-pod-1", 1, 50*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)
	// A late confirmation with an expired claim is refused: the result may
	// be published again, and BPM deduplicates by requestGuid.
	if err := repo.Published(ctx, guid, claimed[0].ClaimToken); !errors.Is(err, result.ErrClaimLost) {
		t.Errorf("expired claim: err = %v, want ErrClaimLost", err)
	}
	if again := claimResults(t, repo); len(again) != 1 {
		t.Errorf("the expired result must be claimable again, got %d", len(again))
	}
}

// Like several publisher pods asking at once: no result goes to two of them.
func TestResultClaim_ConcurrentPublishersNeverShare(t *testing.T) {
	pool := newPool(t)
	otherPool, err := pgxpool.New(context.Background(), os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherPool.Close)

	const tasks = 10
	for range tasks {
		succeededTask(t, pool)
	}
	repos := []*postgres.ResultRepo{postgres.NewResultRepo(pool, queryTimeout), postgres.NewResultRepo(otherPool, queryTimeout)}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[uuid.UUID]int)
	)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := repos[i%2].Claim(context.Background(), "pod", 3, resultLease)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Error(err)
			}
			for _, c := range claimed {
				seen[c.RequestGUID]++
			}
		}()
	}
	wg.Wait()

	if len(seen) != tasks {
		t.Errorf("claimed %d distinct results, want all %d", len(seen), tasks)
	}
	for guid, n := range seen {
		if n != 1 {
			t.Errorf("result %s handed out %d times", guid, n)
		}
	}
}

// Documents that cannot be copied put the task into NEEDS_ATTENTION: no result.
func TestResultClaim_DocumentFailureProducesNone(t *testing.T) {
	pool := newPool(t)
	documents := postgres.NewDocumentRepo(pool, queryTimeout)
	_, claimed := claimTask(t, pool, documents)
	failure := document.CopyFailure{Code: "SOURCE_NOT_FOUND", Message: "404", Retryable: false}
	if err := documents.Fail(context.Background(), claimed[0].ID, claimed[0].ClaimToken, failure, testPolicy); err != nil {
		t.Fatal(err)
	}

	if got := claimResults(t, postgres.NewResultRepo(pool, queryTimeout)); len(got) != 0 {
		t.Errorf("claimed %d results, want none", len(got))
	}
}
