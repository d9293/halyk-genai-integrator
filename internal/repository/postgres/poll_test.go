package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/d9293/halyk-genai-integrator/internal/domain"
	"github.com/d9293/halyk-genai-integrator/internal/repository/postgres"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/poll"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/submission"
)

const pollLease = 30 * time.Second

// testRules: no minimum window, so three NOT_FOUND in a row are enough in tests.
var testRules = poll.Rules{
	NextPoll:         15 * time.Second,
	NotFoundDelay:    30 * time.Second,
	ErrorDelay:       time.Minute,
	LostThreshold:    3,
	LostMinWindow:    0,
	MaxResubmissions: 3,
	ResultMaxBytes:   1024,
}

// atAI registers a SOCIAL_MEDIA task, sends it and reports ACCEPTED.
// The attempt is made due for a status check right away.
func atAI(t *testing.T, pool *pgxpool.Pool) (requestGUID, attemptID uuid.UUID) {
	t.Helper()
	requestGUID = readySocialMedia(t, pool, 1)[0]
	submissions := postgres.NewSubmissionRepo(pool, queryTimeout)
	s := claimSubmission(t, submissions)
	accepted := submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}
	if err := submissions.Report(context.Background(), s.AttemptID, s.ClaimToken, accepted, pollDelay); err != nil {
		t.Fatal(err)
	}
	makeDue(t, pool, s.AttemptID)
	return requestGUID, s.AttemptID
}

func makeDue(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE integration.attempts SET poll_at = now() - interval '1 second' WHERE attempt_id = $1`, attemptID)
	if err != nil {
		t.Fatal(err)
	}
}

// claimPoll claims the status check of attemptID.
func claimPoll(t *testing.T, repo *postgres.PollRepo, attemptID uuid.UUID) poll.ClaimedPoll {
	t.Helper()
	claimed, err := repo.Claim(context.Background(), "response-pod-1", 10, pollLease)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range claimed {
		if p.AttemptID == attemptID {
			return p
		}
	}
	t.Fatalf("attempt %s was not claimed; got %+v", attemptID, claimed)
	return poll.ClaimedPoll{}
}

// observe claims a status check of attemptID and reports obs.
func observe(t *testing.T, pool *pgxpool.Pool, repo *postgres.PollRepo, attemptID uuid.UUID, obs poll.Observation, rules poll.Rules) {
	t.Helper()
	makeDue(t, pool, attemptID)
	p := claimPoll(t, repo, attemptID)
	if err := repo.Observe(context.Background(), attemptID, p.ClaimToken, obs, rules); err != nil {
		t.Fatalf("observe %s: %v", obs.Outcome, err)
	}
}

type attemptRow struct {
	state         string
	notFoundCount int
	pollIn        *float64 // seconds until poll_at
	errorCode     *string
	claimed       bool
}

func readAttempt(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) attemptRow {
	t.Helper()
	var a attemptRow
	err := pool.QueryRow(context.Background(), `
		SELECT state, not_found_count, extract(epoch FROM poll_at - now()), error_code, claim_token IS NOT NULL
		FROM integration.attempts WHERE attempt_id = $1`, attemptID,
	).Scan(&a.state, &a.notFoundCount, &a.pollIn, &a.errorCode, &a.claimed)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

type taskRow struct {
	status     string
	retryCount int
	errorCode  *string
	result     []byte
	completed  bool
}

func readTask(t *testing.T, pool *pgxpool.Pool, requestGUID uuid.UUID) taskRow {
	t.Helper()
	var r taskRow
	err := pool.QueryRow(context.Background(), `
		SELECT status, retry_count, error_code, result_payload, completed_at IS NOT NULL
		FROM integration.queue WHERE request_guid = $1`, requestGUID,
	).Scan(&r.status, &r.retryCount, &r.errorCode, &r.result, &r.completed)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func pollIn(a attemptRow, from, to float64) bool {
	return a.pollIn != nil && *a.pollIn >= from && *a.pollIn <= to
}

func TestPollClaim_OnlyDueAndUnclaimed(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	_, due := atAI(t, pool)
	_, notDue := atAI(t, pool)
	_, err := pool.Exec(ctx, `UPDATE integration.attempts SET poll_at = now() + interval '1 hour' WHERE attempt_id = $1`, notDue)
	if err != nil {
		t.Fatal(err)
	}
	readySocialMedia(t, pool, 1)
	sending := claimSubmission(t, postgres.NewSubmissionRepo(pool, queryTimeout)) // SUBMITTING, live claim
	repo := postgres.NewPollRepo(pool, queryTimeout)

	claimed, err := repo.Claim(ctx, "response-pod-1", 10, pollLease)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].AttemptID != due {
		t.Fatalf("claimed %+v, want only the due attempt", claimed)
	}
	p := claimed[0]
	if p.RequestID != camundaRequest || p.Operation != domain.OperationSocialMedia || p.AttemptNo != 1 || p.ClaimToken == uuid.Nil {
		t.Errorf("unexpected poll %+v", p)
	}
	if again, _ := repo.Claim(ctx, "response-pod-2", 10, pollLease); len(again) != 0 {
		t.Errorf("an attempt being checked was handed out again: %+v", again)
	}
	if a := readAttempt(t, pool, sending.AttemptID); a.state != "SUBMITTING" {
		t.Errorf("an attempt being sent must not be touched, state = %s", a.state)
	}
}

func TestPollClaim_RecoversAbandonedSubmission(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	guid := readySocialMedia(t, pool, 1)[0]
	submissions := postgres.NewSubmissionRepo(pool, queryTimeout)
	claimed, err := submissions.Claim(ctx, "request-pod-1", 1, 50*time.Millisecond, maxInFlight)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("submission claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond) // the sender "died" before reporting

	p := claimPoll(t, postgres.NewPollRepo(pool, queryTimeout), claimed[0].AttemptID)

	if a := readAttempt(t, pool, p.AttemptID); a.state != "UNKNOWN" || a.errorCode == nil || *a.errorCode != "SUBMISSION_ABANDONED" {
		t.Errorf("attempt = %+v, want UNKNOWN with SUBMISSION_ABANDONED", a)
	}
	if task := readTask(t, pool, guid); task.status != "SUBMISSION_UNKNOWN" {
		t.Errorf("task = %s, want SUBMISSION_UNKNOWN", task.status)
	}
	if n := auditCount(t, pool, guid, "SUBMISSION_RECOVERED"); n != 1 {
		t.Errorf("SUBMISSION_RECOVERED audit rows = %d", n)
	}
	// Recovered, not re-sent: still one attempt, still holding its place.
	if n := activeAttempts(t, pool); n != 1 {
		t.Errorf("active attempts = %d, want 1", n)
	}
}

func TestObserve_Progress(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)

	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeProcessing}, testRules)
	a := readAttempt(t, pool, attempt)
	if a.state != "PROCESSING" || a.claimed || !pollIn(a, 10, 16) {
		t.Errorf("attempt = %+v, want PROCESSING, unclaimed, next check in ~15s", a)
	}
	if task := readTask(t, pool, guid); task.status != "REMOTE_RUNNING" {
		t.Errorf("task = %s", task.status)
	}

	// A late QUEUED does not move the task backwards.
	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeQueued}, testRules)
	if a := readAttempt(t, pool, attempt); a.state != "PROCESSING" {
		t.Errorf("after late QUEUED: attempt = %s, want PROCESSING", a.state)
	}
	if n := activeAttempts(t, pool); n != 1 {
		t.Errorf("active attempts = %d, want 1 (still at AI)", n)
	}
}

func TestObserve_UnknownSubmissionResolves(t *testing.T) {
	pool := newPool(t)
	guid := readySocialMedia(t, pool, 1)[0]
	submissions := postgres.NewSubmissionRepo(pool, queryTimeout)
	s := claimSubmission(t, submissions)
	if err := submissions.Report(context.Background(), s.AttemptID, s.ClaimToken, submission.Report{Outcome: submission.OutcomeUnknown}, pollDelay); err != nil {
		t.Fatal(err)
	}

	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), s.AttemptID, poll.Observation{Outcome: poll.OutcomeQueued}, testRules)

	if task := readTask(t, pool, guid); task.status != "REMOTE_QUEUED" {
		t.Errorf("task = %s, want REMOTE_QUEUED (AI did get it)", task.status)
	}
}

func TestObserve_Succeeded(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	result := json.RawMessage(`{"summary":{"counts":{"both":12}},"report_url":"https://s3.example/r.docx"}`)
	obs := poll.Observation{Outcome: poll.OutcomeSucceeded, Result: result}

	makeDue(t, pool, attempt)
	p := claimPoll(t, repo, attempt)
	if err := repo.Observe(context.Background(), attempt, p.ClaimToken, obs, testRules); err != nil {
		t.Fatal(err)
	}

	task := readTask(t, pool, guid)
	var stored map[string]any
	if err := json.Unmarshal(task.result, &stored); err != nil || stored["report_url"] != "https://s3.example/r.docx" {
		t.Errorf("stored result %s, err %v", task.result, err)
	}
	if task.status != "SUCCEEDED" || !task.completed || task.errorCode != nil {
		t.Errorf("task = %+v", task)
	}
	if a := readAttempt(t, pool, attempt); a.state != "DONE" || a.pollIn != nil {
		t.Errorf("attempt = %+v, want DONE and no more checks", a)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0 (place freed)", n)
	}
	if n := auditCount(t, pool, guid, "TASK_SUCCEEDED"); n != 1 {
		t.Errorf("TASK_SUCCEEDED audit rows = %d", n)
	}

	// Repeated after a lost response: OK, nothing new. A different answer: too late.
	if err := repo.Observe(context.Background(), attempt, p.ClaimToken, obs, testRules); err != nil {
		t.Errorf("repeated SUCCEEDED: err = %v", err)
	}
	failed := poll.Observation{Outcome: poll.OutcomeFailed, Failure: &domain.Failure{Code: "X", Message: "y"}}
	if err := repo.Observe(context.Background(), attempt, p.ClaimToken, failed, testRules); !errors.Is(err, poll.ErrClaimLost) {
		t.Errorf("FAILED after SUCCEEDED: err = %v, want ErrClaimLost", err)
	}

	// The task shows the result to BPM.
	got, err := newUseCase(t, pool).Get(context.Background(), "bpm", guid)
	if err != nil || got.Status != domain.StatusSucceeded || len(got.Result) == 0 {
		t.Errorf("GET task: %+v, err %v", got, err)
	}
}

func TestObserve_Failed(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	failure := &domain.Failure{Code: "EXTRACTION_FAILED", Message: "Could not read document 2"}

	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, poll.Observation{Outcome: poll.OutcomeFailed, Failure: failure}, testRules)

	task := readTask(t, pool, guid)
	if task.status != "FAILED" || task.errorCode == nil || *task.errorCode != failure.Code || !task.completed {
		t.Errorf("task = %+v", task)
	}
	if a := readAttempt(t, pool, attempt); a.state != "ERROR" {
		t.Errorf("attempt = %s, want ERROR", a.state)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0 (place freed)", n)
	}
}

func TestObserve_ResultTooLargeNeedsAttention(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	big := json.RawMessage(`{"data":"` + strings.Repeat("x", testRules.ResultMaxBytes) + `"}`)

	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, poll.Observation{Outcome: poll.OutcomeSucceeded, Result: big}, testRules)

	task := readTask(t, pool, guid)
	if task.status != "NEEDS_ATTENTION" || task.errorCode == nil || *task.errorCode != "RESULT_TOO_LARGE" || task.result != nil || task.completed {
		t.Errorf("task = %+v, want NEEDS_ATTENTION, no result stored", task)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0: AI is done, the place is freed", n)
	}
}

func TestObserve_TechnicalErrorChangesNothing(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	technical := poll.Observation{Outcome: poll.OutcomeTechnicalError, Failure: &domain.Failure{Code: "AI_TIMEOUT", Message: "10s"}}

	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeNotFound}, testRules)
	makeDue(t, pool, attempt)
	p := claimPoll(t, repo, attempt)
	if err := repo.Observe(context.Background(), attempt, p.ClaimToken, technical, testRules); err != nil {
		t.Fatal(err)
	}

	a := readAttempt(t, pool, attempt)
	if a.state != "QUEUED" || a.notFoundCount != 1 || !pollIn(a, 55, 61) || a.errorCode == nil || *a.errorCode != "AI_TIMEOUT" {
		t.Errorf("attempt = %+v, want QUEUED, NOT_FOUND count kept at 1, next check in ~60s", a)
	}
	if task := readTask(t, pool, guid); task.status != "REMOTE_QUEUED" {
		t.Errorf("task = %s", task.status)
	}
	// Repeating it must not be applied twice.
	if err := repo.Observe(context.Background(), attempt, p.ClaimToken, technical, testRules); !errors.Is(err, poll.ErrClaimLost) {
		t.Errorf("repeated TECHNICAL_ERROR: err = %v, want ErrClaimLost", err)
	}
}

func TestObserve_LostTaskIsSentAgain(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	notFound := poll.Observation{Outcome: poll.OutcomeNotFound}

	// Two NOT_FOUND, then a valid answer: the streak resets.
	observe(t, pool, repo, attempt, notFound, testRules)
	observe(t, pool, repo, attempt, notFound, testRules)
	if a := readAttempt(t, pool, attempt); a.state != "QUEUED" || a.notFoundCount != 2 || !pollIn(a, 25, 31) {
		t.Errorf("after 2 NOT_FOUND: %+v, want still QUEUED, count 2, next check in ~30s", a)
	}
	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeQueued}, testRules)
	if a := readAttempt(t, pool, attempt); a.notFoundCount != 0 {
		t.Errorf("a valid answer must reset the count, got %d", a.notFoundCount)
	}

	// Three in a row: lost, place freed, task ready to be sent again.
	for range 3 {
		observe(t, pool, repo, attempt, notFound, testRules)
	}
	if a := readAttempt(t, pool, attempt); a.state != "LOST" || a.pollIn != nil {
		t.Errorf("attempt = %+v, want LOST", a)
	}
	task := readTask(t, pool, guid)
	if task.status != "READY_TO_SUBMIT" || task.retryCount != 1 {
		t.Errorf("task = %+v, want READY_TO_SUBMIT with retry_count 1", task)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0", n)
	}
	if n := auditCount(t, pool, guid, "AI_TASK_LOST_RESUBMIT"); n != 1 {
		t.Errorf("AI_TASK_LOST_RESUBMIT audit rows = %d", n)
	}

	// The next submission claim sends it again as attempt 2.
	again := claimSubmission(t, postgres.NewSubmissionRepo(pool, queryTimeout))
	if again.RequestGUID != guid || again.AttemptNo != 2 || again.AttemptID == attempt {
		t.Errorf("re-send = %+v", again)
	}
}

func TestObserve_LostNeedsTheMinimumWindow(t *testing.T) {
	pool := newPool(t)
	_, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	rules := testRules
	rules.LostMinWindow = time.Hour

	for range 4 {
		observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeNotFound}, rules)
	}
	if a := readAttempt(t, pool, attempt); a.state != "QUEUED" || a.notFoundCount != 4 {
		t.Errorf("attempt = %+v: NOT_FOUND within the window must not mark it lost", a)
	}
}

func TestObserve_LostTooOftenFails(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	_, err := pool.Exec(context.Background(), `UPDATE integration.queue SET retry_count = 3 WHERE request_guid = $1`, guid)
	if err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewPollRepo(pool, queryTimeout)

	for range 3 {
		observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeNotFound}, testRules)
	}

	task := readTask(t, pool, guid)
	if task.status != "FAILED" || task.errorCode == nil || *task.errorCode != "AI_TASK_LOST_RETRIES_EXHAUSTED" || !task.completed {
		t.Errorf("task = %+v, want FAILED with AI_TASK_LOST_RETRIES_EXHAUSTED", task)
	}
	if again, _ := postgres.NewSubmissionRepo(pool, queryTimeout).Claim(context.Background(), "pod", 1, lease, maxInFlight); len(again) != 0 {
		t.Errorf("a failed task must not be sent again")
	}
}

func TestObserve_WrongCalls(t *testing.T) {
	pool := newPool(t)
	_, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	ctx := context.Background()
	queued := poll.Observation{Outcome: poll.OutcomeQueued}

	// Not claimed: anything that would change the attempt is refused.
	processing := poll.Observation{Outcome: poll.OutcomeProcessing}
	if err := repo.Observe(ctx, attempt, uuid.New(), processing, testRules); !errors.Is(err, poll.ErrClaimLost) {
		t.Errorf("unclaimed change: err = %v, want ErrClaimLost", err)
	}
	// Reporting the state the attempt is already in is taken as a repeat:
	// OK, nothing written (option B: repeats are recognized from the data).
	if err := repo.Observe(ctx, attempt, uuid.New(), queued, testRules); err != nil {
		t.Errorf("unclaimed repeat of the current state: err = %v, want nil", err)
	}
	if a := readAttempt(t, pool, attempt); a.state != "QUEUED" || a.claimed {
		t.Errorf("attempt changed: %+v", a)
	}
	if err := repo.Observe(ctx, uuid.New(), uuid.New(), queued, testRules); !errors.Is(err, poll.ErrAttemptNotFound) {
		t.Errorf("unknown attempt: err = %v", err)
	}

	claimed, err := repo.Claim(ctx, "response-pod-1", 1, 50*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := repo.Observe(ctx, attempt, claimed[0].ClaimToken, queued, testRules); !errors.Is(err, poll.ErrClaimLost) {
		t.Errorf("expired claim: err = %v", err)
	}
}

// Many copies of the observation that crosses the threshold arrive at once
// (e.g. retries): exactly one LOST, one re-send, one freed place.
func TestObserve_ConcurrentObservationsApplyOnce(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)
	notFound := poll.Observation{Outcome: poll.OutcomeNotFound}
	observe(t, pool, repo, attempt, notFound, testRules)
	observe(t, pool, repo, attempt, notFound, testRules)

	makeDue(t, pool, attempt)
	p := claimPoll(t, repo, attempt)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		applied int
	)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := repo.Observe(context.Background(), attempt, p.ClaimToken, notFound, testRules)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				applied++
			case !errors.Is(err, poll.ErrClaimLost):
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if applied != 1 {
		t.Errorf("applied %d times, want exactly 1", applied)
	}
	task := readTask(t, pool, guid)
	if task.status != "READY_TO_SUBMIT" || task.retryCount != 1 {
		t.Errorf("task = %+v, want one re-send", task)
	}
}

// The whole way: BPM registers a REGISTER_COMPARE task, its documents are
// copied, it is sent to AI, AI works on it and finishes, BPM reads the result.
func TestFullJourney(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	guid := readyRegisterCompare(t, pool)

	submissions := postgres.NewSubmissionRepo(pool, queryTimeout)
	s := claimSubmission(t, submissions)
	if len(s.Documents) != 2 {
		t.Fatalf("submission without documents: %+v", s)
	}
	if err := submissions.Report(ctx, s.AttemptID, s.ClaimToken, submission.Report{Outcome: submission.OutcomeAccepted, RemoteStatus: "QUEUED"}, pollDelay); err != nil {
		t.Fatal(err)
	}

	polls := postgres.NewPollRepo(pool, queryTimeout)
	observe(t, pool, polls, s.AttemptID, poll.Observation{Outcome: poll.OutcomeQueued}, testRules)
	observe(t, pool, polls, s.AttemptID, poll.Observation{Outcome: poll.OutcomeProcessing}, testRules)
	observe(t, pool, polls, s.AttemptID, poll.Observation{Outcome: poll.OutcomeSucceeded, Result: json.RawMessage(`{"summary":{"both":3}}`)}, testRules)

	got, err := newUseCase(t, pool).Get(ctx, "bpm", guid)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusSucceeded || got.CompletedAt == nil || len(got.Result) == 0 {
		t.Errorf("task = %+v", got)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0", n)
	}

	var actions []string
	rows, err := pool.Query(ctx, `SELECT action FROM integration.audit WHERE request_guid = $1 ORDER BY audit_id`, guid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		actions = append(actions, a)
	}
	want := "TASK_REGISTERED DOCUMENTS_PREPARING DOCUMENT_READY DOCUMENT_READY TASK_READY SUBMISSION_STARTED SUBMISSION_ACCEPTED REMOTE_STATUS TASK_SUCCEEDED"
	if got := strings.Join(actions, " "); got != want {
		t.Errorf("history:\n%s\nwant\n%s", got, want)
	}
}

// noProgressRules flag any task still at AI right away, so tests need not wait hours.
func noProgressRules() poll.Rules {
	rules := testRules
	rules.NoProgressTimeout = time.Nanosecond
	return rules
}

func TestObserve_NoProgressIsFlaggedOnceAndStillPolled(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)

	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeProcessing}, noProgressRules())

	task := readTask(t, pool, guid)
	if task.status != "NEEDS_ATTENTION" || task.errorCode == nil || *task.errorCode != "AI_NO_PROGRESS" || task.completed {
		t.Fatalf("task = %+v, want NEEDS_ATTENTION with AI_NO_PROGRESS", task)
	}
	a := readAttempt(t, pool, attempt)
	if a.state != "PROCESSING" || !pollIn(a, 10, 16) {
		t.Errorf("attempt = %+v, want it still PROCESSING and polled again", a)
	}
	if n := activeAttempts(t, pool); n != 1 {
		t.Errorf("active attempts = %d, want 1: AI may still be working, the place stays taken", n)
	}

	// More progress answers: still flagged, not back to REMOTE_RUNNING, flagged only once.
	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeProcessing}, noProgressRules())
	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeQueued}, noProgressRules())
	if task := readTask(t, pool, guid); task.status != "NEEDS_ATTENTION" {
		t.Errorf("task = %s, want it to stay NEEDS_ATTENTION until a final answer", task.status)
	}
	if n := auditCount(t, pool, guid, "AI_NO_PROGRESS"); n != 1 {
		t.Errorf("AI_NO_PROGRESS audit rows = %d, want 1", n)
	}
	if got := claimResults(t, postgres.NewResultRepo(pool, queryTimeout)); len(got) != 0 {
		t.Errorf("a flagged task must not produce a Kafka message yet, got %d", len(got))
	}

	// AI finishes after all: the task ends normally, with its Kafka message.
	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeSucceeded, Result: json.RawMessage(`{"late":true}`)}, noProgressRules())
	task = readTask(t, pool, guid)
	if task.status != "SUCCEEDED" || task.errorCode != nil || !task.completed {
		t.Errorf("task = %+v, want SUCCEEDED with the flag cleared", task)
	}
	if n := activeAttempts(t, pool); n != 0 {
		t.Errorf("active attempts = %d, want 0", n)
	}
	if got := claimResults(t, postgres.NewResultRepo(pool, queryTimeout)); len(got) != 1 {
		t.Errorf("results to deliver = %d, want 1", len(got))
	}
}

// AI unreachable for hours: only technical errors, never an answer.
func TestObserve_NoProgressAfterOnlyTechnicalErrors(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	technical := poll.Observation{Outcome: poll.OutcomeTechnicalError, Failure: &domain.Failure{Code: "AI_TIMEOUT", Message: "10s"}}

	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, technical, noProgressRules())

	if task := readTask(t, pool, guid); task.status != "NEEDS_ATTENTION" || task.errorCode == nil || *task.errorCode != "AI_NO_PROGRESS" {
		t.Errorf("task = %+v, want NEEDS_ATTENTION with AI_NO_PROGRESS", task)
	}
	if a := readAttempt(t, pool, attempt); a.state != "QUEUED" || !pollIn(a, 55, 61) {
		t.Errorf("attempt = %+v, want unchanged and checked again in ~60s", a)
	}
}

func TestObserve_NoProgressNotBeforeTheTimeout(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	rules := testRules
	rules.NoProgressTimeout = time.Hour

	observe(t, pool, postgres.NewPollRepo(pool, queryTimeout), attempt, poll.Observation{Outcome: poll.OutcomeProcessing}, rules)

	if task := readTask(t, pool, guid); task.status != "REMOTE_RUNNING" {
		t.Errorf("task = %s, want REMOTE_RUNNING: it was sent just now", task.status)
	}
}

// A flagged task whose attempt AI then loses is sent again: a fresh attempt,
// a fresh timeout, and the flag is cleared.
func TestObserve_NoProgressThenLostIsSentAgain(t *testing.T) {
	pool := newPool(t)
	guid, attempt := atAI(t, pool)
	repo := postgres.NewPollRepo(pool, queryTimeout)

	observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeProcessing}, noProgressRules())
	for range 3 {
		observe(t, pool, repo, attempt, poll.Observation{Outcome: poll.OutcomeNotFound}, noProgressRules())
	}

	task := readTask(t, pool, guid)
	if task.status != "READY_TO_SUBMIT" || task.errorCode != nil || task.retryCount != 1 {
		t.Errorf("task = %+v, want READY_TO_SUBMIT with the flag cleared", task)
	}
}
