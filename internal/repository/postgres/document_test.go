package postgres_test

import (
	"context"
	"errors"
	"fmt"
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
)

const lease = time.Minute

// registerCompareTasks registers n REGISTER_COMPARE tasks (two documents each)
// with unique document IDs and returns their requestGuids.
func registerCompareTasks(t *testing.T, pool *pgxpool.Pool, n int) []uuid.UUID {
	t.Helper()
	uc := newUseCase(t, pool)
	guids := make([]uuid.UUID, 0, n)
	for range n {
		in := registerCompare(uuid.New())
		in.Documents = []domain.Document{
			{DocumentID: uuid.New(), AttstoreURL: "https://attstore.example/1"},
			{DocumentID: uuid.New(), AttstoreURL: "https://attstore.example/2"},
		}
		if _, _, err := uc.Register(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		guids = append(guids, in.RequestGUID)
	}
	return guids
}

func taskStatus(t *testing.T, pool *pgxpool.Pool, requestGUID uuid.UUID) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(),
		`SELECT status FROM integration.queue WHERE request_guid = $1`, requestGUID).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func TestClaim_TakesDocumentsAndStartsTask(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	guid := registerCompareTasks(t, pool, 1)[0]
	if _, _, err := newUseCase(t, pool).Register(ctx, socialMedia(uuid.New())); err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewDocumentRepo(pool, queryTimeout)

	before := time.Now()
	claimed, err := repo.Claim(ctx, "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}

	// Only the REGISTER_COMPARE task has documents; SOCIAL_MEDIA never shows up.
	if len(claimed) != 2 {
		t.Fatalf("claimed %d documents, want 2", len(claimed))
	}
	for _, d := range claimed {
		if d.RequestGUID != guid || d.ClaimToken == uuid.Nil || d.AttstoreURL == "" {
			t.Errorf("unexpected document %+v", d)
		}
		if d.ClaimUntil.Before(before.Add(lease-5*time.Second)) || d.ClaimUntil.After(time.Now().Add(lease+5*time.Second)) {
			t.Errorf("claimUntil %v is not about now + %v", d.ClaimUntil, lease)
		}
	}

	var copying int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM integration.documents
		WHERE request_guid = $1 AND status = 'COPYING' AND claim_owner = 'pod-1'`, guid).Scan(&copying)
	if err != nil || copying != 2 {
		t.Errorf("COPYING documents owned by pod-1 = %d, err %v", copying, err)
	}

	if got := taskStatus(t, pool, guid); got != string(domain.StatusPreparingDocuments) {
		t.Errorf("task status = %s, want PREPARING_DOCUMENTS", got)
	}
	var audits int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM integration.audit
		WHERE request_guid = $1 AND action = 'DOCUMENTS_PREPARING' AND actor = 'pod-1'`, guid).Scan(&audits)
	if err != nil || audits != 1 {
		t.Errorf("DOCUMENTS_PREPARING audit rows = %d, err %v", audits, err)
	}

	// Everything is claimed now.
	if again, _ := repo.Claim(ctx, "pod-2", 10, lease); len(again) != 0 {
		t.Errorf("second claim got %d documents, want none", len(again))
	}
}

func TestClaim_RespectsLimitAndOrder(t *testing.T) {
	pool := newPool(t)
	guids := registerCompareTasks(t, pool, 2)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)

	claimed, err := repo.Claim(context.Background(), "pod-1", 3, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d, want 3", len(claimed))
	}
	// Oldest task first: both documents of the first task are among them.
	var fromFirst int
	for _, d := range claimed {
		if d.RequestGUID == guids[0] {
			fromFirst++
		}
	}
	if fromFirst != 2 {
		t.Errorf("documents from the oldest task = %d, want 2", fromFirst)
	}
}

// Like several worker pods asking for work at the same moment.
func TestClaim_ConcurrentWorkersNeverShareDocuments(t *testing.T) {
	pool := newPool(t)
	otherPool, err := pgxpool.New(context.Background(), os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherPool.Close)

	registerCompareTasks(t, pool, 10) // 20 documents
	repos := []*postgres.DocumentRepo{
		postgres.NewDocumentRepo(pool, queryTimeout),
		postgres.NewDocumentRepo(otherPool, queryTimeout),
	}

	const workers = 10
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[uuid.UUID]int)
		errs []error
	)
	for i := range workers {
		wg.Go(func() {
			claimed, err := repos[i%2].Claim(context.Background(), "pod", 3, lease)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			for _, d := range claimed {
				seen[d.ID]++
			}
		})
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("document %s was claimed %d times", id, n)
		}
	}
	// 10 workers x 3 = 30 requested, 20 exist: every document claimed exactly once.
	if len(seen) != 20 {
		t.Errorf("claimed %d distinct documents, want all 20", len(seen))
	}
}

func TestClaim_ExpiredClaimIsTakenAgain(t *testing.T) {
	pool := newPool(t)
	registerCompareTasks(t, pool, 1)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()

	first, err := repo.Claim(ctx, "pod-1", 1, 50*time.Millisecond)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: %v, err %v", first, err)
	}
	time.Sleep(200 * time.Millisecond) // pod-1 "died": no renew

	second, err := repo.Claim(ctx, "pod-2", 2, lease)
	if err != nil {
		t.Fatal(err)
	}
	var retaken *document.ClaimedDocument
	for i := range second {
		if second[i].ID == first[0].ID {
			retaken = &second[i]
		}
	}
	if retaken == nil {
		t.Fatalf("expired document was not claimed again; got %+v", second)
	}
	if retaken.ClaimToken == first[0].ClaimToken {
		t.Error("a new claim must get a new token")
	}
}

func TestClaim_SkipsDocumentsNotDueOrOfStoppedTasks(t *testing.T) {
	pool := newPool(t)
	guids := registerCompareTasks(t, pool, 2)
	ctx := context.Background()

	// Task 0: waiting for a retry later. Task 1: needs attention, so not worked on.
	_, err := pool.Exec(ctx, `UPDATE integration.documents SET retry_at = now() + interval '1 hour' WHERE request_guid = $1`, guids[0])
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		UPDATE integration.queue SET status = 'NEEDS_ATTENTION', error_code = 'X', error_detail = 'test'
		WHERE request_guid = $1`, guids[1])
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := postgres.NewDocumentRepo(pool, queryTimeout).Claim(ctx, "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed %d documents, want none", len(claimed))
	}
}

var testPolicy = document.RetryPolicy{MaxAttempts: 5, BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Minute}

func copyResult(n int) document.CopyResult {
	return document.CopyResult{
		S3URI:          fmt.Sprintf("s3://ai-input/doc-%d", n),
		SizeBytes:      int64(100 + n),
		ChecksumSHA256: strings.Repeat("a", 64),
	}
}

func auditCount(t *testing.T, pool *pgxpool.Pool, requestGUID uuid.UUID, action string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM integration.audit WHERE request_guid = $1 AND action = $2`, requestGUID, action).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// claimTask registers one REGISTER_COMPARE task and claims both its documents.
func claimTask(t *testing.T, pool *pgxpool.Pool, repo *postgres.DocumentRepo) (uuid.UUID, []document.ClaimedDocument) {
	t.Helper()
	guid := registerCompareTasks(t, pool, 1)[0]
	claimed, err := repo.Claim(context.Background(), "pod-1", 2, lease)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	return guid, claimed
}

func TestComplete_LastDocumentMakesTaskReady(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	guid, claimed := claimTask(t, pool, repo)

	if err := repo.Complete(ctx, claimed[0].ID, claimed[0].ClaimToken, copyResult(0)); err != nil {
		t.Fatal(err)
	}
	if got := taskStatus(t, pool, guid); got != string(domain.StatusPreparingDocuments) {
		t.Errorf("after first document: task = %s, want PREPARING_DOCUMENTS", got)
	}

	if err := repo.Complete(ctx, claimed[1].ID, claimed[1].ClaimToken, copyResult(1)); err != nil {
		t.Fatal(err)
	}
	if got := taskStatus(t, pool, guid); got != string(domain.StatusReadyToSubmit) {
		t.Errorf("after last document: task = %s, want READY_TO_SUBMIT", got)
	}

	var status, s3URI string
	var claimToken *uuid.UUID
	err := pool.QueryRow(ctx, `SELECT status, s3_uri, claim_token FROM integration.documents WHERE id = $1`, claimed[1].ID).
		Scan(&status, &s3URI, &claimToken)
	if err != nil {
		t.Fatal(err)
	}
	if status != "READY" || s3URI != copyResult(1).S3URI || claimToken != nil {
		t.Errorf("document: status=%s s3=%s claim=%v", status, s3URI, claimToken)
	}
	if n := auditCount(t, pool, guid, "DOCUMENT_READY"); n != 2 {
		t.Errorf("DOCUMENT_READY audit rows = %d, want 2", n)
	}
	if n := auditCount(t, pool, guid, "TASK_READY"); n != 1 {
		t.Errorf("TASK_READY audit rows = %d, want 1", n)
	}
}

func TestComplete_RetryAndWrongCalls(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	guid, claimed := claimTask(t, pool, repo)
	doc := claimed[0]

	if err := repo.Complete(ctx, doc.ID, uuid.New(), copyResult(0)); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("wrong token: err = %v, want ErrClaimLost", err)
	}
	if err := repo.Complete(ctx, uuid.New(), doc.ClaimToken, copyResult(0)); !errors.Is(err, document.ErrDocumentNotFound) {
		t.Errorf("unknown document: err = %v, want ErrDocumentNotFound", err)
	}

	if err := repo.Complete(ctx, doc.ID, doc.ClaimToken, copyResult(0)); err != nil {
		t.Fatal(err)
	}
	// Response lost, worker repeats the call: OK, nothing written again.
	if err := repo.Complete(ctx, doc.ID, doc.ClaimToken, copyResult(0)); err != nil {
		t.Errorf("repeated complete: err = %v, want nil", err)
	}
	if n := auditCount(t, pool, guid, "DOCUMENT_READY"); n != 1 {
		t.Errorf("DOCUMENT_READY audit rows = %d, want 1", n)
	}
	// Same document, different copy: not a repeat.
	if err := repo.Complete(ctx, doc.ID, doc.ClaimToken, copyResult(9)); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("different result: err = %v, want ErrClaimLost", err)
	}
}

func TestComplete_ExpiredClaimIsRejected(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	registerCompareTasks(t, pool, 1)

	claimed, err := repo.Claim(ctx, "pod-1", 1, 50*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)

	err = repo.Complete(ctx, claimed[0].ID, claimed[0].ClaimToken, copyResult(0))
	if !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("err = %v, want ErrClaimLost", err)
	}
}

// Both documents of a task finish at the same moment, many times over:
// every task must become READY_TO_SUBMIT exactly once.
func TestComplete_ParallelLastDocuments(t *testing.T) {
	pool := newPool(t)
	otherPool, err := pgxpool.New(context.Background(), os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherPool.Close)

	const tasks = 20
	guids := registerCompareTasks(t, pool, tasks)
	repos := []*postgres.DocumentRepo{
		postgres.NewDocumentRepo(pool, queryTimeout),
		postgres.NewDocumentRepo(otherPool, queryTimeout),
	}
	claimed, err := repos[0].Claim(context.Background(), "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	more, err := repos[0].Claim(context.Background(), "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	claimed = append(claimed, more...)
	more, err = repos[0].Claim(context.Background(), "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	claimed = append(claimed, more...)
	more, err = repos[0].Claim(context.Background(), "pod-1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	claimed = append(claimed, more...)
	if len(claimed) != 2*tasks {
		t.Fatalf("claimed %d documents, want %d", len(claimed), 2*tasks)
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i, d := range claimed {
		wg.Go(func() {
			err := repos[i%2].Complete(context.Background(), d.ID, d.ClaimToken, copyResult(i))
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	for _, guid := range guids {
		if got := taskStatus(t, pool, guid); got != string(domain.StatusReadyToSubmit) {
			t.Errorf("task %s = %s, want READY_TO_SUBMIT", guid, got)
		}
		if n := auditCount(t, pool, guid, "TASK_READY"); n != 1 {
			t.Errorf("task %s: TASK_READY audit rows = %d, want exactly 1", guid, n)
		}
	}
}

func TestFail_RetryableGoesBackWithBackoff(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	guid, claimed := claimTask(t, pool, repo)
	doc := claimed[0]

	failure := document.CopyFailure{Code: "SOURCE_UNAVAILABLE", Message: "Attstore returned 503", Retryable: true}
	if err := repo.Fail(ctx, doc.ID, doc.ClaimToken, failure, testPolicy); err != nil {
		t.Fatal(err)
	}

	var status, errorCode string
	var retryCount int
	var waitSeconds float64
	err := pool.QueryRow(ctx, `
		SELECT status, retry_count, error_code, extract(epoch FROM retry_at - now())
		FROM integration.documents WHERE id = $1`, doc.ID).Scan(&status, &retryCount, &errorCode, &waitSeconds)
	if err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || retryCount != 1 || errorCode != failure.Code {
		t.Errorf("document: status=%s retry_count=%d error=%s", status, retryCount, errorCode)
	}
	if waitSeconds < 3 || waitSeconds > 6 {
		t.Errorf("retry_at is %.1fs away, want about 5s (first backoff step)", waitSeconds)
	}
	if got := taskStatus(t, pool, guid); got != string(domain.StatusPreparingDocuments) {
		t.Errorf("task = %s, want PREPARING_DOCUMENTS (still copying)", got)
	}

	// Not due yet: nobody gets it now.
	if again, _ := repo.Claim(ctx, "pod-2", 10, lease); len(again) != 0 {
		t.Errorf("claimed %d documents before retry_at, want none", len(again))
	}
	// Repeating the failure call: the claim is already released.
	if err := repo.Fail(ctx, doc.ID, doc.ClaimToken, failure, testPolicy); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("repeated failure: err = %v, want ErrClaimLost", err)
	}
}

func TestFail_PermanentOrExhaustedNeedsAttention(t *testing.T) {
	tests := []struct {
		name         string
		retryable    bool
		earlierFails int
	}{
		{"permanent failure", false, 0},
		{"fifth failure", true, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newPool(t)
			repo := postgres.NewDocumentRepo(pool, queryTimeout)
			ctx := context.Background()
			guid, claimed := claimTask(t, pool, repo)
			doc := claimed[0]

			_, err := pool.Exec(ctx, `UPDATE integration.documents SET retry_count = $2 WHERE id = $1`, doc.ID, tt.earlierFails)
			if err != nil {
				t.Fatal(err)
			}

			failure := document.CopyFailure{Code: "SOURCE_NOT_FOUND", Message: "404", Retryable: tt.retryable}
			if err := repo.Fail(ctx, doc.ID, doc.ClaimToken, failure, testPolicy); err != nil {
				t.Fatal(err)
			}

			var docStatus string
			if err := pool.QueryRow(ctx, `SELECT status FROM integration.documents WHERE id = $1`, doc.ID).Scan(&docStatus); err != nil {
				t.Fatal(err)
			}
			if docStatus != "NEEDS_ATTENTION" {
				t.Errorf("document = %s, want NEEDS_ATTENTION", docStatus)
			}

			var taskState, errorCode string
			err = pool.QueryRow(ctx, `SELECT status, error_code FROM integration.queue WHERE request_guid = $1`, guid).
				Scan(&taskState, &errorCode)
			if err != nil {
				t.Fatal(err)
			}
			if taskState != "NEEDS_ATTENTION" || errorCode != "DOCUMENT_COPY_FAILED" {
				t.Errorf("task: status=%s error=%s", taskState, errorCode)
			}

			// The task is stopped: its other document is no longer worked on.
			other := claimed[1]
			err = repo.Complete(ctx, other.ID, other.ClaimToken, copyResult(1))
			if err != nil {
				t.Fatal(err)
			}
			if got := taskStatus(t, pool, guid); got != "NEEDS_ATTENTION" {
				t.Errorf("task after other document completed = %s, want it to stay NEEDS_ATTENTION", got)
			}
		})
	}
}

func TestRenew(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	_, claimed := claimTask(t, pool, repo)
	doc := claimed[0]

	until, err := repo.Renew(ctx, doc.ID, doc.ClaimToken, 2*lease)
	if err != nil {
		t.Fatal(err)
	}
	if !until.After(doc.ClaimUntil) {
		t.Errorf("renewed claimUntil %v is not after %v", until, doc.ClaimUntil)
	}

	if _, err := repo.Renew(ctx, doc.ID, uuid.New(), lease); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("wrong token: err = %v, want ErrClaimLost", err)
	}
	if _, err := repo.Renew(ctx, uuid.New(), doc.ClaimToken, lease); !errors.Is(err, document.ErrDocumentNotFound) {
		t.Errorf("unknown document: err = %v, want ErrDocumentNotFound", err)
	}

	if err := repo.Complete(ctx, doc.ID, doc.ClaimToken, copyResult(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Renew(ctx, doc.ID, doc.ClaimToken, lease); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("after complete: err = %v, want ErrClaimLost", err)
	}
}

func TestRenew_ExpiredClaimCannotBeRevived(t *testing.T) {
	pool := newPool(t)
	repo := postgres.NewDocumentRepo(pool, queryTimeout)
	ctx := context.Background()
	registerCompareTasks(t, pool, 1)

	claimed, err := repo.Claim(ctx, "pod-1", 1, 50*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, err %v", claimed, err)
	}
	time.Sleep(200 * time.Millisecond)

	if _, err := repo.Renew(ctx, claimed[0].ID, claimed[0].ClaimToken, lease); !errors.Is(err, document.ErrClaimLost) {
		t.Errorf("err = %v, want ErrClaimLost", err)
	}
}
