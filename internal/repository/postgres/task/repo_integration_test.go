package task_test

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
	taskrepo "github.com/d9293/halyk-genai-integrator/internal/repository/postgres/task"
	"github.com/d9293/halyk-genai-integrator/internal/usecase/task"
)

// These tests need a real PostgreSQL with the schema from scripts/.
// `make test-integration` starts one in Docker and sets the URL; without it they are skipped.
const urlEnv = "INTEGRATION_POSTGRES_URL"

const queryTimeout = 5 * time.Second

var (
	camundaRequest = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	doc1           = domain.Document{DocumentID: uuid.MustParse("22222222-2222-4222-8222-222222222222"), AttstoreURL: "https://attstore.example/1"}
	doc2           = domain.Document{DocumentID: uuid.MustParse("33333333-3333-4333-8333-333333333333"), AttstoreURL: "https://attstore.example/2"}
)

// newPool connects to the test database and empties all tables.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(urlEnv)
	if url == "" {
		t.Skipf("%s not set; run `make test-integration`", urlEnv)
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	_, err = pool.Exec(context.Background(),
		`TRUNCATE integration.audit, integration.documents, integration.attempts, integration.queue`)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func newUseCase(t *testing.T, pool *pgxpool.Pool) *task.UseCase {
	t.Helper()
	uc, err := task.NewUseCase(taskrepo.NewRepo(pool, queryTimeout), task.Config{
		EnabledOperations: []domain.Operation{domain.OperationSocialMedia, domain.OperationRegisterCompare},
		AttstoreHosts:     []string{"attstore.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func registerCompare(requestGUID uuid.UUID) task.RegisterInput {
	return task.RegisterInput{
		CallerID:    "bpm",
		RequestGUID: requestGUID,
		RequestID:   camundaRequest,
		Operation:   domain.OperationRegisterCompare,
		Input:       json.RawMessage(`{}`),
		Documents:   []domain.Document{doc1, doc2},
	}
}

func socialMedia(requestGUID uuid.UUID) task.RegisterInput {
	return task.RegisterInput{
		CallerID:    "bpm",
		RequestGUID: requestGUID,
		RequestID:   camundaRequest,
		Operation:   domain.OperationSocialMedia,
		Input:       json.RawMessage(`{"iin":"000000000000","fullName":"Тест"}`),
		Documents:   []domain.Document{},
	}
}

// rowCounts returns how many queue, documents and audit rows exist.
func rowCounts(t *testing.T, pool *pgxpool.Pool) (tasks, documents, audit int) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM integration.queue),
		       (SELECT count(*) FROM integration.documents),
		       (SELECT count(*) FROM integration.audit)`).Scan(&tasks, &documents, &audit)
	if err != nil {
		t.Fatal(err)
	}
	return tasks, documents, audit
}

func TestRegister_WritesTaskDocumentsAndAudit(t *testing.T) {
	pool := newPool(t)
	uc := newUseCase(t, pool)
	ctx := context.Background()
	requestGUID := uuid.New()

	got, created, err := uc.Register(ctx, registerCompare(requestGUID))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt must come from the database")
	}

	rows, err := pool.Query(ctx, `
		SELECT document_id, position, status FROM integration.documents
		WHERE request_guid = $1 ORDER BY position`, requestGUID)
	if err != nil {
		t.Fatal(err)
	}
	var docIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		var position int
		var status string
		if err := rows.Scan(&id, &position, &status); err != nil {
			t.Fatal(err)
		}
		if position != len(docIDs) || status != "PENDING" {
			t.Errorf("document %s: position=%d status=%s", id, position, status)
		}
		docIDs = append(docIDs, id)
	}
	if len(docIDs) != 2 || docIDs[0] != doc1.DocumentID || docIDs[1] != doc2.DocumentID {
		t.Errorf("documents must be stored in BPM order, got %v", docIDs)
	}

	var actor, action, newState string
	err = pool.QueryRow(ctx, `SELECT actor, action, new_state FROM integration.audit WHERE request_guid = $1`, requestGUID).
		Scan(&actor, &action, &newState)
	if err != nil {
		t.Fatal(err)
	}
	if actor != "bpm" || action != "TASK_REGISTERED" || newState != string(domain.StatusWaitingDocuments) {
		t.Errorf("audit = %s %s %s", actor, action, newState)
	}
}

func TestRegister_SameDocumentsInSeveralTasks(t *testing.T) {
	pool := newPool(t)
	uc := newUseCase(t, pool)

	for range 3 {
		if _, _, err := uc.Register(context.Background(), registerCompare(uuid.New())); err != nil {
			t.Fatal(err)
		}
	}
	if tasks, documents, _ := rowCounts(t, pool); tasks != 3 || documents != 6 {
		t.Errorf("tasks=%d documents=%d, want 3 and 6", tasks, documents)
	}
}

func TestRegister_ReplayAndConflict(t *testing.T) {
	pool := newPool(t)
	uc := newUseCase(t, pool)
	ctx := context.Background()
	requestGUID := uuid.New()

	if _, _, err := uc.Register(ctx, socialMedia(requestGUID)); err != nil {
		t.Fatal(err)
	}

	// Same request again: a retry, nothing new stored.
	if _, created, err := uc.Register(ctx, socialMedia(requestGUID)); err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}

	changed := socialMedia(requestGUID)
	changed.Input = json.RawMessage(`{"iin":"000000000001","fullName":"Тест"}`)
	if _, _, err := uc.Register(ctx, changed); !errors.Is(err, task.ErrIdempotencyConflict) {
		t.Fatalf("changed input: err = %v", err)
	}

	otherCaller := socialMedia(requestGUID)
	otherCaller.CallerID = "someone-else"
	if _, _, err := uc.Register(ctx, otherCaller); !errors.Is(err, task.ErrRequestGUIDUnavailable) {
		t.Fatalf("other caller: err = %v", err)
	}

	if tasks, _, audit := rowCounts(t, pool); tasks != 1 || audit != 1 {
		t.Errorf("tasks=%d audit=%d, want 1 and 1", tasks, audit)
	}
}

// Like two API pods receiving the same request at the same time.
func TestRegister_ConcurrentIdenticalRequests(t *testing.T) {
	pool := newPool(t)
	otherPool, err := pgxpool.New(context.Background(), os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherPool.Close)

	instances := []*task.UseCase{newUseCase(t, pool), newUseCase(t, otherPool)}
	in := registerCompare(uuid.New())

	const requests = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		errs    []error
	)
	for i := range requests {
		wg.Go(func() {
			_, c, err := instances[i%2].Register(context.Background(), in)
			mu.Lock()
			defer mu.Unlock()
			if c {
				created++
			}
			if err != nil {
				errs = append(errs, err)
			}
		})
	}
	wg.Wait()

	if created != 1 || len(errs) != 0 {
		t.Fatalf("created=%d errors=%v, want exactly one creation and no errors", created, errs)
	}
	if tasks, documents, audit := rowCounts(t, pool); tasks != 1 || documents != 2 || audit != 1 {
		t.Errorf("tasks=%d documents=%d audit=%d, want 1, 2, 1", tasks, documents, audit)
	}
}

// A failure after the task row is inserted must leave nothing behind.
func TestCreateIfAbsent_RollsBackOnFailure(t *testing.T) {
	pool := newPool(t)
	repo := taskrepo.NewRepo(pool, queryTimeout)

	// The usecase rejects duplicate documents; calling the repo directly lets the
	// second document insert violate UNIQUE (request_guid, document_id).
	_, _, err := repo.CreateIfAbsent(context.Background(), domain.Task{
		RequestGUID: uuid.New(),
		RequestID:   camundaRequest,
		CallerID:    "bpm",
		Operation:   domain.OperationRegisterCompare,
		Input:       json.RawMessage(`{"adjudicate":true}`),
		RequestHash: "hash",
		Status:      domain.StatusWaitingDocuments,
		Documents:   []domain.Document{doc1, doc1},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if tasks, documents, audit := rowCounts(t, pool); tasks+documents+audit != 0 {
		t.Errorf("tasks=%d documents=%d audit=%d, want nothing stored", tasks, documents, audit)
	}
}

func TestGet(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	requestGUID := uuid.New()
	if _, _, err := newUseCase(t, pool).Register(ctx, socialMedia(requestGUID)); err != nil {
		t.Fatal(err)
	}

	// A fresh pool stands in for an API restart: the task must come from the database.
	restartedPool, err := pgxpool.New(ctx, os.Getenv(urlEnv))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restartedPool.Close)
	uc := newUseCase(t, restartedPool)

	got, err := uc.Get(ctx, "bpm", requestGUID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestID != camundaRequest || got.Operation != domain.OperationSocialMedia ||
		got.Status != domain.StatusReadyToSubmit || got.Result != nil || got.Error != nil {
		t.Errorf("unexpected task %+v", got)
	}

	if _, err := uc.Get(ctx, "someone-else", requestGUID); !errors.Is(err, task.ErrTaskNotFound) {
		t.Errorf("other caller: err = %v, want ErrTaskNotFound", err)
	}
	if _, err := uc.Get(ctx, "bpm", uuid.New()); !errors.Is(err, task.ErrTaskNotFound) {
		t.Errorf("missing task: err = %v", err)
	}
}

func TestUnreachableDatabase(t *testing.T) {
	newPool(t) // only to skip when integration tests are off

	// Nothing listens on port 1.
	pool, err := pgxpool.New(context.Background(), "postgres://admin:admin@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	_, _, err = newUseCase(t, pool).Register(context.Background(), socialMedia(uuid.New()))
	if !errors.Is(err, task.ErrStorageUnavailable) {
		t.Fatalf("err = %v, want ErrStorageUnavailable", err)
	}
}
