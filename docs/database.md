# Database model

Knowledge base for the Integrator.Api PostgreSQL schema: what each table and
column is for, and why the model looks the way it does. Agreed 2026-10-14.

Integrator.Api is the **only** application that reads or writes these tables.
worker.Request and worker.Response never connect to PostgreSQL; they ask the
API for work and report results through its internal HTTP endpoints.

## Overview

All tables live in the schema **`integration`**.

| Table | One row per | Purpose | Created in |
|---|---|---|---|
| [`queue`](#integrationqueue) | Task (`requestGuid`) | What BPM asked, overall state, final result, delivery of the result to BPM via Kafka | Step 2 (Kafka delivery columns: step 6) |
| [`documents`](#integrationdocuments) | Input file of a task | Copy Attstore → S3 before the task can be sent to AI | Step 2 |
| [`audit`](#integrationaudit) | State change | Append-only history of tasks, documents and attempts | Step 2 |
| [`attempts`](#integrationattempts) | POST to the AI team | One send to AI and everything learned by polling it | Step 4 |

Deliberately not separate tables: the AI limit is counted from active
`attempts`; Kafka delivery lives on `queue`; repeated worker calls are
recognized from the data (see [Decisions](#decisions)). Not designed yet: a
global "pause submissions" switch, for the admin panel later.

```
BPM POST /api/v1/tasks
        │
        ▼
   queue ─────────────┬──▶ documents    files to copy Attstore → S3
   (1 row per task)   │                 (operations with files only)
        │             │
        │             └──▶ audit        every state change
        │
        │ POST to AI (worker.Request)
        ▼
   attempts           1 row per POST; GET polls update the same row
   (queue.active_attempt_id → current one)
```

### Lifecycle of a task

```
                 ┌── operation without files ───────────────┐
registration ────┤                                          ▼
                 └── WAITING_DOCUMENTS → PREPARING_DOCUMENTS → READY_TO_SUBMIT
                                                                │  POST (worker.Request)
                                                                ▼
                                                           SUBMITTING
                                     ┌──────────────┬───────────┼─────────────────┐
                                     ▼              ▼           ▼                 ▼
                          SUBMISSION_UNKNOWN  REMOTE_QUEUED → REMOTE_RUNNING   (rejected →
                          (timeout; resolved        │               │          READY_TO_SUBMIT
                           by polling)              └───────┬───────┘          with retry_at)
                                                            ▼  GET polls (worker.Response)
                                                  SUCCEEDED / FAILED

AI lost the task (3× NOT_FOUND) → attempt LOST → task back to READY_TO_SUBMIT, retry_count + 1
No safe automatic way forward  → NEEDS_ATTENTION (a person has to look)
```

## Conventions

These apply to every table.

| Convention | Meaning |
|---|---|
| IDs are `uuid` | Except `audit.audit_id`, which is an identity `bigint`. |
| Times are `timestamptz`, UTC | Every table has `created_at` and `updated_at` (`NOT NULL DEFAULT now()`). |
| Enums are `text` + `CHECK` | Not PostgreSQL `ENUM` types: a `CHECK` is easier to change in a migration. |
| `retry_at` | `NULL` = may be processed **now**. A timestamp = wait until then. Set only after a failure. |
| `retry_count` | How many times the row was retried after a failure. Starts at `0`. |
| `error_code` + `error_detail` | Why the row failed or needs attention. Both set or both `NULL` (`CHECK`). Never contains IIN, full name or signed URLs. |
| `claim_token` + `claim_until` + `claim_owner` | Worker ownership of a row, see [Claims](#claims). All three set or all `NULL` (`CHECK`). |

### Claims

Several worker pods run at once. To stop two pods from processing the same row,
the API **claims** the row for one worker when handing it out:

| Column | Purpose |
|---|---|
| `claim_token` | Random UUID given to the worker. It must send the token back when saving a result; this is the proof of ownership. |
| `claim_until` | When ownership expires (e.g. now + 60 s). The worker can renew it while still working. |
| `claim_owner` | Name of the pod holding the claim (the worker sends its `HOSTNAME`, which in Kubernetes is the unique pod name). For debugging only; never checked. |

```
pod-1 asks for work → API sets claim_token / claim_until / claim_owner and returns the row
pod-2 asks for work → the claimed row is skipped (SELECT ... FOR UPDATE SKIP LOCKED)
pod-1 saves result  → API checks the token matches and claim_until has not passed
```

If a pod dies, `claim_until` passes and another pod can take the row. If a
frozen pod wakes up later with its old token, its save is rejected, so it can
never overwrite newer state.

---

## `integration.queue`

One row per task (`requestGuid`). It holds what BPM asked for (never changed),
where the task is, its sending to AI, its final outcome, and the delivery of
that outcome to BPM through Kafka. One task produces exactly one Kafka message,
so the delivery state lives on the task row itself (see [Decisions](#decisions)).

### Columns

| Column | Type | Null | Default / check | Purpose | At registration |
|---|---|---|---|---|---|
| **Identity** | | | | | |
| `request_guid` | `uuid` | NOT NULL | **PK** | BPM's unique ID of this task. Idempotency key: one `requestGuid` = one task; BPM reuses it only when retrying the *same* registration. Also identifies the task's Kafka message: BPM deduplicates by it. Never sent to AI. | BPM `requestGuid` |
| `request_id` | `uuid` | NOT NULL | indexed | The Camunda request the task belongs to. **Not unique**: one Camunda request can have many tasks, even several with the same operation. Kafka message key. | BPM `requestId` |
| `caller_id` | `text` | NOT NULL | | Who registered the task. Fixed from config while auth is off; later the authenticated principal. GET only returns the caller's own tasks. | `bpm` |
| **What was asked** (never changes) | | | | | |
| `operation` | `text` | NOT NULL | | Operation code: `SOCIAL_MEDIA`, `REGISTER_COMPARE`. Selects validation and the AI endpoint. | from request |
| `request_payload` | `jsonb` | NOT NULL | | The operation input, **normalized** (fixed key order, defaults filled in). Sent to AI on every POST. | normalized `input` |
| `request_hash` | `text` | NOT NULL | | SHA-256 of requestId + operation + input + documents. A repeated registration with the same `requestGuid` but a different hash is rejected (`409`). | computed |
| **State** | | | | | |
| `status` | `text` | NOT NULL | `CHECK`, see [Task statuses](#task-statuses) | Where the task is overall. | `READY_TO_SUBMIT` or `WAITING_DOCUMENTS` |
| `active_attempt_id` | `uuid` | NULL | FK → `attempts` | The current, or last, POST to AI. `NULL` before the first send. Kept after the task ends, for diagnostics. | `NULL` |
| **Sending to AI** | | | | | |
| `retry_at` | `timestamptz` | NULL | | Don't send to AI before this time. `NULL` = as soon as an AI place is free. Not set by the current flows; reserved for delaying a re-send. | `NULL` |
| `retry_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | How many times the task was re-sent because AI **lost** it (`NOT_FOUND` rule). After `MAX_RESUBMISSIONS` the task fails. Other failures do not count. | `0` |
| **Outcome** | | | | | |
| `result_payload` | `jsonb` | NULL | | AI's result JSON **as AI returned it**, report links included. Only for `SUCCEEDED`. (`jsonb` may reorder keys and whitespace; the content is identical.) | `NULL` |
| `error_code` | `text` | NULL | | Why the task is `FAILED` or `NEEDS_ATTENTION`, e.g. `AI_TASK_LOST_RETRIES_EXHAUSTED`, `RESULT_TOO_LARGE`, `DOCUMENT_COPY_FAILED`, or AI's own code. | `NULL` |
| `error_detail` | `text` | NULL | | Human-readable explanation of `error_code`. No personal data. | `NULL` |
| `completed_at` | `timestamptz` | NULL | | When the task became `SUCCEEDED` or `FAILED`. Sent as `occurredAt` in the Kafka message. | `NULL` |
| **Delivery to BPM via Kafka** | | | | | |
| `published_at` | `timestamptz` | NULL | | When Kafka confirmed (broker ACK) the task's result message. `NULL` = still to deliver. It does not mean BPM has processed it. | `NULL` |
| `publish_retry_at` | `timestamptz` | NULL | | After a failed publish: don't try again before this time. `NULL` = now. | `NULL` |
| `publish_retry_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | Failed publish tries; drives the backoff. The message is never dropped, however high it gets. | `0` |
| `publish_claim_token` | `uuid` | NULL | | Proof of ownership while a worker publishes; see [Claims](#claims). | `NULL` |
| `publish_claim_until` | `timestamptz` | NULL | | When that ownership expires. | `NULL` |
| `publish_claim_owner` | `text` | NULL | | Which pod is publishing. Debugging only. | `NULL` |
| `publish_error_code` | `text` | NULL | | Last publish error, e.g. `KAFKA_UNAVAILABLE`. | `NULL` |
| `publish_error_detail` | `text` | NULL | | Its details. | `NULL` |
| **Timestamps** | | | | | |
| `created_at` | `timestamptz` | NOT NULL | `now()` | Registration time. | `now()` |
| `updated_at` | `timestamptz` | NOT NULL | `now()` | Last change of the row. | `now()` |

The prefix `publish_` keeps delivering to BPM (Kafka) apart from sending to AI
(`retry_*`, `error_*` without prefix).

### Task statuses

```
                ┌── operation without files ──────────────────────────┐
registration ───┤                                                     ▼
                └── WAITING_DOCUMENTS → PREPARING_DOCUMENTS ──▶ READY_TO_SUBMIT ◀── AI lost it (re-send)
                                                                      │
                                                                  SUBMITTING
                                       ┌──────────────────────────────┼──────────────┐
                                       ▼                              ▼              ▼
                              SUBMISSION_UNKNOWN ───▶ REMOTE_QUEUED ─▶ REMOTE_RUNNING   FAILED (rejected)
                                                            │              │
                                                            └──────┬───────┘
                                                                   ▼
                                                     SUCCEEDED / FAILED / NEEDS_ATTENTION
```

| Status | Meaning | Set by | Next | Holds an AI place | Kafka message |
|---|---|---|---|---|---|
| `WAITING_DOCUMENTS` | Registered; its files are waiting to be copied Attstore → S3. | registration (operation with files) | `PREPARING_DOCUMENTS` | no | no |
| `PREPARING_DOCUMENTS` | worker.Request is copying the files; some may be done. | first `documents/claim` | `READY_TO_SUBMIT`, `NEEDS_ATTENTION` | no | no |
| `READY_TO_SUBMIT` | Everything prepared; waits for a free AI place (`AI_MAX_IN_FLIGHT`). | registration (no files), last `documents/complete`, lost-task re-send | `SUBMITTING` | no | no |
| `SUBMITTING` | worker.Request is sending the POST to AI right now (owns the attempt). | `submissions/claim` | `REMOTE_QUEUED`, `REMOTE_RUNNING`, `SUBMISSION_UNKNOWN`, `FAILED` | **yes** | no |
| `SUBMISSION_UNKNOWN` | The POST got no reliable answer (timeout, lost response, sender died). AI may or may not have it; polling finds out. **Never** re-sent blindly. | outcome `UNKNOWN`, recovery | `REMOTE_QUEUED`, `REMOTE_RUNNING`, `SUCCEEDED`, `FAILED`, `READY_TO_SUBMIT` (lost) | **yes** | no |
| `REMOTE_QUEUED` | AI confirmed it has the task; waiting in AI's own queue. | outcome `ACCEPTED`, poll `QUEUED` | `REMOTE_RUNNING`, `SUCCEEDED`, `FAILED`, `READY_TO_SUBMIT` (lost) | **yes** | no |
| `REMOTE_RUNNING` | AI is working on it. | outcome `ACCEPTED` (processing), poll `PROCESSING` | `SUCCEEDED`, `FAILED`, `NEEDS_ATTENTION` (too big a result, or no progress), `READY_TO_SUBMIT` (lost) | **yes** | no |
| `NEEDS_ATTENTION` | A person should look; `error_code` says why. Not final. `DOCUMENT_COPY_FAILED` / `RESULT_TOO_LARGE`: stuck until someone acts. `AI_NO_PROGRESS`: still at AI and still polled; a final answer from AI later finishes it normally. | `documents/failure` (out of retries), poll `SUCCEEDED` with an oversized result, any non-final poll answer more than `AI_NO_PROGRESS_TIMEOUT` after sending | manual action ([admin panel](admin-panel.md), later); for `AI_NO_PROGRESS` also `SUCCEEDED` / `FAILED` / `READY_TO_SUBMIT` (lost) | only for `AI_NO_PROGRESS` | no (visible via GET) |
| `SUCCEEDED` | **Final.** AI finished; `result_payload` holds its result. | poll `SUCCEEDED` | — | no | **yes** |
| `FAILED` | **Final.** `error_code` / `error_detail` say why: AI's business error, AI rejected the task, or AI lost it too often. | poll `FAILED`, outcome `REJECTED`, lost-task rule | — | no | **yes** |

"Holds an AI place" follows the task's attempt: the place is counted from
active attempts (see [`attempts`](#integrationattempts)), not from this status.

### Constraints and indexes

| Name | Rule |
|---|---|
| `queue_status_check` | `status` is one of the statuses above. |
| `queue_retry_count_check` | `retry_count >= 0`. |
| `queue_completed_at_check` | `completed_at` is set exactly when `status` is `SUCCEEDED` or `FAILED`. |
| `queue_error_check` | `error_code` and `error_detail` both set or both `NULL`. |
| `queue_active_attempt_fk` | `(request_guid, active_attempt_id)` → `attempts (request_guid, attempt_id)`: a task can only point to its own attempt. |
| `queue_published_check` | `published_at` can only be set on a `SUCCEEDED` or `FAILED` task. |
| `queue_publish_retry_count_check` | `publish_retry_count >= 0`. |
| `queue_publish_claim_check` | The three `publish_claim_*` columns are all set or all `NULL`. |
| `queue_publish_error_check` | `publish_error_code` and `publish_error_detail` both set or both `NULL`. |
| `queue_request_id_idx` | Index on `request_id`. |
| `queue_ready_idx` | Partial index on `created_at` where `status = 'READY_TO_SUBMIT'`: next task to send. |
| `queue_unpublished_idx` | Partial index on `completed_at` where the task is final and not yet published: next message to deliver. |

### How workers pick tasks

```sql
-- submissions/claim: the next task to send to AI (under the AI limit)
WHERE status = 'READY_TO_SUBMIT' AND (retry_at IS NULL OR retry_at <= now())
ORDER BY created_at

-- outbox/claim: the next result to deliver to BPM
WHERE status IN ('SUCCEEDED', 'FAILED') AND published_at IS NULL
  AND (publish_retry_at IS NULL OR publish_retry_at <= now())
  AND (publish_claim_until IS NULL OR publish_claim_until < now())
ORDER BY completed_at
```

### Delivering results to BPM (worker.Response)

The API never talks to Kafka: worker.Response publishes and reports back.
These calls only touch the `published_at` / `publish_*` columns; `updated_at`
is left alone because BPM sees it, and the task itself does not change.

| Call | What happens |
|---|---|
| `results/claim` | Up to `limit` final tasks with `published_at IS NULL`, due (`publish_retry_at`) and not being published (`SKIP LOCKED`, index `queue_unpublished_idx`), oldest finished first. Each gets a `publish_claim_token` for `RESULT_CLAIM_LEASE`. Returns the topic (`KAFKA_RESULT_TOPIC`), key (`request_id`) and the message built from the row. |
| `results/{requestGuid}/published` | After the broker ACK: `published_at = now()`, claim released. Never handed out again. Repeating it returns OK. A late call with an expired claim is refused (`409`); the result may then be published again, which BPM handles by deduplicating on `requestGuid`. |
| `results/{requestGuid}/failure` | Kafka did not accept it: claim released, error saved, `publish_retry_at = now() + RESULT_RETRY_BASE_DELAY · 2^(earlier failures)`, at most `RESULT_RETRY_MAX_DELAY`. Never dropped. |

### Kafka message (v1)

One message per task when it becomes `SUCCEEDED` or `FAILED`, built from the
row. Key: `request_id`. Delivery is at-least-once: BPM may receive a message
twice and must ignore repeats by `requestGuid`.

```json
{
  "schemaVersion": 1,
  "eventType": "TASK_COMPLETED",
  "occurredAt": "2026-10-16T10:00:00Z",
  "requestId": "11111111-1111-4111-8111-111111111111",
  "requestGuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "operation": "REGISTER_COMPARE",
  "status": "SUCCEEDED",
  "result": { "summary": { "counts": { "both": 12 } }, "report_url": "https://…" },
  "error": null
}
```

| Field | From |
|---|---|
| `occurredAt` | `completed_at`: when the task finished, not when the message was sent |
| `requestId`, `requestGuid`, `operation`, `status` | the same columns |
| `result` | `result_payload` (`null` for `FAILED`) |
| `error` | `{code, message}` from `error_code` / `error_detail` (`null` for `SUCCEEDED`) |

---

## `integration.documents`

One row per input file of a task. Only operations with files have rows here
(e.g. `REGISTER_COMPARE` has exactly two; `SOCIAL_MEDIA` has none). AI cannot
reach Attstore, so worker.Request copies each file to S3 through the existing
S3 API, and AI receives the S3 link.

| Column | Type | Null | Default / check | Purpose | At registration |
|---|---|---|---|---|---|
| `id` | `uuid` | NOT NULL | **PK** | Our ID for this file in this task, generated at registration. Workers address documents by it. | generated |
| `request_guid` | `uuid` | NOT NULL | FK → `queue` | The task this file belongs to. | the task |
| `document_id` | `uuid` | NOT NULL | | BPM's ID for the file. Unique within one task only: the same file may be used by several tasks. | BPM `documentId` |
| `position` | `int` | NOT NULL | `CHECK (>= 0)` | Index in BPM's `documents[]` array. Keeps BPM's order: AI receives `files[]` in this order, and AI results refer to files by position (e.g. `only_a` = position 0). | `0`, `1`, … |
| `attstore_url` | `text` | NOT NULL | | Where to download the file: any `http(s)` link, without credentials in it. Never changes; files are assumed not to change in Attstore. | BPM `attstoreUrl` |
| `status` | `text` | NOT NULL | `'PENDING'`, `CHECK`, see below | Copy progress. | `PENDING` |
| `s3_uri` | `text` | NULL | | Link to the copy in S3 that AI receives, e.g. `s3://ai-input/...`. S3 objects are permanent, so a re-send to AI reuses it without copying again. | `NULL` |
| `size_bytes` | `bigint` | NULL | `CHECK (>= 0)` | Size of the copied file, reported by the worker. | `NULL` |
| `checksum_sha256` | `text` | NULL | | SHA-256 of the copied file, reported by the worker. For traceability. | `NULL` |
| `retry_at` | `timestamptz` | NULL | | When the next copy attempt may happen. `NULL` = now. Backoff 5 s, 10 s, 20 s … max 5 min. | `NULL` |
| `retry_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | Failed copy attempts. After 5 the document (and task) go to `NEEDS_ATTENTION`. | `0` |
| `claim_token` | `uuid` | NULL | | See [Claims](#claims). | `NULL` |
| `claim_until` | `timestamptz` | NULL | | See [Claims](#claims). | `NULL` |
| `claim_owner` | `text` | NULL | | See [Claims](#claims). | `NULL` |
| `error_code` | `text` | NULL | | Why the last copy failed, e.g. `SOURCE_UNAVAILABLE`. | `NULL` |
| `error_detail` | `text` | NULL | | Explanation for `error_code`. | `NULL` |
| `created_at` | `timestamptz` | NOT NULL | `now()` | | `now()` |
| `updated_at` | `timestamptz` | NOT NULL | `now()` | | `now()` |

### Document statuses

| Status | Meaning |
|---|---|
| `PENDING` | Waiting to be copied (first time, or after a failed attempt once `retry_at` passes). |
| `COPYING` | Claimed by a worker that is copying it now. |
| `READY` | Copied. `s3_uri`, `size_bytes`, `checksum_sha256` are set. When **all** documents of a task are `READY`, the task becomes `READY_TO_SUBMIT`. |
| `NEEDS_ATTENTION` | Copy failed permanently or ran out of retries. |

### Constraints

| Constraint | Rule |
|---|---|
| `documents_task_position_key` | `UNIQUE (request_guid, position)`. |
| `documents_task_document_key` | `UNIQUE (request_guid, document_id)`: the same file cannot appear twice in one task. |
| `documents_position_check` | `position >= 0`. |
| `documents_status_check` | `status` is one of the statuses above. |
| `documents_size_check` | `size_bytes >= 0`. |
| `documents_retry_count_check` | `retry_count >= 0`. |
| `documents_ready_check` | `status = 'READY'` exactly when `s3_uri`, `size_bytes` and `checksum_sha256` are all set. |
| `documents_claim_check` | `claim_*` all set or all `NULL`. |
| `documents_error_check` | `error_code` and `error_detail` both set or both `NULL`. |
| `documents_claimable_idx` | Partial index on `created_at` where `status` is `PENDING` or `COPYING`: keeps the claim query fast. |

### Copying documents (worker.Request)

The API never touches the files. worker.Request claims documents, copies them
Attstore → S3, and reports back through `/internal/v1/documents/...`:

```
PENDING ──claim──▶ COPYING ──complete──▶ READY
   ▲                  │
   └──failure─────────┤  retryable and under the limit: retry_at = now + 5s, 10s, 20s … max 5m
                      └──failure (permanent, or 5th failure) ──▶ NEEDS_ATTENTION (task too)
COPYING with expired claim_until ──▶ can be claimed again by another worker
```

| Call | What happens |
|---|---|
| `claim` | Up to `limit` documents that are `PENDING` and due, or `COPYING` with an expired claim, of tasks still waiting for documents. Oldest first. `FOR UPDATE SKIP LOCKED`, so parallel workers never get the same document. Each gets a new `claim_token` and `claim_until = now() + DOCUMENT_CLAIM_LEASE`. The task's first claim moves it `WAITING_DOCUMENTS` → `PREPARING_DOCUMENTS`. |
| `renew` | `claim_until = now() + lease`, only while the claim is still alive. An expired claim cannot be revived. |
| `complete` | Document → `READY` with its S3 copy. The task's last `READY` document moves the task to `READY_TO_SUBMIT`. Repeating the call with the same data returns OK and writes nothing. |
| `failure` | Retryable and under `DOCUMENT_MAX_COPY_ATTEMPTS`: back to `PENDING` with backoff. Otherwise document and task → `NEEDS_ATTENTION` (`error_code = DOCUMENT_COPY_FAILED`). Repeating the call gets `409 CLAIM_LOST`: the claim was already released. |

Every call is one short transaction; nothing stays locked while the worker
copies. The worker's ownership lives in the `claim_*` columns, not in a database lock.

**Lock order** in every transaction: document row(s), then the task row, then
writes. Several tasks are locked in `request_guid` order. The task is locked
before any audit row is written, because the audit foreign key takes a shared
lock on the task; two transactions holding that shared lock while waiting for
the exclusive one would deadlock. Parallel completions of one task's documents
therefore wait for each other on the task row, and exactly one of them moves
the task to `READY_TO_SUBMIT`.

---

## `integration.attempts`

Created in step 4. One row per **POST to the AI team**. The GET polls for that
POST update the same row, so 1000 polls are still one row. A task sent 3 times
has 3 rows; only the latest is active and `queue.active_attempt_id` points to it.

The request sent to AI is not stored here: it is rebuilt on every POST from
`queue.request_payload`, `documents.s3_uri` (by `position`) and `attempt_id`.

| Column | Type | Null | Default / check | Purpose |
|---|---|---|---|---|
| `attempt_id` | `uuid` | NOT NULL | **PK** | Our ID for this POST, generated by the API. Sent to AI as its `task_id`. A re-send gets a new one. |
| `request_guid` | `uuid` | NOT NULL | FK → `queue` | The task. |
| `attempt_no` | `int` | NOT NULL | `CHECK (>= 1)` | Sequence number of this POST within the task: 1, 2, 3 … For history and readability. |
| `state` | `text` | NOT NULL | `CHECK`, see below | Where this POST is. |
| `poll_at` | `timestamptz` | NULL | | When worker.Response should next GET the status. Set to about now + 10–20 s after each "still working" answer. |
| `poll_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | GET polls made so far. Lets endless polling be detected and flagged. |
| `not_found_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | AI answers "task not found" (404) **in a row**. Any normal answer resets it to 0. |
| `first_not_found_at` | `timestamptz` | NULL | | Start of the current not-found streak. The task is declared lost only after 3 in a row over at least 60 s. |
| `claim_token` | `uuid` | NULL | | See [Claims](#claims). |
| `claim_until` | `timestamptz` | NULL | | See [Claims](#claims). |
| `claim_owner` | `text` | NULL | | See [Claims](#claims). |
| `error_code` | `text` | NULL | | Last technical error (timeout, 5xx …). |
| `error_detail` | `text` | NULL | | Explanation for `error_code`. |
| `created_at` | `timestamptz` | NOT NULL | `now()` | When the POST started. |
| `updated_at` | `timestamptz` | NOT NULL | `now()` | |
| `finished_at` | `timestamptz` | NULL | | When we saw AI finish (done or error). |

### Attempt states

| State | Active | Meaning |
|---|---|---|
| `SUBMITTING` | yes | POST in progress. |
| `UNKNOWN` | yes | POST timed out or its response was lost. Polled to find out whether AI has it. |
| `QUEUED` | yes | AI accepted it; waiting in AI's queue. |
| `PROCESSING` | yes | AI is working on it. |
| `NOT_ACCEPTED` | no | AI clearly rejected the POST, or it never left. The task gets `retry_at`. |
| `DONE` | no | AI finished; the result is saved in `queue.result_payload`. |
| `ERROR` | no | AI returned a business error. |
| `LOST` | no | AI answered not-found 3 times in a row (it forgets tasks on restart). The task is re-sent with a new attempt and `queue.retry_count + 1`. |

### Constraints

| Constraint | Rule |
|---|---|
| `attempts_state_check` | `state` is one of the states above. |
| `attempts_attempt_no_check` | `attempt_no >= 1`. |
| `attempts_counts_check` | `poll_count >= 0` and `not_found_count >= 0`. |
| `attempts_task_attempt_no_key` | `UNIQUE (request_guid, attempt_no)`. |
| `attempts_task_attempt_key` | `UNIQUE (request_guid, attempt_id)`; target of `queue_active_attempt_fk`. |
| `attempts_one_active_idx` | Partial unique index on `request_guid` where `state` is active: at most one POST in flight per task. |
| `attempts_claim_check` | `claim_*` all set or all `NULL`. |
| `attempts_error_check` | `error_code` and `error_detail` both set or both `NULL`. |

### Sending tasks to AI (worker.Request)

The limit of tasks at AI is `AI_MAX_IN_FLIGHT` (10). A task holds an AI place
while its attempt is **active**: `SUBMITTING`, `UNKNOWN`, `QUEUED` or
`PROCESSING`, i.e. from the moment we start sending until we know for sure
AI is done with it or never had it. There is no separate slots table: the
count comes from `attempts` (index `attempts_one_active_idx`).

| Call | What happens |
|---|---|
| `submissions/claim` | One short transaction: `pg_advisory_xact_lock` (claims take turns for a few ms, so two claims can never both see the last free place) → count active attempts → if places are free, the oldest due `READY_TO_SUBMIT` tasks (`SKIP LOCKED`, index `queue_ready_idx`) → for each a new attempt (`attempt_no` = previous + 1) in `SUBMITTING` with a claim, task → `SUBMITTING` with `active_attempt_id`, audit. Returns the stored input and the documents' `s3_uri` in `position` order. |
| `submissions/{attemptId}/outcome` | `ACCEPTED` → attempt `QUEUED`/`PROCESSING`, task `REMOTE_QUEUED`/`REMOTE_RUNNING`. `UNKNOWN` → attempt `UNKNOWN`, task `SUBMISSION_UNKNOWN`. Both keep the place and set `poll_at = now() + SUBMISSION_POLL_DELAY`. `REJECTED` → attempt `NOT_ACCEPTED`, task `FAILED` with the error: the place is freed. The same report repeated after a lost response returns OK and writes nothing. |
| `submissions/{attemptId}/renew` | Extends a live `SUBMITTING` claim. |

Lock order: attempt, then task, then writes (claim: advisory lock, then tasks).

**Recovery:** if a worker dies between claim and outcome, its attempt stays
`SUBMITTING`. The first step of every `polls/claim` turns such attempts with
an expired claim into `UNKNOWN`, so polling finds out whether AI has them.
They are never re-sent blindly.

### Checking task status (worker.Response)

worker.Response asks the API which attempts are due for a status check,
makes one GET to AI per attempt, and reports the answer.

| Call | What happens |
|---|---|
| `polls/claim` | First **recovery**: attempts still `SUBMITTING` after their claim expired (the sender died) → `UNKNOWN`, due now, task `SUBMISSION_UNKNOWN`, audit `SUBMISSION_RECOVERED` by `system`. They keep their AI place and are never re-sent blindly. Then up to `limit` attempts in `UNKNOWN`/`QUEUED`/`PROCESSING` with `poll_at <= now()` and no live claim (`SKIP LOCKED`, index `attempts_pollable_idx`), each claimed for `POLL_CLAIM_LEASE` (one GET). `SUBMITTING` with a live claim is never touched. |
| `polls/{attemptId}/observation` | Applies AI's answer, see below, and releases the claim. |

| AI answered | Attempt | Task | AI place | Next check |
|---|---|---|---|---|
| `QUEUED` / `PROCESSING` | same (a late `QUEUED` after `PROCESSING` keeps `PROCESSING`); NOT_FOUND streak reset | `REMOTE_QUEUED` / `REMOTE_RUNNING` | kept | `POLL_INTERVAL` ± jitter |
| `SUCCEEDED` | `DONE` | `SUCCEEDED`, `result_payload` = AI's JSON as-is | freed | none |
| `SUCCEEDED`, result > `RESULT_MAX_BYTES` | `DONE` | `NEEDS_ATTENTION`, `RESULT_TOO_LARGE`, no result stored | freed | none |
| `FAILED` | `ERROR` | `FAILED` with AI's error | freed | none |
| `NOT_FOUND` | `not_found_count + 1` | unchanged | kept | `POLL_NOT_FOUND_DELAY` |
| `NOT_FOUND`, the `LOST_TASK_NOT_FOUND_THRESHOLD`th in a row and the first ≥ `LOST_TASK_MIN_WINDOW` ago | `LOST` | `READY_TO_SUBMIT`, `retry_count + 1` (re-sent as a new attempt), or after `MAX_RESUBMISSIONS` re-sends `FAILED` `AI_TASK_LOST_RETRIES_EXHAUSTED` | freed | none |
| `TECHNICAL_ERROR` | unchanged; error saved; NOT_FOUND streak neither extended nor reset | unchanged | kept | `POLL_ERROR_DELAY` |
| any answer that leaves the attempt at AI, more than `AI_NO_PROGRESS_TIMEOUT` (2h) after it was sent | as above | **`NEEDS_ATTENTION`** `AI_NO_PROGRESS`, once; a later `QUEUED`/`PROCESSING` does not clear it, a final answer does | kept | as above |

`NOT_FOUND` means AI's confirmed "task not found" (`404` with
`{"detail": "Задача не найдена: <task_id>"}`). A `404` from a proxy or wrong
route is a `TECHNICAL_ERROR` and never leads to a re-send.

**Repeats** (option B): an observation without a live claim that reports the
state the attempt is already in (`QUEUED`, `PROCESSING`, `SUCCEEDED` → `DONE`,
`FAILED` → `ERROR`) returns OK and writes nothing. Anything else without a live
claim is `409`; in particular a repeated `NOT_FOUND` or `TECHNICAL_ERROR` is
never applied twice.

Lock order: attempt, then task, then writes. Concurrent copies of one
observation apply exactly once.

**No-progress safety net:** a task still at AI more than `AI_NO_PROGRESS_TIMEOUT`
after its attempt was sent (counted from `attempts.created_at`; a re-send
starts a new count) becomes `NEEDS_ATTENTION` with `AI_NO_PROGRESS`, so a
person notices. It keeps being polled and keeps its AI place: AI may still be
working. Only checked when the task is polled, so it does not fire while no
worker.Response runs.

---

## `integration.audit`

Append-only history: one row per **state change** of a task, document or
attempt. Rows are never updated or deleted. Answers "what happened to task X,
and when?".

| Column | Type | Null | Default / check | Purpose |
|---|---|---|---|---|
| `audit_id` | `bigint` | NOT NULL | **PK**, identity | Auto number. |
| `request_guid` | `uuid` | NOT NULL | FK → `queue` | The task. |
| `document_id` | `uuid` | NULL | FK → `documents.id` | Set when the event is about a document. |
| `attempt_id` | `uuid` | NULL | FK → `attempts` (step 4) | Set when the event is about a POST / poll. |
| `actor` | `text` | NOT NULL | | Who caused it: the caller (`bpm`), a worker pod name, or `system` (the API itself, e.g. an expired claim). |
| `action` | `text` | NOT NULL | | What happened, e.g. `TASK_REGISTERED`, `DOCUMENT_READY`, `ATTEMPT_LOST`, `TASK_SUCCEEDED`. No `CHECK`: the list grows with each step and is kept as constants in Go. |
| `old_state` | `text` | NULL | | State before. `NULL` on creation. |
| `new_state` | `text` | NULL | | State after. |
| `detail` | `text` | NULL | | Optional reason, e.g. an error code. No IIN, full name or signed URLs. |
| `created_at` | `timestamptz` | NOT NULL | `now()` | When it happened. |

Index: `audit_task_idx (request_guid, created_at)` for "history of task X, in order".

Rules:

- Only **real** state changes are written. A poll that returns "still processing" is not, otherwise one long task would add thousands of rows.
- No retention policy yet; the table grows forever. Fine at current volume.

Example history of one `REGISTER_COMPARE` task:

| actor | action | about | old → new | detail |
|---|---|---|---|---|
| `bpm` | `TASK_REGISTERED` | | → `WAITING_DOCUMENTS` | |
| `worker-request-x2kqp` | `DOCUMENTS_PREPARING` | | `WAITING_DOCUMENTS` → `PREPARING_DOCUMENTS` | |
| `worker-request-x2kqp` | `DOCUMENT_COPY_FAILED` | document 1 | `COPYING` → `PENDING` | `SOURCE_UNAVAILABLE` |
| `worker-request-x2kqp` | `DOCUMENT_READY` | document 0 | `COPYING` → `READY` | |
| `worker-request-x2kqp` | `DOCUMENT_READY` | document 1 | `COPYING` → `READY` | |
| `worker-request-x2kqp` | `TASK_READY` | | `PREPARING_DOCUMENTS` → `READY_TO_SUBMIT` | |
| `worker-request-x2kqp` | `ATTEMPT_ACCEPTED` | attempt 1 | `SUBMITTING` → `QUEUED` | |
| `worker-response-m8zt1` | `ATTEMPT_LOST` | attempt 1 | `PROCESSING` → `LOST` | `AI_TASK_NOT_FOUND` |
| `worker-response-m8zt1` | `TASK_SUCCEEDED` | attempt 2 | `REMOTE_RUNNING` → `SUCCEEDED` | |

---

## Task registration

`POST /api/v1/tasks` runs one transaction:

```
BEGIN
  INSERT INTO queue ... ON CONFLICT (request_guid) DO NOTHING
  ├─ nothing inserted → read the existing row
  │     different caller_id     → 409 REQUEST_GUID_UNAVAILABLE
  │     different request_hash  → 409 IDEMPOTENCY_CONFLICT
  │     same                    → 200, current state (a retry by BPM)
  └─ inserted → INSERT documents (one per file, new id, PENDING, position = array index)
              → INSERT audit (TASK_REGISTERED)
COMMIT → only now respond 202
```

| Operation | Rows written |
|---|---|
| `SOCIAL_MEDIA` | 1 `queue` + 1 `audit` |
| `REGISTER_COMPARE` | 1 `queue` + 2 `documents` + 1 `audit` |

Any failure rolls the whole registration back; there are never partial documents.

---

## Decisions

Deliberate differences from the original spec (`Final Requirements/Integrator.Api/storage.md`), agreed 2026-10-14.

| Decision | Reason |
|---|---|
| Schema `integration` (spec: `aitask`); tables `queue`, `documents`, `attempts`, `audit` | Naming. |
| `operation` stays a `text` code, no reference table | An operation is code (validation, AI mapping), not data. A reference table can be added later with the code as its PK, with no data migration. |
| Removed `operation_version` | One input format per operation for now. Adding versions later needs a migration and an API change. |
| Kept `request_hash` | Catches BPM reusing a `requestGuid` with different data, which would otherwise deliver one person's result into another process. Costs microseconds. |
| Removed `state_version` | Only one Kafka event per task; BPM does not need change counters. |
| Removed `result_policy`, `result_links` | The AI result is stored and returned as-is; report links are already inside it. |
| Merged `attention_code/detail` into `error_code/detail` | `status` already says whether it is `FAILED` or `NEEDS_ATTENTION`. |
| `next_submit_at` → `retry_at` (`NULL` = now); `resubmit_count` → `retry_count` | Simpler: a value is set only after a failure. Same pattern in `documents`. |
| Documents: removed `role`, `ordinal`, `source_version`; added `position` | All documents are equal, one file each, and files do not change. `position` keeps BPM's order, which AI results depend on. |
| Documents: removed `fetch_url`, `expires_at`, `content_type`, `expected_checksum`, `target_bucket/key/version_id`, `ready_at` | Link refresh is not in v1; `s3_uri` already holds bucket and key; the rest is not used. |
| Attempts: removed `outbound_payload`, `is_active`, `remote_status`, `final_response`, `last_successful_poll_at`, `post_delivery_attempts`, `last_http_status`, `TERMINATED` state | Rebuilt from stored data, derivable from `state`, stored in `queue`, or not needed in v1. |
| Kept `claim_owner` | Only for debugging, but nearly free and useful in incidents. |
| No `execution_slots` table; the AI limit counts active attempts under an advisory lock | Same safety with one table fewer; the limit is config (`AI_MAX_IN_FLIGHT`), so 10 → 50 is a config change. |
| `REJECTED` submission fails the task right away (spec: `NEEDS_ATTENTION` until step 5) | AI clearly refused it; nothing to recover. |
| Task key is BPM's `request_guid`; `request_id` (Camunda request) is a plain, non-unique column | One Camunda request can run several operations, including the same operation more than once. BPM matches results by `requestId` (which request) + `requestGuid` (which task). |
| Documents get our own `id`; BPM's `document_id` is unique per task only | The same file can be used by several tasks. |
| Document links: any `http(s)` link is accepted, no https-only rule and no host allowlist (spec: "HTTPS host Attstore из allowlist"); only links with credentials are refused | BPM is a trusted internal caller and its links are not restricted in practice. Known risk: the worker downloads whatever link it is given (SSRF), so the trust boundary is BPM. Decision 2026-10-18. |
| A BPM retry is a **new `requestGuid`** (a new task), never a reset of the old one | No reset logic, no "did the file change?" checks, and one task always means exactly one final outcome. Our own re-send after AI lost a task stays within the same task and reuses its S3 copies. |
| Kafka delivery state lives on `queue` (`published_at`, `publish_*`), no separate outbox table, no `event_id` | One task = exactly one Kafka message, so the task row is the outbox row and `requestGuid` identifies the message. Finishing a task already creates its "message to send". A separate table would only be needed for several messages per task. |

## API impact of these decisions

Applied in the code:

- `POST /api/v1/tasks`: no `operationVersion`; a document is only `{documentId, attstoreUrl}`, and its position is its index in `documents[]`.
- `POST /api/v1/tasks`: `requestId` (Camunda request, shared) and `requestGuid` (this task, unique, reused on retry) are both required.
- `GET /api/v1/tasks/{requestGuid}`: no `operationVersion`, `stateVersion`, `reports`, `attention`; `result` is AI's JSON as-is; `error` covers `FAILED` and `NEEDS_ATTENTION`.
- `409 REQUEST_GUID_UNAVAILABLE` when the `requestGuid` belongs to another caller.
- `503 SERVICE_UNAVAILABLE` (retryable) when the database cannot be reached.
