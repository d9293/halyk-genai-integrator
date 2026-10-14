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
| [`queue`](#integrationqueue) | BPM task | Overall task state, submit schedule, final result | Step 2 |
| [`documents`](#integrationdocuments) | Input file of a task | Copy Attstore → S3 before the task can be sent to AI | Step 2 |
| [`audit`](#integrationaudit) | State change | Append-only history of tasks, documents and attempts | Step 2 |
| [`attempts`](#integrationattempts) | POST to the AI team | One send to AI and everything learned by polling it | Step 4 |

Not designed yet (later steps): execution slots (the limit of 10), outbox
(Kafka delivery), command idempotency for workers, global submission switch.

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

One row per BPM task. Holds the original request (never changed), the current
state, when the next POST to AI may happen, and the final result.

| Column | Type | Null | Default / check | Purpose | At registration |
|---|---|---|---|---|---|
| `request_guid` | `uuid` | NOT NULL | **PK** | BPM's unique ID for this task. Idempotency key: one `requestGuid` = one task; a retry reuses it. Never sent to AI. | BPM `requestGuid` |
| `request_id` | `uuid` | NOT NULL | index | The Camunda request the task belongs to. **Not unique**: one Camunda request can have many tasks, even several with the same operation. Returned to BPM with every result. | BPM `requestId` |
| `caller_id` | `text` | NOT NULL | | Who registered the task. Fixed from config while auth is off; later from the authenticated principal. GET only returns tasks of the same caller. | `bpm` |
| `operation` | `text` | NOT NULL | | Operation code, e.g. `SOCIAL_MEDIA`, `REGISTER_COMPARE`. Selects validation and the AI endpoint. | from request |
| `request_payload` | `jsonb` | NOT NULL | | The operation input, **normalized** (fixed key order, defaults filled in). Never changes. Used to build the request to AI on every POST. | normalized `input` |
| `request_hash` | `text` | NOT NULL | | SHA-256 of requestId + operation + input + documents. Detects BPM reusing a `requestGuid` with different data (→ `409`). | computed |
| `status` | `text` | NOT NULL | `CHECK`, see below | Where the task is overall. | `READY_TO_SUBMIT` or `WAITING_DOCUMENTS` |
| `active_attempt_id` | `uuid` | NULL | FK → `attempts` (step 4) | The current POST to AI. `NULL` before the first POST. | `NULL` |
| `retry_at` | `timestamptz` | NULL | | When the next POST to AI may happen. `NULL` = now. Set after a rejected POST, to delay the retry. | `NULL` |
| `retry_count` | `int` | NOT NULL | `0`, `CHECK (>= 0)` | Re-sends after AI **lost** the task (3× NOT_FOUND). Max 3, then the task is `FAILED`. Other failures do not count. | `0` |
| `result_payload` | `jsonb` | NULL | | AI's result JSON, **as AI returned it**. Includes report links (Word/Excel). | `NULL` |
| `error_code` | `text` | NULL | | Why the task is `FAILED` or `NEEDS_ATTENTION`, e.g. `AI_TASK_LOST_RETRIES_EXHAUSTED`. | `NULL` |
| `error_detail` | `text` | NULL | | Human-readable explanation for `error_code`. | `NULL` |
| `created_at` | `timestamptz` | NOT NULL | `now()` | Registration time. | `now()` |
| `updated_at` | `timestamptz` | NOT NULL | `now()` | Last change of the row. | `now()` |
| `completed_at` | `timestamptz` | NULL | | When the task reached `SUCCEEDED` or `FAILED`. | `NULL` |

### Task statuses

| Status | Meaning |
|---|---|
| `WAITING_DOCUMENTS` | Registered; files not yet picked up for copying. |
| `PREPARING_DOCUMENTS` | A worker is copying files Attstore → S3. |
| `READY_TO_SUBMIT` | Ready to POST to AI; waits for a free slot (max 10 at AI) and for `retry_at`. |
| `SUBMITTING` | A POST to AI is in progress. |
| `SUBMISSION_UNKNOWN` | The POST timed out or its response was lost; AI may or may not have the task. Resolved by polling, **never** by blindly POSTing again. |
| `REMOTE_QUEUED` | AI accepted the task and it waits in AI's own queue. |
| `REMOTE_RUNNING` | AI is processing the task. |
| `NEEDS_ATTENTION` | No safe automatic way forward; a person has to look. Not terminal. |
| `SUCCEEDED` | Terminal. `result_payload` is set. |
| `FAILED` | Terminal. `error_code` / `error_detail` are set. |

### Constraints

| Constraint | Rule |
|---|---|
| `queue_status_check` | `status` is one of the statuses above. |
| `queue_retry_count_check` | `retry_count >= 0`. |
| `queue_completed_at_check` | `completed_at` is set exactly when `status` is `SUCCEEDED` or `FAILED`. |
| `queue_error_check` | `error_code` and `error_detail` both set or both `NULL`. |
| `queue_active_attempt_fk` (step 4) | `(request_guid, active_attempt_id)` → `attempts (request_guid, attempt_id)`. Uses both columns so a task can never point to another task's attempt. |

### How the submit worker picks tasks (step 4)

```sql
WHERE status = 'READY_TO_SUBMIT'
  AND (retry_at IS NULL OR retry_at <= now())
ORDER BY created_at          -- oldest first
```

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
| `attstore_url` | `text` | NOT NULL | | Where to download the file. Must be https, no credentials, host from the allowlist. Never changes; files are assumed not to change in Attstore. | BPM `attstoreUrl` |
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

### How the polling worker picks attempts (step 5)

worker.Response calls the API; the API runs roughly:

```sql
SELECT a.attempt_id, a.request_guid, q.operation
FROM integration.attempts a
JOIN integration.queue q USING (request_guid)
WHERE a.state IN ('UNKNOWN', 'QUEUED', 'PROCESSING')
  AND a.poll_at <= now()
  AND (a.claim_until IS NULL OR a.claim_until < now())
ORDER BY a.poll_at
LIMIT $1
FOR UPDATE OF a SKIP LOCKED
```

Only attempts that are due and not claimed are returned, in small batches.

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
| `worker-request-x2kqp` | `DOCUMENT_READY` | document 0 | `COPYING` → `READY` | |
| `worker-request-x2kqp` | `DOCUMENT_READY` | document 1 | `COPYING` → `READY` | |
| `system` | `TASK_READY` | | `PREPARING_DOCUMENTS` → `READY_TO_SUBMIT` | |
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
| Task key is BPM's `request_guid`; `request_id` (Camunda request) is a plain, non-unique column | One Camunda request can run several operations, including the same operation more than once. BPM matches results by `requestId` (which request) + `requestGuid` (which task). |
| Documents get our own `id`; BPM's `document_id` is unique per task only | The same file can be used by several tasks. |

## API impact of these decisions

Applied in the code:

- `POST /api/v1/tasks`: no `operationVersion`; a document is only `{documentId, attstoreUrl}`, and its position is its index in `documents[]`.
- `POST /api/v1/tasks`: `requestId` (Camunda request, shared) and `requestGuid` (this task, unique, reused on retry) are both required.
- `GET /api/v1/tasks/{requestGuid}`: no `operationVersion`, `stateVersion`, `reports`, `attention`; `result` is AI's JSON as-is; `error` covers `FAILED` and `NEEDS_ATTENTION`.
- `409 REQUEST_GUID_UNAVAILABLE` when the `requestGuid` belongs to another caller.
- `503 SERVICE_UNAVAILABLE` (retryable) when the database cannot be reached.
