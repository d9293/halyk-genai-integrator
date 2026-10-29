-- All Integrator.Api tables: integration_queue, integration_documents,
-- integration_attempts, integration_audit. They live in the public schema;
-- the integration_ prefix on tables, indexes and constraints keeps them apart
-- from other applications' objects.
-- Column meanings and the reasoning behind them: docs/database.md.

-- +goose Up
-- +goose StatementBegin

-- One row per BPM task, including the delivery of its result to BPM through
-- Kafka: one task produces exactly one Kafka message (docs/database.md, Decisions).
CREATE TABLE integration_queue (
    request_guid         uuid        PRIMARY KEY,
    request_id           uuid        NOT NULL,
    caller_id            text        NOT NULL,
    operation            text        NOT NULL,
    request_payload      jsonb       NOT NULL,
    request_hash         text        NOT NULL,
    status               text        NOT NULL,
    active_attempt_id    uuid,
    retry_at             timestamptz,
    retry_count          int         NOT NULL DEFAULT 0,
    result_payload       jsonb,
    error_code           text,
    error_detail         text,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    completed_at         timestamptz,
    published_at         timestamptz,
    publish_retry_at     timestamptz,
    publish_retry_count  int         NOT NULL DEFAULT 0,
    publish_claim_token  uuid,
    publish_claim_until  timestamptz,
    publish_claim_owner  text,
    publish_error_code   text,
    publish_error_detail text,

    CONSTRAINT integration_queue_status_check CHECK (status IN (
        'WAITING_DOCUMENTS', 'PREPARING_DOCUMENTS', 'READY_TO_SUBMIT',
        'SUBMITTING', 'SUBMISSION_UNKNOWN', 'REMOTE_QUEUED', 'REMOTE_RUNNING',
        'NEEDS_ATTENTION', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT integration_queue_retry_count_check CHECK (retry_count >= 0),
    CONSTRAINT integration_queue_completed_at_check
        CHECK ((status IN ('SUCCEEDED', 'FAILED')) = (completed_at IS NOT NULL)),
    CONSTRAINT integration_queue_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL)),
    CONSTRAINT integration_queue_published_check
        CHECK (published_at IS NULL OR status IN ('SUCCEEDED', 'FAILED')),
    CONSTRAINT integration_queue_publish_retry_count_check CHECK (publish_retry_count >= 0),
    CONSTRAINT integration_queue_publish_claim_check CHECK (
        (publish_claim_token IS NULL) = (publish_claim_until IS NULL) AND
        (publish_claim_token IS NULL) = (publish_claim_owner IS NULL)),
    CONSTRAINT integration_queue_publish_error_check
        CHECK ((publish_error_code IS NULL) = (publish_error_detail IS NULL))
);

-- One row per input file of a task.
CREATE TABLE integration_documents (
    id              uuid        PRIMARY KEY,
    request_guid    uuid        NOT NULL,
    document_id     uuid        NOT NULL,
    position        int         NOT NULL,
    attstore_url    text        NOT NULL,
    status          text        NOT NULL DEFAULT 'PENDING',
    s3_uri          text,
    size_bytes      bigint,
    checksum_sha256 text,
    retry_at        timestamptz,
    retry_count     int         NOT NULL DEFAULT 0,
    claim_token     uuid,
    claim_until     timestamptz,
    claim_owner     text,
    error_code      text,
    error_detail    text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT integration_documents_task_fk
        FOREIGN KEY (request_guid) REFERENCES integration_queue (request_guid),
    CONSTRAINT integration_documents_task_position_key UNIQUE (request_guid, position),
    CONSTRAINT integration_documents_task_document_key UNIQUE (request_guid, document_id),
    CONSTRAINT integration_documents_position_check CHECK (position >= 0),
    CONSTRAINT integration_documents_status_check
        CHECK (status IN ('PENDING', 'COPYING', 'READY', 'NEEDS_ATTENTION')),
    CONSTRAINT integration_documents_size_check CHECK (size_bytes >= 0),
    CONSTRAINT integration_documents_retry_count_check CHECK (retry_count >= 0),
    CONSTRAINT integration_documents_ready_check CHECK (
        (status = 'READY') = (s3_uri IS NOT NULL AND size_bytes IS NOT NULL AND checksum_sha256 IS NOT NULL)),
    CONSTRAINT integration_documents_claim_check CHECK (
        (claim_token IS NULL) = (claim_until IS NULL) AND
        (claim_token IS NULL) = (claim_owner IS NULL)),
    CONSTRAINT integration_documents_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL))
);

-- One row per POST to AI; GET polls update the same row.
CREATE TABLE integration_attempts (
    attempt_id         uuid        PRIMARY KEY,
    request_guid       uuid        NOT NULL,
    attempt_no         int         NOT NULL,
    state              text        NOT NULL,
    poll_at            timestamptz,
    poll_count         int         NOT NULL DEFAULT 0,
    not_found_count    int         NOT NULL DEFAULT 0,
    first_not_found_at timestamptz,
    claim_token        uuid,
    claim_until        timestamptz,
    claim_owner        text,
    error_code         text,
    error_detail       text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz,

    CONSTRAINT integration_attempts_task_fk
        FOREIGN KEY (request_guid) REFERENCES integration_queue (request_guid),
    CONSTRAINT integration_attempts_state_check CHECK (state IN (
        'SUBMITTING', 'UNKNOWN', 'NOT_ACCEPTED', 'QUEUED', 'PROCESSING',
        'DONE', 'ERROR', 'LOST')),
    CONSTRAINT integration_attempts_attempt_no_check CHECK (attempt_no >= 1),
    CONSTRAINT integration_attempts_counts_check CHECK (poll_count >= 0 AND not_found_count >= 0),
    CONSTRAINT integration_attempts_claim_check CHECK (
        (claim_token IS NULL) = (claim_until IS NULL) AND
        (claim_token IS NULL) = (claim_owner IS NULL)),
    CONSTRAINT integration_attempts_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL)),
    CONSTRAINT integration_attempts_task_attempt_no_key UNIQUE (request_guid, attempt_no),
    CONSTRAINT integration_attempts_task_attempt_key UNIQUE (request_guid, attempt_id)
);

-- A task can only point to its own attempt.
ALTER TABLE integration_queue
    ADD CONSTRAINT integration_queue_active_attempt_fk
    FOREIGN KEY (request_guid, active_attempt_id)
    REFERENCES integration_attempts (request_guid, attempt_id);

-- Append-only history of state changes.
CREATE TABLE integration_audit (
    audit_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_guid uuid        NOT NULL,
    document_id  uuid,
    attempt_id   uuid,
    actor        text        NOT NULL,
    action       text        NOT NULL,
    old_state    text,
    new_state    text,
    detail       text,
    created_at   timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT integration_audit_task_fk
        FOREIGN KEY (request_guid) REFERENCES integration_queue (request_guid),
    CONSTRAINT integration_audit_document_fk
        FOREIGN KEY (document_id) REFERENCES integration_documents (id),
    CONSTRAINT integration_audit_attempt_fk
        FOREIGN KEY (attempt_id) REFERENCES integration_attempts (attempt_id)
);

-- Indexes.
CREATE INDEX integration_queue_request_id_idx ON integration_queue (request_id);
CREATE INDEX integration_audit_task_idx ON integration_audit (request_guid, created_at);

-- At most one in-flight POST per task; also counts tasks at AI for the AI limit.
CREATE UNIQUE INDEX integration_attempts_one_active_idx ON integration_attempts (request_guid)
    WHERE state IN ('SUBMITTING', 'UNKNOWN', 'QUEUED', 'PROCESSING');

-- Claiming documents to copy (POST /internal/v1/documents/claim). Only PENDING
-- and COPYING rows can be claimed, so READY and NEEDS_ATTENTION stay out.
CREATE INDEX integration_documents_claimable_idx ON integration_documents (created_at)
    WHERE status IN ('PENDING', 'COPYING');

-- Picking the next task to send to AI (POST /internal/v1/submissions/claim).
CREATE INDEX integration_queue_ready_idx ON integration_queue (created_at)
    WHERE status = 'READY_TO_SUBMIT';

-- Picking attempts whose status is due to be checked (POST /internal/v1/polls/claim).
CREATE INDEX integration_attempts_pollable_idx ON integration_attempts (poll_at)
    WHERE state IN ('UNKNOWN', 'QUEUED', 'PROCESSING');

-- Next result to deliver to BPM (POST /internal/v1/results/claim).
CREATE INDEX integration_queue_unpublished_idx ON integration_queue (completed_at)
    WHERE status IN ('SUCCEEDED', 'FAILED') AND published_at IS NULL;

-- Comments: integration_queue.
COMMENT ON TABLE integration_queue IS
    'One row per task (requestGuid): what BPM asked for, where the task is, its sending to AI, '
    'its final outcome, and the delivery of that outcome to BPM through Kafka (one message per task).';

-- Identity
COMMENT ON COLUMN integration_queue.request_guid IS
    'BPM''s unique ID of this task. Idempotency key; also identifies the task''s Kafka message (BPM deduplicates by it). Never sent to AI.';
COMMENT ON COLUMN integration_queue.request_id IS
    'Camunda request the task belongs to. Not unique: one request can have many tasks. Kafka message key.';
COMMENT ON COLUMN integration_queue.caller_id IS
    'Who registered the task (fixed from config while auth is off). GET returns only the caller''s own tasks.';

-- What was asked (never changes)
COMMENT ON COLUMN integration_queue.operation IS
    'Operation code: SOCIAL_MEDIA, REGISTER_COMPARE. Selects validation and the AI endpoint.';
COMMENT ON COLUMN integration_queue.request_payload IS
    'Operation input, normalized (fixed key order, defaults filled in). Sent to AI on every POST. Never changes.';
COMMENT ON COLUMN integration_queue.request_hash IS
    'SHA-256 of requestId, operation, input and documents. Same requestGuid with a different hash is rejected (409).';

-- State
COMMENT ON COLUMN integration_queue.status IS
    'Where the task is. '
    'WAITING_DOCUMENTS: files wait to be copied Attstore -> S3. '
    'PREPARING_DOCUMENTS: worker.Request is copying the files. '
    'READY_TO_SUBMIT: prepared, waits for a free AI place. '
    'SUBMITTING: the POST to AI is being sent right now. '
    'SUBMISSION_UNKNOWN: no reliable answer to the POST; AI may have it; polling finds out, never re-sent blindly. '
    'REMOTE_QUEUED: AI has the task, not started. '
    'REMOTE_RUNNING: AI is working on it. '
    'NEEDS_ATTENTION: no safe automatic way forward, a person must look (see error_code); not final. '
    'SUCCEEDED: final, result_payload holds AI''s result. '
    'FAILED: final, error_code/error_detail say why. '
    'Only SUCCEEDED and FAILED produce a Kafka message.';
COMMENT ON COLUMN integration_queue.active_attempt_id IS
    'Current, or last, POST to AI (integration_attempts). NULL before the first send; kept after the task ends.';

-- Sending to AI
COMMENT ON COLUMN integration_queue.retry_at IS
    'Do not send to AI before this time; NULL = as soon as an AI place is free. Not set by current flows.';
COMMENT ON COLUMN integration_queue.retry_count IS
    'Re-sends because AI lost the task (NOT_FOUND rule). After MAX_RESUBMISSIONS the task fails.';

-- Outcome
COMMENT ON COLUMN integration_queue.result_payload IS
    'AI''s result JSON as AI returned it (report links included). Only for SUCCEEDED.';
COMMENT ON COLUMN integration_queue.error_code IS
    'Why the task is FAILED or NEEDS_ATTENTION, e.g. AI_TASK_LOST_RETRIES_EXHAUSTED, RESULT_TOO_LARGE, DOCUMENT_COPY_FAILED, or AI''s code.';
COMMENT ON COLUMN integration_queue.error_detail IS
    'Human-readable explanation of error_code. No personal data.';
COMMENT ON COLUMN integration_queue.completed_at IS
    'When the task became SUCCEEDED or FAILED. Sent as occurredAt in the Kafka message.';

-- Delivery to BPM via Kafka
COMMENT ON COLUMN integration_queue.published_at IS
    'When Kafka confirmed (broker ACK) the result message. NULL = still to deliver. Does not mean BPM processed it.';
COMMENT ON COLUMN integration_queue.publish_retry_at IS
    'After a failed publish: do not try again before this time. NULL = now.';
COMMENT ON COLUMN integration_queue.publish_retry_count IS
    'Failed publish tries; drives the backoff. The message is never dropped.';
COMMENT ON COLUMN integration_queue.publish_claim_token IS
    'Proof of ownership while a worker publishes the message.';
COMMENT ON COLUMN integration_queue.publish_claim_until IS
    'When the publishing worker''s ownership expires; another worker may take over after it.';
COMMENT ON COLUMN integration_queue.publish_claim_owner IS
    'Pod publishing the message. Debugging only.';
COMMENT ON COLUMN integration_queue.publish_error_code IS
    'Last publish error, e.g. KAFKA_UNAVAILABLE.';
COMMENT ON COLUMN integration_queue.publish_error_detail IS
    'Details of publish_error_code.';

-- Timestamps
COMMENT ON COLUMN integration_queue.created_at IS 'Registration time.';
COMMENT ON COLUMN integration_queue.updated_at IS 'Last change of the row.';

-- Comments: integration_documents.
COMMENT ON TABLE integration_documents IS 'Input files of a task, copied Attstore -> S3 before submission';
COMMENT ON COLUMN integration_documents.id IS 'Our ID for this file in this task';
COMMENT ON COLUMN integration_documents.document_id IS 'BPM ID of the file; the same file may appear in several tasks';
COMMENT ON COLUMN integration_documents.position IS 'Index in the BPM documents[] array; AI receives files in this order';
COMMENT ON COLUMN integration_documents.s3_uri IS 'Link to the copy in S3 that AI receives';
COMMENT ON COLUMN integration_documents.retry_at IS 'When the next copy attempt may happen; NULL = now';
COMMENT ON COLUMN integration_documents.claim_owner IS 'Pod holding the claim; debugging only';

-- Comments: integration_attempts.
COMMENT ON TABLE integration_attempts IS 'One row per POST to AI; polls update the same row';
COMMENT ON COLUMN integration_attempts.attempt_id IS 'Our ID for this POST; sent to AI as task_id';
COMMENT ON COLUMN integration_attempts.poll_at IS 'When to GET the status next';
COMMENT ON COLUMN integration_attempts.not_found_count IS 'AI not-found answers in a row; 3 over >= 60s means LOST';
COMMENT ON COLUMN integration_attempts.claim_owner IS 'Pod holding the claim; debugging only';

-- Comments: integration_audit.
COMMENT ON TABLE integration_audit IS 'Append-only history of task, document and attempt state changes';
COMMENT ON COLUMN integration_audit.actor IS 'Caller (bpm), worker pod name, or system';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE integration_audit;
ALTER TABLE integration_queue DROP CONSTRAINT integration_queue_active_attempt_fk;
DROP TABLE integration_attempts;
DROP TABLE integration_documents;
DROP TABLE integration_queue;
-- +goose StatementEnd
