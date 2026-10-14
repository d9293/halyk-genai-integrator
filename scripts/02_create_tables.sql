-- GENERATED from schema/postgresql/migration/20261014000001_init_tables.sql by 'make sql-scripts'. Do not edit; change the migration instead.
-- Run in the app database; see scripts/README.md.

CREATE SCHEMA IF NOT EXISTS integration;

-- One row per BPM task.
CREATE TABLE integration.queue (
    request_guid      uuid        PRIMARY KEY,
    request_id        uuid        NOT NULL,
    caller_id         text        NOT NULL,
    operation         text        NOT NULL,
    request_payload   jsonb       NOT NULL,
    request_hash      text        NOT NULL,
    status            text        NOT NULL,
    active_attempt_id uuid,
    retry_at          timestamptz,
    retry_count       int         NOT NULL DEFAULT 0,
    result_payload    jsonb,
    error_code        text,
    error_detail      text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,

    CONSTRAINT queue_status_check CHECK (status IN (
        'WAITING_DOCUMENTS', 'PREPARING_DOCUMENTS', 'READY_TO_SUBMIT',
        'SUBMITTING', 'SUBMISSION_UNKNOWN', 'REMOTE_QUEUED', 'REMOTE_RUNNING',
        'NEEDS_ATTENTION', 'SUCCEEDED', 'FAILED')),
    CONSTRAINT queue_retry_count_check CHECK (retry_count >= 0),
    CONSTRAINT queue_completed_at_check
        CHECK ((status IN ('SUCCEEDED', 'FAILED')) = (completed_at IS NOT NULL)),
    CONSTRAINT queue_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL))
);

COMMENT ON TABLE integration.queue IS 'One row per BPM task';
COMMENT ON COLUMN integration.queue.request_guid IS 'Unique task ID from BPM; idempotency key';
COMMENT ON COLUMN integration.queue.request_id IS 'Camunda request; several tasks can share it';
COMMENT ON COLUMN integration.queue.request_payload IS 'Normalized operation input; never changes';
COMMENT ON COLUMN integration.queue.request_hash IS 'SHA-256 of requestId, operation, input and documents; detects requestGuid reuse with different data';
COMMENT ON COLUMN integration.queue.active_attempt_id IS 'Current POST to AI; NULL before the first one';
COMMENT ON COLUMN integration.queue.retry_at IS 'When the next POST to AI may happen; NULL = now';
COMMENT ON COLUMN integration.queue.retry_count IS 'Re-sends after AI lost the task (3x NOT_FOUND); max 3';
COMMENT ON COLUMN integration.queue.result_payload IS 'AI result JSON as returned by AI';
COMMENT ON COLUMN integration.queue.error_code IS 'Reason for FAILED or NEEDS_ATTENTION';

-- One row per input file of a task.
CREATE TABLE integration.documents (
    id              uuid        PRIMARY KEY,
    request_guid    uuid        NOT NULL REFERENCES integration.queue (request_guid),
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

    CONSTRAINT documents_task_position_key UNIQUE (request_guid, position),
    CONSTRAINT documents_task_document_key UNIQUE (request_guid, document_id),
    CONSTRAINT documents_position_check CHECK (position >= 0),
    CONSTRAINT documents_status_check CHECK (status IN ('PENDING', 'COPYING', 'READY', 'NEEDS_ATTENTION')),
    CONSTRAINT documents_size_check CHECK (size_bytes >= 0),
    CONSTRAINT documents_retry_count_check CHECK (retry_count >= 0),
    CONSTRAINT documents_ready_check CHECK (
        (status = 'READY') = (s3_uri IS NOT NULL AND size_bytes IS NOT NULL AND checksum_sha256 IS NOT NULL)),
    CONSTRAINT documents_claim_check CHECK (
        (claim_token IS NULL) = (claim_until IS NULL) AND
        (claim_token IS NULL) = (claim_owner IS NULL)),
    CONSTRAINT documents_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL))
);

COMMENT ON TABLE integration.documents IS 'Input files of a task, copied Attstore -> S3 before submission';
COMMENT ON COLUMN integration.documents.id IS 'Our ID for this file in this task';
COMMENT ON COLUMN integration.documents.document_id IS 'BPM ID of the file; the same file may appear in several tasks';
COMMENT ON COLUMN integration.documents.position IS 'Index in the BPM documents[] array; AI receives files in this order';
COMMENT ON COLUMN integration.documents.s3_uri IS 'Link to the copy in S3 that AI receives';
COMMENT ON COLUMN integration.documents.retry_at IS 'When the next copy attempt may happen; NULL = now';
COMMENT ON COLUMN integration.documents.claim_owner IS 'Pod holding the claim; debugging only';

-- Append-only history of state changes.
CREATE TABLE integration.audit (
    audit_id     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_guid uuid        NOT NULL REFERENCES integration.queue (request_guid),
    document_id  uuid        REFERENCES integration.documents (id),
    attempt_id   uuid,
    actor        text        NOT NULL,
    action       text        NOT NULL,
    old_state    text,
    new_state    text,
    detail       text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE integration.audit IS 'Append-only history of task, document and attempt state changes';
COMMENT ON COLUMN integration.audit.actor IS 'Caller (bpm), worker pod name, or system';

CREATE INDEX queue_request_id_idx ON integration.queue (request_id);
CREATE INDEX audit_task_idx ON integration.audit (request_guid, created_at);

