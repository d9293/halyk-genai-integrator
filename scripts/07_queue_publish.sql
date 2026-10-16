-- GENERATED from schema/postgresql/migration/20261016000002_queue_publish.sql by 'make sql-scripts'. Do not edit; change the migration instead.
-- Run in the app database; see scripts/README.md.

ALTER TABLE integration.queue
    ADD COLUMN published_at         timestamptz,
    ADD COLUMN publish_retry_at     timestamptz,
    ADD COLUMN publish_retry_count  int NOT NULL DEFAULT 0,
    ADD COLUMN publish_claim_token  uuid,
    ADD COLUMN publish_claim_until  timestamptz,
    ADD COLUMN publish_claim_owner  text,
    ADD COLUMN publish_error_code   text,
    ADD COLUMN publish_error_detail text,

    ADD CONSTRAINT queue_published_check
        CHECK (published_at IS NULL OR status IN ('SUCCEEDED', 'FAILED')),
    ADD CONSTRAINT queue_publish_retry_count_check CHECK (publish_retry_count >= 0),
    ADD CONSTRAINT queue_publish_claim_check CHECK (
        (publish_claim_token IS NULL) = (publish_claim_until IS NULL) AND
        (publish_claim_token IS NULL) = (publish_claim_owner IS NULL)),
    ADD CONSTRAINT queue_publish_error_check
        CHECK ((publish_error_code IS NULL) = (publish_error_detail IS NULL));

-- Next result to deliver to BPM (POST /internal/v1/outbox/claim).
CREATE INDEX queue_unpublished_idx ON integration.queue (completed_at)
    WHERE status IN ('SUCCEEDED', 'FAILED') AND published_at IS NULL;

COMMENT ON TABLE integration.queue IS
    'One row per task (requestGuid): what BPM asked for, where the task is, its sending to AI, '
    'its final outcome, and the delivery of that outcome to BPM through Kafka (one message per task).';

-- Identity
COMMENT ON COLUMN integration.queue.request_guid IS
    'BPM''s unique ID of this task. Idempotency key; also identifies the task''s Kafka message (BPM deduplicates by it). Never sent to AI.';
COMMENT ON COLUMN integration.queue.request_id IS
    'Camunda request the task belongs to. Not unique: one request can have many tasks. Kafka message key.';
COMMENT ON COLUMN integration.queue.caller_id IS
    'Who registered the task (fixed from config while auth is off). GET returns only the caller''s own tasks.';

-- What was asked (never changes)
COMMENT ON COLUMN integration.queue.operation IS
    'Operation code: SOCIAL_MEDIA, REGISTER_COMPARE. Selects validation and the AI endpoint.';
COMMENT ON COLUMN integration.queue.request_payload IS
    'Operation input, normalized (fixed key order, defaults filled in). Sent to AI on every POST. Never changes.';
COMMENT ON COLUMN integration.queue.request_hash IS
    'SHA-256 of requestId, operation, input and documents. Same requestGuid with a different hash is rejected (409).';

-- State
COMMENT ON COLUMN integration.queue.status IS
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
COMMENT ON COLUMN integration.queue.active_attempt_id IS
    'Current, or last, POST to AI (integration.attempts). NULL before the first send; kept after the task ends.';

-- Sending to AI
COMMENT ON COLUMN integration.queue.retry_at IS
    'Do not send to AI before this time; NULL = as soon as an AI place is free. Not set by current flows.';
COMMENT ON COLUMN integration.queue.retry_count IS
    'Re-sends because AI lost the task (NOT_FOUND rule). After MAX_RESUBMISSIONS the task fails.';

-- Outcome
COMMENT ON COLUMN integration.queue.result_payload IS
    'AI''s result JSON as AI returned it (report links included). Only for SUCCEEDED.';
COMMENT ON COLUMN integration.queue.error_code IS
    'Why the task is FAILED or NEEDS_ATTENTION, e.g. AI_TASK_LOST_RETRIES_EXHAUSTED, RESULT_TOO_LARGE, DOCUMENT_COPY_FAILED, or AI''s code.';
COMMENT ON COLUMN integration.queue.error_detail IS
    'Human-readable explanation of error_code. No personal data.';
COMMENT ON COLUMN integration.queue.completed_at IS
    'When the task became SUCCEEDED or FAILED. Sent as occurredAt in the Kafka message.';

-- Delivery to BPM via Kafka
COMMENT ON COLUMN integration.queue.published_at IS
    'When Kafka confirmed (broker ACK) the result message. NULL = still to deliver. Does not mean BPM processed it.';
COMMENT ON COLUMN integration.queue.publish_retry_at IS
    'After a failed publish: do not try again before this time. NULL = now.';
COMMENT ON COLUMN integration.queue.publish_retry_count IS
    'Failed publish tries; drives the backoff. The message is never dropped.';
COMMENT ON COLUMN integration.queue.publish_claim_token IS
    'Proof of ownership while a worker publishes the message.';
COMMENT ON COLUMN integration.queue.publish_claim_until IS
    'When the publishing worker''s ownership expires; another worker may take over after it.';
COMMENT ON COLUMN integration.queue.publish_claim_owner IS
    'Pod publishing the message. Debugging only.';
COMMENT ON COLUMN integration.queue.publish_error_code IS
    'Last publish error, e.g. KAFKA_UNAVAILABLE.';
COMMENT ON COLUMN integration.queue.publish_error_detail IS
    'Details of publish_error_code.';

-- Timestamps
COMMENT ON COLUMN integration.queue.created_at IS 'Registration time.';
COMMENT ON COLUMN integration.queue.updated_at IS 'Last change of the row.';

