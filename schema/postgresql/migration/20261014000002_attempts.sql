-- POSTs to the AI team, and the foreign keys that point at them.
-- Column meanings and the reasoning behind them: docs/database.md.

-- +goose Up
-- +goose StatementBegin
-- One row per POST to AI; GET polls update the same row.
CREATE TABLE integration.attempts (
    attempt_id         uuid        PRIMARY KEY,
    request_guid       uuid        NOT NULL REFERENCES integration.queue (request_guid),
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

    CONSTRAINT attempts_state_check CHECK (state IN (
        'SUBMITTING', 'UNKNOWN', 'NOT_ACCEPTED', 'QUEUED', 'PROCESSING',
        'DONE', 'ERROR', 'LOST')),
    CONSTRAINT attempts_attempt_no_check CHECK (attempt_no >= 1),
    CONSTRAINT attempts_counts_check CHECK (poll_count >= 0 AND not_found_count >= 0),
    CONSTRAINT attempts_claim_check CHECK (
        (claim_token IS NULL) = (claim_until IS NULL) AND
        (claim_token IS NULL) = (claim_owner IS NULL)),
    CONSTRAINT attempts_error_check CHECK ((error_code IS NULL) = (error_detail IS NULL)),
    CONSTRAINT attempts_task_attempt_no_key UNIQUE (request_guid, attempt_no),
    CONSTRAINT attempts_task_attempt_key UNIQUE (request_guid, attempt_id)
);

COMMENT ON TABLE integration.attempts IS 'One row per POST to AI; polls update the same row';
COMMENT ON COLUMN integration.attempts.attempt_id IS 'Our ID for this POST; sent to AI as task_id';
COMMENT ON COLUMN integration.attempts.poll_at IS 'When to GET the status next';
COMMENT ON COLUMN integration.attempts.not_found_count IS 'AI not-found answers in a row; 3 over >= 60s means LOST';
COMMENT ON COLUMN integration.attempts.claim_owner IS 'Pod holding the claim; debugging only';

-- At most one in-flight POST per task.
CREATE UNIQUE INDEX attempts_one_active_idx ON integration.attempts (request_guid)
    WHERE state IN ('SUBMITTING', 'UNKNOWN', 'QUEUED', 'PROCESSING');

-- A task can only point to its own attempt.
ALTER TABLE integration.queue
    ADD CONSTRAINT queue_active_attempt_fk
    FOREIGN KEY (request_guid, active_attempt_id)
    REFERENCES integration.attempts (request_guid, attempt_id);

ALTER TABLE integration.audit
    ADD CONSTRAINT audit_attempt_fk
    FOREIGN KEY (attempt_id) REFERENCES integration.attempts (attempt_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE integration.audit DROP CONSTRAINT audit_attempt_fk;
ALTER TABLE integration.queue DROP CONSTRAINT queue_active_attempt_fk;
DROP TABLE integration.attempts;
-- +goose StatementEnd
