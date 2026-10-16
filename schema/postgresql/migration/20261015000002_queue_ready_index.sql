-- Index for picking the next task to send to AI (POST /internal/v1/submissions/claim).
-- Counting tasks at AI uses the existing partial index attempts_one_active_idx.

-- +goose Up
-- +goose StatementBegin
CREATE INDEX queue_ready_idx ON integration.queue (created_at)
    WHERE status = 'READY_TO_SUBMIT';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX integration.queue_ready_idx;
-- +goose StatementEnd
