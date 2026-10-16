-- Index for picking attempts whose status is due to be checked
-- (POST /internal/v1/polls/claim).

-- +goose Up
-- +goose StatementBegin
CREATE INDEX attempts_pollable_idx ON integration.attempts (poll_at)
    WHERE state IN ('UNKNOWN', 'QUEUED', 'PROCESSING');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX integration.attempts_pollable_idx;
-- +goose StatementEnd
