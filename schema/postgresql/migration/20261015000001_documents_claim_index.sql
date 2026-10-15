-- Index for claiming documents to copy (POST /internal/v1/documents/claim).
-- Only PENDING and COPYING rows can be claimed, so READY and NEEDS_ATTENTION
-- rows stay out of the index.

-- +goose Up
-- +goose StatementBegin
CREATE INDEX documents_claimable_idx ON integration.documents (created_at)
    WHERE status IN ('PENDING', 'COPYING');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX integration.documents_claimable_idx;
-- +goose StatementEnd
