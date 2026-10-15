-- GENERATED from schema/postgresql/migration/20261015000001_documents_claim_index.sql by 'make sql-scripts'. Do not edit; change the migration instead.
-- Run in the app database; see scripts/README.md.

CREATE INDEX documents_claimable_idx ON integration.documents (created_at)
    WHERE status IN ('PENDING', 'COPYING');

