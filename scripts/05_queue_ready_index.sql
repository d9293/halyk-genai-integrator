-- GENERATED from schema/postgresql/migration/20261015000002_queue_ready_index.sql by 'make sql-scripts'. Do not edit; change the migration instead.
-- Run in the app database; see scripts/README.md.

CREATE INDEX queue_ready_idx ON integration.queue (created_at)
    WHERE status = 'READY_TO_SUBMIT';

