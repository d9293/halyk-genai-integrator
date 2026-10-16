-- GENERATED from schema/postgresql/migration/20261016000001_attempts_poll_index.sql by 'make sql-scripts'. Do not edit; change the migration instead.
-- Run in the app database; see scripts/README.md.

CREATE INDEX attempts_pollable_idx ON integration.attempts (poll_at)
    WHERE state IN ('UNKNOWN', 'QUEUED', 'PROCESSING');

