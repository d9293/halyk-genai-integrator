# Admin panel: backlog

Not built yet. This is what the admin panel will need from Integrator.Api,
agreed 2026-10-17, so the design is ready when the work starts.

Today's endpoints are not enough: `/api/v1` serves BPM (register and read one
of its own tasks) and `/internal/v1` serves the workers (claim and report). An
admin needs to see across all tasks and act on them. That is a new route group
`/admin/v1`, with its own auth (admin role) once auth exists.

No redesign is needed: everything the panel shows is already stored in
`queue`, `documents`, `attempts` and `audit` (see [database.md](database.md)).

## Reading

Safe and straightforward.

| Endpoint | Returns |
|---|---|
| `GET /admin/v1/tasks?status=&operation=&requestId=&from=&to=&page=` | Task list with filters and paging, newest first. |
| `GET /admin/v1/tasks/{requestGuid}` | One task with everything: its documents (status, S3 copy, errors), all attempts (state, polls, NOT_FOUND count, errors), Kafka delivery state, and the audit history. Any caller's task. |
| `GET /admin/v1/stats` | Tasks per status and operation; AI places used / `AI_MAX_IN_FLIGHT`; oldest `SUBMISSION_UNKNOWN` and `NEEDS_ATTENTION`; unpublished results and the age of the oldest. |

Never show `request_payload` fields that hold personal data (IIN, full name)
unless the admin role is allowed to see them.

## Actions

Each action is a specific command with its own rules, never a free-form
"set status". Every action writes an `audit` row with the admin as `actor` and
a reason in `detail`. All must respect the AI limit and the rule "one task,
one final result, one Kafka message".

| Action | Allowed when | Effect |
|---|---|---|
| `POST /admin/v1/tasks/{requestGuid}/release-attempt` | Task is `NEEDS_ATTENTION` / `AI_NO_PROGRESS` (or stuck `SUBMISSION_UNKNOWN`) **and** the AI team confirmed AI no longer runs it | Attempt → final (e.g. `TERMINATED`), AI place freed; task → `READY_TO_SUBMIT` (re-send, counts against `MAX_RESUBMISSIONS`) or `FAILED`, as the admin chooses. Never free a place without that confirmation: AI could end up over its limit. |
| `POST /admin/v1/tasks/{requestGuid}/fail` | Task is not final | Task → `FAILED` with `ADMIN_FAILED` and the admin's reason; BPM gets the Kafka message. An active attempt must be released first (see above). |
| `POST /admin/v1/tasks/{requestGuid}/retry-documents` | Task is `NEEDS_ATTENTION` / `DOCUMENT_COPY_FAILED` and the cause is fixed (e.g. file restored in Attstore) | Failed documents back to `PENDING` with `retry_count = 0`; task → `PREPARING_DOCUMENTS`. |
| `POST /admin/v1/tasks/{requestGuid}/republish` | Task is final and BPM reports it lost the message | `published_at = NULL`, so the result is delivered again (BPM deduplicates by `requestGuid`). |
| `PUT /admin/v1/settings` | Anytime | Change the AI limit, pause / resume sending to AI, without a restart. |

## Database additions

| Addition | For |
|---|---|
| Table `integration_control`, always one row: `max_in_flight`, `submission_enabled`, `pause_reason`, `updated_at`, `updated_by` | `settings`. `submissions/claim` reads it in its transaction instead of `AI_MAX_IN_FLIGHT`; when paused it hands out nothing, while copying, polling and delivery go on. |
| Attempt state `TERMINATED` in `integration_attempts_state_check` | `release-attempt`: "we stopped tracking it", distinct from `LOST` (AI said not found). |
| Indexes for the list filters (e.g. `queue (status, created_at)`, `queue (request_id)` exists) | `tasks` list with paging. |

## Open questions

- Who may do what: read-only viewers vs operators who may act?
- Should BPM get a Kafka message for `NEEDS_ATTENTION` (currently GET only)?
- Should `fail` and `release-attempt` require a second person (four-eyes)?
