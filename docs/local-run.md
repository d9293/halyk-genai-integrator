# Running the whole integration locally

How to run all four apps of the BPM ↔ AI integration on one machine and send
a task from "BPM" all the way to the Kafka message BPM receives.

```
BPM ──POST /api/v1/tasks──► Integrator.Api ◄── claims ── worker.Request ──► Attstore → S3, POST to AI
         (curl here)          (Postgres)    ◄── claims ── worker.Response ──► GET from AI, publish to Kafka ──► BPM
```

Integrator.Api holds all state and makes every decision; the two workers only
do the outside calls and report back.

## What runs where

| App | Repo | Port | Health |
|---|---|---|---|
| Integrator.Api | `halyk-genai-integrator` | 8080 | http://localhost:8080/health/readiness |
| mock-ai-api (plays the AI team) | `mock-ai-api` | 8081 | http://localhost:8081/register-compare/api/v1/health |
| worker.Request | `halyk-genai-worker-request` | 8083 | http://localhost:8083/health |
| worker.Response | `halyk-genai-worker-response` | 8084 | http://localhost:8084/health |

From the local infra (`~/Documents/work/projects/infra`, `docker compose up -d`):

| Service | Address | Used by |
|---|---|---|
| Postgres | `localhost:5432`, db `postgres`, user `admin` | Integrator.Api |
| MinIO | `localhost:9000`, console http://localhost:9001, bucket `bpm-bucket` | worker.Request (S3 copies), sample "Attstore" files |
| Kafka | `localhost:29092` | worker.Response |
| Kafka UI | http://localhost:8090 | you, to see the messages |

## Once

```sh
cd halyk-genai-integrator
make db-init      # integration_* tables in the infra Postgres
make minio-init   # sample files in bpm-bucket: attstore-mock/registry-{a,b}.pdf and AI report samples
```

## Start

Four terminals, in this order (each `make run` = `go run . --config_file dev.env`):

```sh
cd halyk-genai-integrator      && make run
cd mock-ai-api                 && make run
cd halyk-genai-worker-request  && make run
cd halyk-genai-worker-response && make run
```

Each worker logs `started … loops=2`. worker.Request refuses to start if the
S3 bucket is unreachable, worker.Response if Kafka is.

## Send a task

A social media check (no documents):

```sh
curl -s -XPOST localhost:8080/api/v1/tasks -H 'Content-Type: application/json' -d '{
  "requestId":   "11111111-1111-4111-8111-111111111111",
  "requestGuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "operation":   "SOCIAL_MEDIA",
  "input":       {"iin": "000000000000", "fullName": "Тестовый клиент"},
  "documents":   []}'
```

A Reestr comparison (two documents, copied to S3 first):

```sh
curl -s -XPOST localhost:8080/api/v1/tasks -H 'Content-Type: application/json' -d '{
  "requestId":   "44444444-4444-4444-8444-444444444444",
  "requestGuid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "operation":   "REGISTER_COMPARE",
  "input":       {"adjudicate": true},
  "documents": [
    {"documentId": "22222222-2222-4222-8222-222222222222", "attstoreUrl": "http://localhost:9000/bpm-bucket/attstore-mock/registry-a.pdf"},
    {"documentId": "33333333-3333-4333-8333-333333333333", "attstoreUrl": "http://localhost:9000/bpm-bucket/attstore-mock/registry-b.pdf"}]}'
```

`api.http` has the same requests for GoLand. Use a new `requestGuid` for every
new task; sending the same one again just returns the existing task.

## Watch it

```sh
curl -s localhost:8080/api/v1/tasks/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb
```

| Status | Who moves it on |
|---|---|
| `WAITING_DOCUMENTS` / `PREPARING_DOCUMENTS` | worker.Request copies the documents Attstore → S3 (`bpm-bucket/register-compare/{date}/…`) |
| `READY_TO_SUBMIT` | worker.Request sends it to AI once a place is free (`AI_MAX_IN_FLIGHT=10`) |
| `SUBMITTING` → `REMOTE_QUEUED` | AI accepted it |
| `SUBMISSION_UNKNOWN` | no clear answer from AI; polling finds out whether AI has it |
| `REMOTE_QUEUED` → `REMOTE_RUNNING` | worker.Response checks on it every `POLL_INTERVAL` (15s) |
| `SUCCEEDED` / `FAILED` | final; the result message is published to Kafka |
| `NEEDS_ATTENTION` | a person must look (a document could not be copied, no progress for 2h, result too large) |

With the default timings a Reestr task takes about a minute (the mock "works"
30s, social media 10s).

The message BPM receives: Kafka UI → topic `integrator.task-results.v1`, one
message per task, key = `requestId`. Or from the command line:

```sh
docker exec infra-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic integrator.task-results.v1 --from-beginning --property print.key=true
```

## Try what goes wrong

mock-ai-api's test controls (more in its `api.http`) change what the AI does next:

| Do | Then |
|---|---|
| `curl -XPOST localhost:8081/register-compare/__test/scenarios -d '{"fail":true,"errorMessage":"Нет доступа к файлу"}'`, send a Reestr task | `FAILED`, `AI_TASK_FAILED`; the Kafka message carries the error |
| `… -d '{"submitDelay":"40s"}'`, send a Reestr task | the POST times out (30s): `SUBMISSION_UNKNOWN`; polling finds AI has it, `SUCCEEDED` on attempt 1, never sent twice |
| send a Reestr task, wait for `REMOTE_QUEUED`, `curl -XPOST localhost:8081/register-compare/__test/restart` | AI forgot it: 3× "not found" over ≥ 60s → attempt 1 `LOST` → sent again as attempt 2 → `SUCCEEDED` |
| send a Reestr task with an `attstoreUrl` that does not exist | `SOURCE_NOT_FOUND` → `NEEDS_ATTENTION` |

See every attempt of a task:

```sh
docker exec infra-postgres psql -U admin -d postgres -c \
  "select attempt_no, state, error_code from integration_attempts where request_guid = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' order by attempt_no"
```

## Faster runs

Timings are env variables; set them in front of `make run` instead of editing `dev.env`:

```sh
# Integrator.Api: first check 2s after sending, then every ~3s; lost after 6s of "not found"
SUBMISSION_POLL_DELAY=2s POLL_INTERVAL=3s POLL_INTERVAL_JITTER=1s POLL_NOT_FOUND_DELAY=2s LOST_TASK_MIN_WINDOW=6s make run
# mock-ai-api: tasks take 8s / 4s
REGISTER_COMPARE_DURATION=8s SOCIAL_MEDIA_DURATION=4s make run
# workers: wait at most 3s between claims when idle (default 10s)
LOOP_MAX_IDLE_DELAY=3s make run
```

## Contract tests

Each client has a test against the real counterpart, skipped unless its
variable is set:

| Repo | Command | Needs |
|---|---|---|
| worker.Request | `CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/` | Integrator.Api |
| worker.Response | `CONTRACT_INTEGRATOR_URL=http://localhost:8080 go test -run Contract ./internal/client/integrator/` | Integrator.Api started with `SUBMISSION_POLL_DELAY=1s` |
| worker.Response | `CONTRACT_AI_URL=http://localhost:8081 go test -run Contract ./internal/client/ai/` | mock-ai-api |
| worker.Response | `CONTRACT_KAFKA_BROKERS=localhost:29092 go test -run Contract ./internal/broker/kafka/` | Kafka |

They create real tasks; run them against local services only.

## Start over

```sh
make db-reset && make db-init                                  # in halyk-genai-integrator
curl -XPOST localhost:8081/register-compare/__test/reset       # mock AI forgets everything
curl -XPOST localhost:8081/social-media/__test/reset
```

Copies in `bpm-bucket/register-compare/` and `bpm-bucket/social-media/` stay;
delete them in the MinIO console if you want.
