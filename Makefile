CONFIG ?= dev.env

.PHONY: run test test-integration swagger mocks fmt vet check db-init sql-scripts minio-init docker sonar

IMAGE ?= halyk-genai-integrator:local

run: ## Run the app with $(CONFIG)
	go run . --config_file $(CONFIG)

docker: ## Build the Docker image $(IMAGE)
	docker build -t $(IMAGE) .

test: ## Run all tests with the race detector (integration tests are skipped)
	go test -race ./...

TEST_PG := integrator-test-pg

test-integration: ## Run all tests, including integration tests against a throwaway Postgres in Docker
	@docker rm -f $(TEST_PG) >/dev/null 2>&1 || true
	@docker run -d --rm --name $(TEST_PG) -p 55432:5432 -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin postgres:17-alpine >/dev/null
	@# TCP only answers once initialization is finished.
	@until docker exec $(TEST_PG) pg_isready -q -h 127.0.0.1 -U admin; do sleep 0.5; done
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/02_create_tables.sql
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/03_create_attempts.sql
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/04_documents_claim_index.sql
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/05_queue_ready_index.sql
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/06_attempts_poll_index.sql
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/07_queue_publish.sql
	@INTEGRATION_POSTGRES_URL=postgres://admin:admin@localhost:55432/postgres?sslmode=disable \
		go test -race -count=1 $(TEST_FLAGS) ./... $(TEST_OUT); status=$$?; docker stop $(TEST_PG) >/dev/null; exit $$status

swagger: ## Regenerate docs/swagger from handler annotations
	swag init -g main.go -o docs/swagger --parseInternal

SONAR_URL ?= http://sonarqube:9000
# Scanner token; by default from the local infra's .env. Exported, never echoed.
SONAR_TOKEN ?= $(shell sed -n 's/^SONAR_TOKEN=//p' $(HOME)/Documents/work/projects/infra/.env 2>/dev/null)
export SONAR_TOKEN
SCANNER := sonarsource/sonar-scanner-cli:12.2.0.4256_8.1.0

sonar: ## Tests with the throwaway Postgres and coverage, then analysis on the local SonarQube (infra)
	@mkdir -p .sonar
	$(MAKE) test-integration TEST_FLAGS="-coverpkg=./... -coverprofile=.sonar/coverage.out -json" TEST_OUT="> .sonar/tests.json"
	docker run --rm --network infra -e SONAR_HOST_URL=$(SONAR_URL) -e SONAR_TOKEN \
		-v "$(CURDIR):/usr/src" $(SCANNER)

mocks: ## Regenerate moq mocks (moq is pinned in the go:generate lines)
	go generate ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test ## Vet and test

db-init: ## Create schema and tables on the local infra Postgres (once)
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/02_create_tables.sql
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/03_create_attempts.sql
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/04_documents_claim_index.sql
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/05_queue_ready_index.sql
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/06_attempts_poll_index.sql
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/07_queue_publish.sql

minio-init: ## Upload sample documents and reports to the local MinIO (bpm-bucket); safe to run again
	./scripts/minio/init.sh

MIGRATIONS := schema/postgresql/migration

sql-scripts: ## Regenerate plain SQL scripts in scripts/ from the migrations
	@for pair in 02_create_tables:20261014000001_init_tables 03_create_attempts:20261014000002_attempts 04_documents_claim_index:20261015000001_documents_claim_index 05_queue_ready_index:20261015000002_queue_ready_index 06_attempts_poll_index:20261016000001_attempts_poll_index 07_queue_publish:20261016000002_queue_publish; do \
		out=scripts/$${pair%%:*}.sql; src=$(MIGRATIONS)/$${pair#*:}.sql; \
		{ echo "-- GENERATED from $$src by 'make sql-scripts'. Do not edit; change the migration instead."; \
		  echo "-- Run in the app database; see scripts/README.md."; echo; \
		  sed -n '/+goose Up/,/+goose Down/p' $$src | grep -v '+goose'; } > $$out; \
		echo "wrote $$out"; \
	done
