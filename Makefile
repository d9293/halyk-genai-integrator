CONFIG ?= dev.env

.PHONY: run test test-integration swagger mocks fmt vet check db-init db-reset sql-scripts minio-init docker sonar

IMAGE ?= halyk-genai-integrator:local

# Creates all tables; generated from the migration by sql-scripts.
SQL_SCRIPT := scripts/01_init.sql

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
	@docker exec -i $(TEST_PG) psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < $(SQL_SCRIPT)
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

db-init: ## Create the tables on the local infra Postgres (once)
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < $(SQL_SCRIPT)

db-reset: ## Drop the tables on the local infra Postgres (deletes all tasks); run db-init after
	docker exec infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres -c \
		'DROP TABLE IF EXISTS integration_audit, integration_attempts, integration_documents, integration_queue CASCADE'

minio-init: ## Upload sample documents and reports to the local MinIO (bpm-bucket); safe to run again
	./scripts/minio/init.sh

MIGRATION := schema/postgresql/migration/20261014000001_init.sql

sql-scripts: ## Regenerate the plain SQL script $(SQL_SCRIPT) from the migration
	@{ echo "-- GENERATED from $(MIGRATION) by 'make sql-scripts'. Do not edit; change the migration instead."; \
	  echo "-- Run in the app database; see scripts/README.md."; echo; \
	  sed -n '/+goose Up/,/+goose Down/p' $(MIGRATION) | grep -v '+goose'; } > $(SQL_SCRIPT)
	@echo "wrote $(SQL_SCRIPT)"
