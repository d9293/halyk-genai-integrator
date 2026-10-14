CONFIG ?= dev.env

.PHONY: run test swagger mocks fmt vet check db-init sql-scripts

run: ## Run the app with $(CONFIG)
	go run . --config_file $(CONFIG)

test: ## Run all tests with the race detector
	go test -race ./...

swagger: ## Regenerate docs/swagger from handler annotations
	swag init -g main.go -o docs/swagger --parseInternal

mocks: ## Regenerate moq mocks
	go generate ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

check: vet test ## Vet and test

db-init: ## Create the project's database and user on the local infra Postgres (once)
	docker exec -i infra-postgres psql -q -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/01_create_database.sql

MIGRATIONS := schema/postgresql/migration

sql-scripts: ## Regenerate plain SQL scripts in scripts/ from the migrations
	@for pair in 02_create_tables:20261014000001_init_tables 03_create_attempts:20261014000002_attempts; do \
		out=scripts/$${pair%%:*}.sql; src=$(MIGRATIONS)/$${pair#*:}.sql; \
		{ echo "-- GENERATED from $$src by 'make sql-scripts'. Do not edit; change the migration instead."; \
		  echo "-- Run as the integrator user on the integrator database."; echo; \
		  sed -n '/+goose Up/,/+goose Down/p' $$src | grep -v '+goose'; } > $$out; \
		echo "wrote $$out"; \
	done
