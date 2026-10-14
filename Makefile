CONFIG ?= dev.env

.PHONY: run test swagger mocks fmt vet check

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
