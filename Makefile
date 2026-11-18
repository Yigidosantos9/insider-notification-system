.PHONY: up down test lint run-api run-worker run-mock smoke

# Path of the Docker socket *inside* the Docker host, used by testcontainers' reaper.
# Same as the default on Linux; required on macOS with Colima.
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock

up: ## Build and start the whole stack
	docker compose up --build

down: ## Stop the stack and remove volumes
	docker compose down -v

test: ## Run all tests (requires a running Docker daemon for testcontainers)
	go test -race -count=1 ./...

lint:
	golangci-lint run

run-api:
	go run ./cmd/notifier api

run-worker:
	go run ./cmd/notifier worker

run-mock:
	go run ./cmd/mockprovider

smoke: ## End-to-end check against a running stack
	./scripts/smoke.sh
