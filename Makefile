SHELL := /bin/sh

MODULE := github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing
COMPOSE := docker compose -f deployments/docker/docker-compose.yml

PROTO_FILES := proto/consensus.proto proto/kv.proto proto/backup.proto

.PHONY: help tidy fmt vet test build ci proto buf docker-build docker-up docker-ps docker-logs docker-down docker-clean run-proxy run-backup-service run-node

help:
	@echo "Target disponibili:"
	@echo "  make tidy           - esegue go mod tidy e verifica go.mod/go.sum"
	@echo "  make fmt            - formatta il codice Go con gofmt"
	@echo "  make vet            - esegue go vet ./..."
	@echo "  make test           - esegue go test ./..."
	@echo "  make build          - compila i binari principali"
	@echo "  make ci             - controlli locali principali"
	@echo "  make proto          - rigenera gli stub Go dai .proto"
	@echo "  make buf            - esegue buf lint e format check"
	@echo "  make docker-build   - build immagini Docker Compose"
	@echo "  make docker-up      - avvia il cluster Docker Compose con build"
	@echo "  make docker-ps      - mostra stato container"
	@echo "  make docker-logs    - mostra log cluster"
	@echo "  make docker-down    - ferma cluster senza cancellare volumi"
	@echo "  make docker-clean   - ferma cluster e cancella volumi"


tidy:
	go mod tidy
	git diff --exit-code go.mod go.sum

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

build:
	go build ./cmd/consensus-node
	go build ./cmd/client-proxy
	go build ./cmd/backup-service
	go build ./cmd/bench-client
	go build ./cmd/backup-client

ci: tidy fmt vet test build

proto:
	protoc --go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

buf:
	buf lint
	buf format --diff --exit-code

docker-build:
	$(COMPOSE) build

docker-up:
	$(COMPOSE) up -d --build

docker-ps:
	$(COMPOSE) ps

docker-logs:
	$(COMPOSE) logs -f

docker-down:
	$(COMPOSE) down

docker-clean:
	$(COMPOSE) down -v

run-proxy:
	go run ./cmd/client-proxy

run-backup-service:
	go run ./cmd/backup-service

run-node:
	go run ./cmd/consensus-node
