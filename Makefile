ifeq ($(OS),Windows_NT)
SHELL := cmd.exe
.SHELLFLAGS := /C
else
SHELL := /bin/sh
endif

MODULE := github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing

COMPOSE_3 := docker compose -f deployments/docker/docker-compose.yml
COMPOSE_5 := docker compose -f deployments/docker/docker-compose-5nodes.yml
COMPOSE_7 := docker compose -f deployments/docker/docker-compose-7nodes.yml

COMPOSE := $(COMPOSE_3)

PROTO_FILES := proto/consensus.proto proto/kv.proto proto/backup.proto

TARGET ?= localhost:8080
BACKUP_TARGET ?= localhost:9090
KEY ?= test-key
VALUE ?= test-value

CLUSTER_SIZE ?= 3
TRIALS ?= 10
CONCURRENCY ?= 1
WARMUP_PUTS ?= 30
WARMUP_GETS ?= 30
PUTS ?= 300
GETS ?= 300
KEY_PREFIX ?= local-test

PERF_CSV_OUT ?= reports/raw/scalability_$(CLUSTER_SIZE)nodes.csv
PERF_SUMMARY_OUT ?= reports/processed/scalability_$(CLUSTER_SIZE)nodes_summary.csv
FAILOVER_CSV_OUT ?= reports/raw/failover_trials.csv

DATASET_SIZES ?= 100,300,600
BACKUP_CSV_OUT ?= reports/raw/backup_compaction_results.csv
FORCE_SNAPSHOT ?= true
COMPACT_AFTER_DOWNLOAD ?= true

ifeq ($(OS),Windows_NT)
RUN_BENCH_LEADER = set "TARGET=$(TARGET)" && set "OP=leader" && go run ./cmd/bench-client
RUN_BENCH_PUT = set "TARGET=$(TARGET)" && set "OP=put" && set "KEY=$(KEY)" && set "VALUE=$(VALUE)" && go run ./cmd/bench-client
RUN_BENCH_GET = set "TARGET=$(TARGET)" && set "OP=get" && set "KEY=$(KEY)" && go run ./cmd/bench-client
RUN_BENCH_DELETE = set "TARGET=$(TARGET)" && set "OP=delete" && set "KEY=$(KEY)" && go run ./cmd/bench-client
RUN_BACKUP_STATUS = set "TARGET=$(BACKUP_TARGET)" && set "OP=status" && go run ./cmd/backup-client
RUN_BACKUP_RUN = set "TARGET=$(BACKUP_TARGET)" && set "OP=backup" && set "FORCE_SNAPSHOT=$(FORCE_SNAPSHOT)" && set "COMPACT_AFTER_DOWNLOAD=$(COMPACT_AFTER_DOWNLOAD)" && go run ./cmd/backup-client
RUN_PERF = set "TARGET=$(TARGET)" && set "CLUSTER_SIZE=$(CLUSTER_SIZE)" && set "WARMUP_PUTS=$(WARMUP_PUTS)" && set "WARMUP_GETS=$(WARMUP_GETS)" && set "PUTS=$(PUTS)" && set "GETS=$(GETS)" && set "CONCURRENCY=$(CONCURRENCY)" && set "KEY_PREFIX=$(KEY_PREFIX)" && set "CSV_OUT=$(PERF_CSV_OUT)" && set "SUMMARY_OUT=$(PERF_SUMMARY_OUT)" && go run ./cmd/perf-client
RUN_FAILOVER = set "TARGET=$(TARGET)" && set "CLUSTER_SIZE=$(CLUSTER_SIZE)" && set "TRIALS=$(TRIALS)" && set "CSV_OUT=$(FAILOVER_CSV_OUT)" && set "KEY_PREFIX=$(KEY_PREFIX)" && go run ./cmd/failover-client
RUN_BACKUP_BENCH = set "KV_TARGET=$(TARGET)" && set "BACKUP_TARGET=$(BACKUP_TARGET)" && set "DATASET_SIZES=$(DATASET_SIZES)" && set "CSV_OUT=$(BACKUP_CSV_OUT)" && set "KEY_PREFIX=$(KEY_PREFIX)" && set "FORCE_SNAPSHOT=$(FORCE_SNAPSHOT)" && set "COMPACT_AFTER_DOWNLOAD=$(COMPACT_AFTER_DOWNLOAD)" && go run ./cmd/backup-benchmark-client
else
RUN_BENCH_LEADER = TARGET=$(TARGET) OP=leader go run ./cmd/bench-client
RUN_BENCH_PUT = TARGET=$(TARGET) OP=put KEY=$(KEY) VALUE=$(VALUE) go run ./cmd/bench-client
RUN_BENCH_GET = TARGET=$(TARGET) OP=get KEY=$(KEY) go run ./cmd/bench-client
RUN_BENCH_DELETE = TARGET=$(TARGET) OP=delete KEY=$(KEY) go run ./cmd/bench-client
RUN_BACKUP_STATUS = TARGET=$(BACKUP_TARGET) OP=status go run ./cmd/backup-client
RUN_BACKUP_RUN = TARGET=$(BACKUP_TARGET) OP=backup FORCE_SNAPSHOT=$(FORCE_SNAPSHOT) COMPACT_AFTER_DOWNLOAD=$(COMPACT_AFTER_DOWNLOAD) go run ./cmd/backup-client
RUN_PERF = TARGET=$(TARGET) CLUSTER_SIZE=$(CLUSTER_SIZE) WARMUP_PUTS=$(WARMUP_PUTS) WARMUP_GETS=$(WARMUP_GETS) PUTS=$(PUTS) GETS=$(GETS) CONCURRENCY=$(CONCURRENCY) KEY_PREFIX=$(KEY_PREFIX) CSV_OUT=$(PERF_CSV_OUT) SUMMARY_OUT=$(PERF_SUMMARY_OUT) go run ./cmd/perf-client
RUN_FAILOVER = TARGET=$(TARGET) CLUSTER_SIZE=$(CLUSTER_SIZE) TRIALS=$(TRIALS) CSV_OUT=$(FAILOVER_CSV_OUT) KEY_PREFIX=$(KEY_PREFIX) go run ./cmd/failover-client
RUN_BACKUP_BENCH = KV_TARGET=$(TARGET) BACKUP_TARGET=$(BACKUP_TARGET) DATASET_SIZES=$(DATASET_SIZES) CSV_OUT=$(BACKUP_CSV_OUT) KEY_PREFIX=$(KEY_PREFIX) FORCE_SNAPSHOT=$(FORCE_SNAPSHOT) COMPACT_AFTER_DOWNLOAD=$(COMPACT_AFTER_DOWNLOAD) go run ./cmd/backup-benchmark-client
endif

.PHONY: help doctor tidy fmt vet test build ci proto buf
.PHONY: docker-build docker-up docker-ps docker-logs docker-down docker-clean
.PHONY: docker-up-3 docker-up-5 docker-up-7 docker-ps-3 docker-ps-5 docker-ps-7
.PHONY: docker-logs-3 docker-logs-5 docker-logs-7 docker-down-3 docker-down-5 docker-down-7
.PHONY: docker-clean-3 docker-clean-5 docker-clean-7
.PHONY: run-proxy run-backup-service run-node
.PHONY: bench-leader bench-put bench-get bench-delete backup-status backup-run
.PHONY: perf failover backup-bench
.PHONY: ec2-install-docker ec2-install-compose ec2-install-buildx ec2-doctor ec2-health
.PHONY: ec2-up-3 ec2-up-5 ec2-up-7 ec2-down-3 ec2-down-5 ec2-down-7 ec2-clean-3 ec2-clean-5 ec2-clean-7

help:
	@echo Target disponibili:
	@echo   make doctor              - mostra versioni strumenti principali
	@echo   make tidy                - esegue go mod tidy e verifica go.mod/go.sum
	@echo   make fmt                 - formatta il codice Go con gofmt
	@echo   make vet                 - esegue go vet ./...
	@echo   make test                - esegue go test ./...
	@echo   make build               - compila tutti i comandi in cmd/
	@echo   make ci                  - esegue tidy, fmt, vet, test e build
	@echo   make proto               - rigenera gli stub Go dai file .proto
	@echo   make buf                 - esegue buf lint e format check
	@echo ------------------------------------------------------------
	@echo Docker Compose:
	@echo   make docker-up            - avvia cluster 3 nodi con build
	@echo   make docker-ps            - mostra stato container cluster 3 nodi
	@echo   make docker-logs          - mostra log cluster 3 nodi
	@echo   make docker-down          - ferma cluster 3 nodi senza cancellare volumi
	@echo   make docker-clean         - ferma cluster 3 nodi e cancella volumi
	@echo   make docker-up-3          - avvia cluster 3 nodi
	@echo   make docker-up-5          - avvia cluster 5 nodi
	@echo   make docker-up-7          - avvia cluster 7 nodi
	@echo   make docker-ps-3          - mostra stato container cluster 3 nodi
	@echo   make docker-ps-5          - mostra stato container cluster 5 nodi
	@echo   make docker-ps-7          - mostra stato container cluster 7 nodi
	@echo   make docker-down-3        - ferma cluster 3 nodi
	@echo   make docker-down-5        - ferma cluster 5 nodi
	@echo   make docker-down-7        - ferma cluster 7 nodi
	@echo   make docker-clean-3       - ferma cluster 3 nodi e cancella volumi
	@echo   make docker-clean-5       - ferma cluster 5 nodi e cancella volumi
	@echo   make docker-clean-7       - ferma cluster 7 nodi e cancella volumi
	@echo ------------------------------------------------------------
	@echo Client manuali:
	@echo   make bench-leader         - interroga il leader tramite bench-client
	@echo   make bench-put            - esegue Put, usare KEY=... VALUE=...
	@echo   make bench-get            - esegue Get, usare KEY=...
	@echo   make bench-delete         - esegue Delete, usare KEY=...
	@echo   make backup-status        - legge lo stato del Backup Service
	@echo   make backup-run           - avvia TriggerBackup
	@echo ------------------------------------------------------------
	@echo Benchmark:
	@echo   make perf                 - esegue benchmark Put/Get con perf-client
	@echo   make failover             - esegue failover-client per crash leader
	@echo   make backup-bench         - esegue benchmark Backup Service
	@echo ------------------------------------------------------------
	@echo EC2 Learner Lab:
	@echo   make ec2-install-docker   - installa Docker, Git e Make su Amazon Linux 2023
	@echo   make ec2-install-compose  - installa Docker Compose plugin
	@echo   make ec2-install-buildx   - installa Docker Buildx, se necessario
	@echo   make ec2-doctor           - verifica strumenti installati su EC2
	@echo   make ec2-health           - verifica /health del Proxy su EC2
	@echo   make ec2-up-3             - avvia cluster 3 nodi su EC2
	@echo   make ec2-up-5             - avvia cluster 5 nodi su EC2
	@echo   make ec2-up-7             - avvia cluster 7 nodi su EC2
	@echo   make ec2-down-3           - ferma cluster 3 nodi su EC2
	@echo   make ec2-down-5           - ferma cluster 5 nodi su EC2
	@echo   make ec2-down-7           - ferma cluster 7 nodi su EC2
	@echo   make ec2-clean-3          - ferma cluster 3 nodi e cancella volumi su EC2
	@echo   make ec2-clean-5          - ferma cluster 5 nodi e cancella volumi su EC2
	@echo   make ec2-clean-7          - ferma cluster 7 nodi e cancella volumi su EC2
	@echo ------------------------------------------------------------
	@echo Variabili utili:
	@echo   TARGET=localhost:8080
	@echo   BACKUP_TARGET=localhost:9090
	@echo   KEY=test-key
	@echo   VALUE=test-value
	@echo   CLUSTER_SIZE=3
	@echo   TRIALS=10
	@echo ------------------------------------------------------------
	@echo Esempi:
	@echo   make bench-put KEY=a VALUE=b
	@echo   make bench-get KEY=a
	@echo   make perf CLUSTER_SIZE=5 TARGET=34.207.234.177:8080
	@echo   make backup-run BACKUP_TARGET=34.207.234.177:9090

doctor:
	go version
	protoc --version
	docker --version
	docker compose version
	buf --version

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
	go build ./cmd/...

ci: tidy fmt vet test build

proto:
	protoc --go_out=. --go_opt=module=$(MODULE) --go-grpc_out=. --go-grpc_opt=module=$(MODULE) $(PROTO_FILES)

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

docker-up-3:
	$(COMPOSE_3) up -d --build

docker-up-5:
	$(COMPOSE_5) up -d --build

docker-up-7:
	$(COMPOSE_7) up -d --build

docker-ps-3:
	$(COMPOSE_3) ps

docker-ps-5:
	$(COMPOSE_5) ps

docker-ps-7:
	$(COMPOSE_7) ps

docker-logs-3:
	$(COMPOSE_3) logs -f

docker-logs-5:
	$(COMPOSE_5) logs -f

docker-logs-7:
	$(COMPOSE_7) logs -f

docker-down-3:
	$(COMPOSE_3) down

docker-down-5:
	$(COMPOSE_5) down

docker-down-7:
	$(COMPOSE_7) down

docker-clean-3:
	$(COMPOSE_3) down -v

docker-clean-5:
	$(COMPOSE_5) down -v

docker-clean-7:
	$(COMPOSE_7) down -v

run-proxy:
	go run ./cmd/client-proxy

run-backup-service:
	go run ./cmd/backup-service

run-node:
	go run ./cmd/consensus-node

bench-leader:
	$(RUN_BENCH_LEADER)

bench-put:
	$(RUN_BENCH_PUT)

bench-get:
	$(RUN_BENCH_GET)

bench-delete:
	$(RUN_BENCH_DELETE)

backup-status:
	$(RUN_BACKUP_STATUS)

backup-run:
	$(RUN_BACKUP_RUN)

perf:
	$(RUN_PERF)

failover:
	$(RUN_FAILOVER)

backup-bench:
	$(RUN_BACKUP_BENCH)

ec2-install-docker:
	sudo dnf update -y
	sudo dnf install -y docker git make
	sudo systemctl start docker
	sudo systemctl enable docker
	sudo usermod -aG docker ec2-user
	@echo Docker installato. Disconnettersi e riconnettersi via SSH prima di usare docker senza sudo.

ec2-install-compose:
	sudo mkdir -p /usr/local/lib/docker/cli-plugins
	sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$$(uname -m)" -o /usr/local/lib/docker/cli-plugins/docker-compose
	sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
	docker compose version

ec2-install-buildx:
	sudo mkdir -p /usr/local/lib/docker/cli-plugins
	sudo curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" -o /usr/local/lib/docker/cli-plugins/docker-buildx
	sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
	docker buildx version

ec2-doctor:
	git --version
	make --version
	docker --version
	docker compose version
	docker buildx version || true
	go version || true

ec2-health:
	curl http://localhost:8081/health

ec2-up-3:
	$(COMPOSE_3) up -d --build

ec2-up-5:
	$(COMPOSE_5) up -d --build

ec2-up-7:
	$(COMPOSE_7) up -d --build

ec2-down-3:
	$(COMPOSE_3) down

ec2-down-5:
	$(COMPOSE_5) down

ec2-down-7:
	$(COMPOSE_7) down

ec2-clean-3:
	$(COMPOSE_3) down -v

ec2-clean-5:
	$(COMPOSE_5) down -v

ec2-clean-7:
	$(COMPOSE_7) down -v
