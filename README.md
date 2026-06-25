# Progetto Sistemi Distribuiti e Cloud Computing - Distributed Key-Value Storage

Sistema distribuito in Go che implementa uno storage chiave-valore replicato tramite un protocollo ispirato a Raft, con persistenza locale, snapshot, log compaction, Client Proxy, Backup Service, containerizzazione Docker Compose, CI GitHub Actions e deploy su Amazon EC2 tramite AWS Academy Learner Lab.

## Componenti principali

Il sistema è composto da:

```text
3+ Consensus Node
1 Client Proxy
1 Backup Service
```

### Consensus Node

Ogni Consensus Node gestisce:

```text
- elezione leader;
- replica del log;
- commit tramite quorum;
- state machine key-value;
- WAL persistente;
- snapshot locale;
- log compaction;
- InstallSnapshot;
- RPC per backup remoto.
```

### Client Proxy

Il Client Proxy espone un punto di ingresso unico verso il cluster:

```text
- riceve Put/Get/Delete/GetLeader;
- scopre il leader;
- inoltra le richieste al nodo corretto;
- usa retry, backoff e circuit breaker;
- espone /health su porta HTTP dedicata.
```

### Backup Service

Il Backup Service orchestra backup e snapshot:

```text
- forza snapshot remoti sui nodi;
- scarica snapshot dai Consensus Node;
- salva snapshot in /backup-data;
- richiede CompactLog dopo il download;
- tollera nodi non disponibili tramite Circuit Breaker;
- espone TriggerBackup e GetBackupStatus.
```

---

## Struttura del repository

```text
cmd/
  consensus-node/      # entrypoint Consensus Node
  client-proxy/        # entrypoint Client Proxy
  backup-service/      # entrypoint Backup Service
  bench-client/        # client manuale per Put/Get/Delete/GetLeader
  backup-client/       # client manuale per Backup Service

internal/
  consensus/           # logica Raft, persistenza, snapshot, RPC
  proxy/               # logica Client Proxy
  backup/              # logica Backup Service
  persistence/         # encoder/decoder e gestione storage locale

proto/
  consensus.proto
  kv.proto
  backup.proto

gen/go/
  consensuspb/
  kvpb/
  backuppb/

deployments/docker/
  Dockerfile.consensus-node
  Dockerfile.client-proxy
  Dockerfile.backup-service
  docker-compose.yml

documents/
  documentazione delle fasi di progetto

.github/workflows/
  ci.yml
  proto.yml
  docker-build-scan.yml
  compose-integration.yml
```

---

## Requisiti

Vedi anche:

```text
REQUIREMENTS.md
```

Requisiti principali:

```text
Go >= 1.25.4
Docker Engine
Docker Compose plugin
Docker Buildx >= 0.17.0
protoc
protoc-gen-go
protoc-gen-go-grpc
buf CLI
```

Nel progetto Go, il file equivalente a un `requirements.txt` Python è `go.mod`, che definisce modulo, versione Go e dipendenze Go.

---

## Dipendenze Go principali

Le dipendenze Go sono gestite da:

```text
go.mod
go.sum
```

Dipendenze principali:

```text
google.golang.org/grpc
google.golang.org/protobuf
github.com/sony/gobreaker/v2
github.com/stretchr/testify
```

Per riallineare le dipendenze:

```bash
go mod tidy
```

---

## Protobuf

I contratti gRPC sono definiti in:

```text
proto/consensus.proto
proto/kv.proto
proto/backup.proto
```

Gli stub Go generati sono in:

```text
gen/go/
```

Rigenerazione manuale:

```bash
make proto
```

oppure direttamente:

```bash
protoc --go_out=. --go_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing \
  --go-grpc_out=. --go-grpc_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing \
  proto/consensus.proto proto/kv.proto proto/backup.proto
```

Controlli Buf:

```bash
make buf
```

---

## Esecuzione locale con Docker Compose

Build e avvio del cluster:

```bash
make docker-up
```

Equivalente:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

Stato container:

```bash
make docker-ps
```

Health del Proxy:

```bash
curl http://localhost:8081/health
```

Stop senza cancellare i volumi:

```bash
make docker-down
```

Stop con cancellazione volumi:

```bash
make docker-clean
```

---

## Test manuali locali

### Leader discovery

```bash
TARGET=localhost:8080 OP=leader go run ./cmd/bench-client
```

Su Windows CMD:

```cmd
set TARGET=localhost:8080
set OP=leader
go run .\cmd\bench-client
```

### Put/Get

```bash
TARGET=localhost:8080 OP=put KEY=test-key VALUE=test-value go run ./cmd/bench-client
TARGET=localhost:8080 OP=get KEY=test-key go run ./cmd/bench-client
```

### Backup

```bash
TARGET=localhost:9090 OP=status go run ./cmd/backup-client
TARGET=localhost:9090 OP=backup FORCE_SNAPSHOT=true COMPACT_AFTER_DOWNLOAD=true go run ./cmd/backup-client
```

---

## Deploy su Amazon EC2 Learner Lab

La documentazione completa è in:

```text
documents/Fase10-deployEC2LearnerLab.md
```

Comandi principali su EC2:

```bash
sudo dnf update -y
sudo dnf install -y docker git
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
```

Docker Compose:

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

Buildx, se necessario:

```bash
sudo curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" -o /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
```

Avvio cluster su EC2:

```bash
git clone https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

---

## GitHub Actions

Workflow attivi:

```text
Go CI
Protobuf CI
Docker Build and Security Scan
Docker Compose Integration
```

La pipeline verifica:

```text
- go mod tidy;
- gofmt;
- go vet;
- go test ./...;
- build binari principali;
- buf lint e format;
- drift degli stub Protobuf;
- build immagini Docker;
- scan Trivy;
- smoke test Docker Compose.
```

---

## Comandi Make principali

```bash
make help
make tidy
make fmt
make vet
make test
make build
make ci
make proto
make buf
make docker-build
make docker-up
make docker-ps
make docker-down
make docker-clean
```

---

## Note su dati e volumi

Le directory runtime locali non devono essere committate:

```text
data/
backup-data/
```

In Docker, la persistenza è gestita tramite volumi:

```text
node1-data
node2-data
node3-data
backup-data
```

Usare:

```bash
docker compose -f deployments/docker/docker-compose.yml down
```

per fermare i container senza cancellare i volumi.

Usare:

```bash
docker compose -f deployments/docker/docker-compose.yml down -v
```

solo quando si vuole cancellare completamente lo stato.

---

## Stato del progetto

Funzionalità completate:

```text
[OK] Consensus Node
[OK] elezione leader
[OK] replica log
[OK] Client Proxy
[OK] WAL e recovery
[OK] snapshot locale
[OK] log compaction
[OK] Backup Service
[OK] Docker Compose
[OK] GitHub Actions
[OK] Deploy EC2 Learner Lab
[OK] test remoti Put/Get/Backup
[OK] persistenza volumi
```

Prossima fase:

```text
Fase 11 - Benchmark, scalabilità, fault tolerance e grafici finali
```
