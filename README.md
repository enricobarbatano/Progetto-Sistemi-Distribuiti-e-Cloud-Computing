# Progetto Sistemi Distribuiti e Cloud Computing - Distributed Key-Value Storage

Sistema distribuito in Go che implementa uno **storage chiave-valore replicato** tramite un protocollo di consenso basato su log replicato e ispirato a Raft.

Il progetto elimina la dipendenza da un database centralizzato: le operazioni vengono coordinate da un leader, replicate sui follower e confermate tramite commit su quorum. Il sistema include persistenza locale tramite WAL, snapshot, compattazione logica, Client Proxy, Backup Service, Circuit Breaker, containerizzazione Docker Compose, CI GitHub Actions e deployment su Amazon EC2 tramite AWS Academy Learner Lab.

---

## Indice

- [Componenti principali](#componenti-principali)
- [Architettura e pattern](#architettura-e-pattern)
- [Struttura del repository](#struttura-del-repository)
- [Requisiti](#requisiti)
- [Dipendenze Go principali](#dipendenze-go-principali)
- [Protobuf](#protobuf)
- [Esecuzione locale con Docker Compose](#esecuzione-locale-con-docker-compose)
- [Configurazioni cluster](#configurazioni-cluster)
- [Test manuali locali](#test-manuali-locali)
- [Benchmark e test sperimentali](#benchmark-e-test-sperimentali)
- [Deploy su Amazon EC2 Learner Lab](#deploy-su-amazon-ec2-learner-lab)
- [GitHub Actions](#github-actions)
- [Comandi Make principali](#comandi-make-principali)
- [Note su dati e volumi](#note-su-dati-e-volumi)
- [Risultati sperimentali principali](#risultati-sperimentali-principali)
- [Limitazioni note](#limitazioni-note)
- [Pulizia ambiente](#pulizia-ambiente)
- [Stato del progetto](#stato-del-progetto)

---

## Componenti principali

Il sistema è composto da:

```text
3+ Consensus Node
1 Client Proxy
1 Backup Service
```

### Consensus Node

Ogni Consensus Node è un componente stateful e gestisce:

```text
- elezione del leader;
- replica del log;
- commit tramite quorum;
- state machine key-value;
- WAL persistente;
- snapshot locale;
- compattazione logica;
- InstallSnapshot;
- RPC per backup remoto.
```

Ogni nodo mantiene su volume Docker file persistenti come:

```text
node-X_wal.log
node-X_snapshot.json
node-X_state.json
```

Le operazioni modificanti, come `Put` e `Delete`, vengono trasformate in entry del log e applicate alla state machine solo dopo il commit su quorum.

### Client Proxy

Il Client Proxy espone un punto di ingresso unico verso il cluster.

Responsabilità principali:

```text
- riceve Put/Get/Delete/GetLeader;
- scopre dinamicamente il leader;
- inoltra le richieste al nodo corretto;
- usa leader_hint per aggiornare la cache del leader;
- usa retry, backoff e Circuit Breaker;
- espone /health su porta HTTP dedicata.
```

Il client esterno non deve conoscere la topologia del cluster, il numero di nodi o quale nodo sia leader.

### Backup Service

Il Backup Service è stateless rispetto al consenso e allo stato applicativo: non partecipa a Raft e non mantiene una replica attiva della state machine. Agisce invece come gestore esterno di artefatti persistenti.

Responsabilità principali:

```text
- forza snapshot remoti sui nodi;
- scarica snapshot dai Consensus Node;
- salva snapshot in /backup-data;
- richiede CompactLog dopo il download;
- tollera nodi non disponibili tramite Circuit Breaker;
- espone TriggerBackup e GetBackupStatus.
```

---

## Architettura e pattern

Il sistema usa:

```text
- gRPC e Protocol Buffers per comunicazione tipizzata;
- Client-side Service Discovery nel Proxy;
- Circuit Breaker per isolare nodi non disponibili;
- Docker Compose per orchestrazione locale/cloud;
- WAL e snapshot per persistenza e recovery.
```

### Client-side Service Discovery

Il leader può cambiare dopo timeout, crash o nuova elezione. Per questo il leader non è configurato staticamente nel client.

Il Proxy:

```text
- mantiene una cache del leader;
- interroga i nodi tramite GetLeader;
- usa leader_hint restituiti dai follower;
- aggiorna dinamicamente il routing.
```

### Circuit Breaker

Ogni nodo è protetto da un circuito indipendente tramite `gobreaker`.

Il Circuit Breaker evita chiamate ripetute verso nodi non disponibili e preserva risorse del Proxy e del Backup Service. Nei test EC2, con un nodo offline, il Backup Service ha completato il backup scaricando `4 snapshot su 5`.

---

## Struttura del repository

```text
cmd/
  consensus-node/             # entrypoint Consensus Node
  client-proxy/               # entrypoint Client Proxy
  backup-service/             # entrypoint Backup Service
  bench-client/               # client manuale per Put/Get/Delete/GetLeader
  backup-client/              # client manuale per Backup Service
  perf-client/                # client benchmark latenza/throughput
  failover-client/            # client per test crash leader
  backup-benchmark-client/    # client benchmark Backup Service

internal/
  consensus/                  # logica consenso, replica, snapshot, RPC
  proxy/                      # logica Client Proxy, discovery, circuit breaker
  backup/                     # logica Backup Service
  persistence/                # encoder/decoder e gestione storage locale
  storage/                    # state machine key-value

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
  docker-compose-5nodes.yml
  docker-compose-7nodes.yml

documents/
  documentazione delle fasi di progetto

reports/
  raw/                        # CSV raw dei benchmark
  processed/                  # CSV aggregati
  figures/                    # grafici generati

scripts/
  plot_results.py
  plot_throughput.py
  plot_failover.py
  plot_backup_results.py
  measure_node_storage.sh
  run_scalability_test.sh

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
docs/REQUIREMENTS.md
```

Requisiti principali:

```text
Go >= 1.22
Docker Engine
Docker Compose plugin
Docker Buildx >= 0.17.0
protoc
protoc-gen-go
protoc-gen-go-grpc
buf CLI
Python 3.10+
```

Per i grafici Python:

```bash
pip install pandas matplotlib
```

Verifica versioni:

```bash
go version
docker --version
docker compose version
python --version
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

Oppure direttamente:

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

Build e avvio del cluster base a 3 nodi:

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

Equivalente:

```bash
docker compose -f deployments/docker/docker-compose.yml ps
```

Health del Proxy:

```bash
curl http://localhost:8081/health
```

Output atteso:

```json
{"status":"ok"}
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

## Configurazioni cluster

### Cluster a 3 nodi

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

### Cluster a 5 nodi

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml up -d --build
```

### Cluster a 7 nodi

```bash
docker compose -f deployments/docker/docker-compose-7nodes.yml up -d --build
```

### Arresto cluster

3 nodi:

```bash
docker compose -f deployments/docker/docker-compose.yml down
```

5 nodi:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml down
```

7 nodi:

```bash
docker compose -f deployments/docker/docker-compose-7nodes.yml down
```

Aggiungere `-v` solo se si vogliono cancellare anche i volumi persistenti.

---

## Test manuali locali

### Leader discovery

```bash
TARGET=localhost:8080 OP=leader go run ./cmd/bench-client
```

Su Windows CMD:

```cmd
set "TARGET=localhost:8080"
set "OP=leader"
go run .\cmd\bench-client
```

### Put/Get

Linux/macOS:

```bash
TARGET=localhost:8080 OP=put KEY=test-key VALUE=test-value go run ./cmd/bench-client
TARGET=localhost:8080 OP=get KEY=test-key go run ./cmd/bench-client
```

Windows CMD:

```cmd
set "TARGET=localhost:8080"
set "OP=put"
set "KEY=test-key"
set "VALUE=test-value"
go run .\cmd\bench-client

set "OP=get"
go run .\cmd\bench-client
```

### Backup

Linux/macOS:

```bash
TARGET=localhost:9090 OP=status go run ./cmd/backup-client
TARGET=localhost:9090 OP=backup FORCE_SNAPSHOT=true COMPACT_AFTER_DOWNLOAD=true go run ./cmd/backup-client
```

Windows CMD:

```cmd
set "TARGET=localhost:9090"
set "OP=backup"
set "FORCE_SNAPSHOT=true"
set "COMPACT_AFTER_DOWNLOAD=true"
go run .\cmd\backup-client
```

---

## Benchmark e test sperimentali

### Benchmark scalabilità con perf-client

Linux/macOS:

```bash
TARGET=localhost:8080 \
CLUSTER_SIZE=3 \
WARMUP_PUTS=30 \
WARMUP_GETS=30 \
PUTS=300 \
GETS=300 \
CONCURRENCY=1 \
KEY_PREFIX=local-scalability-3 \
CSV_OUT=reports/raw/scalability_3nodes.csv \
SUMMARY_OUT=reports/processed/scalability_3nodes_summary.csv \
go run ./cmd/perf-client
```

Windows CMD:

```cmd
set "TARGET=localhost:8080"
set "CLUSTER_SIZE=3"
set "WARMUP_PUTS=30"
set "WARMUP_GETS=30"
set "PUTS=300"
set "GETS=300"
set "CONCURRENCY=1"
set "KEY_PREFIX=local-scalability-3"
set "CSV_OUT=reports/raw/scalability_3nodes.csv"
set "SUMMARY_OUT=reports/processed/scalability_3nodes_summary.csv"
go run .\cmd\perf-client
```

### Failover client

Per misurare il recupero dopo crash del leader su cluster a 5 nodi:

```bash
TARGET=localhost:8080 \
CLUSTER_SIZE=5 \
TRIALS=10 \
CSV_OUT=reports/raw/failover_trials.csv \
KEY_PREFIX=local-failover \
go run ./cmd/failover-client
```

Il client stampa il leader corrente e chiede di fermare il container corrispondente.

Esempio:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml stop node-3
```

Dopo il trial, riavviare il nodo:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml start node-3
```

### Backup benchmark client

```bash
KV_TARGET=localhost:8080 \
BACKUP_TARGET=localhost:9090 \
DATASET_SIZES=100,300,600 \
CSV_OUT=reports/raw/backup_compaction_results.csv \
KEY_PREFIX=local-backup \
FORCE_SNAPSHOT=true \
COMPACT_AFTER_DOWNLOAD=true \
go run ./cmd/backup-benchmark-client
```

---

## Generazione report e grafici

### Scalabilità

```bash
python scripts/plot_results.py
```

Output principali:

```text
reports/processed/scalability_summary.csv
reports/figures/latency_avg_vs_cluster_size.png
reports/figures/latency_p95_vs_cluster_size.png
reports/figures/latency_p99_vs_cluster_size.png
reports/figures/success_rate_vs_cluster_size.png
```

### Throughput

```bash
python scripts/plot_throughput.py
```

Output:

```text
reports/processed/throughput_summary.csv
reports/figures/throughput_vs_cluster_size.png
```

### Failover

```bash
python scripts/plot_failover.py
```

Output:

```text
reports/processed/failover_summary.csv
reports/figures/failover_downtime_histogram.png
reports/figures/failover_downtime_cdf.png
reports/figures/failover_failed_puts.png
```

### Backup

```bash
python scripts/plot_backup_results.py
```

Output:

```text
reports/processed/backup_compaction_summary.csv
reports/figures/backup_duration_vs_dataset_size.png
reports/figures/downloaded_snapshots_vs_dataset_size.png
```

### Misura storage dei nodi

Da eseguire su EC2 o sull'host Docker:

```bash
chmod +x scripts/measure_node_storage.sh
CLUSTER_SIZE=5 LABEL=after-backup ./scripts/measure_node_storage.sh
cat reports/raw/node_storage_measurements.csv
```

---

## Deploy su Amazon EC2 Learner Lab

La documentazione completa è in:

```text
documents/Fase10-deployEC2LearnerLab.md
```

### Preparazione istanza EC2

Comandi principali su Amazon Linux 2023:

```bash
sudo dnf update -y
sudo dnf install -y docker git
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
```

Dopo `usermod`, disconnettersi e riconnettersi via SSH.

### Docker Compose

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

### Buildx, se necessario

```bash
sudo curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" -o /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
```

### Avvio cluster su EC2

```bash
git clone https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
docker compose -f deployments/docker/docker-compose.yml up -d --build
curl http://localhost:8081/health
```

### Porte Security Group

| Porta | Servizio |
|---:|---|
| 22 | SSH |
| 8080 | Client Proxy gRPC |
| 8081 | Proxy `/health` |
| 9090 | Backup Service gRPC |

### Benchmark remoto da PC locale

Usare l'IPv4 pubblico dell'istanza EC2.

Windows CMD:

```cmd
set "TARGET=<EC2_PUBLIC_IP>:8080"
set "CLUSTER_SIZE=5"
set "WARMUP_PUTS=30"
set "WARMUP_GETS=30"
set "PUTS=300"
set "GETS=300"
set "CONCURRENCY=1"
set "KEY_PREFIX=ec2-scalability-5"
set "CSV_OUT=reports/raw/scalability_5nodes.csv"
set "SUMMARY_OUT=reports/processed/scalability_5nodes_summary.csv"
go run .\cmd\perf-client
```

Il percorso misurato nei benchmark remoti è:

```text
PC locale -> Internet -> Security Group -> EC2 -> Proxy -> Cluster Docker
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

## Risultati sperimentali principali

Benchmark EC2 finali:

```text
[OK] success rate 100% con cluster da 3, 5 e 7 nodi
[OK] throughput vicino a 9 ops/s
[OK] rielezione leader media circa 1.27 s
[OK] downtime mediano percepito circa 1.23 s
[OK] backup standard: 5 snapshot su 5 nodi
[OK] backup con nodo offline: 4 snapshot su 5 nodi
```

Sintesi prestazioni:

| Nodi | Operazione | Avg ms | P99 ms | ops/s |
|---:|---|---:|---:|---:|
| 3 | Get | 107.05 | 109.46 | 9.37 |
| 5 | Get | 121.61 | 274.56 | 8.25 |
| 7 | Get | 106.29 | 112.14 | 9.44 |
| 3 | Put | 109.81 | 124.46 | 9.14 |
| 5 | Put | 123.16 | 152.66 | 8.15 |
| 7 | Put | 110.88 | 119.13 | 9.05 |

---

## Limitazioni note

```text
- La compattazione è logica: gli snapshot consolidano lo stato, ma il WAL fisico resta append-only.
- Non sono implementate ottimizzazioni Raft avanzate come ReadIndex o lease-based reads.
- Le misure di failover sono basate su polling con risoluzione di circa 100 ms.
- I test coprono principalmente guasti fail-stop tramite arresto container.
- Le RPC non usano TLS/autenticazione nella configurazione didattica.
```

---

## Pulizia ambiente

Arresto e rimozione volumi del cluster 3 nodi:

```bash
docker compose -f deployments/docker/docker-compose.yml down -v
```

Per 5 nodi:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml down -v
```

Per 7 nodi:

```bash
docker compose -f deployments/docker/docker-compose-7nodes.yml down -v
```

Rimozione immagini non usate:

```bash
docker system prune -f
```

---

## Stato del progetto

Funzionalità completate:

```text
[OK] Consensus Node
[OK] elezione leader
[OK] replica log
[OK] commit su quorum
[OK] Client Proxy
[OK] Client-side Service Discovery
[OK] Circuit Breaker
[OK] WAL e recovery
[OK] snapshot locale
[OK] compattazione logica
[OK] Backup Service
[OK] Docker Compose
[OK] GitHub Actions
[OK] Deploy EC2 Learner Lab
[OK] benchmark scalabilità 3/5/7 nodi
[OK] test failover leader
[OK] test Backup Service
[OK] test backup parziale con nodo offline
[OK] persistenza volumi
```

---

## Riferimenti

- Diego Ongaro, John Ousterhout, *In Search of an Understandable Consensus Algorithm*, USENIX ATC, 2014.
- gRPC Documentation for Go: https://grpc.io/docs/languages/go/
- Docker Compose Documentation: https://docs.docker.com/compose/
- Circuit Breaker Pattern: https://microservices.io/patterns/reliability/circuit-breaker.html
- gobreaker: https://github.com/sony/gobreaker

---

## Autore

**Enrico Barbatano**  
Università degli Studi di Roma Tor Vergata  
Corso di Sistemi Distribuiti e Cloud Computing
