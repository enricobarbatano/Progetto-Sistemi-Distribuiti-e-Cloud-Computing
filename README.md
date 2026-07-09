# Progetto Sistemi Distribuiti e Cloud Computing - Distributed Key-Value Storage

Sistema distribuito in Go che implementa uno storage chiave-valore replicato tramite un protocollo di consenso basato su log replicato e ispirato a Raft.

Il progetto elimina la dipendenza da un database centralizzato: le operazioni vengono coordinate da un leader, replicate sui follower e confermate tramite commit su quorum. Il sistema include persistenza locale tramite WAL, snapshot, compattazione logica, Client Proxy, Backup Service, Circuit Breaker, Docker Compose, GitHub Actions e deployment su Amazon EC2 tramite AWS Academy Learner Lab.

---

## Obiettivo del progetto

L'obiettivo è realizzare uno storage key-value distribuito, resiliente e deployabile in cloud.

Il sistema fornisce:

```text
Put
Get
Delete
GetLeader
Snapshot
Backup
Compattazione logica
Recovery da stato persistente
Failover del leader
```

Il client non comunica direttamente con i nodi del cluster, ma usa il Client Proxy. Il Proxy scopre dinamicamente il leader e inoltra le richieste al nodo corretto.

---

## Componenti principali

Il sistema è composto da:

```text
3+ Consensus Node
1 Client Proxy
1 Backup Service
```

### Consensus Node

Ogni Consensus Node è un componente stateful.

Gestisce:

```text
elezione del leader
replica del log
commit tramite quorum
state machine key-value
WAL persistente
snapshot locale
compattazione logica
InstallSnapshot
RPC per backup remoto
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
riceve Put/Get/Delete/GetLeader
scopre dinamicamente il leader
inoltra le richieste al nodo corretto
usa leader_hint per aggiornare la cache del leader
usa retry, backoff e Circuit Breaker
espone /health su porta HTTP dedicata
```

Il client esterno non deve conoscere la topologia del cluster, il numero di nodi o quale nodo sia leader.

### Backup Service

Il Backup Service è stateless rispetto al consenso e allo stato applicativo: non partecipa a Raft e non mantiene una replica attiva della state machine.

Agisce invece come gestore esterno di artefatti persistenti.

Responsabilità principali:

```text
forza snapshot remoti sui nodi
scarica snapshot dai Consensus Node
salva snapshot in /backup-data
richiede CompactLog dopo il download
tollera nodi non disponibili tramite Circuit Breaker
espone TriggerBackup e GetBackupStatus
```

---

## Architettura e pattern

Il sistema usa:

```text
gRPC e Protocol Buffers per comunicazione tipizzata
Client-side Service Discovery nel Proxy
Circuit Breaker per isolare nodi non disponibili
Docker Compose per orchestrazione locale e cloud
WAL e snapshot per persistenza e recovery
```

### Client-side Service Discovery

Il leader può cambiare dopo timeout, crash o nuova elezione. Per questo motivo il leader non è configurato staticamente nel client.

Il Proxy:

```text
mantiene una cache del leader
interroga i nodi tramite GetLeader
usa leader_hint restituiti dai follower
aggiorna dinamicamente il routing
```

### Circuit Breaker

Ogni nodo è protetto da un circuito indipendente tramite `gobreaker`.

Il Circuit Breaker evita chiamate ripetute verso nodi non disponibili e preserva risorse del Proxy e del Backup Service.

Nei test EC2, con un nodo offline, il Backup Service ha completato il backup scaricando 4 snapshot su 5.

---

## Struttura del repository

```text
cmd/
  consensus-node/             entrypoint Consensus Node
  client-proxy/               entrypoint Client Proxy
  backup-service/             entrypoint Backup Service
  bench-client/               client manuale per Put/Get/Delete/GetLeader
  backup-client/              client manuale per Backup Service
  perf-client/                client benchmark latenza/throughput
  failover-client/            client per test crash leader
  backup-benchmark-client/    client benchmark Backup Service

internal/
  consensus/                  logica consenso, replica, snapshot, RPC
  proxy/                      logica Client Proxy, discovery, circuit breaker
  backup/                     logica Backup Service
  persistence/                gestione WAL, state file e snapshot locali
  storage/                    state machine key-value

proto/
  consensus.proto             RPC interne di consenso
  kv.proto                    RPC key-value
  backup.proto                RPC backup e snapshot

gen/go/
  consensuspb/                stub Go generati da consensus.proto
  kvpb/                       stub Go generati da kv.proto
  backuppb/                   stub Go generati da backup.proto

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
  raw/                        CSV raw dei benchmark
  processed/                  CSV aggregati
  figures/                    grafici generati

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

Requisiti principali:

```text
Go >= 1.22
Docker Engine
Docker Compose plugin
Docker Buildx >= 0.17.0
GNU Make
protoc
protoc-gen-go
protoc-gen-go-grpc
buf CLI
Python 3.10+
```

---

## Installazione rapida su Windows

Su Windows è possibile installare gli strumenti principali tramite `winget`.

Aprire PowerShell come amministratore ed eseguire:

```powershell
winget install -e --id GoLang.Go --accept-package-agreements --accept-source-agreements
winget install -e --id Docker.DockerDesktop --accept-package-agreements --accept-source-agreements
winget install -e --id GnuWin32.Make --accept-package-agreements --accept-source-agreements
winget install -e --id Google.Protobuf --accept-package-agreements --accept-source-agreements
winget install -e --id bufbuild.buf --accept-package-agreements --accept-source-agreements
```

Dopo l'installazione, chiudere e riaprire PowerShell o VS Code.

Se `make` non viene riconosciuto, aggiungere GnuWin32 Make al PATH utente:

```powershell
$makePath = "C:\Program Files (x86)\GnuWin32\bin"
$currentPath = [System.Environment]::GetEnvironmentVariable("Path", "User")

if ($currentPath -notlike "*$makePath*") {
    [System.Environment]::SetEnvironmentVariable("Path", "$currentPath;$makePath", "User")
}

$env:Path = "$env:Path;$makePath"
```

Verifica:

```powershell
make --version
```

Se il terminale integrato di VS Code era già aperto, chiuderlo e riaprirlo dopo la modifica del PATH.

---

## Installazione plugin Go per Protobuf

Dopo aver installato Go e `protoc`, installare i plugin Go necessari:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Assicurarsi che la directory dei binari Go sia nel PATH.

Su Windows di solito è:

```text
%USERPROFILE%\go\bin
```

Su Linux/macOS di solito è:

```text
$HOME/go/bin
```

---

## Installazione dipendenze Python per i grafici

Gli script Python usati per generare i grafici richiedono `pandas` e `matplotlib`.

Installazione semplice:

```bash
pip install pandas matplotlib
```

Con virtual environment su Windows:

```powershell
python -m venv myenv
myenv\Scripts\activate
pip install pandas matplotlib
```

Con virtual environment su Linux/macOS:

```bash
python3 -m venv myenv
source myenv/bin/activate
pip install pandas matplotlib
```

---

## Dipendenze Go

Le dipendenze Go sono gestite da:

```text
go.mod
go.sum
```

Il file `go.mod` definisce il modulo Go, la versione di Go e le librerie richieste dal progetto.

Il file `go.sum` contiene i checksum usati da Go per verificare l'integrità delle dipendenze scaricate.

Le principali dipendenze Go sono:

```text
google.golang.org/grpc
google.golang.org/protobuf
github.com/sony/gobreaker/v2
github.com/stretchr/testify
```

Per scaricare e riallineare automaticamente le dipendenze:

```bash
go mod tidy
```

oppure tramite Make:

```bash
make tidy
```

Il target `make tidy` esegue:

```text
go mod tidy
git diff --exit-code go.mod go.sum
```

Quindi verifica anche che `go.mod` e `go.sum` siano allineati allo stato corrente del codice.

---

## Verifica rapida ambiente

Dalla root del progetto:

```bash
make doctor
```

Il comando mostra le versioni di:

```text
Go
protoc
Docker
Docker Compose
buf
```

Esempio di output verificato localmente:

```text
go version go1.25.4 windows/amd64
libprotoc 33.0
Docker version 29.5.3
Docker Compose version v5.1.4
buf 1.71.0
```

---

## Controlli locali consigliati

Prima di un commit o della consegna:

```bash
make ci
```

Il target `make ci` esegue:

```text
make tidy
make fmt
make vet
make test
make build
```

Significato:

```text
make tidy   riallinea go.mod/go.sum
make fmt    formatta il codice Go
make vet    esegue analisi statica
make test   esegue go test ./...
make build  compila tutti i comandi in cmd/
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

Rigenerazione:

```bash
make proto
```

Controllo lint e formattazione:

```bash
make buf
```

Dopo aver modificato i file `.proto`, eseguire:

```bash
make proto
make ci
```

---

## Avvio rapido del sistema

Per avviare un cluster locale a 3 nodi:

```bash
make docker-up-3
```

Verificare lo stato del Client Proxy:

```bash
curl.exe http://localhost:8081/health
```

Su PowerShell, in alternativa:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8081/health
```

Output atteso:

```json
{"status":"ok"}
```

Test rapido completo:

```bash
make docker-up-3
curl.exe http://localhost:8081/health
make bench-leader
make bench-put KEY=test-key VALUE=test-value
make bench-get KEY=test-key
make backup-status
make backup-run
make docker-down-3
```

Questa sequenza:

```text
avvia il cluster a 3 nodi
verifica il Client Proxy
scopre il leader
esegue una Put
esegue una Get
verifica il Backup Service
avvia un backup
spegne il cluster senza cancellare i volumi
```

---

## Docker Compose

Avvio cluster a 3 nodi:

```bash
make docker-up-3
```

Avvio cluster a 5 nodi:

```bash
make docker-up-5
```

Avvio cluster a 7 nodi:

```bash
make docker-up-7
```

Stato container:

```bash
make docker-ps-3
make docker-ps-5
make docker-ps-7
```

Log container:

```bash
make docker-logs-3
make docker-logs-5
make docker-logs-7
```

Stop senza cancellare i volumi:

```bash
make docker-down-3
make docker-down-5
make docker-down-7
```

Stop con cancellazione dei volumi:

```bash
make docker-clean-3
make docker-clean-5
make docker-clean-7
```

Usare `docker-clean` solo quando si vuole eliminare completamente lo stato persistente, inclusi WAL, snapshot e state file dei Consensus Node.

---

## Operazioni key-value tramite Make

Scoprire il leader corrente:

```bash
make bench-leader
```

Eseguire una Put:

```bash
make bench-put KEY=nome VALUE=rosalie
```

Eseguire una Get:

```bash
make bench-get KEY=nome
```

Eseguire una Delete:

```bash
make bench-delete KEY=nome
```

Esempio completo:

```bash
make bench-put KEY=a VALUE=b
make bench-get KEY=a
make bench-delete KEY=a
make bench-get KEY=a
```

Dopo la Delete, l'ultima Get deve restituire chiave non trovata.

Per usare un'istanza EC2:

```bash
make bench-leader TARGET=<EC2_PUBLIC_IP>:8080
make bench-put TARGET=<EC2_PUBLIC_IP>:8080 KEY=a VALUE=b
make bench-get TARGET=<EC2_PUBLIC_IP>:8080 KEY=a
```

---

## Backup Service tramite Make

Leggere lo stato del Backup Service:

```bash
make backup-status
```

Avviare un backup manuale:

```bash
make backup-run
```

Esempio verso EC2:

```bash
make backup-status BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
make backup-run BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```

Variabili principali:

```text
BACKUP_TARGET             default localhost:9090
FORCE_SNAPSHOT            default true
COMPACT_AFTER_DOWNLOAD    default true
```

---

## Benchmark Put/Get

Eseguire benchmark locale:

```bash
make perf CLUSTER_SIZE=3
```

Esempio remoto verso EC2:

```bash
make perf TARGET=<EC2_PUBLIC_IP>:8080 CLUSTER_SIZE=5 KEY_PREFIX=ec2-scalability-5
```

Variabili principali:

```text
TARGET             default localhost:8080
CLUSTER_SIZE       default 3
WARMUP_PUTS        default 30
WARMUP_GETS        default 30
PUTS               default 300
GETS               default 300
CONCURRENCY        default 1
KEY_PREFIX         default local-test
PERF_CSV_OUT       default reports/raw/scalability_<CLUSTER_SIZE>nodes.csv
PERF_SUMMARY_OUT   default reports/processed/scalability_<CLUSTER_SIZE>nodes_summary.csv
```

---

## Benchmark failover

Eseguire il client di failover:

```bash
make failover
```

Esempio su cluster a 5 nodi:

```bash
make failover TARGET=<EC2_PUBLIC_IP>:8080 CLUSTER_SIZE=5 TRIALS=10
```

Il client indica il leader corrente e richiede di fermare manualmente il container corrispondente.

Esempio:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml stop node-3
```

Dopo il trial, riavviare il nodo:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml start node-3
```

Variabili principali:

```text
TARGET             default localhost:8080
CLUSTER_SIZE       default 3
TRIALS             default 10
FAILOVER_CSV_OUT   default reports/raw/failover_trials.csv
KEY_PREFIX         default local-test
```

---

## Benchmark Backup Service

Eseguire benchmark del Backup Service:

```bash
make backup-bench
```

Esempio remoto verso EC2:

```bash
make backup-bench TARGET=<EC2_PUBLIC_IP>:8080 BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```

Variabili principali:

```text
TARGET                    default localhost:8080
BACKUP_TARGET             default localhost:9090
DATASET_SIZES             default 100,300,600
BACKUP_CSV_OUT            default reports/raw/backup_compaction_results.csv
KEY_PREFIX                default local-test
FORCE_SNAPSHOT            default true
COMPACT_AFTER_DOWNLOAD    default true
```

---

## Generazione grafici

Scalabilità:

```bash
python scripts/plot_results.py
```

Throughput:

```bash
python scripts/plot_throughput.py
```

Failover:

```bash
python scripts/plot_failover.py
```

Backup:

```bash
python scripts/plot_backup_results.py
```

Output principali:

```text
reports/processed/
reports/figures/
```

---

## Deploy su Amazon EC2 Learner Lab

Questa sezione descrive il flusso essenziale per accedere all'istanza EC2 dal terminale del Learner Lab, avviare il progetto tramite Makefile e testarlo da PC locale.

### 1. Accesso all'istanza EC2 dal Learner Lab

Dopo aver avviato il Learner Lab, aprire il terminale integrato. Il prompt sarà simile a:

```bash
eee_...@runweb...:~$
```

Questo terminale non è ancora l'istanza EC2. Da qui bisogna collegarsi all'istanza tramite SSH.

Per trovare il percorso della chiave `.pem`:

```bash
find ~ -name "*.pem"
```

Esempio di output:

```text
/mnt/vocwork4/work/.../.ssh/labsuser.pem
```

Impostare i permessi della chiave:

```bash
chmod 400 <PEM_PATH>
```

Collegarsi all'istanza EC2 usando il Public IPv4 dell'istanza:

```bash
ssh -i <PEM_PATH> ec2-user@<EC2_PUBLIC_IP>
```

Esempio:

```bash
ssh -i /mnt/vocwork4/work/.../.ssh/labsuser.pem ec2-user@18.215.171.188
```

Alla prima connessione, se richiesto, rispondere:

```bash
yes
```

Se l'accesso ha successo, il prompt diventa simile a:

```bash
[ec2-user@ip-172-31-xx-xx ~]$
```

Per uscire dall'istanza EC2 e tornare al terminale Learner Lab:

```bash
exit
```

---

### 2. Prima configurazione dell'istanza EC2

Se l'istanza è nuova o l'ambiente è stato resettato, installare Docker, Git e Make:

```bash
sudo dnf update -y
sudo dnf install -y docker git make
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
```

Dopo `usermod`, uscire e rientrare via SSH:

```bash
exit
ssh -i <PEM_PATH> ec2-user@<EC2_PUBLIC_IP>
```

Questo passaggio è necessario per applicare il gruppo Docker all'utente `ec2-user`.

---

### 3. Caso istanza già configurata

Se l'istanza era solo stoppata e poi riavviata, non è necessario reinstallare Docker, Git, Make, Docker Compose o Buildx.

Dopo l'accesso SSH:

```bash
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
git pull
make help
make ec2-doctor
```

Se `make ec2-doctor` mostra Docker, Docker Compose e Buildx, l'ambiente è pronto.
Nota: se l'istanza viene stoppata e poi riavviata, il Public IPv4 può cambiare. In quel caso aggiornare i comandi usando il nuovo IP pubblico mostrato nella console EC2.

---

### 4. Clone del repository

Se il repository non è ancora presente sull'istanza:

```bash
git clone https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
```

Se il repository è già presente:

```bash
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
git pull
```

---

### 5. Installazione Docker Compose e Buildx tramite Make

Dentro la cartella del progetto:

```bash
make ec2-install-compose
make ec2-install-buildx
make ec2-doctor
```

Se `make ec2-doctor` mostra `go: command not found`, non è un problema per il deploy Docker. Go serve sull'istanza solo se si vogliono eseguire direttamente comandi come `make ci` o i client con `go run`. Le immagini Docker vengono compilate usando l'immagine Go definita nei Dockerfile.

---

### 6. Avvio cluster su EC2

Cluster a 3 nodi:

```bash
make ec2-up-3
```

Cluster a 5 nodi:

```bash
make ec2-up-5
```

Cluster a 7 nodi:

```bash
make ec2-up-7
```

Controllare i container:

```bash
make docker-ps-3
make docker-ps-5
make docker-ps-7
```

Verificare l'health check del Client Proxy direttamente da EC2:

```bash
make ec2-health
```

Output atteso:

```json
{"status":"ok"}
```

---

### 7. Porte Security Group

Nel Security Group dell'istanza EC2 devono essere aperte le seguenti porte:

```text
22    SSH
8080  Client Proxy gRPC
8081  Proxy health check
9090  Backup Service gRPC
```

I Consensus Node comunicano principalmente nella rete Docker interna. Dall'esterno si usano il Client Proxy e il Backup Service.

---

### 8. Test dal PC locale verso EC2

Dopo aver avviato il cluster su EC2, dal PC locale usare il public IPv4 dell'istanza.

Il Client Proxy è esposto sulla porta `8080`.

Leader discovery:

```bash
make bench-leader TARGET=<EC2_PUBLIC_IP>:8080
```

Put:

```bash
make bench-put TARGET=<EC2_PUBLIC_IP>:8080 KEY=test-key VALUE=test-value
```

Get:

```bash
make bench-get TARGET=<EC2_PUBLIC_IP>:8080 KEY=test-key
```

Delete:

```bash
make bench-delete TARGET=<EC2_PUBLIC_IP>:8080 KEY=test-key
```

Esempio:

```bash
make bench-leader TARGET=18.215.171.188:8080
make bench-put TARGET=18.215.171.188:8080 KEY=test-key VALUE=test-value
make bench-get TARGET=18.215.171.188:8080 KEY=test-key
make bench-delete TARGET=18.215.171.188:8080 KEY=test-key
```

Il Backup Service è esposto sulla porta `9090`.

Backup status:

```bash
make backup-status BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```

Trigger backup:

```bash
make backup-run BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```

Esempio:

```bash
make backup-status BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
make backup-run BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```

Health check del Proxy da PC locale:

```bash
curl.exe http://<EC2_PUBLIC_IP>:8081/health
```

Esempio:

```bash
curl.exe http://18.215.171.188:8081/health
```

Output atteso:

```json
{"status":"ok"}
```

---

### 9. Stop e pulizia su EC2

Fermare il cluster senza cancellare i volumi:

```bash
make ec2-down-3
make ec2-down-5
make ec2-down-7
```

Cancellare anche i volumi persistenti, quindi WAL, snapshot e state file:

```bash
make ec2-clean-3
make ec2-clean-5
make ec2-clean-7
```

Usare i target `ec2-clean-*` solo quando si vuole ripartire da stato completamente pulito.

```

Cancellare anche i volumi persistenti, quindi WAL, snapshot e state file:

```bash
make ec2-clean-3
make ec2-clean-5
make ec2-clean-7
```

Usare i target `ec2-clean-*` solo quando si vuole ripartire da stato completamente pulito.

---

### 10. Sequenza rapida consigliata

Prima configurazione di una nuova istanza:

```bash
find ~ -name "*.pem"
chmod 400 <PEM_PATH>
ssh -i <PEM_PATH> ec2-user@<EC2_PUBLIC_IP>
sudo dnf update -y
sudo dnf install -y docker git make
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
exit
```

Rientro e avvio progetto:

```bash
ssh -i <PEM_PATH> ec2-user@<EC2_PUBLIC_IP>
git clone https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
make ec2-install-compose
make ec2-install-buildx
make ec2-doctor
make ec2-up-3
make ec2-health
```

Se l'istanza era già configurata:

```bash
ssh -i <PEM_PATH> ec2-user@<EC2_PUBLIC_IP>
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
git pull
make ec2-doctor
make ec2-up-3
make ec2-health
```

Dal PC locale:

```bash
make bench-leader TARGET=<EC2_PUBLIC_IP>:8080
make bench-put TARGET=<EC2_PUBLIC_IP>:8080 KEY=test-key VALUE=test-value
make bench-get TARGET=<EC2_PUBLIC_IP>:8080 KEY=test-key
make backup-status BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
make backup-run BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
```


## GitHub Actions

Workflow presenti:

```text
Go CI
Protobuf CI
Docker Build and Security Scan
Docker Compose Integration
```

La pipeline verifica:

```text
go mod tidy
gofmt
go vet
go test ./...
build binari principali
buf lint e format
drift degli stub Protobuf
build immagini Docker
scan Trivy
smoke test Docker Compose
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

Per fermare i container senza cancellare i volumi:

```bash
make docker-down-3
```

Per cancellare completamente lo stato:

```bash
make docker-clean-3
```

---

## Risultati sperimentali principali

Benchmark EC2 finali:

```text
success rate 100% con cluster da 3, 5 e 7 nodi
throughput vicino a 9 ops/s
rielezione leader media circa 1.27 s
downtime mediano percepito circa 1.23 s
backup standard: 5 snapshot su 5 nodi
backup con nodo offline: 4 snapshot su 5 nodi
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

## Pulizia ambiente

Arresto e rimozione volumi del cluster 3 nodi:

```bash
make docker-clean-3
```

Per 5 nodi:

```bash
make docker-clean-5
```

Per 7 nodi:

```bash
make docker-clean-7
```

Rimozione immagini non usate:

```bash
docker system prune -f
```

---

## Riferimenti

- Diego Ongaro, John Ousterhout, In Search of an Understandable Consensus Algorithm, USENIX ATC, 2014.
- gRPC Documentation for Go: https://grpc.io/docs/languages/go/
- Docker Compose Documentation: https://docs.docker.com/compose/
- Circuit Breaker Pattern: https://microservices.io/patterns/reliability/circuit-breaker.html
- gobreaker: https://github.com/sony/gobreaker

---

## Autore

Enrico Barbatano  
Università degli Studi di Roma Tor Vergata  
Corso di Sistemi Distribuiti e Cloud Computing