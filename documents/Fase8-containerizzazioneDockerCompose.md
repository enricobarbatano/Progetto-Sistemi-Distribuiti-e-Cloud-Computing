# Fase 9 - Containerizzazione con Docker Compose

Questo documento descrive la Fase 9 del progetto, dedicata alla containerizzazione dei componenti principali del sistema distribuito e alla loro orchestrazione tramite Docker Compose.

Dopo la Fase 8, il progetto disponeva già di tre componenti distinti:

- **Consensus Node**, componente stateful che implementa il protocollo di consenso, la replica del log, la persistenza, gli snapshot locali e le RPC di backup;
- **Client Proxy**, componente stateless che espone un punto di ingresso unico verso il cluster e instrada le richieste verso il leader corrente;
- **Backup Service**, componente amministrativo che orchestra snapshot, download degli snapshot, compaction remota e backup parziale in caso di nodo non disponibile.

La Fase 9 ha trasformato questi componenti in servizi containerizzati, avviabili come cluster locale tramite Docker Compose e pronti per una successiva esecuzione su Amazon EC2.

---

## 1. Obiettivo della fase

L'obiettivo della Fase 9 è stato rendere il sistema:

```text
riproducibile
portabile
cloud-ready
orchestrabile
isolato dall'ambiente locale
```

In particolare, la fase ha introdotto:

```text
1. Dockerfile multi-stage per i tre servizi principali;
2. docker-compose.yml per avviare l'intero cluster;
3. rete Docker dedicata;
4. volumi Docker persistenti;
5. healthcheck del Client Proxy;
6. configurazione tramite variabili d'ambiente;
7. test runtime del cluster containerizzato;
8. aggiornamento delle GitHub Actions per validare Docker e Compose in CI.
```

---

## 2. Struttura dei file Docker

I file Docker sono stati collocati nella cartella:

```text
deployments/docker
```

La struttura finale è:

```text
deployments/
  docker/
    Dockerfile.consensus-node
    Dockerfile.client-proxy
    Dockerfile.backup-service
    docker-compose.yml
```

La scelta di usare `deployments/docker` mantiene ordinata la root del progetto e separa il codice applicativo dai file di deployment.

La root del progetto mantiene invece:

```text
.dockerignore
```

perché il build context Docker resta la root del repository.

---

## 3. `.dockerignore`

È stato aggiunto un file `.dockerignore` nella root del progetto.

Il file esclude dal build context contenuti non necessari o non adatti all'immagine Docker:

```dockerignore
.git
.gitignore

data
backup-data
documents

*.log
*.tmp

.vscode
.idea

.DS_Store

bin
dist
coverage.out
```

Questa scelta evita di copiare nelle immagini:

```text
file Git
stato persistente locale
backup runtime
documentazione
file temporanei
cartelle dell'IDE
```

In particolare, `data` e `backup-data` non devono entrare nelle immagini: in Docker la persistenza viene gestita tramite volumi dedicati.

---

## 4. Dockerfile multi-stage

Sono stati creati tre Dockerfile, uno per ogni servizio.

Tutti usano lo stesso schema:

```text
Stage 1: builder Go
Stage 2: immagine runtime minimale Alpine
```

La build Go viene eseguita con:

```dockerfile
CGO_ENABLED=0 GOOS=linux go build
```

Così si ottiene un binario Linux statico o comunque facilmente eseguibile nel container finale.

---

## 5. Versione Go nei Dockerfile

Durante la prima build Docker è emerso questo errore:

```text
go.mod requires go >= 1.25.4
running go 1.24.13
```

Il problema era causato dal fatto che i Dockerfile iniziali usavano:

```dockerfile
FROM golang:1.24-alpine AS builder
```

mentre il progetto richiede Go almeno alla versione:

```text
1.25.4
```

La correzione è stata applicata aggiornando i Dockerfile a:

```dockerfile
FROM golang:1.25.4-alpine AS builder
```

Dopo questa modifica, la build Docker è andata a buon fine.

---

## 6. `Dockerfile.consensus-node`

Il Dockerfile del Consensus Node compila il binario:

```text
cmd/consensus-node
```

ed espone la porta gRPC interna:

```text
50051
```

Schema logico:

```dockerfile
FROM golang:1.25.4-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/consensus-node ./cmd/consensus-node

FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/consensus-node /usr/local/bin/consensus-node
EXPOSE 50051
ENTRYPOINT ["consensus-node"]
```

Il servizio è stateful e usa un volume Docker montato in:

```text
/data
```

---

## 7. `Dockerfile.client-proxy`

Il Dockerfile del Client Proxy compila il binario:

```text
cmd/client-proxy
```

ed espone due porte:

```text
8080 -> gRPC verso client esterni
8081 -> endpoint HTTP /health
```

Nel runtime viene installato anche `wget`, usato dall'healthcheck di Docker Compose:

```dockerfile
RUN apk add --no-cache ca-certificates wget
```

Questo permette al container di eseguire:

```text
wget -qO- http://localhost:8081/health
```

per verificare lo stato del Proxy.

---

## 8. `Dockerfile.backup-service`

Il Dockerfile del Backup Service compila il binario:

```text
cmd/backup-service
```

ed espone la porta gRPC amministrativa:

```text
9090
```

Il servizio salva gli snapshot scaricati nel percorso interno:

```text
/backup-data
```

che viene montato su un volume Docker dedicato.

---

## 9. Docker Compose

È stato creato il file:

```text
deployments/docker/docker-compose.yml
```

Il Compose definisce cinque servizi:

```text
node-1
node-2
node-3
client-proxy
backup-service
```

Inoltre definisce:

```text
rete Docker dedicata
volumi persistenti per i nodi
volume persistente per il backup-service
```

---

## 10. Servizi Consensus Node

I tre nodi sono definiti come tre servizi separati:

```text
node-1
node-2
node-3
```

Ogni nodo usa lo stesso Dockerfile:

```text
deployments/docker/Dockerfile.consensus-node
```

ma riceve configurazione diversa tramite variabili d'ambiente.

---

### 10.1 Node 1

```yaml
environment:
  NODE_ID: node-1
  NODE_ADDRESS: node-1:50051
  PORT: "50051"
  DATA_DIR: /data
  PEERS: node-2=node-2:50051,node-3=node-3:50051
  SNAPSHOT_THRESHOLD: "5"
volumes:
  - node1-data:/data
ports:
  - "50051:50051"
```

---

### 10.2 Node 2

```yaml
environment:
  NODE_ID: node-2
  NODE_ADDRESS: node-2:50051
  PORT: "50051"
  DATA_DIR: /data
  PEERS: node-1=node-1:50051,node-3=node-3:50051
  SNAPSHOT_THRESHOLD: "5"
volumes:
  - node2-data:/data
ports:
  - "50052:50051"
```

---

### 10.3 Node 3

```yaml
environment:
  NODE_ID: node-3
  NODE_ADDRESS: node-3:50051
  PORT: "50051"
  DATA_DIR: /data
  PEERS: node-1=node-1:50051,node-2=node-2:50051
  SNAPSHOT_THRESHOLD: "5"
volumes:
  - node3-data:/data
ports:
  - "50053:50051"
```

---

## 11. Networking Docker

Docker Compose crea una rete bridge dedicata:

```yaml
networks:
  sdcc-net:
    driver: bridge
```

Tutti i servizi sono collegati a questa rete.

All'interno della rete Docker, i container non si contattano tramite `localhost`, ma tramite il nome del servizio:

```text
node-1:50051
node-2:50051
node-3:50051
```

Questa scelta è fondamentale: dentro un container, `localhost` indica il container stesso, non gli altri servizi.

Per questo motivo, nel Compose, i peer Raft e i nodi conosciuti dal Proxy e dal Backup Service usano hostname Docker.

---

## 12. Mapping delle porte

I Consensus Node ascoltano tutti internamente sulla porta:

```text
50051
```

ma vengono esposti sull'host con porte diverse:

```text
node-1 -> localhost:50051
node-2 -> localhost:50052
node-3 -> localhost:50053
```

Nel Compose:

```yaml
ports:
  - "50051:50051"
  - "50052:50051"
  - "50053:50051"
```

Il Proxy espone:

```text
8080 -> gRPC verso client esterni
8081 -> HTTP /health
```

Il Backup Service espone:

```text
9090 -> gRPC amministrativo
```

---

## 13. Volumi Docker

Sono stati definiti quattro volumi:

```yaml
volumes:
  node1-data:
  node2-data:
  node3-data:
  backup-data:
```

I volumi dei nodi servono a preservare:

```text
state.json
wal.log
snapshot.json
```

Il volume del Backup Service serve a preservare:

```text
snapshot scaricati dai nodi
```

Questa scelta permette di riavviare o ricreare i container senza perdere lo stato persistente del cluster.

---

## 14. Client Proxy in Docker

Il servizio `client-proxy` è configurato così:

```yaml
environment:
  PROXY_PORT: "8080"
  PROXY_HEALTH_PORT: "8081"
  CONSENSUS_NODES: node-1:50051,node-2:50051,node-3:50051
  RPC_TIMEOUT_MS: "800"
  MAX_RETRIES: "3"
  BACKOFF_MS: "100"
  MAX_BACKOFF_MS: "1500"
  JITTER_RATIO: "100"
```

Il Proxy non usa indirizzi host come `localhost:50051`, ma hostname Docker:

```text
node-1:50051,node-2:50051,node-3:50051
```

Questo consente al Proxy di contattare direttamente i Consensus Node nella rete Docker.

---

## 15. Healthcheck del Proxy

Il Compose include un healthcheck sul Proxy:

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:8081/health"]
  interval: 10s
  timeout: 3s
  retries: 5
  start_period: 10s
```

Durante i test, il Proxy è risultato:

```text
Up (healthy)
```

Il test manuale ha confermato:

```cmd
curl http://localhost:8081/health
```

Output:

```json
{"status":"ok","timestamp":"2026-06-25T09:18:26.77396515Z"}
```

---

## 16. Backup Service in Docker

Il servizio `backup-service` è configurato così:

```yaml
environment:
  BACKUP_PORT: "9090"
  BACKUP_SERVICE_ID: backup-service-1
  CONSENSUS_NODES: node-1:50051,node-2:50051,node-3:50051
  BACKUP_DIR: /backup-data
  RPC_TIMEOUT_MS: "1000"
  MAX_RETRIES: "3"
  BACKOFF_MS: "200"
  BACKUP_INTERVAL_MS: "0"
  COMPACT_AFTER_DOWNLOAD: "true"
volumes:
  - backup-data:/backup-data
```

Il Backup Service comunica con i nodi tramite:

```text
BackupNodeService
```

esposto da ogni Consensus Node.

---

## 17. Comandi di build e avvio

Dalla root del progetto, la build viene eseguita con:

```cmd
docker compose -f deployments\docker\docker-compose.yml build --no-cache
```

L'avvio del cluster viene eseguito con:

```cmd
docker compose -f deployments\docker\docker-compose.yml up -d
```

Il controllo dello stato dei container viene eseguito con:

```cmd
docker compose -f deployments\docker\docker-compose.yml ps
```

---

## 18. Build Docker eseguita

La build è stata eseguita con:

```cmd
docker compose -f deployments\docker\docker-compose.yml build --no-cache
```

Dopo la correzione della versione Go nei Dockerfile, la build ha completato correttamente tutti i target:

```text
[+] build 5/5
 ✔ Image docker-node-2         Built
 ✔ Image docker-node-3         Built
 ✔ Image docker-client-proxy   Built
 ✔ Image docker-backup-service Built
 ✔ Image docker-node-1         Built
```

Le immagini create sono:

```text
docker-node-1:latest
docker-node-2:latest
docker-node-3:latest
docker-client-proxy:latest
docker-backup-service:latest
```

---

## 19. Avvio del cluster

Il cluster è stato avviato con:

```cmd
docker compose -f deployments\docker\docker-compose.yml up -d
```

Docker Compose ha creato:

```text
network docker_sdcc-net
volume docker_node1-data
volume docker_node2-data
volume docker_node3-data
volume docker_backup-data
```

I container avviati sono:

```text
sdcc-node-1
sdcc-node-2
sdcc-node-3
sdcc-client-proxy
sdcc-backup-service
```

Lo stato ottenuto con `docker compose ps` è stato:

```text
sdcc-backup-service   Up
sdcc-client-proxy     Up (healthy)
sdcc-node-1           Up
sdcc-node-2           Up
sdcc-node-3           Up
```

---

## 20. Test leader discovery

È stato eseguito:

```cmd
set TARGET=localhost:8080
set OP=leader
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-2 leader_address=node-2:50051 term=1
```

Questo conferma che:

```text
[OK] i nodi comunicano tra loro dentro Docker;
[OK] l'elezione Raft avviene correttamente;
[OK] node-2 è stato eletto leader;
[OK] il Proxy scopre correttamente il leader;
[OK] l'indirizzo del leader usa hostname Docker.
```

---

## 21. Test Put/Get via Proxy

È stata eseguita una scrittura:

```cmd
set TARGET=localhost:8080
set OP=put
set KEY=docker-key
set VALUE=docker-value
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=docker-key value=docker-value error= leader_hint=
```

Poi è stata eseguita una lettura:

```cmd
set TARGET=localhost:8080
set OP=get
set KEY=docker-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=docker-key value=docker-value error= leader_hint=node-2:50051
```

Questo conferma che il percorso:

```text
client esterno -> Proxy container -> leader container -> replica Raft -> state machine
```

funziona correttamente.

---

## 22. Test Backup Service in Docker

È stato verificato lo stato iniziale del Backup Service:

```cmd
set TARGET=localhost:9090
set OP=status
go run .\cmd\backup-client
```

Output atteso:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=0 downloaded_snapshots=0 last_backup_id= last_snapshot_id= last_error=
```

È stato poi eseguito un backup manuale:

```cmd
set TARGET=localhost:9090
set OP=backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-client
```

Il test ha confermato che il Backup Service containerizzato riesce a contattare i nodi tramite gli hostname Docker e a scaricare snapshot dai nodi attivi.

---

## 23. Test di tolleranza ai guasti

È stato eseguito un test di fault tolerance fermando un nodo del cluster Docker:

```cmd
docker compose -f deployments\docker\docker-compose.yml stop node-2
```

Successivamente sono stati verificati:

```text
health del Proxy
leader discovery dopo lo stop
Put/Get dopo failover
backup parziale con nodo non disponibile
```

Il comportamento atteso e validato è:

```text
[OK] il cluster resta operativo con due nodi;
[OK] il Proxy resta raggiungibile;
[OK] il Backup Service non si blocca sul nodo fermato;
[OK] il backup parziale viene accettato;
[OK] il Circuit Breaker isola il nodo non disponibile.
```

---

## 24. Test di persistenza dei volumi

Il Compose usa volumi Docker dedicati:

```text
node1-data
node2-data
node3-data
backup-data
```

Questo consente di mantenere stato e snapshot anche dopo:

```cmd
docker compose -f deployments\docker\docker-compose.yml down
```

senza usare l'opzione:

```cmd
-v
```

La cancellazione completa dello stato avviene solo con:

```cmd
docker compose -f deployments\docker\docker-compose.yml down -v
```

---

## 25. Aggiornamento delle GitHub Actions

La Fase 9 ha richiesto anche l'allineamento delle GitHub Actions ai nuovi file Docker.

Sono stati aggiornati o introdotti:

```text
.github/workflows/docker-build-scan.yml
.github/workflows/compose-integration.yml
.github/workflows/ci.yml
.github/workflows/proto.yml
.github/dependabot.yml
buf.yaml
```

---

## 26. Docker Build and Security Scan

Il workflow:

```text
.github/workflows/docker-build-scan.yml
```

esegue:

```text
build delle immagini Docker
security scan con Trivy
```

La matrix del workflow usa i Dockerfile in:

```text
deployments/docker/Dockerfile.consensus-node
deployments/docker/Dockerfile.client-proxy
deployments/docker/Dockerfile.backup-service
```

Il tag Trivy è stato corretto a:

```yaml
uses: aquasecurity/trivy-action@v0.36.0
```

Inoltre il gate di sicurezza è stato configurato su:

```yaml
severity: CRITICAL
```

per evitare di bloccare la pipeline su vulnerabilità `HIGH` delle immagini base durante questa fase, mantenendo comunque il blocco su vulnerabilità critiche.

---

## 27. Docker Compose Integration

Il workflow:

```text
.github/workflows/compose-integration.yml
```

esegue uno smoke test completo:

```text
1. avvia il cluster con Docker Compose;
2. attende /health del Proxy;
3. attende la presenza di un leader Raft;
4. esegue Put/Get via Proxy;
5. esegue TriggerBackup;
6. ferma un nodo;
7. verifica che il Backup Service accetti un backup parziale;
8. spegne il cluster e rimuove i volumi temporanei.
```

Durante la stabilizzazione del workflow è emerso che i client Go usano `log.Printf`, che scrive su `stderr`.

Per questo motivo le pipeline con `grep` non trovavano stringhe come:

```text
has_leader=true
success=true
found=true
accepted=true
```

La correzione è stata redirigere `stderr` su `stdout`:

```bash
go run ./cmd/bench-client 2>&1 | tee leader.log
```

Lo stesso fix è stato applicato anche a `backup-client`.

---

## 28. Protobuf CI

Il workflow:

```text
.github/workflows/proto.yml
```

esegue:

```text
buf lint
buf format check
buf breaking sulle pull request
rigenerazione stub Go
verifica git diff
go test ./...
```

Durante la stabilizzazione sono stati affrontati due problemi.

### 28.1 Regole Buf su layout flat

Buf richiedeva:

```text
package versionati come backup.v1, consensus.v1, kv.v1
un solo package per directory
layout directory coerente col package
```

Il progetto mantiene invece una struttura flat:

```text
proto/backup.proto
proto/consensus.proto
proto/kv.proto
```

Per evitare un refactor invasivo dei contratti gRPC già stabilizzati, `buf.yaml` è stato configurato con eccezioni mirate:

```yaml
except:
  - DIRECTORY_SAME_PACKAGE
  - PACKAGE_DIRECTORY_MATCH
  - PACKAGE_SAME_DIRECTORY
  - PACKAGE_VERSION_SUFFIX
```

### 28.2 Toolchain Protobuf deterministica

La CI inizialmente rigenerava stub diversi da quelli committati perché usava versioni diverse di:

```text
protoc
protoc-gen-go
protoc-gen-go-grpc
```

Il workflow è stato corretto fissando le versioni:

```text
protoc-gen-go      v1.36.10
protoc-gen-go-grpc v1.5.1
protoc             33.0
```

Questo evita drift nei file generati.

---

## 29. Dependabot

È stato aggiornato:

```text
.github/dependabot.yml
```

per controllare:

```text
Go modules
GitHub Actions
Dockerfile in deployments/docker
```

Sono stati inoltre bloccati i major bump automatici delle GitHub Actions principali, per evitare aggiornamenti invasivi non richiesti.

Dependabot ha aperto PR sulle immagini base Docker, tra cui:

```text
alpine 3.20 -> 3.24
golang 1.25.4-alpine -> 1.26.4-alpine
```

L'aggiornamento Alpine è stato considerato utile perché riguarda lo stage runtime. L'aggiornamento Go è più delicato perché cambia la toolchain di build e va valutato separatamente.

---

## 30. Stato finale delle GitHub Actions

Al termine della stabilizzazione, le Actions sono passate correttamente:

```text
[OK] Go CI
[OK] Protobuf CI
[OK] Docker Build and Security Scan
[OK] Docker Compose Integration
```

Questo conferma che:

```text
[OK] il codice Go compila e supera i test;
[OK] i file proto sono coerenti e formattati;
[OK] gli stub generati sono allineati;
[OK] le immagini Docker vengono costruite in CI;
[OK] le immagini vengono scansionate;
[OK] il cluster Docker Compose parte in CI;
[OK] il Proxy risponde su /health;
[OK] il cluster elegge un leader;
[OK] Put/Get via Proxy funzionano;
[OK] il Backup Service funziona in Docker;
[OK] il sistema tollera il fermo di un nodo nello smoke test.
```

---

## 31. Checklist finale Fase 9

```text
[OK] .dockerignore creato
[OK] Dockerfile.consensus-node creato
[OK] Dockerfile.client-proxy creato
[OK] Dockerfile.backup-service creato
[OK] docker-compose.yml creato in deployments/docker
[OK] build context configurato correttamente
[OK] Go 1.25.4 usato nei Dockerfile
[OK] rete Docker dedicata creata
[OK] volumi persistenti creati
[OK] 3 Consensus Node avviati
[OK] Client Proxy avviato
[OK] Backup Service avviato
[OK] Proxy healthcheck attivo
[OK] leader election in Docker funzionante
[OK] Put/Get via Proxy funzionanti
[OK] Backup Service funzionante in Docker
[OK] fault tolerance testata
[OK] persistenza via Docker volumes predisposta
[OK] GitHub Actions aggiornate
[OK] CI pipeline completa verde
```

---

## 32. Limiti residui

Restano alcuni miglioramenti futuri:

```text
[DA FARE] deploy su Amazon EC2;
[DA FARE] configurazione Security Group AWS;
[DA FARE] documentazione dei comandi EC2;
[DA FARE] eventuale push immagini su Docker Hub o Amazon ECR;
[DA FARE] metriche più precise sul tempo di rielezione;
[DA FARE] benchmark di latenza sotto carico;
[DA FARE] health endpoint HTTP anche per backup-service;
[DA FARE] test automatici più approfonditi su snapshot divergenti.
```

---

## 33. Preparazione alla fase successiva

Con questa fase, il progetto è pronto per il deploy cloud.

La fase successiva potrà concentrarsi su:

```text
1. preparazione istanza Amazon EC2;
2. installazione Docker e Docker Compose sull'istanza;
3. clonazione del repository;
4. esecuzione del cluster con docker compose;
5. apertura della porta 8080 per test esterni;
6. eventuale esposizione controllata di 8081 e 9090;
7. test di fault tolerance su EC2;
8. raccolta dei risultati finali per il bando B5.
```

La containerizzazione locale e la validazione CI rendono ora il sistema riproducibile e pronto per essere eseguito in ambiente cloud.
