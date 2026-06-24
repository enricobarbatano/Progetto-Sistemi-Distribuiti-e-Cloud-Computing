# Fase 8 - Snapshot & Backup Service

Questo documento descrive la Fase 8 del progetto, dedicata allo sviluppo dello **Snapshot & Backup Service**, cioè il terzo componente principale richiesto dall'architettura del sistema distribuito.

Dopo la Fase 6.bis, il sistema disponeva già di:

- WAL event-based;
- snapshot locale reale;
- log compaction locale;
- `lastIncludedIndex` e `lastIncludedTerm` persistiti;
- `InstallSnapshot` reale;
- fast back-off per `AppendEntries`;
- quorum-check sulle letture;
- Client Proxy rafforzato.

La Fase 8 aggiunge un componente esterno dedicato all'orchestrazione di backup e snapshot, separando la logica di manutenzione e persistenza a lungo termine dalla logica di consenso.

---

## 1. Obiettivo della fase

L'obiettivo della Fase 8 è introdurre un nuovo servizio:

```text
backup-service
```

Il servizio ha il compito di:

```text
1. forzare la creazione di snapshot sui Consensus Node;
2. scaricare gli snapshot dai nodi;
3. salvarli localmente in una directory dedicata;
4. richiedere la compattazione del log dopo il salvataggio;
5. esporre uno stato amministrativo del servizio;
6. tollerare nodi non disponibili tramite Circuit Breaker.
```

Il Backup Service non partecipa al protocollo Raft, non decide commit e non modifica direttamente la state machine dei nodi.

La sua responsabilità è di tipo operativo e amministrativo: coordinare snapshot, backup e compaction remota.

---

## 2. Architettura logica

La fase introduce una separazione netta tra:

```text
Consensus Node
  -> espone operazioni di backup sul proprio stato locale

Backup Service
  -> orchestra i backup contattando i nodi
```

La struttura finale è:

```text
proto/
  backup.proto

cmd/
  consensus-node/
    main.go

  backup-service/
    main.go

  backup-client/
    main.go

internal/
  consensus/
    rpc_backup.go

  backup/
    config.go
    node_client.go
    circuit_breaker.go
    snapshot_syncer.go
    log_compactor.go
    manager.go
    server.go
```

---

## 3. Aggiornamento di `backup.proto`

Il file:

```text
proto/backup.proto
```

è stato riorganizzato separando due servizi distinti:

```proto
service BackupNodeService
service BackupService
```

Questa separazione evita di mischiare responsabilità diverse nello stesso servizio gRPC.

---

### 3.1 `BackupNodeService`

`BackupNodeService` è implementato dai **Consensus Node**.

Espone RPC operative sul singolo nodo:

```proto
service BackupNodeService {
  rpc TriggerSnapshot(TriggerSnapshotRequest) returns (TriggerSnapshotResponse);
  rpc DownloadSnapshot(DownloadSnapshotRequest) returns (DownloadSnapshotResponse);
  rpc CompactLog(CompactLogRequest) returns (CompactLogResponse);
}
```

Responsabilità:

```text
TriggerSnapshot
  forza il nodo a creare uno snapshot locale;

DownloadSnapshot
  permette al Backup Service di scaricare lo snapshot locale;

CompactLog
  chiede al nodo di compattare il log fino a un indice sicuro.
```

Queste RPC sono implementate nel package:

```text
internal/consensus
```

perché solo il Consensus Node possiede direttamente:

```text
state machine
log Raft
snapshot locale
lastIncludedIndex
lastIncludedTerm
persistenceManager
```

---

### 3.2 `BackupService`

`BackupService` è implementato dal componente esterno:

```text
cmd/backup-service
```

Espone RPC amministrative:

```proto
service BackupService {
  rpc TriggerBackup(TriggerBackupRequest) returns (TriggerBackupResponse);
  rpc GetBackupStatus(GetBackupStatusRequest) returns (GetBackupStatusResponse);
}
```

Responsabilità:

```text
TriggerBackup
  avvia un ciclo coordinato di backup sui nodi configurati;

GetBackupStatus
  restituisce lo stato aggregato del Backup Service.
```

---

### 3.3 Campi principali dei messaggi

`TriggerSnapshotResponse` restituisce:

```proto
bool accepted = 1;
string node_id = 2;
string snapshot_id = 3;
uint64 last_included_index = 4;
uint64 last_included_term = 5;
string error = 6;
```

`DownloadSnapshotResponse` restituisce:

```proto
bool success = 1;
string node_id = 2;
string snapshot_id = 3;
bytes snapshot_data = 4;
uint64 last_included_index = 5;
uint64 last_included_term = 6;
string error = 7;
```

`CompactLogRequest` contiene:

```proto
string requester_id = 1;
string snapshot_id = 2;
uint64 up_to_index = 3;
```

`GetBackupStatusResponse` contiene:

```proto
string service_id = 1;
string status = 2;
uint64 created_backups = 3;
uint64 downloaded_snapshots = 4;
string last_backup_id = 5;
string last_snapshot_id = 6;
string last_error = 7;
```

---

## 4. Rigenerazione degli stub Go

Dopo la modifica di `backup.proto`, sono stati rigenerati gli stub Go con:

```cmd
protoc --go_out=. --go_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing --go-grpc_out=. --go-grpc_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing proto\backup.proto
```

Sono stati aggiornati:

```text
gen/go/backuppb/backup.pb.go
gen/go/backuppb/backup_grpc.pb.go
```

Dopo la rigenerazione è stato eseguito:

```cmd
go test ./...
```

con esito positivo.

---

## 5. Integrazione lato Consensus Node

I Consensus Node espongono ora anche il servizio:

```text
BackupNodeService
```

---

### 5.1 Modifica a `cmd/consensus-node/main.go`

Nel main del Consensus Node è stato aggiunto l'import:

```go
backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
```

ed è stata aggiunta la registrazione del servizio gRPC:

```go
backuppb.RegisterBackupNodeServiceServer(server, node)
```

Ora il nodo espone tre servizi:

```text
ConsensusService
KeyValueService
BackupNodeService
```

---

### 5.2 Modifica a `internal/consensus/node.go`

La struct `ConsensusNode` include ora anche:

```go
backuppb.UnimplementedBackupNodeServiceServer
```

insieme agli altri servizi gRPC:

```go
consensuspb.UnimplementedConsensusServiceServer
kvpb.UnimplementedKeyValueServiceServer
backuppb.UnimplementedBackupNodeServiceServer
```

---

### 5.3 Nuovo file `internal/consensus/rpc_backup.go`

È stato creato:

```text
internal/consensus/rpc_backup.go
```

Questo file implementa le RPC esposte dal singolo nodo al Backup Service.

---

## 6. RPC lato Consensus Node

### 6.1 `TriggerSnapshot`

`TriggerSnapshot` forza il nodo a creare uno snapshot locale.

La logica è:

```text
1. acquisisce il lock del nodo;
2. verifica che esista almeno una entry applicata;
3. chiama saveSnapshotLocked();
4. persiste lo stato corrente;
5. carica lo snapshot appena creato;
6. restituisce snapshot_id, lastIncludedIndex e lastIncludedTerm.
```

Se non esiste alcuna entry applicata, la RPC risponde con:

```text
accepted=false
error="no applied entry available for snapshot"
```

---

### 6.2 `DownloadSnapshot`

`DownloadSnapshot` restituisce lo snapshot locale più recente.

Il nodo:

```text
1. carica snapshot.json tramite persistenceManager.LoadSnapshot();
2. costruisce uno snapshot_id leggibile;
3. serializza lo snapshot con persistence.EncodeSnapshot();
4. restituisce i bytes al Backup Service.
```

Se non esiste uno snapshot locale, risponde con:

```text
success=false
error="snapshot not found"
```

---

### 6.3 `CompactLog`

`CompactLog` permette al Backup Service di richiedere la compattazione del log.

Per sicurezza, il nodo permette la compattazione solo fino all'indice già coperto dal proprio snapshot locale:

```text
up_to_index <= lastIncludedIndex
```

Questo evita di eliminare entry che non sono ancora rappresentate da uno snapshot sicuro.

La logica è:

```text
1. valida up_to_index;
2. verifica che non superi lastIncludedIndex;
3. conta le entry fisicamente compattabili;
4. chiama compactLogUpToLocked(up_to_index);
5. persiste lo stato;
6. restituisce compacted_entries.
```

---

## 7. Package `internal/backup`

Il package:

```text
internal/backup
```

contiene la logica interna del Backup Service.

La progettazione evita una God Class, separando responsabilità diverse in file diversi.

---

### 7.1 `config.go`

Legge la configurazione da variabili d'ambiente:

```text
BACKUP_SERVICE_ID
BACKUP_PORT
CONSENSUS_NODES
BACKUP_DIR
RPC_TIMEOUT_MS
MAX_RETRIES
BACKOFF_MS
BACKUP_INTERVAL_MS
COMPACT_AFTER_DOWNLOAD
```

Esempio di configurazione locale:

```cmd
set BACKUP_PORT=9090
set BACKUP_SERVICE_ID=backup-service-1
set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
set BACKUP_DIR=backup-data
set RPC_TIMEOUT_MS=1000
set MAX_RETRIES=3
set BACKOFF_MS=200
set BACKUP_INTERVAL_MS=0
set COMPACT_AFTER_DOWNLOAD=true
```

---

### 7.2 `node_client.go`

Gestisce le connessioni gRPC verso i Consensus Node.

Espone metodi wrapper per:

```go
TriggerSnapshot
DownloadSnapshot
CompactLog
```

Usa client gRPC generati da:

```go
backuppb.NewBackupNodeServiceClient(conn)
```

---

### 7.3 `circuit_breaker.go`

Gestisce un Circuit Breaker per ogni Consensus Node.

La mappa logica è:

```text
localhost:50051 -> breaker dedicato
localhost:50052 -> breaker dedicato
localhost:50053 -> breaker dedicato
```

In caso di fallimenti consecutivi, il circuito del nodo viene aperto.

Durante il test con un nodo spento, il servizio ha registrato:

```text
backup circuit breaker backup-node-localhost:50052 changed state from closed to open
```

Questo conferma che il nodo non disponibile viene isolato senza bloccare l'intero backup.

---

### 7.4 `snapshot_syncer.go`

Salva su disco gli snapshot ricevuti dai nodi.

Gli snapshot vengono salvati nella directory configurata:

```text
backup-data
```

con nomi del tipo:

```text
node-1_node-1_snapshot_12_term_19_index_12_term_19.json
node-2_node-2_snapshot_11_term_16_index_11_term_16.json
node-3_node-3_snapshot_12_term_19_index_12_term_19.json
```

La scrittura avviene usando file temporaneo e rename finale:

```text
file.tmp -> file.json
```

---

### 7.5 `log_compactor.go`

Invia richieste `CompactLog` ai Consensus Node.

Non scarica snapshot e non decide quando avviare il ciclo. La sua responsabilità è solo inviare il comando di compattazione remota.

---

### 7.6 `manager.go`

Coordina un ciclo completo di backup.

Il ciclo è:

```text
1. genera un backup_id;
2. per ogni nodo configurato:
   a. opzionalmente chiama TriggerSnapshot;
   b. chiama DownloadSnapshot;
   c. salva lo snapshot tramite SnapshotSyncer;
   d. opzionalmente chiama CompactLog;
3. aggiorna lo stato interno del servizio.
```

Mantiene anche lo stato aggregato:

```text
status
createdBackups
downloadedSnapshots
lastBackupID
lastSnapshotID
lastError
```

---

### 7.7 `server.go`

Implementa il servizio gRPC esterno:

```text
BackupService
```

Espone:

```go
TriggerBackup
GetBackupStatus
```

Il server non parla direttamente con i nodi: delega al `BackupManager`.

---

## 8. Comandi di avvio locale

### 8.1 Node 1

```cmd
set NODE_ID=node-1
set NODE_ADDRESS=localhost:50051
set PORT=50051
set DATA_DIR=data\node-1
set PEERS=node-2=localhost:50052,node-3=localhost:50053
set SNAPSHOT_THRESHOLD=5
go run .\cmd\consensus-node
```

---

### 8.2 Node 2

```cmd
set NODE_ID=node-2
set NODE_ADDRESS=localhost:50052
set PORT=50052
set DATA_DIR=data\node-2
set PEERS=node-1=localhost:50051,node-3=localhost:50053
set SNAPSHOT_THRESHOLD=5
go run .\cmd\consensus-node
```

---

### 8.3 Node 3

```cmd
set NODE_ID=node-3
set NODE_ADDRESS=localhost:50053
set PORT=50053
set DATA_DIR=data\node-3
set PEERS=node-1=localhost:50051,node-2=localhost:50052
set SNAPSHOT_THRESHOLD=5
go run .\cmd\consensus-node
```

---

### 8.4 Client Proxy

```cmd
set PROXY_PORT=8080
set PROXY_HEALTH_PORT=8081
set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
set RPC_TIMEOUT_MS=800
set MAX_RETRIES=3
set BACKOFF_MS=100
set MAX_BACKOFF_MS=1500
set JITTER_RATIO=100
go run .\cmd\client-proxy
```

---

### 8.5 Backup Service

```cmd
set BACKUP_PORT=9090
set BACKUP_SERVICE_ID=backup-service-1
set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
set BACKUP_DIR=backup-data
set RPC_TIMEOUT_MS=1000
set MAX_RETRIES=3
set BACKOFF_MS=200
set BACKUP_INTERVAL_MS=0
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-service
```

Output atteso:

```text
backup service backup-service-1 listening on port 9090 with consensus nodes [localhost:50051 localhost:50052 localhost:50053]
```

---

## 9. Backup Client

È stato aggiunto un piccolo client amministrativo:

```text
cmd/backup-client/main.go
```

Serve per testare manualmente:

```text
TriggerBackup
GetBackupStatus
```

---

### 9.1 Status

```cmd
set TARGET=localhost:9090
set OP=status
go run .\cmd\backup-client
```

Output iniziale atteso:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=0 downloaded_snapshots=0 last_backup_id= last_snapshot_id= last_error=
```

---

### 9.2 Trigger backup

Prima viene eseguita una scrittura via Proxy:

```cmd
set TARGET=localhost:8080
set OP=put
set KEY=backup-phase-key-2
set VALUE=backup-phase-value-2
go run .\cmd\bench-client
```

Poi viene lanciato il backup:

```cmd
set TARGET=localhost:9090
set OP=backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-client
```

Output ottenuto:

```text
TriggerBackup response: accepted=true backup_id=backup_1782317192914310700 downloaded_snapshots=3 error=
```

---

### 9.3 Status dopo backup

```cmd
set TARGET=localhost:9090
set OP=status
go run .\cmd\backup-client
```

Output ottenuto:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=1 downloaded_snapshots=3 last_backup_id=backup_1782317192914310700 last_snapshot_id=node-3_snapshot_12_term_19 last_error=
```

---

## 10. Test effettuati

### 10.1 Health del Proxy

```cmd
curl http://localhost:8081/health
```

Output:

```json
{"status":"ok","timestamp":"2026-06-24T16:05:29.147593Z"}
```

---

### 10.2 Leader discovery

```cmd
set TARGET=localhost:8080
set OP=leader
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=localhost:50053 term=19
```

---

### 10.3 Put/Get prima del backup

```text
Put response: success=true key=backup-phase-key-2 value=backup-phase-value-2 error= leader_hint=
Get response: found=true key=backup-phase-key-2 value=backup-phase-value-2 error= leader_hint=localhost:50053
```

---

### 10.4 TriggerBackup con tre nodi attivi

```text
TriggerBackup response: accepted=true backup_id=backup_1782317192914310700 downloaded_snapshots=3 error=
```

File salvati:

```text
node-1_node-1_snapshot_12_term_19_index_12_term_19.json
node-2_node-2_snapshot_11_term_16_index_11_term_16.json
node-3_node-3_snapshot_12_term_19_index_12_term_19.json
```

---

### 10.5 Verifica contenuto snapshot

Snapshot node-1:

```json
{
  "last_included_index": 12,
  "last_included_term": 19,
  "data": {
    "backup-phase-key": "backup-phase-value",
    "backup-phase-key-2": "backup-phase-value-2",
    "compact-key-2": "compact-value-2",
    "fast-backoff-key": "fast-backoff-value",
    "install-snapshot-key": "install-snapshot-value",
    "read-quorum-final-key": "read-quorum-final-value",
    "read-quorum-key": "read-quorum-value",
    "snap-a": "1",
    "snap-b": "2",
    "snap-c": "3",
    "snap-d": "4"
  }
}
```

Snapshot node-2:

```json
{
  "last_included_index": 11,
  "last_included_term": 16,
  "data": {
    "backup-phase-key": "backup-phase-value",
    "compact-key-2": "compact-value-2",
    "fast-backoff-key": "fast-backoff-value",
    "install-snapshot-key": "install-snapshot-value",
    "read-quorum-final-key": "read-quorum-final-value",
    "read-quorum-key": "read-quorum-value",
    "snap-a": "1",
    "snap-b": "2",
    "snap-c": "3",
    "snap-d": "4"
  }
}
```

Snapshot node-3:

```json
{
  "last_included_index": 12,
  "last_included_term": 19,
  "data": {
    "backup-phase-key": "backup-phase-value",
    "backup-phase-key-2": "backup-phase-value-2",
    "compact-key-2": "compact-value-2",
    "fast-backoff-key": "fast-backoff-value",
    "install-snapshot-key": "install-snapshot-value",
    "read-quorum-final-key": "read-quorum-final-value",
    "read-quorum-key": "read-quorum-value",
    "snap-a": "1",
    "snap-b": "2",
    "snap-c": "3",
    "snap-d": "4"
  }
}
```

Il fatto che node-2 sia a indice 11 mentre node-1 e node-3 siano a indice 12 è coerente con lo stato del cluster durante i test: node-2 era leggermente indietro o non raggiungibile in uno dei cicli successivi.

---

### 10.6 Test Circuit Breaker con nodo non disponibile

È stato spento un nodo, corrispondente a:

```text
localhost:50052
```

Poi è stato lanciato un nuovo backup:

```cmd
set TARGET=localhost:9090
set OP=backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-client
```

Output ottenuto:

```text
TriggerBackup response: accepted=true backup_id=backup_1782317339764589900 downloaded_snapshots=2 error=
```

Nel log del Backup Service:

```text
backup saved snapshot from node node-1 to backup-data\node-1_node-1_snapshot_12_term_19_index_12_term_19.json
backup circuit breaker backup-node-localhost:50052 changed state from closed to open
backup saved snapshot from node node-3 to backup-data\node-3_node-3_snapshot_12_term_19_index_12_term_19.json
```

Questo conferma che:

```text
[OK] il nodo non disponibile non blocca il Backup Service;
[OK] il Circuit Breaker si apre sul nodo guasto;
[OK] gli altri nodi vengono comunque salvati;
[OK] il backup parziale è accettato;
[OK] il servizio torna idle dopo il ciclo.
```

Status finale:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=2 downloaded_snapshots=5 last_backup_id=backup_1782317339764589900 last_snapshot_id=node-3_snapshot_12_term_19 last_error=
```

---

## 11. Stato finale della Fase 8

Checklist finale:

```text
[OK] backup.proto aggiornato con BackupNodeService e BackupService
[OK] stub backuppb rigenerati
[OK] Consensus Node registra BackupNodeService
[OK] internal/consensus/rpc_backup.go operativo
[OK] internal/backup creato con responsabilità separate
[OK] backup-service avviabile
[OK] backup-client amministrativo funzionante
[OK] GetBackupStatus funzionante
[OK] TriggerBackup funzionante
[OK] TriggerSnapshot remoto sui nodi funzionante
[OK] DownloadSnapshot remoto funzionante
[OK] salvataggio snapshot in backup-data funzionante
[OK] CompactLog remoto integrato dopo download
[OK] Circuit Breaker integrato
[OK] Circuit Breaker testato con nodo spento
[OK] backup parziale funzionante in caso di nodo non disponibile
[OK] snapshot reali verificati su disco
```

---

## 12. Limiti residui

Rimangono alcuni miglioramenti futuri:

```text
[DA FARE] storico versionato di più snapshot per lo stesso nodo;
[DA FARE] upload su storage esterno reale, ad esempio S3;
[DA FARE] API di restore amministrativo;
[DA FARE] metriche strutturate per durata backup e dimensione snapshot;
[DA FARE] test automatici di backup parziale;
[DA FARE] integrazione del backup-service in docker-compose.yml;
[DA FARE] volume dedicato per backup-data in Docker;
[DA FARE] health endpoint HTTP del backup-service.
```

---

## 13. Preparazione alla Dockerizzazione

Con la Fase 8 completata, la futura configurazione Docker Compose potrà includere:

```text
3 consensus-node
1 client-proxy
1 backup-service
```

Il Backup Service sarà configurato con:

```yaml
environment:
  BACKUP_PORT: "9090"
  BACKUP_SERVICE_ID: "backup-service-1"
  CONSENSUS_NODES: "node-1:50051,node-2:50051,node-3:50051"
  BACKUP_DIR: "/backup-data"
  RPC_TIMEOUT_MS: "1000"
  MAX_RETRIES: "3"
  BACKOFF_MS: "200"
  BACKUP_INTERVAL_MS: "0"
  COMPACT_AFTER_DOWNLOAD: "true"
volumes:
  - backup-data:/backup-data
```

Questo permetterà di testare in Docker:

```text
- backup manuale;
- backup periodico;
- nodo non disponibile;
- Circuit Breaker;
- persistenza degli snapshot scaricati;
- restart del backup-service senza perdita dei file salvati.
```
