# Fase 6.bis - Risanamento del debito tecnico pre-Docker

Questo documento descrive il risanamento tecnico effettuato dopo la Fase 6, prima della Fase 7 di dockerizzazione.

L'obiettivo è stato consolidare il sistema distribuito dopo l'introduzione del Client Proxy Service, intervenendo sui principali limiti tecnici emersi nelle fasi precedenti:

- persistenza non ancora realmente event-based;
- snapshot locale non ancora usato come vero checkpoint di compaction;
- log Raft destinato a crescere indefinitamente;
- `InstallSnapshot` ancora minimale;
- replica non ottimizzata in caso di conflitti tra log;
- letture servite dal leader senza verifica esplicita del quorum;
- Proxy da rafforzare in vista della dockerizzazione.

Il lavoro è stato svolto mantenendo il principio già adottato nelle fasi precedenti: evitare nuove God Class e mantenere responsabilità separate tra consensus, persistence, storage e proxy.

---

## 1. Stato di partenza

Alla fine della Fase 6 il sistema aveva già raggiunto una buona separazione architetturale:

```text
internal/
  consensus/
    election.go
    log_helpers.go
    node.go
    peers.go
    replication.go
    rpc_kv.go
    rpc_snapshot.go
    state_recovery.go

  persistence/
    manager.go

  proxy/
    config.go
    leader_cache.go
    node_client.go
    circuit_breaker.go
    router.go
    service.go

  storage/
    kv_store.go
```

Il Client Proxy funzionava come punto di ingresso unico per i client esterni e il cluster era in grado di gestire:

- elezione leader;
- scritture replicate su quorum;
- letture dal leader;
- snapshot locale minimale;
- recovery da `state.json` e WAL;
- discovery dinamica del leader tramite Proxy.

Tuttavia, restavano diversi debiti tecnici che avrebbero reso più fragile la successiva dockerizzazione.

---

## 2. Obiettivi del risanamento

Gli obiettivi principali di questa fase sono stati:

```text
1. rafforzare il Proxy prima della dockerizzazione;
2. trasformare il WAL da snapshot-based a event-based;
3. rendere lo snapshot locale un checkpoint reale;
4. introdurre log compaction locale;
5. persistere lastIncludedIndex e lastIncludedTerm;
6. implementare fast back-off per AppendEntries;
7. implementare InstallSnapshot reale;
8. rafforzare la consistenza delle letture tramite quorum-check;
9. aggiornare i file .proto quando il contratto di rete lo richiede;
10. mantenere test runtime manuali per ogni blocco critico.
```

---

## 3. Hardening del Client Proxy

Il Proxy è stato rafforzato per essere più adatto a un ambiente containerizzato.

### 3.1 LeaderInfo nella LeaderCache

La `LeaderCache` non conserva più solo l'indirizzo del leader, ma una struttura logica:

```go
type LeaderInfo struct {
    ID      string
    Address string
    Term    uint64
}
```

Questo permette al Proxy di restituire informazioni più complete tramite `GetLeader`:

```text
leader_id
leader_address
term
```

Prima il Proxy poteva restituire:

```text
leader_id=
term=0
```

Dopo il risanamento restituisce valori reali, ad esempio:

```text
GetLeader response: has_leader=true leader_id=node-1 leader_address=localhost:50051 term=35
```

---

### 3.2 Endpoint HTTP `/health`

È stato aggiunto un health endpoint HTTP nel file:

```text
internal/proxy/health.go
```

Il Proxy espone:

```text
GET /health
```

su porta configurabile:

```cmd
set PROXY_HEALTH_PORT=8081
```

Esempio di risposta:

```json
{"status":"ok","timestamp":"2026-06-24T14:02:11.9594395Z"}
```

Questo endpoint sarà utile nella Fase 7 per configurare health check in Docker Compose o in un orchestratore cloud.

---

### 3.3 Exponential backoff con jitter

Il backoff lineare del Proxy è stato sostituito con un backoff esponenziale con jitter.

Sono state introdotte nuove configurazioni:

```cmd
set BACKOFF_MS=100
set MAX_BACKOFF_MS=1500
set JITTER_RATIO=100
```

Il delay dei retry cresce in modo esponenziale e viene perturbato da jitter casuale per evitare che molte richieste ritentino nello stesso momento.

---

### 3.4 Circuit Breaker per nodo

È stato mantenuto un Circuit Breaker separato per ogni Consensus Node:

```text
localhost:50051 -> breaker dedicato
localhost:50052 -> breaker dedicato
localhost:50053 -> breaker dedicato
```

Così il malfunzionamento di un nodo non blocca automaticamente tutto il cluster.

---

## 4. Aggiornamento dei file `.proto`

Poiché i `.proto` definiscono il contratto di comunicazione tra servizi, sono stati aggiornati solo dove necessario.

---

### 4.1 `proto/consensus.proto`

Il messaggio `AppendEntriesResponse` è stato esteso con:

```proto
uint64 conflict_index = 4;
uint64 conflict_term = 5;
```

Versione aggiornata:

```proto
message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 match_index = 3;

  uint64 conflict_index = 4;
  uint64 conflict_term = 5;
}
```

Questi campi permettono al follower di indicare al leader il punto da cui riprovare la replica, evitando il vecchio decremento lineare di `nextIndex`.

---

### 4.2 `proto/kv.proto`

Il messaggio `GetRequest` è stato arricchito con:

```proto
string request_id = 2;
```

Versione aggiornata:

```proto
message GetRequest {
  string key = 1;
  string request_id = 2;
}
```

Il campo è opzionale ed è utile per logging, tracciamento e futuri meccanismi di Read-Index.

Il messaggio `GetLeaderResponse` era già adeguato, perché conteneva:

```proto
bool has_leader = 1;
string leader_id = 2;
string leader_address = 3;
uint64 term = 4;
```

---

### 4.3 `InstallSnapshotRequest`

`InstallSnapshotRequest` era già sufficientemente completo:

```proto
message InstallSnapshotRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  uint64 offset = 5;
  bytes data = 6;
  bool done = 7;
}
```

Quindi non è stato necessario modificarlo. La logica reale è stata implementata lato Go.

---

### 4.4 Rigenerazione stub Go

Dopo la modifica dei proto sono stati rigenerati gli stub Go:

```cmd
protoc --go_out=. --go_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing --go-grpc_out=. --go-grpc_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing proto\consensus.proto proto\kv.proto
```

Sono stati aggiornati:

```text
gen/go/consensuspb/consensus.pb.go
gen/go/consensuspb/consensus_grpc.pb.go
gen/go/kvpb/kv.pb.go
gen/go/kvpb/kv_grpc.pb.go
```

---

## 5. WAL event-based

Prima del risanamento, il WAL registrava uno snapshot completo dello stato a ogni salvataggio.

Questo produceva record ridondanti e non rappresentava un vero Write-Ahead Log event-based.

Il file interessato è:

```text
internal/persistence/manager.go
```

---

### 5.1 Nuovo formato del WAL

Il WAL ora registra eventi granulari:

```json
{"type":"term_updated","term":5}
{"type":"vote_updated","voted_for":"node-1"}
{"type":"log_appended","entry":{"index":1,"term":5,"operation":1,"key":"x","value":"y"}}
{"type":"snapshot_saved","last_included_index":1,"last_included_term":5,"data":{"x":"y"}}
{"type":"log_truncated","truncate_from":1}
{"type":"commit_advanced","commit_index":1}
{"type":"state_applied","last_applied":1}
```

Gli eventi principali sono:

```text
term_updated
vote_updated
log_appended
log_truncated
commit_advanced
state_applied
snapshot_saved
```

---

### 5.2 `state.json` come checkpoint compatto

`state.json` resta presente e viene usato come checkpoint veloce.

La strategia finale è:

```text
state.json -> checkpoint compatto corrente
wal.log    -> sequenza append-only di eventi
snapshot   -> checkpoint della state machine
```

---

### 5.3 Replay del WAL

Se `state.json` manca, il nodo può ricostruire lo stato leggendo il WAL sequenzialmente.

È stato testato eliminando gli state file:

```cmd
del data\node-1\node-1_state.json
del data\node-2\node-2_state.json
del data\node-3\node-3_state.json
```

Dopo il riavvio, la `Get` ha restituito ancora il valore corretto:

```text
Get response: found=true key=compact-key value=compact-value
```

---

## 6. Snapshot reale locale e log compaction

Lo snapshot locale è stato trasformato in un vero checkpoint della state machine.

Sono stati introdotti nel `ConsensusNode`:

```go
lastIncludedIndex uint64
lastIncludedTerm  uint64
```

Questi campi rappresentano il prefisso del log già coperto dallo snapshot.

---

### 6.1 Persistenza di `lastIncludedIndex` e `lastIncludedTerm`

La struct `persistence.State` ora contiene:

```go
LastIncludedIndex uint64 `json:"last_included_index"`
LastIncludedTerm  uint64 `json:"last_included_term"`
```

Esempio di `state.json` dopo compaction:

```json
{
  "current_term": 2,
  "voted_for": "node-1",
  "log": [],
  "commit_index": 1,
  "last_applied": 1,
  "data": {
    "compact-key": "compact-value"
  },
  "last_included_index": 1,
  "last_included_term": 2
}
```

---

### 6.2 Log compaction locale

Dopo il salvataggio dello snapshot, il log viene compattato:

```go
n.compactLogUpToLocked(snapshot.LastIncludedIndex)
```

Le entry con indice minore o uguale a `lastIncludedIndex` vengono rimosse dal log fisico.

Esempio dopo `SNAPSHOT_THRESHOLD=1`:

```json
"log": []
```

---

### 6.3 Helper log aggiornati

Il file:

```text
internal/consensus/log_helpers.go
```

è stato aggiornato per gestire la differenza tra:

```text
indice logico Raft
indice fisico nella slice del log
```

Gli helper ora considerano `lastIncludedIndex` e `lastIncludedTerm`:

```go
lastLogIndexLocked()
lastLogTermLocked()
logTermAtIndexLocked(index)
hasLogEntryLocked(index, term)
entryByIndexLocked(index)
truncateLogFromIndexLocked(index)
compactLogUpToLocked(index)
```

---

## 7. Fast back-off AppendEntries

Il leader non decrementa più sempre `nextIndex` di uno alla volta.

Ora il follower, in caso di conflitto, restituisce:

```text
conflict_index
conflict_term
```

Il leader usa queste informazioni in:

```go
updateNextIndexFromConflictLocked(peerID, resp)
```

La logica è:

```text
1. se il follower comunica conflict_term, il leader cerca l'ultima entry locale con quel termine;
2. se la trova, riparte da lastIndex + 1;
3. altrimenti usa conflict_index;
4. se non ci sono informazioni, usa fallback conservativo.
```

Questo riduce il numero di RPC necessarie quando follower e leader hanno log divergenti.

---

## 8. InstallSnapshot reale

Il precedente `InstallSnapshot` era solo uno stub logico.

Ora è stata implementata la logica reale in:

```text
internal/consensus/rpc_snapshot.go
```

---

### 8.1 Invio snapshot lato leader

In `replication.go` è stata aggiunta la funzione:

```go
sendSnapshotToPeer(ctx, peerID, peerAddress)
```

Quando un follower è troppo indietro e il leader ha già compattato le entry richieste, il leader invia uno snapshot con:

```proto
offset = 0
data = snapshot serializzato
done = true
```

In questa fase lo snapshot viene inviato in un unico chunk. Il proto è già pronto per chunk multipli.

---

### 8.2 Applicazione snapshot lato follower

Il follower, ricevuto `InstallSnapshot`:

```text
1. rifiuta termini vecchi;
2. aggiorna currentTerm se necessario;
3. passa a follower;
4. riconosce il leader;
5. decodifica lo snapshot;
6. ripristina la state machine;
7. aggiorna lastIncludedIndex e lastIncludedTerm;
8. aggiorna commitIndex e lastApplied;
9. compatta il log locale;
10. salva snapshot e stato persistente;
11. resetta il timer di elezione.
```

Sono state aggiunte in `persistence.Manager` le funzioni:

```go
EncodeSnapshot(snapshot Snapshot) ([]byte, error)
DecodeSnapshot(data []byte) (Snapshot, error)
```

---

## 9. Letture con quorum-check

La `Get` non si limita più a verificare che il nodo sia leader.

Prima di leggere dallo store, il leader chiama:

```go
confirmLeadershipWithQuorum(ctx)
```

Questa funzione invia heartbeat `AppendEntries` vuoti ai peer e richiede una maggioranza di risposte valide.

Se il leader non riesce a contattare la maggioranza, la lettura fallisce con:

```text
leader quorum unavailable
```

---

### 9.1 Test positivo

Con quorum disponibile:

```text
Put response: success=true key=read-quorum-final-key value=read-quorum-final-value
Get response: found=true key=read-quorum-final-key value=read-quorum-final-value
```

---

### 9.2 Test negativo

Dopo aver lasciato il leader senza quorum, la `Get` restituisce:

```text
Get response: found=false key=read-quorum-final-key value= error=leader quorum unavailable leader_hint=localhost:50051
```

Questo conferma che un leader isolato non serve letture potenzialmente stale.

---

## 10. Test effettuati

### 10.1 Compilazione generale

È stato eseguito:

```cmd
go test ./...
```

Output sintetico:

```text
? cmd/backup-service [no test files]
? cmd/bench-client [no test files]
? cmd/client-proxy [no test files]
? cmd/consensus-node [no test files]
? gen/go/backuppb [no test files]
? gen/go/consensuspb [no test files]
? gen/go/kvpb [no test files]
? internal/consensus [no test files]
? internal/persistence [no test files]
? internal/proxy [no test files]
? internal/storage [no test files]
```

---

### 10.2 Test Proxy e GetLeader arricchito

```text
GetLeader response: has_leader=true leader_id=node-1 leader_address=localhost:50051 term=35
```

Successivamente:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=localhost:50053 term=12
```

---

### 10.3 Test health endpoint

```cmd
curl http://localhost:8081/health
```

Output:

```json
{"status":"ok","timestamp":"2026-06-24T14:02:11.9594395Z"}
```

---

### 10.4 Test WAL event-based e compaction

Dopo una Put con `SNAPSHOT_THRESHOLD=1`, il WAL contiene:

```json
{"type":"log_appended","entry":{"index":1,"term":5,"operation":1,"key":"compact-key-2","value":"compact-value-2"}}
{"type":"snapshot_saved","last_included_index":1,"last_included_term":5,"data":{"compact-key-2":"compact-value-2"}}
{"type":"log_truncated","truncate_from":1}
{"type":"commit_advanced","commit_index":1}
{"type":"state_applied","last_applied":1}
```

È stata corretta anche la duplicazione iniziale dell'evento `snapshot_saved`.

---

### 10.5 Test recovery senza state.json

Dopo eliminazione degli state file:

```cmd
del data\node-1\node-1_state.json
del data\node-2\node-2_state.json
del data\node-3\node-3_state.json
```

La lettura ha restituito ancora il valore corretto:

```text
Get response: found=true key=compact-key value=compact-value
```

---

### 10.6 Test fast back-off base

Dopo l'aggiornamento di `AppendEntriesResponse` e `replication.go`:

```text
Put response: success=true key=fast-backoff-key value=fast-backoff-value
Get response: found=true key=fast-backoff-key value=fast-backoff-value
```

Il test dimostra assenza di regressioni sulla replica normale. Un test specifico su follower divergente verrà automatizzato in seguito.

---

### 10.7 Test InstallSnapshot base

Dopo l'implementazione reale:

```text
Put response: success=true key=install-snapshot-key value=install-snapshot-value
Get response: found=true key=install-snapshot-key value=install-snapshot-value
```

È stata inoltre eseguita una sequenza:

```text
snap-a = 1
snap-b = 2
snap-c = 3
snap-d = 4
```

con verifica finale:

```text
Get response: found=true key=snap-d value=4
```

---

### 10.8 Test quorum-check letture

Caso positivo:

```text
Put response: success=true key=read-quorum-final-key value=read-quorum-final-value
Get response: found=true key=read-quorum-final-key value=read-quorum-final-value
```

Caso negativo:

```text
Get response: found=false key=read-quorum-final-key value= error=leader quorum unavailable leader_hint=localhost:50051
```

---

## 11. Stato finale del risanamento

Checklist finale:

```text
[OK] Proxy hardening pre-Docker
[OK] GetLeader con leader_id e term reali
[OK] endpoint HTTP /health
[OK] exponential backoff con jitter
[OK] Circuit Breaker per nodo
[OK] kv.proto aggiornato con request_id su GetRequest
[OK] consensus.proto aggiornato con conflict_index e conflict_term
[OK] stub Go rigenerati
[OK] WAL event-based
[OK] replay WAL
[OK] snapshot locale reale
[OK] log compaction locale
[OK] lastIncludedIndex / lastIncludedTerm persistiti
[OK] fast back-off AppendEntries
[OK] InstallSnapshot reale
[OK] quorum-check sulle Get
[OK] recovery senza state.json
[OK] go test ./... passato
[OK] test runtime principali superati
```

---

## 12. Limiti residui

Rimangono alcuni miglioramenti futuri:

```text
[DA FARE] test automatici di fault-injection per follower divergente;
[DA FARE] test automatici per InstallSnapshot forzato con follower offline;
[DA FARE] chunking reale multi-blocco per snapshot molto grandi;
[DA FARE] batching avanzato delle entry client;
[DA FARE] metriche strutturate per Proxy e Consensus Node;
[DA FARE] integrazione completa con Docker healthcheck nella Fase 7.
```

Questi punti non bloccano la Dockerizzazione, perché il protocollo e i componenti principali sono ora più solidi e maturi.

---

## 13. Preparazione alla Fase 7

Dopo questo risanamento, il sistema è pronto per la Fase 7 di Dockerizzazione perché:

- il Proxy espone un endpoint `/health`;
- la persistenza è più robusta e meno ridondante;
- il WAL è event-based;
- i nodi recuperano da WAL e snapshot;
- il log non cresce indefinitamente grazie alla compaction locale;
- la replica gestisce meglio i conflitti;
- i follower arretrati possono recuperare tramite snapshot;
- le letture non vengono servite da leader isolati;
- il contratto `.proto` è stato aggiornato e gli stub sono coerenti.

La Fase 7 potrà quindi concentrarsi su:

```text
1. Dockerfile per consensus-node, client-proxy e backup-service;
2. docker-compose.yml con rete dedicata;
3. volumi persistenti per data/;
4. healthcheck del Proxy;
5. variabili d'ambiente per nodi e cluster;
6. test end-to-end in ambiente containerizzato.
```
