# Fase 5.bis - Refactor SRP del Consensus Node

Questo documento descrive il refactor effettuato dopo la Fase 5 e il relativo hardening, con l'obiettivo di ridurre la complessità di `internal/consensus/node.go` e separare le responsabilità secondo il principio **Single Responsibility Principle**.

Prima di questo intervento, `ConsensusNode` stava assumendo troppe responsabilità: gestione del protocollo Raft, storage key-value, persistenza su disco, snapshot, connessioni verso i peer, RPC client-facing e RPC interne al consenso.

Dopo il refactor, il sistema mantiene la stessa semantica funzionale già validata nella Fase 5, ma il codice è stato riorganizzato in componenti più piccoli e più facili da testare, modificare e comprendere.

---

## 1. Obiettivo del refactor

L'obiettivo principale era evitare che `ConsensusNode` diventasse un *God Object*.

La classe iniziale gestiva contemporaneamente:

- elezione del leader;
- replicazione del log;
- gestione dei peer gRPC;
- applicazione delle entry alla mappa key-value;
- persistenza su `state.json`;
- WAL append-only;
- snapshot locale;
- recovery dopo restart;
- RPC `Put`, `Get`, `Delete`, `GetLeader`;
- RPC `RequestVote`, `AppendEntries`, `InstallSnapshot`.

Il refactor ha separato queste responsabilità in file e package dedicati, mantenendo invariata la logica già testata.

---

## 2. Struttura finale dei file

Dopo il refactor, la struttura rilevante è:

```text
internal/
  consensus/
    election.go
    log_helpers.go
    node.go
    peers.go
    replication.go
    rpc_kv.go
    state_recovery.go

  persistence/
    manager.go

  storage/
    kv_store.go
```

Questa divisione consente di isolare meglio i motivi di cambiamento:

- modifiche alla macchina a stati vanno in `internal/storage`;
- modifiche alla persistenza vanno in `internal/persistence`;
- modifiche a Raft restano in `internal/consensus`;
- modifiche alle RPC key-value stanno in `rpc_kv.go`;
- modifiche alla replicazione stanno in `replication.go`;
- modifiche all'elezione stanno in `election.go`.

---

## 3. `internal/storage/kv_store.go`

Il file `kv_store.go` contiene la macchina a stati key-value.

Responsabilità principali:

- mantenere la mappa `map[string]string`;
- applicare entry già committed;
- servire letture locali dallo store;
- produrre snapshot della mappa;
- ripristinare la mappa durante il recovery;
- resettare la state machine prima di una ricostruzione.

La struct principale è:

```go
type KVStore struct {
    mu   sync.Mutex
    data map[string]string
}
```

I metodi principali sono:

```go
func NewKVStore() *KVStore
func (s *KVStore) Apply(entry *consensuspb.LogEntry)
func (s *KVStore) Get(key string) (string, bool)
func (s *KVStore) Snapshot() map[string]string
func (s *KVStore) Restore(data map[string]string)
func (s *KVStore) Reset()
```

Dopo questo refactor, `ConsensusNode` non manipola più direttamente la mappa dati. Invece usa:

```go
n.store.Apply(entry)
n.store.Get(req.Key)
n.store.Snapshot()
n.store.Restore(data)
n.store.Reset()
```

Questo separa la responsabilità della state machine dal protocollo di consenso.

---

## 4. `internal/persistence/manager.go`

Il file `manager.go` contiene la gestione della persistenza su disco.

Responsabilità principali:

- salvare `state.json`;
- scrivere record append-only nel WAL;
- caricare `state.json`;
- recuperare l'ultimo record valido dal WAL se `state.json` manca;
- salvare snapshot locali;
- caricare snapshot locali.

La struct principale è:

```go
type Manager struct {
    nodeID       string
    stateFile    string
    walFile      string
    snapshotFile string
}
```

Le struct dati principali sono:

```go
type State struct {
    CurrentTerm uint64
    VotedFor    string
    Log         []*consensuspb.LogEntry
    CommitIndex uint64
    LastApplied uint64
    Data        map[string]string
}
```

```go
type Snapshot struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    Data              map[string]string
}
```

I metodi principali sono:

```go
func NewManager(nodeID string, dataDir string) *Manager
func (m *Manager) Save(state State) error
func (m *Manager) Load() (State, bool, error)
func (m *Manager) SaveSnapshot(snapshot Snapshot) error
func (m *Manager) LoadSnapshot() (Snapshot, bool, error)
```

Dopo questo refactor, `ConsensusNode` non scrive più direttamente file JSON o WAL. Il nodo costruisce uno stato logico e delega la persistenza a `persistence.Manager`.

---

## 5. `internal/consensus/node.go`

Dopo il refactor, `node.go` è stato alleggerito.

Responsabilità rimaste:

- definizione della struct `ConsensusNode`;
- costruttore `NewConsensusNode`;
- inizializzazione dei componenti interni;
- `Start()`;
- `Stop()`;
- configurazione base del nodo;
- embedding degli stub gRPC generati.

La struct ora contiene componenti più specializzati:

```go
store *storage.KVStore
persistenceManager *persistence.Manager
```

Questi campi sostituiscono la gestione diretta di:

```go
data map[string]string
stateFile string
walFile string
snapshotFile string
```

Il nodo resta l'orchestratore del protocollo, ma non contiene più tutta la logica operativa in un unico file.

---

## 6. `internal/consensus/peers.go`

Il file `peers.go` contiene la gestione dei peer del cluster.

Responsabilità principali:

- creare client gRPC verso i peer;
- riusare connessioni gRPC esistenti;
- marcare peer offline;
- marcare peer online;
- limitare il logging ripetitivo dei peer non raggiungibili.

Metodi spostati:

```go
func (n *ConsensusNode) getPeerClientLocked(peerID string, peerAddress string) (consensuspb.ConsensusServiceClient, error)
func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error)
func (n *ConsensusNode) markPeerOnlineLocked(peerID string)
```

Questa separazione consente di tenere il codice di rete di supporto separato dalla logica principale di Raft.

---

## 7. `internal/consensus/election.go`

Il file `election.go` contiene la logica di elezione del leader.

Responsabilità principali:

- timeout randomizzato di elezione;
- reset del timer di elezione;
- loop di elezione;
- transizione a Candidate;
- incremento del termine;
- voto per sé stesso;
- invio parallelo di `RequestVote`;
- conteggio dei voti;
- transizione a Leader;
- gestione RPC `RequestVote`.

Funzioni/metodi principali:

```go
func randomElectionTimeout() time.Duration
func (n *ConsensusNode) resetElectionTimer()
func (n *ConsensusNode) electionLoop()
func (n *ConsensusNode) startElection()
func (n *ConsensusNode) becomeLeaderLocked()
func (n *ConsensusNode) RequestVote(ctx context.Context, req *consensuspb.RequestVoteRequest) (*consensuspb.RequestVoteResponse, error)
```

Questo file isola la parte di Raft relativa alla leadership.

---

## 8. `internal/consensus/replication.go`

Il file `replication.go` contiene la logica di replicazione del log.

Responsabilità principali:

- heartbeat periodici del leader;
- invio `AppendEntries` vuote;
- replica delle entry verso i follower;
- gestione `nextIndex` e `matchIndex`;
- retry decrementando `nextIndex`;
- replica fino al quorum;
- gestione di `AppendEntries` lato follower;
- append e risoluzione conflitti delle entry ricevute dal leader;
- gestione comune di `Put` e `Delete` tramite `handleWriteOperation`.

Metodi principali:

```go
func (n *ConsensusNode) heartbeatLoop()
func (n *ConsensusNode) sendHeartbeats()
func (n *ConsensusNode) replicateLogToPeer(ctx context.Context, peerID string, peerAddress string, targetIndex uint64) bool
func (n *ConsensusNode) replicateEntryToQuorum(ctx context.Context, entryIndex uint64) bool
func (n *ConsensusNode) handleWriteOperation(ctx context.Context, operation consensuspb.LogOperation, key string, value string) (bool, string, string, error)
func (n *ConsensusNode) AppendEntries(ctx context.Context, req *consensuspb.AppendEntriesRequest) (*consensuspb.AppendEntriesResponse, error)
```

Questo file separa la log replication dalla struttura base del nodo.

---

## 9. `internal/consensus/rpc_kv.go`

Il file `rpc_kv.go` contiene le RPC esposte al client o al futuro Client Proxy.

Responsabilità principali:

- `Put`;
- `Get`;
- `Delete`;
- `GetLeader`.

Le scritture non modificano direttamente la state machine. Vengono trasformate in entry del log e replicate tramite Raft.

Le letture sono servite solo dal leader:

```text
Get su leader   -> legge dallo store
Get su follower -> restituisce node is not leader + leader_hint
```

Questo rende il file il punto di ingresso delle richieste client-facing, lasciando la replica a `replication.go`.

---

## 10. `internal/consensus/log_helpers.go`

Il file `log_helpers.go` contiene helper interni per lavorare sul log Raft.

Responsabilità principali:

- cercare entry per indice;
- ottenere ultimo indice e ultimo termine;
- ottenere il termine di una entry specifica;
- verificare `prevLogIndex` e `prevLogTerm`;
- troncare il log in caso di conflitti;
- verificare se il log di un candidato è aggiornato;
- calcolare la maggioranza del cluster.

Metodi principali:

```go
func (n *ConsensusNode) entryByIndexLocked(index uint64) *consensuspb.LogEntry
func (n *ConsensusNode) lastLogIndexLocked() uint64
func (n *ConsensusNode) lastLogTermLocked() uint64
func (n *ConsensusNode) logTermAtIndexLocked(index uint64) (uint64, bool)
func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool
func (n *ConsensusNode) truncateLogFromIndexLocked(index uint64)
func (n *ConsensusNode) isCandidateLogUpToDateLocked(candidateLastLogIndex uint64, candidateLastLogTerm uint64) bool
func (n *ConsensusNode) majority() int
```

---

## 11. `internal/consensus/state_recovery.go`

Il file `state_recovery.go` contiene la logica di coordinamento tra consenso, persistenza e storage.

Responsabilità principali:

- costruire lo stato persistente corrente;
- chiamare `persistence.Manager.Save`;
- salvare snapshot locali;
- decidere quando generare snapshot;
- caricare lo stato persistente all'avvio;
- applicare snapshot se più recente;
- ripristinare campi Raft persistenti;
- ricostruire la state machine dal log committed;
- applicare entry committed allo store.

Metodi principali:

```go
func (n *ConsensusNode) persistentStateLocked() persistence.State
func (n *ConsensusNode) persistLocked() error
func (n *ConsensusNode) saveSnapshotLocked() error
func (n *ConsensusNode) maybeSaveSnapshotLocked()
func (n *ConsensusNode) loadPersistentState() error
func (n *ConsensusNode) restoreSnapshotIfNewerLocked(snapshot persistence.Snapshot) bool
func (n *ConsensusNode) restorePersistentStateLocked(state persistence.State)
func (n *ConsensusNode) rebuildStateMachineFromCommittedLogLocked()
func (n *ConsensusNode) applyCommittedEntriesLocked()
```

Questo file mantiene nel package consensus la logica che dipende dagli indici Raft `commitIndex` e `lastApplied`, ma delega i dettagli fisici al package persistence e la mappa dati al package storage.

---

## 12. Test di compilazione

Dopo il refactor è stato eseguito:

```cmd
gofmt -w internal\consensus\node.go internal\consensus\election.go internal\consensus\log_helpers.go internal\consensus\peers.go internal\consensus\replication.go internal\consensus\rpc_kv.go internal\consensus\state_recovery.go internal\persistence\manager.go internal\storage\kv_store.go
```

Poi:

```cmd
go test ./...
```

Output sintetico:

```text
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/backup-service [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/bench-client [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/client-proxy [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/consensus-node [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/consensus [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/storage [no test files]
```

Il risultato conferma che il refactor non ha introdotto errori di compilazione.

---

## 13. Test runtime

Dopo il refactor è stato avviato un cluster a tre nodi.

È stato eseguito un test `Put` sul leader:

```cmd
set OP=put
set KEY=srp-key
set VALUE=srp-value
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=srp-key value=srp-value error= leader_hint=
```

Poi una `Get` sul leader:

```cmd
set OP=get
set KEY=srp-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=srp-key value=srp-value error= leader_hint=localhost:50051
```

Questo conferma che il refactor non ha rotto:

- scrittura su leader;
- replica su quorum;
- commit;
- apply su `KVStore`;
- lettura dallo store.

---

## 14. Verifica dello spostamento delle funzioni

Sono stati eseguiti controlli `findstr` per verificare che le funzioni fossero state rimosse da `node.go` e spostate nei file corretti.

Esempi:

```cmd
findstr /n /C:"func (n *ConsensusNode) RequestVote" internal\consensus\node.go
findstr /n /C:"func (n *ConsensusNode) RequestVote" internal\consensus\election.go
```

Risultato:

```text
RequestVote non è più in node.go
RequestVote è in election.go
```

Analogamente:

```text
AppendEntries -> replication.go
Put/Get/Delete/GetLeader -> rpc_kv.go
log helpers -> log_helpers.go
peer helpers -> peers.go
state recovery -> state_recovery.go
```

---

## 15. Stato finale del refactor

Checklist finale:

```text
[OK] KVStore estratto in internal/storage
[OK] Persistence Manager estratto in internal/persistence
[OK] Peer management spostato in peers.go
[OK] Leader election spostata in election.go
[OK] Log replication spostata in replication.go
[OK] RPC key-value spostate in rpc_kv.go
[OK] Helper del log spostati in log_helpers.go
[OK] Recovery/apply/snapshot orchestration spostati in state_recovery.go
[OK] node.go alleggerito
[OK] gofmt eseguito
[OK] go test ./... superato
[OK] Put/Get runtime superato
```

---

## 16. Benefici ottenuti

Il refactor porta diversi benefici:

- `node.go` è più piccolo e leggibile;
- ogni file ha una responsabilità più chiara;
- lo storage key-value è testabile separatamente;
- la persistenza è testabile separatamente;
- le funzioni Raft sono raggruppate per area logica;
- il futuro Client Proxy potrà essere sviluppato su una base più ordinata;
- sarà più semplice aggiungere test automatici mirati;
- sarà più semplice evolvere WAL, snapshot e log compaction.

---

## 17. Limiti ancora presenti

Il refactor è principalmente strutturale. Non risolve ancora alcuni limiti architetturali già noti:

- il WAL è ancora snapshot-based e non event-based;
- `InstallSnapshot` non applica ancora snapshot remoti reali;
- non esiste ancora log compaction reale;
- il retry di `nextIndex` è ancora a passo singolo;
- le letture sono forti solo nella forma base, perché passano dal leader ma non usano ancora ReadIndex;
- mancano test automatici completi per crash e recovery.

Questi punti potranno essere affrontati nelle fasi successive.

---

## 18. Prossimi step

Dopo questo refactor, il sistema è più pronto per lo sviluppo del Client Proxy Service.

I prossimi passi consigliati sono:

```text
1. completare il commit del refactor SRP;
2. sviluppare il Client Proxy Service;
3. implementare leader discovery lato proxy;
4. usare leader_hint per aggiornare la cache del leader;
5. aggiungere retry controllati sul proxy;
6. valutare circuit breaker per nodi non raggiungibili;
7. aggiungere test automatici di failover leader;
8. proseguire poi con WAL event-based, InstallSnapshot reale e log compaction.
```
