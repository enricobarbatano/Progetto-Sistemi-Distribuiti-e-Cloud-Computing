# Fase 5 - Replicazione Atomica dei Log

Questo documento descrive la **Fase 5** del progetto, dedicata alla replicazione atomica dei log nel cluster di Consensus Node, e il successivo hardening di persistenza/recovery effettuato prima dello sviluppo del Client Proxy Service.

La fase rappresenta il passaggio da un cluster capace di eleggere un leader a un sistema di storage replicato con conferma su quorum. Una scrittura non viene più applicata localmente in modo immediato e isolato, ma viene prima inserita nel log del leader, persistita, replicata sui follower e considerata completata solo dopo il raggiungimento della maggioranza dei nodi.

In aggiunta, prima di procedere con il Proxy, sono stati introdotti:

- recovery robusto della state machine dopo riavvio;
- persistenza di `commitIndex`, `lastApplied` e `data`;
- fallback dall'ultimo record valido del WAL;
- snapshot locale minimale della mappa key-value.

---

## 1. Obiettivo della fase

Prima della Fase 5, il cluster era già in grado di:

- eleggere un leader;
- mantenere heartbeat periodici;
- propagare il leader noto tramite `GetLeader`;
- persistere `currentTerm`, `votedFor` e log su disco;
- gestire connessioni gRPC persistenti verso i peer;
- tracciare lo stato online/offline dei peer;
- eseguire `Put` e `Delete` solo localmente sul leader, senza vera replica su quorum.

La Fase 5 introduce invece:

- append locale delle entry sul leader;
- persistenza immediata della nuova entry tramite WAL e `state.json`;
- replica delle entry ai follower con `AppendEntries`;
- controllo di coerenza del log con `prevLogIndex` e `prevLogTerm`;
- gestione di `nextIndex` e `matchIndex` per ogni follower;
- retry decrementando `nextIndex` quando un follower rifiuta una replica;
- commit solo dopo replica su maggioranza;
- applicazione alla state machine solo dopo commit;
- risposta al client solo dopo il commit;
- `Get` servita solo dal leader per evitare letture stale.

---

## 2. File modificati

I file principali modificati in questa fase sono:

```text
internal/consensus/node.go
cmd/bench-client/main.go
documents/Fase5-replicazioneAtomicaDeiLog.md
```

### `internal/consensus/node.go`

Contiene la logica del Consensus Node.

In questa fase sono state aggiunte le funzioni necessarie per:

- selezionare le entry da replicare a partire da un certo indice;
- replicare il log verso un singolo peer;
- replicare una entry fino al raggiungimento del quorum;
- gestire `Put` e `Delete` in modo comune;
- forzare le letture `Get` solo sul leader;
- ripristinare lo stato applicato dopo un riavvio;
- salvare snapshot locali della state machine.

### `cmd/bench-client/main.go`

È stato aggiornato per supportare test manuali più completi tramite variabili d'ambiente:

```text
OP=leader
OP=get
OP=put
OP=delete
OP=all
```

---

## 3. Modello di consistenza adottato

Il sistema adotta una forma base di consistenza forte:

- solo il leader accetta scritture;
- solo il leader serve letture;
- una scrittura viene confermata solo dopo replica su quorum;
- una entry viene applicata alla mappa key-value solo dopo essere committed;
- i follower rispondono alle richieste client con errore e `leader_hint`.

Il comportamento scelto è:

```text
Put/Delete su leader   -> possibile successo dopo quorum
Put/Delete su follower -> rifiuto con leader_hint
Get su leader          -> lettura dalla state machine locale
Get su follower        -> rifiuto con leader_hint
```

Questa scelta evita letture stale dai follower.

---

## 4. Append locale sul leader

Quando il leader riceve una richiesta `Put` o `Delete`, crea una nuova `LogEntry`.

Per una `Put`, la entry contiene:

```go
entry := &consensuspb.LogEntry{
    Index:     n.lastLogIndexLocked() + 1,
    Term:      n.currentTerm,
    Operation: consensuspb.LogOperation_LOG_OPERATION_PUT,
    Key:       key,
    Value:     value,
}
```

Per una `Delete`, la entry usa invece:

```go
Operation: consensuspb.LogOperation_LOG_OPERATION_DELETE
```

La entry viene aggiunta al log locale del leader:

```go
n.log = append(n.log, entry)
```

Subito dopo viene persistita tramite:

```go
n.persistLocked()
```

---

## 5. Persistenza prima della replica

Il leader persiste la entry prima di iniziare la replica.

Il flusso è:

```text
1. crea LogEntry
2. append nel log locale
3. persist su WAL
4. aggiorna state.json
5. replica ai follower
```

Questo riduce il rischio di perdere una entry già presa in carico in caso di crash immediato dopo l'append locale.

---

## 6. Helper per la gestione del log

Per supportare la replicazione sono stati introdotti helper specifici.

### `entriesFromIndexLocked`

Restituisce tutte le entry del log a partire da un certo indice:

```go
func (n *ConsensusNode) entriesFromIndexLocked(index uint64) []*consensuspb.LogEntry
```

### `logTermAtIndexLocked`

Restituisce il termine della entry con un certo indice:

```go
func (n *ConsensusNode) logTermAtIndexLocked(index uint64) (uint64, bool)
```

### `hasLogEntryLocked`

Verifica se il nodo possiede una entry con indice e termine specifici:

```go
func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool
```

### `truncateLogFromIndexLocked`

Rimuove tutte le entry con indice maggiore o uguale a quello specificato:

```go
func (n *ConsensusNode) truncateLogFromIndexLocked(index uint64)
```

### `appendEntriesFromLeaderLocked`

Appende al follower le entry ricevute dal leader e gestisce eventuali conflitti:

```go
func (n *ConsensusNode) appendEntriesFromLeaderLocked(entries []*consensuspb.LogEntry) bool
```

---

## 7. AppendEntries lato follower

La RPC `AppendEntries` segue questo flusso:

```text
1. se req.Term < currentTerm, rifiuta;
2. se req.Term > currentTerm, aggiorna currentTerm e votedFor;
3. riconosce il leader e passa a follower;
4. verifica prevLogIndex e prevLogTerm;
5. se il controllo fallisce, risponde success=false;
6. se il controllo passa, appende le entry;
7. in caso di conflitto, tronca il log e riallinea;
8. aggiorna commitIndex se leaderCommit è maggiore;
9. applica le entry committed alla state machine;
10. persiste lo stato se è cambiato;
11. resetta il timer di elezione.
```

Il controllo principale è:

```go
if !n.hasLogEntryLocked(req.PrevLogIndex, req.PrevLogTerm) {
    return &consensuspb.AppendEntriesResponse{
        Term:       n.currentTerm,
        Success:    false,
        MatchIndex: n.lastLogIndexLocked(),
    }, nil
}
```

---

## 8. Gestione di nextIndex e matchIndex

Quando un nodo diventa leader, inizializza per ogni follower:

```go
nextIndex := n.lastLogIndexLocked() + 1

for peerID := range n.peers {
    n.nextIndex[peerID] = nextIndex
    n.matchIndex[peerID] = 0
}
```

Il significato è:

```text
nextIndex[peer]  = prossima entry da inviare a quel follower
matchIndex[peer] = ultima entry nota come replicata con successo su quel follower
```

Durante la replica:

```go
n.matchIndex[peerID] = resp.MatchIndex
n.nextIndex[peerID] = resp.MatchIndex + 1
```

Se la replica fallisce per inconsistenza del log, il leader decrementa `nextIndex` e riprova.

---

## 9. Replica verso un peer

La funzione:

```go
func (n *ConsensusNode) replicateLogToPeer(ctx context.Context, peerID string, peerAddress string, targetIndex uint64) bool
```

si occupa di replicare il log verso un singolo follower.

Il flusso è:

```text
1. legge nextIndex[peer];
2. calcola prevLogIndex = nextIndex - 1;
3. calcola prevLogTerm;
4. prende le entry da nextIndex in poi;
5. invia AppendEntries;
6. se il follower risponde success=true, aggiorna matchIndex e nextIndex;
7. se il follower risponde success=false, decrementa nextIndex e riprova;
8. se il contesto scade o il nodo perde leadership, fallisce.
```

---

## 10. Replica fino al quorum

La funzione:

```go
func (n *ConsensusNode) replicateEntryToQuorum(ctx context.Context, entryIndex uint64) bool
```

coordina la replica verso tutti i follower.

Il leader conta sé stesso come primo nodo che possiede la entry:

```go
replicatedCount := 1
```

Con 3 nodi, la maggioranza è 2:

```text
leader + 1 follower = quorum
```

Se viene raggiunta la maggioranza, la funzione restituisce `true`. Altrimenti restituisce `false`.

---

## 11. Gestione comune di Put e Delete

`Put` e `Delete` usano una funzione comune:

```go
func (n *ConsensusNode) handleWriteOperation(ctx context.Context, operation consensuspb.LogOperation, key string, value string) (bool, string, string, error)
```

Questa funzione:

1. verifica che il nodo sia leader;
2. crea una nuova `LogEntry`;
3. appende la entry al log locale;
4. persiste lo stato;
5. replica la entry su quorum;
6. se il quorum viene raggiunto, aggiorna `commitIndex`;
7. applica la entry alla state machine;
8. persiste di nuovo lo stato applicato;
9. invia heartbeat per propagare il nuovo `commitIndex`;
10. restituisce successo al client.

---

## 12. Get solo dal leader

Per evitare letture stale dai follower, `Get` viene servita solo dal leader.

Se un follower riceve una `Get`, risponde:

```text
found=false
error=node is not leader
leader_hint=<leader address>
```

Se il leader riceve una `Get`, legge dalla propria state machine locale.

---

## 13. Recovery robusto

Dopo i primi test della Fase 5 è emerso un limite importante: il nodo persisteva il log, ma la mappa key-value in memoria poteva non essere ricostruita correttamente dopo un restart.

Per risolvere il problema, `persistentState` è stato esteso con:

```go
CommitIndex uint64            `json:"commit_index"`
LastApplied uint64            `json:"last_applied"`
Data        map[string]string `json:"data"`
```

Anche `walRecord` è stato esteso con gli stessi campi.

Ora `state.json` contiene:

```json
{
  "current_term": 6,
  "voted_for": "node-1",
  "log": [
    {
      "index": 1,
      "term": 3,
      "operation": 1,
      "key": "recovery-key",
      "value": "recovery-value"
    }
  ],
  "commit_index": 1,
  "last_applied": 1,
  "data": {
    "recovery-key": "recovery-value"
  }
}
```

Il recovery segue questo ordine:

```text
1. prova a caricare state.json;
2. se state.json manca, prova a ricostruire dall'ultimo record valido del WAL;
3. se esiste uno snapshot più recente, lo usa come base della data map;
4. se mancano dati applicati, ricostruisce applicando solo entry committed;
5. non applica entry non committed.
```

Il test di recovery ha confermato che, dopo stop e restart dei nodi senza cancellare `data/`, il nuovo leader legge ancora:

```text
Get response: found=true key=recovery-key value=recovery-value
```

---

## 14. Snapshot locale minimale

Prima di passare al Client Proxy, è stato aggiunto uno snapshot locale minimale della state machine.

Lo snapshot viene salvato in:

```text
data/node-X/node-X_snapshot.json
```

La struttura è:

```go
type snapshotState struct {
    LastIncludedIndex uint64            `json:"last_included_index"`
    LastIncludedTerm  uint64            `json:"last_included_term"`
    Data              map[string]string `json:"data"`
}
```

Esempio di snapshot generato:

```json
{
  "last_included_index": 1,
  "last_included_term": 1,
  "data": {
    "snapshot-key": "snapshot-value"
  }
}
```

Lo snapshot è atomico: viene scritto prima su file temporaneo e poi sostituito tramite rename.

---

## 15. Soglia snapshot configurabile

La soglia snapshot predefinita è:

```go
const defaultSnapshotThreshold uint64 = 1000
```

Per test locali si può usare la variabile d'ambiente:

```cmd
set SNAPSHOT_THRESHOLD=1
```

In questo modo lo snapshot viene creato dopo la prima entry applicata.

Questo evita di modificare il codice solo per testare il comportamento.

---

## 16. Caricamento snapshot all'avvio

All'avvio, dopo aver caricato `state.json` o il fallback WAL, il nodo prova a caricare lo snapshot locale:

```go
func (n *ConsensusNode) loadSnapshotIfNewerLocked() error
```

Lo snapshot viene applicato solo se:

```text
snapshot.LastIncludedIndex > n.lastApplied
```

Questo evita di sovrascrivere uno stato più recente già presente in `state.json`.

Per ora lo snapshot è un checkpoint locale della state machine. Non implementa ancora:

- log compaction reale;
- offset del log;
- InstallSnapshot remoto leader -> follower;
- invio snapshot a follower troppo indietro.

---

## 17. Test: Put sul leader

Durante il test, il leader era:

```text
node-2 -> localhost:50052
```

È stata eseguita una `Put` sul leader:

```cmd
set TARGET=localhost:50052
set OP=put
set KEY=name
set VALUE=enrico
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=name value=enrico error= leader_hint=
```

---

## 18. Test: Get sul leader dopo Put

```cmd
set TARGET=localhost:50052
set OP=get
set KEY=name
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=name value=enrico error= leader_hint=localhost:50052
```

---

## 19. Test: Get su follower

```cmd
set TARGET=localhost:50051
set OP=get
set KEY=name
go run .\cmd\bench-client
```

Output:

```text
Get response: found=false key=name value= error=node is not leader leader_hint=localhost:50052
```

---

## 20. Test: Put su follower

```cmd
set TARGET=localhost:50051
set OP=put
set KEY=city
set VALUE=roma
go run .\cmd\bench-client
```

Output:

```text
Put response: success=false key=city value=roma error=node is not leader leader_hint=localhost:50052
```

---

## 21. Test: Put con un follower spento

Con un cluster a 3 nodi, è stato spento un follower. Il leader ha comunque accettato una scrittura perché leader + un follower raggiungibile formano ancora quorum.

Output:

```text
Put response: success=true key=country value=italia error= leader_hint=
Get response: found=true key=country value=italia error= leader_hint=localhost:50052
```

---

## 22. Test: Put senza quorum

Quando il leader è rimasto senza maggioranza, una `Put` è stata rifiutata.

Output:

```text
Put response: success=false key=fail-test value=no-quorum error=failed to replicate entry to quorum leader_hint=localhost:50052
```

---

## 23. Test: Delete sul leader

Dopo un reset pulito del cluster, il leader era:

```text
node-3 -> localhost:50053
```

La `Put` verso il leader è riuscita:

```text
Put response: success=true key=country value=italia error= leader_hint=
```

La `Delete` è riuscita:

```text
Delete response: success=true key=country error= leader_hint=
```

La `Get` successiva ha confermato la cancellazione:

```text
Get response: found=false key=country value= error= leader_hint=localhost:50053
```

---

## 24. Test: recovery robusto

È stata scritta la chiave:

```text
recovery-key = recovery-value
```

Dopo lo stop e restart dei tre nodi, senza cancellare `data/`, il nuovo leader ha restituito:

```text
Get response: found=true key=recovery-key value=recovery-value error= leader_hint=localhost:50051
```

Questo conferma che la state machine viene recuperata correttamente.

---

## 25. Test: snapshot locale minimale

Per testare rapidamente lo snapshot è stata usata la soglia:

```cmd
set SNAPSHOT_THRESHOLD=1
```

Dopo una `Put`:

```cmd
set TARGET=localhost:50053
set OP=put
set KEY=snapshot-key
set VALUE=snapshot-value
go run .\cmd\bench-client
```

è stato generato lo snapshot su tutti e tre i nodi:

```json
{
  "last_included_index": 1,
  "last_included_term": 1,
  "data": {
    "snapshot-key": "snapshot-value"
  }
}
```

La `Get` sul leader ha confermato il valore:

```text
Get response: found=true key=snapshot-key value=snapshot-value error= leader_hint=localhost:50053
```

Questo conferma che lo snapshot locale viene generato correttamente dopo l'applicazione della entry committed.

---

## 26. Verifica tramite go test

Dopo le modifiche è stato eseguito:

```cmd
gofmt -w internal\consensus\node.go cmd\bench-client\main.go
go test ./...
```

Il comando ha compilato correttamente tutti i package.

---

## 27. Stato finale della Fase 5 e hardening 5.5

Checklist:

```text
[OK] AppendEntries lato follower con prevLogIndex/prevLogTerm
[OK] Gestione conflitti base nel log del follower
[OK] nextIndex inizializzato per ogni follower
[OK] matchIndex inizializzato per ogni follower
[OK] Replica delle entry verso i follower
[OK] Retry con decremento di nextIndex
[OK] Put accettata solo dal leader
[OK] Delete accettata solo dal leader
[OK] Put/Delete rifiutate dai follower con leader_hint
[OK] Commit solo dopo quorum
[OK] Apply dopo commit
[OK] Get servita solo dal leader
[OK] Get su follower rifiutata con leader_hint
[OK] Scrittura con quorum disponibile riuscita
[OK] Scrittura senza quorum fallita
[OK] Delete replicata e applicata
[OK] Persistenza di commitIndex, lastApplied e data
[OK] Recovery robusto dopo restart
[OK] Fallback dall'ultimo record WAL valido
[OK] Snapshot locale minimale
[OK] Soglia snapshot configurabile via SNAPSHOT_THRESHOLD
[OK] go test ./... superato
```

---

## 28. Limiti attuali

La Fase 5 implementa la replicazione base su quorum e un primo hardening di recovery/snapshot, ma rimangono alcuni limiti:

- il WAL è ancora snapshot-based e non event-based;
- lo snapshot è locale e non viene ancora inviato ai follower;
- non esiste ancora log compaction reale;
- `InstallSnapshot` è ancora uno stub logico;
- `Get` è servita solo dal leader, senza ReadIndex;
- la gestione dei retry decrementa `nextIndex` di uno alla volta;
- non è ancora presente una suite automatica di crash/recovery;
- la replica non è ancora ottimizzata con batching esplicito.

---

## 29. Prossimi obiettivi

Le prossime attività consigliate sono:

```text
1. sviluppare il Client Proxy Service;
2. rafforzare il proxy per seguire leader_hint automaticamente;
3. aggiungere test automatici di crash leader e recovery;
4. migliorare il WAL con record granulari event-based;
5. implementare InstallSnapshot reale;
6. introdurre log compaction;
7. valutare ReadIndex per letture linearizzabili più efficienti.
```
