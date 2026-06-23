# Fase 5 - Replicazione Atomica dei Log

Questo documento descrive la **Fase 5** del progetto, dedicata alla replicazione atomica dei log nel cluster di Consensus Node.

Questa fase rappresenta il passaggio dal semplice meccanismo di elezione del leader a un sistema di storage replicato con conferma su quorum. Dopo questa fase, una scrittura non viene più applicata localmente in modo immediato e isolato, ma viene prima inserita nel log del leader, persistita, replicata sui follower e considerata completata solo dopo il raggiungimento della maggioranza dei nodi.

L'obiettivo è avvicinare il comportamento del sistema a quello richiesto da Raft: un leader coordina le modifiche allo stato, i follower replicano il log, e la state machine key-value applica solo entry committed.

---

## 1. Obiettivo della fase

Prima della Fase 5, il cluster era già in grado di:

- eleggere un leader;
- mantenere heartbeat periodici;
- propagare il leader noto tramite `GetLeader`;
- persistere lo stato su `state.json` e WAL append-only;
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
```

### `internal/consensus/node.go`

Contiene la logica del Consensus Node.

In questa fase sono state aggiunte le funzioni necessarie per:

- selezionare le entry da replicare a partire da un certo indice;
- replicare il log verso un singolo peer;
- replicare una entry fino al raggiungimento del quorum;
- gestire le scritture `Put` e `Delete` in modo comune;
- forzare le letture `Get` solo sul leader.

### `cmd/bench-client/main.go`

È stato aggiornato per supportare test manuali più completi tramite variabili d'ambiente.

Ora supporta:

```text
OP=leader
OP=get
OP=put
OP=delete
OP=all
```

Questo ha permesso di testare direttamente scritture, letture, cancellazioni e redirect tramite `leader_hint`.

---

## 3. Modello di consistenza adottato

In questa fase il sistema adotta una forma base di consistenza forte:

- solo il leader accetta scritture;
- solo il leader serve letture;
- una scrittura viene confermata solo dopo replica su quorum;
- una entry viene applicata alla mappa key-value solo dopo essere committed;
- i follower rispondono alle richieste client con errore e `leader_hint`.

Questa scelta evita che un client legga da un follower che potrebbe non aver ancora ricevuto l'ultimo `commitIndex`.

In particolare, il comportamento scelto è:

```text
Put/Delete su leader   -> possibile successo dopo quorum
Put/Delete su follower -> rifiuto con leader_hint
Get su leader          -> lettura dalla state machine locale
Get su follower        -> rifiuto con leader_hint
```

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

Questa funzione scrive prima nel WAL append-only e poi aggiorna lo snapshot `state.json`.

---

## 5. Persistenza prima della replica

Una proprietà importante della fase è che il leader persiste la entry prima di iniziare la replica.

Il flusso è:

```text
1. crea LogEntry
2. append nel log locale
3. persist su WAL
4. aggiorna state.json
5. replica ai follower
```

Questo evita che il leader perda una entry già presa in carico in caso di crash immediato dopo l'append locale.

Il WAL resta ancora minimale: ogni record contiene uno snapshot dello stato persistente corrente. Tuttavia, il file è append-only e permette di tracciare l'evoluzione dello stato nel tempo.

---

## 6. Helper per la gestione del log

Per supportare la replicazione sono stati introdotti helper specifici.

### `entriesFromIndexLocked`

Restituisce tutte le entry del log a partire da un certo indice:

```go
func (n *ConsensusNode) entriesFromIndexLocked(index uint64) []*consensuspb.LogEntry
```

Viene usata dal leader per preparare la lista di entry da inviare a un follower a partire da `nextIndex[peer]`.

---

### `logTermAtIndexLocked`

Restituisce il termine della entry con un certo indice:

```go
func (n *ConsensusNode) logTermAtIndexLocked(index uint64) (uint64, bool)
```

Se l'indice è `0`, restituisce termine `0`, perché l'indice `0` rappresenta il punto prima dell'inizio del log.

---

### `hasLogEntryLocked`

Verifica se il follower possiede una entry con indice e termine specifici:

```go
func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool
```

Questa funzione implementa il controllo di coerenza di `AppendEntries`.

---

### `truncateLogFromIndexLocked`

Rimuove tutte le entry con indice maggiore o uguale a quello specificato:

```go
func (n *ConsensusNode) truncateLogFromIndexLocked(index uint64)
```

Serve per gestire conflitti tra il log locale del follower e il log del leader.

---

### `appendEntriesFromLeaderLocked`

Appende al follower le entry ricevute dal leader:

```go
func (n *ConsensusNode) appendEntriesFromLeaderLocked(entries []*consensuspb.LogEntry) bool
```

Se trova una entry locale con stesso indice ma termine diverso, tronca il log e appende le entry del leader da quel punto.

---

## 7. AppendEntries lato follower

La RPC `AppendEntries` è stata rafforzata.

Ora il follower non accetta più entry alla cieca, ma segue questo flusso:

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
10. resetta il timer di elezione.
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

Questo permette al leader di capire quando un follower è indietro o ha un log divergente.

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

- se `AppendEntries` ha successo, il leader aggiorna:

```go
n.matchIndex[peerID] = resp.MatchIndex
n.nextIndex[peerID] = resp.MatchIndex + 1
```

- se `AppendEntries` fallisce per inconsistenza del log, il leader decrementa:

```go
n.nextIndex[peerID]--
```

Questo permette al leader di cercare progressivamente il punto di incontro con il log del follower.

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

Questa funzione è chiamata in parallelo per i vari peer.

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

Poi invia la replica ai peer e aspetta risposte positive.

Con 3 nodi, la maggioranza è 2:

```text
leader + 1 follower = quorum
```

Se viene raggiunta la maggioranza, la funzione restituisce `true`.

Se non viene raggiunta entro timeout, restituisce `false`.

---

## 11. Gestione comune di Put e Delete

Per evitare duplicazione, `Put` e `Delete` usano una funzione comune:

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
8. invia heartbeat per propagare il nuovo `commitIndex`;
9. restituisce successo al client.

Se il nodo non è leader, restituisce:

```text
success=false
error=node is not leader
leader_hint=<leader address>
```

Se non viene raggiunto il quorum, restituisce:

```text
success=false
error=failed to replicate entry to quorum
leader_hint=<leader address>
```

---

## 12. Put

La RPC `Put` ora chiama:

```go
n.handleWriteOperation(ctx, consensuspb.LogOperation_LOG_OPERATION_PUT, req.Key, req.Value)
```

Il leader risponde `success=true` solo se la entry è stata replicata su quorum e applicata localmente.

Un follower rifiuta la richiesta e restituisce `leader_hint`.

---

## 13. Delete

La RPC `Delete` usa la stessa logica di `Put`, ma con operazione:

```go
consensuspb.LogOperation_LOG_OPERATION_DELETE
```

Anche `Delete` viene confermata solo dopo replica su quorum e apply locale.

---

## 14. Get solo dal leader

Per evitare letture stale dai follower, `Get` viene servita solo dal leader.

Se un follower riceve una `Get`, risponde:

```text
found=false
error=node is not leader
leader_hint=<leader address>
```

Se il leader riceve una `Get`, legge dalla propria state machine locale:

```go
value, ok := n.data[req.Key]
```

Questo approccio è più semplice di un meccanismo read-index, ma garantisce che le letture passino dal nodo che ha coordinato il commit.

---

## 15. Aggiornamento del bench-client

Il client di test `cmd/bench-client/main.go` è stato aggiornato per supportare:

```text
OP=leader
OP=get
OP=put
OP=delete
OP=all
```

Esempio `Put`:

```cmd
set TARGET=localhost:50052
set OP=put
set KEY=name
set VALUE=enrico
go run .\cmd\bench-client
```

Esempio `Get`:

```cmd
set TARGET=localhost:50052
set OP=get
set KEY=name
go run .\cmd\bench-client
```

Esempio `Delete`:

```cmd
set TARGET=localhost:50052
set OP=delete
set KEY=name
go run .\cmd\bench-client
```

---

## 16. Test: Put sul leader

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

Questo conferma che il leader ha accettato la scrittura, l'ha replicata su quorum, committata e applicata alla state machine.

---

## 17. Test: Get sul leader dopo Put

È stata poi eseguita una `Get` sul leader:

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

Questo conferma che la entry è stata applicata correttamente alla mappa key-value del leader.

---

## 18. Test: Get su follower

Una `Get` verso un follower è stata rifiutata:

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

Questo dimostra che i follower non servono letture potenzialmente stale.

---

## 19. Test: Put su follower

Una `Put` verso un follower è stata rifiutata:

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

Il follower restituisce correttamente l'indirizzo del leader noto.

---

## 20. Test: Put con un follower spento

Con un cluster a 3 nodi, è stato spento un follower. Il leader ha comunque accettato una scrittura perché leader + un follower raggiungibile formano ancora quorum.

Comando:

```cmd
set TARGET=localhost:50052
set OP=put
set KEY=country
set VALUE=italia
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=country value=italia error= leader_hint=
```

Verifica successiva:

```cmd
set TARGET=localhost:50052
set OP=get
set KEY=country
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=country value=italia error= leader_hint=localhost:50052
```

Questo conferma che il sistema resta disponibile alle scritture finché esiste una maggioranza.

---

## 21. Test: Put senza quorum

Quando il leader è rimasto senza maggioranza, una `Put` è stata rifiutata.

Comando:

```cmd
set TARGET=localhost:50052
set OP=put
set KEY=fail-test
set VALUE=no-quorum
go run .\cmd\bench-client
```

Output:

```text
Put response: success=false key=fail-test value=no-quorum error=failed to replicate entry to quorum leader_hint=localhost:50052
```

Questo è il comportamento corretto: il leader non conferma una scrittura se non riesce a replicarla su maggioranza.

---

## 22. Test: Delete sul leader

Dopo un reset pulito del cluster, il leader era:

```text
node-3 -> localhost:50053
```

Una `Put` verso un follower è stata rifiutata:

```text
Put response: success=false key=country value=italia error=node is not leader leader_hint=localhost:50053
```

La stessa `Put` verso il leader è riuscita:

```text
Put response: success=true key=country value=italia error= leader_hint=
```

La `Get` sul leader ha confermato il valore:

```text
Get response: found=true key=country value=italia error= leader_hint=localhost:50053
```

Poi è stata eseguita la `Delete`:

```cmd
set TARGET=localhost:50053
set OP=delete
set KEY=country
go run .\cmd\bench-client
```

Output:

```text
Delete response: success=true key=country error= leader_hint=
```

Infine, la `Get` sul leader ha confermato la cancellazione:

```text
Get response: found=false key=country value= error= leader_hint=localhost:50053
```

Questo valida anche il ciclo completo di cancellazione replicata.

---

## 23. Verifica tramite go test

Dopo le modifiche è stato eseguito:

```cmd
gofmt -w internal\consensus\node.go cmd\bench-client\main.go
go test ./...
```

Il comando ha compilato correttamente tutti i package.

---

## 24. Stato finale della Fase 5

La Fase 5 base può essere considerata completata.

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
[OK] go test ./... superato
```

---

## 25. Limiti attuali

La Fase 5 implementa la replicazione base su quorum, ma rimangono alcuni limiti:

- il WAL è ancora snapshot-based e non event-based;
- non esiste ancora replay completo del WAL in assenza di `state.json`;
- non è ancora implementata la compattazione del log;
- non sono ancora implementati snapshot reali;
- `Get` è servita solo dal leader, senza read-index ottimizzato;
- la gestione dei retry è semplice e decrementa `nextIndex` di uno alla volta;
- non è ancora presente una suite di test automatici per scenari di crash e recovery;
- la replica non è ancora ottimizzata con batching esplicito.

Questi punti potranno essere affrontati nelle fasi successive.

---

## 26. Prossimi obiettivi

Le prossime attività consigliate sono:

```text
1. aggiungere test automatici unit/integration per la replicazione;
2. migliorare il WAL con record granulari;
3. implementare replay WAL completo;
4. aggiungere snapshot e log compaction;
5. rafforzare il Client Proxy per seguire leader_hint automaticamente;
6. testare scenari di crash/restart con dati persistenti.
```
