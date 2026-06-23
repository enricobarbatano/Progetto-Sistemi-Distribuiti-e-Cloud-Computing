# 4 - Algoritmo di Elezione del Leader

Questo documento descrive la realizzazione della **Fase 4** del progetto: l'implementazione dell'algoritmo di elezione del leader nel cluster di consenso.

L'obiettivo della fase era trasformare i Consensus Node da semplici server gRPC stateful a nodi capaci di coordinarsi autonomamente per eleggere un leader, secondo una versione semplificata del protocollo Raft.

---

## 1. Obiettivo della fase

Prima di questa fase, ogni nodo era in grado di:

- avviarsi come server gRPC;
- mantenere stato persistente e volatile;
- esporre le RPC definite nei file `.proto`;
- rispondere a chiamate minime come `GetLeader` e `Get`;
- salvare su disco `currentTerm`, `votedFor` e `log`.

Tuttavia, mancava ancora la capacità del cluster di eleggere autonomamente un leader.

La Fase 4 introduce quindi:

- timer di elezione randomizzati;
- transizione da `Follower` a `Candidate`;
- incremento del termine Raft;
- voto per sé stessi;
- invio parallelo di `RequestVote` agli altri peer;
- conteggio dei voti;
- transizione da `Candidate` a `Leader` al raggiungimento del quorum;
- invio periodico di heartbeat tramite `AppendEntries` vuote;
- aggiornamento del leader noto per supportare il Service Discovery tramite `GetLeader`.

---

## 2. File modificati

I file principali coinvolti sono:

```text
internal/consensus/node.go
cmd/consensus-node/main.go
cmd/bench-client/main.go
```

In particolare:

- `internal/consensus/node.go` contiene la logica dell'elezione e degli heartbeat;
- `cmd/consensus-node/main.go` avvia il nodo e chiama `Start()` per far partire i loop interni;
- `cmd/bench-client/main.go` viene usato come client gRPC minimale per verificare `GetLeader`.

---

## 3. Stato aggiunto al Consensus Node

Per implementare l'elezione sono stati aggiunti nuovi campi alla struct `ConsensusNode`.

```go
electionResetCh chan struct{}
stopCh          chan struct{}
stopOnce        sync.Once

heartbeatInterval time.Duration

leaderID      string
leaderAddress string
```

### `electionResetCh`

È un canale usato per notificare al loop di elezione che il timer deve essere resettato.

Viene usato quando:

- il nodo riceve un heartbeat valido dal leader;
- il nodo concede un voto a un candidato;
- il nodo diventa leader.

### `stopCh` e `stopOnce`

`stopCh` serve a fermare le goroutine interne del nodo.

`stopOnce` evita di chiudere `stopCh` più di una volta, evitando panic.

### `heartbeatInterval`

Indica ogni quanto tempo un leader invia heartbeat ai follower.

Durante i test locali su Windows è stato scelto un intervallo più largo rispetto ai valori classici, per evitare instabilità dovute a latenza locale, avvio manuale dei processi e uso di `go run`.

### `leaderID` e `leaderAddress`

Questi campi memorizzano il leader attualmente conosciuto dal nodo.

Sono fondamentali per la RPC `GetLeader`, perché permettono anche a un follower di rispondere indicando il leader corrente.

---

## 4. Timer di elezione randomizzato

È stata implementata la funzione:

```go
func randomElectionTimeout() time.Duration
```

Il suo compito è generare un timeout casuale per l'elezione.

L'uso di timeout randomizzati serve a ridurre la probabilità che più nodi diventino `Candidate` nello stesso momento, causando uno split vote.

Per i test locali sono stati usati valori più conservativi:

```go
minTimeout := 1500
maxTimeout := 3000
```

Quindi ogni nodo sceglie un timeout casuale tra 1.5 e 3 secondi.

Questa scelta rende il comportamento più stabile nei test manuali.

---

## 5. Avvio dei loop interni

Sono stati aggiunti due metodi:

```go
func (n *ConsensusNode) Start()
func (n *ConsensusNode) Stop()
```

`Start()` avvia due goroutine:

```go
go n.electionLoop()
go n.heartbeatLoop()
```

`Stop()` chiude il canale `stopCh` e permette ai loop interni di terminare.

Nel file `cmd/consensus-node/main.go`, dopo la creazione del nodo, viene chiamato:

```go
node.Start()
defer node.Stop()
```

In questo modo ogni nodo, appena avviato, inizia a gestire autonomamente timeout di elezione e heartbeat.

---

## 6. Loop di elezione

Il metodo:

```go
func (n *ConsensusNode) electionLoop()
```

gestisce il timer di elezione.

Il comportamento è:

1. crea un timer con durata casuale;
2. attende uno tra questi eventi:
   - stop del nodo;
   - reset del timer;
   - scadenza del timer;
3. se il timer scade e il nodo non è leader, viene avviata una nuova elezione tramite `startElection()`.

Quando arriva un heartbeat valido, il timer viene resettato e il follower non avvia una nuova elezione.

---

## 7. Avvio dell'elezione

Il metodo:

```go
func (n *ConsensusNode) startElection()
```

implementa la transizione a `Candidate`.

Quando parte una nuova elezione, il nodo:

1. acquisisce il lock;
2. verifica di non essere già leader;
3. imposta il ruolo a `Candidate`;
4. incrementa `currentTerm`;
5. vota per sé stesso impostando `votedFor = n.id`;
6. azzera le informazioni sul leader noto;
7. salva su disco lo stato persistente;
8. invia `RequestVote` in parallelo a tutti i peer.

Il voto per sé stesso conta come primo voto.

Con un cluster a 3 nodi, la maggioranza richiesta è 2.

---

## 8. Calcolo del quorum

È stato aggiunto il metodo:

```go
func (n *ConsensusNode) majority() int
```

Il cluster è composto dal nodo corrente più tutti i peer conosciuti.

```go
clusterSize := len(n.peers) + 1
return (clusterSize / 2) + 1
```

Esempi:

```text
3 nodi -> maggioranza 2
5 nodi -> maggioranza 3
1 nodo -> maggioranza 1
```

Questo consente al nodo candidato di sapere quanti voti servono per diventare leader.

---

## 9. Log freshness check in `RequestVote`

La RPC `RequestVote` è stata raffinata.

Prima era uno stub minimale, ora controlla:

- che il termine del candidato non sia più vecchio;
- che il nodo non abbia già votato per un altro candidato nello stesso termine;
- che il log del candidato sia aggiornato almeno quanto quello locale.

È stata aggiunta la funzione:

```go
func (n *ConsensusNode) isCandidateLogUpToDateLocked(candidateLastLogIndex uint64, candidateLastLogTerm uint64) bool
```

Il confronto segue la regola:

```text
1. se il termine dell'ultima entry del candidato è maggiore, il log è più aggiornato;
2. se il termine è uguale, si confronta l'indice dell'ultima entry;
3. il candidato deve avere indice almeno pari a quello locale.
```

Questa logica è importante per evitare che un nodo con log vecchio venga eletto leader.

---

## 10. Transizione a Leader

Quando il candidato ottiene la maggioranza, viene chiamato:

```go
func (n *ConsensusNode) becomeLeaderLocked()
```

Questa funzione:

- imposta il ruolo a `Leader`;
- imposta `leaderID` e `leaderAddress` con i dati del nodo corrente;
- inizializza `nextIndex` e `matchIndex` per ogni peer;
- resetta il timer di elezione;
- scrive un log informativo.

Esempio di log osservato:

```text
node node-1 became leader for term 3
```

---

## 11. Heartbeat tramite AppendEntries vuote

Il leader mantiene la propria autorità inviando heartbeat periodici.

Il loop è implementato da:

```go
func (n *ConsensusNode) heartbeatLoop()
```

Quando il nodo è leader, il loop chiama:

```go
func (n *ConsensusNode) sendHeartbeats()
```

Gli heartbeat sono semplici `AppendEntries` senza log entry:

```go
Entries: nil
```

In questa fase gli heartbeat non replicano ancora log. Servono solo a:

- comunicare ai follower che il leader è vivo;
- impedire ai follower di far scadere il timer di elezione;
- propagare `leaderID` e `leaderAddress`.

---

## 12. Aggiornamento di AppendEntries

La RPC `AppendEntries` è stata aggiornata per funzionare anche come heartbeat.

Quando un nodo riceve `AppendEntries` valida:

1. rifiuta la richiesta se il termine è vecchio;
2. aggiorna `currentTerm` se il termine ricevuto è maggiore;
3. torna follower;
4. aggiorna `leaderID`;
5. calcola `leaderAddress` dai peer conosciuti;
6. applica eventuali entry se presenti;
7. aggiorna `commitIndex` se necessario;
8. resetta il timer di elezione.

Il reset del timer è fondamentale: senza questo passaggio, i follower continuerebbero ad avviare elezioni anche con un leader attivo.

---

## 13. Aggiornamento di GetLeader

La RPC `GetLeader` è stata modificata per usare le informazioni sul leader noto.

Il comportamento ora è:

- se il nodo corrente è leader, restituisce sé stesso;
- se il nodo è follower ma conosce un leader, restituisce quel leader;
- se non conosce alcun leader, restituisce `has_leader=false`.

Questo supporta il pattern di Service Discovery richiesto dal progetto.

Il Client Proxy, in futuro, potrà interrogare un nodo qualsiasi e scoprire il leader corrente.

---

## 14. Test locale effettuato

Sono stati avviati più nodi localmente su porte diverse.

Esempio `node-1`:

```cmd
set NODE_ID=node-1
set NODE_ADDRESS=localhost:50051
set PORT=50051
set DATA_DIR=data\node-1
set PEERS=node-2=localhost:50052,node-3=localhost:50053
go run .\cmd\consensus-node
```

Esempio `node-2`:

```cmd
set NODE_ID=node-2
set NODE_ADDRESS=localhost:50052
set PORT=50052
set DATA_DIR=data\node-2
set PEERS=node-1=localhost:50051,node-3=localhost:50053
go run .\cmd\consensus-node
```

Esempio `node-3`:

```cmd
set NODE_ID=node-3
set NODE_ADDRESS=localhost:50053
set PORT=50053
set DATA_DIR=data\node-3
set PEERS=node-1=localhost:50051,node-2=localhost:50052
go run .\cmd\consensus-node
```

Durante il test è stato osservato che, anche con `node-3` spento accidentalmente, il cluster con `node-1` e `node-2` è riuscito a mantenere il quorum.

Questo è coerente con un cluster a 3 nodi, dove la maggioranza è 2.

---

## 15. Output significativo osservato

Durante il test è stato osservato un leader eletto:

```text
node node-1 became leader for term 3
```

Successivamente, il client gRPC minimale ha interrogato `node-1`:

```cmd
set TARGET=localhost:50051
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-1 leader_address=localhost:50051 term=3
Get response: found=false value= error= leader_hint=localhost:50051
```

Poi è stato interrogato `node-2`:

```cmd
set TARGET=localhost:50052
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-1 leader_address=localhost:50051 term=3
Get response: found=false value= error= leader_hint=localhost:50051
```

Questo risultato conferma che:

- `node-1` è stato eletto leader;
- `node-2` ha ricevuto heartbeat validi;
- `node-2` conosce correttamente il leader;
- `GetLeader` funziona anche interrogando un follower.

---

## 16. Caso node-3 offline

Durante un test, `node-3` è stato fermato accidentalmente con `CTRL+C`.

Di conseguenza, il client verso `localhost:50053` ha restituito:

```text
GetLeader failed: rpc error: code = Unavailable
```

Questo non indica un errore dell'algoritmo, ma semplicemente che il processo del nodo non era in esecuzione.

Anche il leader ha stampato errori di heartbeat verso `node-3`, perché continuava a tentare di raggiungerlo.

Questo comportamento è corretto: un leader prova comunque a comunicare con tutti i peer conosciuti.

---

## 17. Pulizia dei log degli heartbeat falliti

Durante i test iniziali, i log risultavano troppo rumorosi quando un peer era offline.

Il problema era causato dal fatto che ogni heartbeat fallito veniva loggato.

Esempio:

```text
leader node-1 heartbeat to node-3 failed: ...
```

Poiché il leader invia heartbeat periodici, un peer spento generava molti messaggi ripetuti.

Per migliorare la leggibilità dei test locali, sono state previste due azioni:

1. aumentare gli intervalli temporali;
2. ridurre o controllare la verbosità dei log sugli heartbeat falliti.

Gli intervalli usati sono stati resi più conservativi:

```go
heartbeatInterval: 300 * time.Millisecond
```

```go
minTimeout := 1500
maxTimeout := 3000
```

Questa scelta rende l'elezione più stabile e riduce la frequenza degli heartbeat falliti.

Una possibile evoluzione ulteriore sarà loggare il fallimento verso un peer solo al cambio di stato, ad esempio quando un peer passa da raggiungibile a non raggiungibile.

---

## 18. Verifica tramite go test

Dopo le modifiche è stato eseguito:

```cmd
gofmt -w internal\consensus\node.go cmd\consensus-node\main.go
go test ./...
```

Il comando `go test ./...` compila tutti i package e verifica che non ci siano errori di import, tipi o compilazione.

---

## 19. Stato finale della Fase 4

La Fase 4 può essere considerata implementata nella sua versione base.

Checklist:

```text
[OK] Timer di elezione randomizzato
[OK] Transizione Follower -> Candidate
[OK] Incremento currentTerm
[OK] Voto per sé stesso
[OK] Invio RequestVote ai peer
[OK] Controllo termine in RequestVote
[OK] Controllo freschezza del log in RequestVote
[OK] Calcolo della maggioranza
[OK] Transizione Candidate -> Leader
[OK] Invio heartbeat periodici
[OK] AppendEntries usata come heartbeat
[OK] Reset del timer alla ricezione di heartbeat
[OK] GetLeader restituisce il leader noto
[OK] Test locale con leader eletto
[OK] Test locale con follower che riconosce il leader
```

---

## 20. Limiti attuali

La Fase 4 non include ancora la replicazione atomica completa dei log.

In particolare mancano:

- gestione completa di `prevLogIndex` e `prevLogTerm`;
- risoluzione dei conflitti nel log;
- aggiornamento reale di `nextIndex` e `matchIndex` durante la replica;
- commit di una entry solo dopo replica su quorum;
- gestione avanzata di peer offline;
- riuso persistente delle connessioni gRPC;
- WAL append-only completo.

Questi aspetti saranno affrontati nella fase successiva.

---

## 21. Prossimo obiettivo

Il prossimo obiettivo è:

```text
Fase 5: Replicazione Atomica dei Log
```

In quella fase il leader dovrà:

- ricevere richieste `Put` e `Delete`;
- inserirle nel proprio log;
- replicarle sui follower tramite `AppendEntries`;
- attendere la maggioranza;
- marcare le entry come committed;
- applicarle alla macchina a stati;
- rispondere al client solo dopo il commit.

Solo dopo questa fase il key-value store sarà realmente replicato e fortemente consistente.
