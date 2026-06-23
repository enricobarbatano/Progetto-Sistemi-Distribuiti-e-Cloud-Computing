# 4 - Algoritmo di Elezione del Leader

Questo documento descrive la realizzazione della **Fase 4** del progetto: l'implementazione dell'algoritmo di elezione del leader nel cluster di consenso, più gli interventi di hardening infrastrutturale effettuati prima di passare alla Fase 5.

L'obiettivo principale della fase era trasformare i Consensus Node da semplici server gRPC stateful a nodi capaci di coordinarsi autonomamente per eleggere un leader, secondo una versione semplificata del protocollo Raft.

In più, prima di affrontare la replicazione atomica dei log, sono stati sistemati alcuni aspetti tecnici fondamentali:

- riuso delle connessioni gRPC verso i peer;
- gestione più pulita dei peer offline/online;
- riduzione dei log rumorosi sugli heartbeat falliti;
- introduzione di un WAL append-only minimale;
- verifica dell'inizializzazione dello stato volatile del leader.

---

## 1. Obiettivo della Fase 4

Prima di questa fase, ogni nodo era già in grado di:

- avviarsi come server gRPC;
- mantenere stato persistente e volatile;
- esporre le RPC definite nei file `.proto`;
- rispondere a chiamate minime come `GetLeader` e `Get`;
- salvare su disco `currentTerm`, `votedFor` e `log` tramite `state.json`.

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

- `internal/consensus/node.go` contiene la logica dell'elezione, degli heartbeat, delle connessioni persistenti, del tracking dei peer e della persistenza WAL;
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

Durante i test locali su Windows è stato scelto un intervallo più largo rispetto ai valori classici, perché l'avvio manuale dei processi e l'uso di `go run` possono introdurre latenza e jitter.

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

Per i test locali sono stati usati valori conservativi:

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

Dopo l'hardening, `Stop()` chiude anche le connessioni gRPC persistenti verso i peer:

```go
for peerID, conn := range n.peerConns {
    if err := conn.Close(); err != nil {
        log.Printf("node %s cannot close connection to peer %s: %v", n.id, peerID, err)
    }
}
```

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

L'inizializzazione delle mappe segue le regole previste da Raft:

```go
nextIndex := n.lastLogIndexLocked() + 1

for peerID := range n.peers {
    n.nextIndex[peerID] = nextIndex
    n.matchIndex[peerID] = 0
}
```

Quindi:

- `nextIndex[peer]` parte da `lastLogIndex + 1`;
- `matchIndex[peer]` parte da `0`.

Questa parte è fondamentale per la Fase 5, perché il leader userà questi valori per capire da quale entry iniziare a replicare verso ciascun follower.

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

## 14. Hardening gRPC: connessioni persistenti

Prima di passare alla replicazione dei log, è stata migliorata la gestione delle connessioni gRPC tra nodi.

Inizialmente, ogni `RequestVote` e ogni heartbeat potevano aprire una nuova connessione.

Questo approccio funziona per un prototipo, ma diventa inefficiente quando aumenta il numero di messaggi, soprattutto nella Fase 5, dove il leader dovrà inviare molte `AppendEntries`.

Sono quindi stati aggiunti alla struct `ConsensusNode` questi campi:

```go
peerConns   map[string]*grpc.ClientConn
peerClients map[string]consensuspb.ConsensusServiceClient
```

È stata poi introdotta la funzione:

```go
func (n *ConsensusNode) getPeerClientLocked(peerID string, peerAddress string) (consensuspb.ConsensusServiceClient, error)
```

Questa funzione:

1. verifica se esiste già un client gRPC verso il peer;
2. se esiste, lo riusa;
3. se non esiste, crea una nuova connessione;
4. salva connessione e client nelle mappe interne.

In questo modo le RPC successive verso lo stesso peer riutilizzano la stessa connessione.

---

## 15. Gestione peer online/offline

Per evitare log troppo rumorosi, è stata aggiunta una mappa:

```go
peerOnline map[string]bool
```

Questa mappa traccia lo stato di raggiungibilità di ciascun peer.

Sono state aggiunte due funzioni:

```go
func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error)
func (n *ConsensusNode) markPeerOnlineLocked(peerID string)
```

La logica è:

- se un peer passa da online a offline, viene stampato un log;
- se un peer resta offline, non vengono stampati log ripetuti;
- se un peer passa da offline a online, viene stampato un log.

Durante i test è stato osservato questo comportamento:

```text
node node-2 marked peer node-3 as offline: ...
node node-2 marked peer node-3 as online
```

Questo è molto più leggibile rispetto a stampare un errore per ogni heartbeat fallito.

---

## 16. Persistenza: introduzione del WAL append-only

Prima della Fase 5 è stata evoluta la persistenza.

La versione precedente usava soltanto:

```text
node-X_state.json
```

Questo file contiene uno snapshot compatto dello stato persistente corrente.

La nuova versione mantiene `state.json`, ma aggiunge anche:

```text
node-X_wal.log
```

Quindi per ogni nodo si hanno file come:

```text
data/node-1/node-1_state.json
data/node-1/node-1_wal.log
```

Il file WAL è append-only: ogni modifica dello stato persistente viene registrata come una nuova riga JSON.

---

## 17. Struttura del record WAL

È stata introdotta la struct:

```go
type walRecord struct {
    Timestamp   string                  `json:"timestamp"`
    NodeID      string                  `json:"node_id"`
    CurrentTerm uint64                  `json:"current_term"`
    VotedFor    string                  `json:"voted_for"`
    Log         []*consensuspb.LogEntry `json:"log"`
}
```

In questa prima versione, ogni record WAL contiene uno snapshot dello stato persistente corrente.

Questa scelta è semplice e compatibile con il recovery attuale, che continua a caricare rapidamente da `state.json`.

In futuro, il WAL potrà essere raffinato usando record più specifici, per esempio:

```json
{"type":"term_updated","term":4}
{"type":"vote_granted","term":4,"voted_for":"node-2"}
{"type":"log_appended","entry":{}}
```

---

## 18. Scrittura WAL e state.json

È stata aggiunta la funzione:

```go
func (n *ConsensusNode) appendWALLocked() error
```

Questa funzione:

1. crea un `walRecord`;
2. apre il file WAL con `os.O_APPEND`;
3. scrive una nuova riga JSON in fondo al file.

La funzione `persistLocked()` è stata modificata per seguire questo flusso:

```text
1. append su wal.log
2. aggiornamento atomico di state.json
```

Questo rispetta il principio del Write-Ahead Logging: prima si registra la modifica su un log append-only, poi si aggiorna la rappresentazione compatta dello stato.

---

## 19. Test del WAL

È stato eseguito un test locale con un singolo nodo.

Dopo l'avvio e l'elezione, sono stati verificati i file creati:

```cmd
dir data\node-1
```

Output:

```text
node-1_state.json
node-1_wal.log
```

Il contenuto di `node-1_state.json` era:

```json
{
  "current_term": 1,
  "voted_for": "node-1",
  "log": []
}
```

Il contenuto di `node-1_wal.log` era:

```json
{"timestamp":"2026-06-23T12:40:41.1887468Z","node_id":"node-1","current_term":0,"voted_for":"","log":[]}
{"timestamp":"2026-06-23T12:40:43.2259181Z","node_id":"node-1","current_term":1,"voted_for":"node-1","log":[]}
```

Questo dimostra che:

- il file WAL viene creato;
- i record vengono aggiunti in append;
- `state.json` continua a rappresentare lo stato persistente corrente;
- il nodo mantiene una traccia storica delle persistenze.

---

## 20. Test locale dell'elezione

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

## 21. Output significativo osservato

Durante il test è stato osservato un leader eletto:

```text
node node-3 became leader for term 1
```

Successivamente, il client gRPC minimale ha interrogato tutti i nodi.

Verso `node-1`:

```cmd
set TARGET=localhost:50051
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=localhost:50053 term=1
Get response: found=false value= error= leader_hint=localhost:50053
```

Verso `node-2`:

```cmd
set TARGET=localhost:50052
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=localhost:50053 term=1
Get response: found=false value= error= leader_hint=localhost:50053
```

Verso `node-3`:

```cmd
set TARGET=localhost:50053
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=localhost:50053 term=1
Get response: found=false value= error= leader_hint=localhost:50053
```

Questo risultato conferma che:

- `node-3` è stato eletto leader;
- `node-1` e `node-2` hanno ricevuto heartbeat validi;
- tutti i nodi conoscono correttamente il leader;
- `GetLeader` funziona anche interrogando follower.

---

## 22. Verifica tramite go test

Dopo le modifiche è stato eseguito:

```cmd
gofmt -w internal\consensus\node.go
go test ./...
```

Il comando `go test ./...` compila tutti i package e verifica che non ci siano errori di import, tipi o compilazione.

---

## 23. Stato finale della Fase 4 e hardening pre-Fase 5

La Fase 4 può essere considerata implementata nella sua versione base e rafforzata con interventi infrastrutturali.

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
[OK] Inizializzazione corretta di nextIndex e matchIndex
[OK] Invio heartbeat periodici
[OK] AppendEntries usata come heartbeat
[OK] Reset del timer alla ricezione di heartbeat
[OK] GetLeader restituisce il leader noto
[OK] Connessioni gRPC persistenti verso peer
[OK] Tracking peer online/offline
[OK] Log heartbeat falliti puliti
[OK] WAL append-only minimale
[OK] state.json ancora usato per recovery rapido
[OK] Test locale con leader eletto
[OK] Test locale con follower che riconoscono il leader
[OK] go test ./... superato
```

---

## 24. Limiti attuali

La Fase 4 non include ancora la replicazione atomica completa dei log.

In particolare mancano:

- gestione completa di `prevLogIndex` e `prevLogTerm`;
- risoluzione dei conflitti nel log;
- aggiornamento reale di `nextIndex` e `matchIndex` durante la replica;
- commit di una entry solo dopo replica su quorum;
- replay del WAL per recovery completo in assenza di `state.json`;
- record WAL granulari per evento;
- snapshot reali;
- compattazione del log.

Questi aspetti saranno affrontati nelle fasi successive.

---

## 25. Prossimo obiettivo

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
