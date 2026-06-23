# Implementazione della struttura base del Consensus Node

Questo documento descrive il lavoro svolto per completare l'**Obiettivo 3: implementazione della struttura base del Consensus Node stateful** del progetto di Sistemi Distribuiti e Cloud Computing.

L'obiettivo di questa fase non era ancora implementare completamente l'algoritmo Raft, ma costruire l'ossatura del nodo che in seguito gestirà:

- elezione del leader;
- heartbeat;
- replicazione dei log;
- applicazione dei comandi alla macchina a stati;
- persistenza dello stato;
- interazione con gli altri servizi tramite gRPC.

A conclusione di questa fase, il nodo è in grado di:

- essere inizializzato con un identificativo e una configurazione;
- mantenere lo stato Raft principale;
- esporre un server gRPC;
- registrarsi come implementazione dei servizi generati dai file `.proto`;
- ricevere chiamate RPC minime;
- salvare su disco lo stato persistente di base;
- essere verificato tramite un client gRPC minimale;
- essere controllato automaticamente tramite GitHub Actions.

---

## 1. Contesto della fase

Prima di questa fase erano già stati definiti i file `.proto` e generati gli stub Go con `protoc`.

I file generati si trovano nella cartella:

```text
gen/go/
```

con una struttura simile a:

```text
gen/
└── go/
    ├── consensuspb/
    │   ├── consensus.pb.go
    │   └── consensus_grpc.pb.go
    ├── kvpb/
    │   ├── kv.pb.go
    │   └── kv_grpc.pb.go
    └── backuppb/
        ├── backup.pb.go
        └── backup_grpc.pb.go
```

I file `.pb.go` contengono le struct Go corrispondenti ai messaggi Protobuf.

I file `_grpc.pb.go` contengono invece:

- le interfacce server da implementare;
- le funzioni di registrazione dei servizi;
- i client gRPC generati automaticamente.

Questi file non devono essere modificati manualmente. Ogni cambiamento alle interfacce deve essere fatto nei file `.proto`, seguito da una nuova generazione degli stub.

---

## 2. File coinvolti

Per questa fase sono stati introdotti o modificati principalmente questi file:

```text
internal/consensus/node.go
cmd/consensus-node/main.go
cmd/bench-client/main.go
.github/workflows/ci.yml
```

Il ruolo dei file è il seguente:

```text
internal/consensus/node.go
```

contiene la struttura interna del nodo di consenso, lo stato Raft, la persistenza e gli stub delle RPC.

```text
cmd/consensus-node/main.go
```

contiene il punto di ingresso dell'eseguibile `consensus-node` e avvia il server gRPC.

```text
cmd/bench-client/main.go
```

è stato temporaneamente usato come client gRPC minimale per verificare che il nodo riceva e gestisca chiamate RPC.

```text
.github/workflows/ci.yml
```

contiene la GitHub Action per eseguire controlli automatici a ogni push o pull request su `main`.

---

## 3. Implementazione della struct `ConsensusNode`

Il cuore dell'obiettivo 3 è la definizione della struct `ConsensusNode`.

Questa struct rappresenta un nodo stateful del cluster di consenso.

Nel file:

```text
internal/consensus/node.go
```

è stata definita una struttura simile a:

```go
type ConsensusNode struct {
    consensuspb.UnimplementedConsensusServiceServer
    kvpb.UnimplementedKeyValueServiceServer

    mu sync.Mutex

    id      string
    address string
    peers   map[string]string

    currentTerm uint64
    votedFor    string
    log         []*consensuspb.LogEntry

    commitIndex uint64
    lastApplied uint64
    role        consensuspb.NodeRole

    nextIndex  map[string]uint64
    matchIndex map[string]uint64

    data map[string]string

    stateFile string
}
```

Questa struttura è stata progettata per contenere tutti i principali elementi richiesti da Raft e dal progetto.

---

## 4. Implementazione delle interfacce gRPC

La struct `ConsensusNode` incorpora:

```go
consensuspb.UnimplementedConsensusServiceServer
kvpb.UnimplementedKeyValueServiceServer
```

Questo permette al nodo di essere registrato come implementazione dei servizi gRPC generati a partire dai file `.proto`.

In particolare, il nodo implementa gli stub delle seguenti RPC.

### RPC del servizio di consenso

```text
RequestVote
AppendEntries
InstallSnapshot
```

Queste RPC sono legate al protocollo Raft:

- `RequestVote` servirà per l'elezione del leader;
- `AppendEntries` servirà per heartbeat e replicazione dei log;
- `InstallSnapshot` servirà per inviare snapshot a follower troppo arretrati.

### RPC del servizio key-value

```text
Put
Get
Delete
GetLeader
```

Queste RPC sono legate all'interfaccia dello storage:

- `Put` inserisce o aggiorna una coppia chiave-valore;
- `Get` legge il valore associato a una chiave;
- `Delete` elimina una chiave;
- `GetLeader` permette al proxy o a un client di scoprire il leader corrente.

In questa fase le RPC sono implementate in modo minimale. L'obiettivo non è ancora ottenere una replicazione completa, ma avere un server funzionante e pronto per le fasi successive.

---

## 5. Stato Raft persistente

La struct del nodo mantiene lo stato persistente richiesto da Raft:

```go
currentTerm uint64
votedFor    string
log         []*consensuspb.LogEntry
```

### `currentTerm`

Rappresenta il termine più recente conosciuto dal nodo.

Viene inizializzato a `0` e aggiornato quando il nodo riceve messaggi con un termine maggiore.

### `votedFor`

Indica quale candidato ha ricevuto il voto del nodo nel termine corrente.

Serve a evitare che un nodo voti più candidati nello stesso termine.

### `log`

Contiene la sequenza delle operazioni da applicare alla macchina a stati.

Ogni entry è rappresentata da:

```go
*consensuspb.LogEntry
```

cioè dalla struct generata dal file `consensus.proto`.

---

## 6. Stato Raft volatile

Il nodo mantiene anche lo stato volatile:

```go
commitIndex uint64
lastApplied uint64
role        consensuspb.NodeRole
```

### `commitIndex`

Indica l'indice più alto del log noto come committed.

Una entry committed è una entry che può essere applicata alla macchina a stati.

### `lastApplied`

Indica l'indice più alto del log già applicato alla macchina a stati locale.

Il nodo applica entry finché:

```text
lastApplied < commitIndex
```

### `role`

Rappresenta il ruolo corrente del nodo:

```text
Follower
Candidate
Leader
```

In questa fase il nodo parte come `Follower`.

---

## 7. Stato volatile del leader

Sono state previste anche le strutture usate dal leader durante la replicazione:

```go
nextIndex  map[string]uint64
matchIndex map[string]uint64
```

### `nextIndex`

Per ogni follower, indica l'indice della prossima entry da inviare.

### `matchIndex`

Per ogni follower, indica l'indice più alto noto come replicato su quel follower.

In questa fase questi campi sono solo inizializzati. Verranno usati quando sarà implementata la replicazione vera dei log.

---

## 8. Macchina a stati key-value

Il nodo contiene una macchina a stati locale implementata con:

```go
data map[string]string
```

Questa mappa rappresenta lo storage chiave-valore effettivo.

Le entry committed del log vengono applicate alla mappa tramite la funzione:

```go
applyCommittedEntriesLocked()
```

Questa funzione scorre le entry non ancora applicate e gestisce operazioni come:

```text
PUT
DELETE
NOOP
```

Esempio logico:

```text
LogEntry PUT key=a value=1  -> data["a"] = "1"
LogEntry DELETE key=a       -> delete(data, "a")
LogEntry NOOP               -> nessuna modifica
```

---

## 9. Sincronizzazione con `sync.Mutex`

Il nodo usa:

```go
mu sync.Mutex
```

Il mutex è necessario perché gRPC può gestire più richieste in goroutine diverse.

Senza sincronizzazione, due RPC concorrenti potrebbero modificare contemporaneamente:

- `currentTerm`;
- `votedFor`;
- `log`;
- `commitIndex`;
- `lastApplied`;
- `role`;
- `data`.

Questo causerebbe race condition e stati inconsistenti.

Per questo motivo, ogni RPC acquisisce il lock all'inizio:

```go
n.mu.Lock()
defer n.mu.Unlock()
```

Questa scelta è semplice ma adatta alla fase iniziale del progetto.

---

## 10. Costruttore `NewConsensusNode`

È stato implementato il costruttore:

```go
func NewConsensusNode(id string, address string, peers map[string]string, dataDir string) (*ConsensusNode, error)
```

Il costruttore si occupa di:

1. verificare che l'identificativo del nodo non sia vuoto;
2. impostare una directory dati di default se non specificata;
3. creare la directory dati se non esiste;
4. inizializzare lo stato persistente;
5. inizializzare lo stato volatile;
6. inizializzare le mappe `nextIndex`, `matchIndex` e `data`;
7. costruire il percorso del file di stato;
8. caricare da disco eventuale stato persistente già presente.

Il nodo viene inizializzato come follower:

```go
role: consensuspb.NodeRole_NODE_ROLE_FOLLOWER
```

---

## 11. Persistenza dello stato

Per soddisfare il requisito di nodo stateful, è stata introdotta una persistenza minimale su file JSON.

Lo stato persistente è rappresentato da:

```go
type persistentState struct {
    CurrentTerm uint64                  `json:"current_term"`
    VotedFor    string                  `json:"voted_for"`
    Log         []*consensuspb.LogEntry `json:"log"`
}
```

Il file di stato viene salvato nel percorso:

```text
data/<node-id>/<node-id>_state.json
```

Ad esempio:

```text
data/node-1/node-1_state.json
```

Il file contiene dati simili a:

```json
{
  "current_term": 0,
  "voted_for": "",
  "log": []
}
```

---

## 12. Il problema WAL: cosa abbiamo fatto e cosa manca

Il requisito del progetto parla di nodi stateful e suggerisce un approccio di tipo **Write-Ahead Logging**, abbreviato spesso in WAL.

Un WAL vero e proprio è normalmente un file append-only in cui ogni modifica allo stato persistente viene scritta prima di essere considerata effettiva.

L'idea generale è:

```text
1. ricevo una modifica allo stato persistente
2. scrivo la modifica sul WAL
3. confermo la modifica in memoria
4. rispondo alla RPC
```

Questo approccio aiuta a recuperare lo stato dopo un crash, perché il nodo può rileggere il WAL e ricostruire l'ultima sequenza valida di aggiornamenti.

### Soluzione implementata in questa fase

In questa fase non è stato implementato un WAL append-only completo.

È stata invece implementata una persistenza atomica dello stato corrente tramite file JSON.

La funzione principale è:

```go
persistLocked()
```

Questa funzione:

1. crea una struct `persistentState` con `currentTerm`, `votedFor` e `log`;
2. serializza la struct in JSON;
3. scrive il contenuto in un file temporaneo;
4. sostituisce il file definitivo con `os.Rename`.

Il flusso è:

```text
stato in memoria
     ↓
serializzazione JSON
     ↓
scrittura file temporaneo
     ↓
rename sul file definitivo
```

### Perché questa scelta è accettabile ora

Questa soluzione è più semplice di un WAL completo, ma è sufficiente per l'Obiettivo 3 perché:

- rende il nodo stateful;
- salva su disco lo stato Raft persistente;
- permette al nodo di ricaricare `currentTerm`, `votedFor` e `log` al riavvio;
- mantiene il codice semplice in una fase in cui l'algoritmo Raft non è ancora completo.

### Limite della soluzione attuale

Il limite principale è che non abbiamo ancora uno storico append-only delle modifiche.

In caso di crash durante alcune condizioni particolari di scrittura, un WAL completo offrirebbe maggiori garanzie di recovery e audit della sequenza degli aggiornamenti.

Inoltre, con un WAL append-only sarebbe possibile distinguere chiaramente tra:

- log delle operazioni Raft;
- snapshot dello stato corrente;
- ricostruzione dello stato dopo crash.

### Evoluzione futura prevista

In una fase successiva la persistenza potrà essere evoluta in questo modo:

```text
wal.log              -> file append-only con ogni modifica persistente
state.json           -> snapshot periodico dello stato persistente
snapshot-*.bin/json  -> checkpoint della macchina a stati key-value
```

Il flusso futuro potrebbe diventare:

```text
AppendEntries ricevuta
     ↓
scrittura append-only su wal.log
     ↓
aggiornamento log in memoria
     ↓
risposta RPC
```

Questa evoluzione sarà più coerente con una implementazione robusta del requisito stateful.

---

## 13. Implementazione delle RPC principali

### `RequestVote`

La RPC `RequestVote` viene usata dai nodi candidate per richiedere voti.

In questa fase:

- se il termine della richiesta è maggiore del termine locale, il nodo aggiorna `currentTerm`;
- il nodo torna follower;
- il nodo azzera `votedFor`;
- se non ha già votato per un altro candidato, concede il voto;
- salva lo stato persistente su disco.

La logica completa di confronto tra log del candidato e log locale verrà aggiunta nella fase di elezione leader.

---

### `AppendEntries`

La RPC `AppendEntries` viene usata dal leader per:

- inviare heartbeat;
- replicare entry del log.

In questa fase:

- richieste con termine vecchio vengono rifiutate;
- richieste con termine più recente aggiornano il nodo locale;
- eventuali entry ricevute vengono accodate al log;
- `commitIndex` viene aggiornato in base a `leaderCommit`;
- le entry committed vengono applicate alla macchina a stati.

La gestione completa di `prevLogIndex`, `prevLogTerm` e dei conflitti nel log verrà aggiunta successivamente.

---

### `InstallSnapshot`

La RPC `InstallSnapshot` è stata predisposta per il futuro supporto agli snapshot.

In questa fase:

- rifiuta snapshot con termine vecchio;
- aggiorna il termine se riceve uno snapshot da un termine più recente;
- restituisce una risposta coerente.

Non installa ancora realmente i dati dello snapshot.

---

### `Put`

La RPC `Put` serve per inserire o aggiornare una chiave.

In questa fase:

- il nodo accetta la scrittura solo se il suo ruolo è `Leader`;
- crea una nuova `LogEntry` di tipo `PUT`;
- la aggiunge al log locale;
- la considera committed immediatamente;
- applica la modifica alla mappa `data`;
- salva lo stato persistente.

Questa è una semplificazione temporanea. Nella versione completa, la entry dovrà essere replicata su una maggioranza di nodi prima di essere committed.

---

### `Get`

La RPC `Get` legge dalla mappa locale:

```go
data map[string]string
```

Se la chiave esiste, restituisce:

```text
found = true
value = valore trovato
```

Se la chiave non esiste, restituisce:

```text
found = false
```

In futuro, per garantire forte consistenza, le letture dovranno passare dal leader o usare un meccanismo equivalente.

---

### `Delete`

La RPC `Delete` elimina una chiave.

In questa fase:

- viene accettata solo dal leader;
- crea una `LogEntry` di tipo `DELETE`;
- la aggiunge al log locale;
- la considera committed immediatamente;
- applica la cancellazione alla mappa `data`;
- salva lo stato persistente.

Anche qui manca ancora la replicazione su quorum.

---

### `GetLeader`

La RPC `GetLeader` serve per la discovery del leader.

In questa fase:

- se il nodo è leader, restituisce il proprio id e indirizzo;
- se non è leader, restituisce `has_leader = false`.

Quando verranno implementati heartbeat ed elezione, questa RPC potrà restituire informazioni più utili al Client Proxy.

---

## 14. Avvio del server gRPC

Il file:

```text
cmd/consensus-node/main.go
```

contiene il punto di ingresso del servizio.

Il server viene configurato leggendo variabili d'ambiente:

```text
NODE_ID
NODE_ADDRESS
PORT
DATA_DIR
PEERS
```

Esempio di avvio locale:

```cmd
set NODE_ID=node-1
set NODE_ADDRESS=localhost:50051
set PORT=50051
set DATA_DIR=data\node-1
go run .\cmd\consensus-node
```

Output ottenuto:

```text
2026/06/23 13:30:27 consensus node node-1 listening on port 50051
```

Questo conferma che il server gRPC è stato avviato correttamente.

---

## 15. Registrazione dei servizi gRPC

Nel `main.go` del consensus node vengono registrati due servizi:

```go
consensuspb.RegisterConsensusServiceServer(server, node)
kvpb.RegisterKeyValueServiceServer(server, node)
```

Questo significa che lo stesso nodo espone sia:

```text
ConsensusService
```

sia:

```text
KeyValueService
```

Questa scelta è coerente con il progetto perché il Consensus Node deve sia partecipare al protocollo di consenso sia ospitare la macchina a stati key-value.

---

## 16. Parsing dei peer

Il `main.go` contiene anche la funzione:

```go
parsePeers(raw string) map[string]string
```

Questa funzione legge una stringa nel formato:

```text
node-2=localhost:50052,node-3=localhost:50053
```

e la converte in una mappa:

```text
node-2 -> localhost:50052
node-3 -> localhost:50053
```

Questa mappa sarà usata nelle fasi successive per contattare gli altri nodi durante l'elezione del leader e la replicazione del log.

---

## 17. Client gRPC minimale per verifica runtime

Per verificare che il nodo non solo compili, ma riceva davvero chiamate gRPC, è stato usato `cmd/bench-client/main.go` come client minimale.

Il client si collega al target:

```text
localhost:50051
```

ed esegue due RPC:

```text
GetLeader
Get
```

Comando usato:

```cmd
set TARGET=localhost:50051
go run .\cmd\bench-client
```

Output ottenuto:

```text
2026/06/23 13:31:55 GetLeader response: has_leader=false leader_id= leader_address= term=0
2026/06/23 13:31:55 Get response: found=false value= error= leader_hint=
```

Questo output è corretto per la fase attuale.

`has_leader=false` è atteso perché non è ancora stata implementata l'elezione del leader.

`found=false` è atteso perché la chiave `test-key` non era presente nello storage locale.

Il risultato importante è che la chiamata client-server è riuscita.

---

## 18. Verifica tramite `go test`

È stato eseguito:

```cmd
go test ./...
```

Output rilevante:

```text
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/backup-service [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/bench-client [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/client-proxy [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/cmd/consensus-node [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb [no test files]
? github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/consensus [no test files]
```

La dicitura `[no test files]` non indica un errore. Significa solo che non esistono ancora file di test `*_test.go`.

Il comando è comunque utile perché compila tutti i package e verifica che non ci siano errori di import, tipi o dipendenze.

---

## 19. GitHub Actions e Continuous Integration

È stata aggiunta una GitHub Action per eseguire controlli automatici a ogni push o pull request su `main`.

Il workflow si trova in:

```text
.github/workflows/ci.yml
```

Il nome del workflow è:

```text
Go CI
```

Il workflow viene eseguito su:

```text
push su main
pull_request su main
```

La configurazione principale è:

```yaml
name: Go CI

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

permissions:
  contents: read

jobs:
  test:
    name: Build and test
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true

      - name: Download dependencies
        run: go mod download

      - name: Verify dependencies
        run: go mod verify

      - name: Check formatting
        run: |
          test -z "$(gofmt -l .)"

      - name: Run go vet
        run: go vet ./...

      - name: Run tests
        run: go test ./...
```

---

## 20. Controlli eseguiti dalla CI

### Checkout del repository

```yaml
uses: actions/checkout@v4
```

Scarica il codice del repository nel runner GitHub.

### Setup di Go

```yaml
uses: actions/setup-go@v5
with:
  go-version-file: go.mod
  cache: true
```

Configura Go usando la versione indicata nel file `go.mod` e abilita la cache dei moduli.

### Download delle dipendenze

```yaml
run: go mod download
```

Scarica le dipendenze dichiarate dal progetto.

### Verifica delle dipendenze

```yaml
run: go mod verify
```

Controlla che i moduli scaricati corrispondano a quanto registrato in `go.sum`.

### Controllo formattazione

```yaml
run: |
  test -z "$(gofmt -l .)"
```

Fallisce se esistono file Go non formattati con `gofmt`.

### Analisi statica

```yaml
run: go vet ./...
```

Esegue controlli statici sul codice Go.

### Test e compilazione

```yaml
run: go test ./...
```

Compila e testa tutti i package del progetto.

Anche in assenza di test unitari, questo controllo è utile perché intercetta errori di compilazione.

---

## 21. Risultato della GitHub Action

Dopo il push, la GitHub Action è stata eseguita correttamente.

Questo conferma che:

- il codice compila anche in ambiente Linux GitHub-hosted;
- le dipendenze sono risolte correttamente;
- la formattazione Go è corretta;
- `go vet` non segnala problemi bloccanti;
- `go test ./...` passa sull'intero repository.

Questo è importante perché il progetto userà Docker e deploy cloud, quindi è utile verificare già da ora che il codice non dipenda solo dall'ambiente Windows locale.

---

## 22. File runtime esclusi da Git

Durante l'avvio locale del nodo viene creata la cartella:

```text
data/
```

Questa cartella contiene stato runtime del nodo, ad esempio:

```text
data/node-1/node-1_state.json
```

Questi file non devono essere versionati nel repository perché rappresentano dati generati a runtime.

Per questo motivo è stato previsto di ignorare:

```gitignore
data/
```

nel file `.gitignore`.

---

## 23. Stato finale dell'Obiettivo 3

L'Obiettivo 3 può essere considerato completato perché sono stati soddisfatti i punti richiesti.

Checklist finale:

```text
[OK] Stub gRPC generati dai file .proto
[OK] Struct ConsensusNode creata
[OK] Interfacce gRPC implementate con stub funzionanti
[OK] Stato Raft persistente definito
[OK] Stato Raft volatile definito
[OK] Stato volatile del leader predisposto
[OK] Macchina a stati key-value locale definita
[OK] Mutex usato per proteggere lo stato condiviso
[OK] Persistenza base su file JSON implementata
[OK] Server gRPC configurato e avviabile
[OK] ConsensusService registrato sul server gRPC
[OK] KeyValueService registrato sul server gRPC
[OK] Nodo avviato correttamente su localhost:50051
[OK] Client gRPC minimale eseguito con successo
[OK] GitHub Action di Continuous Integration configurata
[OK] GitHub Action eseguita con successo
```

---

## 24. Limiti attuali

La fase 3 non implementa ancora completamente Raft.

In particolare mancano:

- timeout randomizzato dei follower;
- transizione automatica da follower a candidate;
- richiesta voti periodica;
- elezione del leader tramite maggioranza;
- heartbeat periodici del leader;
- reset del timeout alla ricezione di heartbeat;
- controllo completo di `prevLogIndex` e `prevLogTerm`;
- gestione dei conflitti nel log;
- replicazione su quorum;
- implementazione completa di `nextIndex` e `matchIndex`;
- WAL append-only completo;
- snapshot reali;
- compact log reale.

Questi punti saranno affrontati nelle fasi successive.

---

## 25. Prossimo obiettivo

Il prossimo obiettivo naturale è:

```text
Obiettivo 4: sviluppo dell'algoritmo di elezione del leader
```

In quella fase verranno aggiunti:

- timer di elezione;
- timeout randomizzato;
- transizione `Follower -> Candidate`;
- incremento di `currentTerm`;
- invio di `RequestVote` ai peer;
- conteggio dei voti ricevuti;
- transizione `Candidate -> Leader` al raggiungimento della maggioranza;
- heartbeat periodici tramite `AppendEntries`;
- aggiornamento e propagazione dell'informazione sul leader.

Solo dopo questa fase il metodo `GetLeader` potrà iniziare a restituire un leader reale.
