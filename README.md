# Progetto Sistemi Distribuiti e Cloud Computing

## Descrizione

Questo progetto implementa uno **storage chiave-valore distribuito, replicato e fortemente consistente**, sviluppato in **Go**.

Il sistema è pensato come un cluster distribuito composto da nodi di consenso stateful e servizi stateless di supporto. L'obiettivo è garantire consistenza forte, resilienza ai guasti, replica dei dati e gestione controllata delle richieste client tramite un proxy dedicato.

Il progetto è attualmente in fase di sviluppo e include componenti per comunicazione gRPC, serializzazione tramite Protocol Buffers, containerizzazione con Docker e deployment su ambiente cloud AWS EC2.

---

## Componenti principali

### Consensus Node

Nodo stateful che partecipa al protocollo di consenso e mantiene lo stato replicato del sistema.

I Consensus Node sono responsabili di:

- gestire le operazioni sullo storage chiave-valore;
- partecipare al protocollo di consenso;
- replicare lo stato tra i nodi del cluster;
- distinguere tra nodo leader e nodi follower;
- garantire consistenza forte sulle operazioni confermate.

### Client Proxy Service

Servizio stateless che espone l'interfaccia verso i client e inoltra le richieste al leader del cluster.

Il proxy ha il compito di:

- ricevere richieste client;
- individuare il leader corrente;
- inoltrare le operazioni al nodo corretto;
- gestire errori e nodi non raggiungibili;
- applicare pattern di resilienza come Circuit Breaker.

### Snapshot & Backup Service

Servizio stateless dedicato alla gestione di snapshot, backup e compattazione dei log.

Questo componente supporta:

- creazione di snapshot periodici;
- riduzione della dimensione dei log;
- backup dello stato del cluster;
- ripristino e mantenimento della disponibilità del sistema.

---

## Tecnologie utilizzate

- Go
- gRPC
- Protocol Buffers
- Docker
- Docker Compose
- AWS EC2
- Circuit Breaker
- Service Discovery
- Healthcheck containerizzati

---

## Gestione delle dipendenze

Il progetto è sviluppato in **Go**, quindi le dipendenze applicative non vengono gestite tramite `requirements.txt`, che è una convenzione tipica dei progetti Python.

Il punto centrale per la gestione delle librerie Go è il file:

```text
go.mod
```

Il file `go.mod` contiene le dipendenze necessarie alla compilazione del codice, mentre `go.sum` conserva gli hash delle versioni scaricate per garantire build riproducibili.

Per aggiornare e pulire le dipendenze del modulo Go, eseguire:

```bash
go mod tidy
```

Per scaricare le dipendenze dichiarate:

```bash
go mod download
```

Nel repository può essere presente anche un file `requirements.txt`, ma solo come file documentale per riepilogare i requisiti di ambiente e toolchain. Non deve essere installato con `pip`.

---

## Dipendenze Go principali

Le principali librerie utilizzate dal progetto sono:

```text
google.golang.org/grpc
google.golang.org/protobuf
github.com/sony/gobreaker/v2
github.com/stretchr/testify
```

### Descrizione

- `google.golang.org/grpc`  
  Utilizzata per implementare la comunicazione RPC tra Consensus Node, Client Proxy Service e Backup Service.

- `google.golang.org/protobuf`  
  Utilizzata per la serializzazione dei messaggi definiti nei file `.proto`.

- `github.com/sony/gobreaker/v2`  
  Utilizzata per implementare il pattern Circuit Breaker nei servizi stateless, migliorando la resilienza in presenza di nodi isolati o non raggiungibili.

- `github.com/stretchr/testify`  
  Libreria consigliata per test unitari e di integrazione più leggibili e robusti.

---

## Requisiti di ambiente

Per eseguire e sviluppare il progetto sono necessari:

- Go installato;
- Docker;
- Docker Compose;
- Protocol Buffer Compiler, `protoc`;
- plugin Go per Protocol Buffers:
  - `protoc-gen-go`;
  - `protoc-gen-go-grpc`;
- Buf, opzionale, per gestione deterministica della toolchain Protobuf;
- Docker Buildx, consigliato in versione `0.35.0` o superiore per il deployment su EC2;
- `wget` all'interno del container del Client Proxy, se usato negli healthcheck Docker Compose.

---

## Installazione dipendenze Go

Dalla root del progetto:

```bash
go mod tidy
```

Poi:

```bash
go mod download
```

Per verificare che il progetto compili:

```bash
go build ./...
```

Per eseguire i test:

```bash
go test ./...
```

---

## Generazione codice da Protocol Buffers

Se il progetto contiene file `.proto`, è necessario generare gli stub Go tramite i plugin Protobuf.

Installare i plugin:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Assicurarsi che la cartella dei binari Go sia nel `PATH`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

Su Windows, aggiungere al PATH la cartella:

```text
%USERPROFILE%\goin
```

Esempio di generazione manuale:

```bash
protoc --go_out=. --go-grpc_out=. proto/*.proto
```

Se nel progetto viene usato Buf, la generazione può essere gestita con:

```bash
buf generate
```

---

## Avvio locale

Il progetto può essere avviato localmente tramite Docker Compose.

Dalla root del progetto:

```bash
docker compose up --build
```

Per avviare i container in background:

```bash
docker compose up --build -d
```

Per visualizzare i log:

```bash
docker compose logs -f
```

Per arrestare il cluster:

```bash
docker compose down
```

---

## Healthcheck

Il Client Proxy espone un endpoint di healthcheck, ad esempio:

```text
/health
```

Questo endpoint può essere usato da Docker Compose per verificare che il servizio sia attivo e pronto a ricevere richieste.

Se il container utilizza `wget` per l'healthcheck, assicurarsi che il tool sia installato nell'immagine Docker del servizio interessato.

---

## Deployment su AWS EC2

Il progetto è pensato anche per deployment su istanza AWS EC2.

Sull'istanza EC2 devono essere disponibili:

- Docker;
- Docker Compose;
- Docker Buildx;
- accesso alla repository del progetto;
- eventuali variabili d'ambiente necessarie alla configurazione del cluster.

Comando tipico di avvio:

```bash
docker compose up --build -d
```

Durante il deployment è importante verificare la versione di Docker Buildx, poiché versioni troppo vecchie possono causare problemi con `docker compose up --build`.

---

## Pattern architetturali

### Service Discovery

Il sistema utilizza meccanismi di individuazione dei nodi per permettere al proxy e ai servizi di supporto di comunicare con i Consensus Node disponibili.

### Circuit Breaker

Il pattern Circuit Breaker viene utilizzato per evitare chiamate ripetute verso nodi non disponibili o isolati, migliorando la resilienza complessiva del sistema.

### Replica e consistenza forte

Il cluster replica lo stato tra più nodi e conferma le operazioni solo quando vengono soddisfatte le condizioni previste dal protocollo di consenso.

### Snapshot e compattazione

Il Backup Service supporta snapshot e compattazione dei log per ridurre la crescita dello stato persistente e facilitare il ripristino.

---

## Struttura prevista del progetto

La struttura può variare durante lo sviluppo, ma una possibile organizzazione è:

```text
.
├── cmd/
│   ├── consensus-node/
│   ├── client-proxy/
│   └── backup-service/
├── internal/
│   ├── consensus/
│   ├── storage/
│   ├── proxy/
│   └── backup/
├── proto/
├── Dockerfile
├── docker-compose.yml
├── go.mod
├── go.sum
├── requirements.txt
└── README.md
```

---

## Stato del progetto

Il progetto è attualmente in fase di sviluppo.

Le funzionalità implementate e la struttura del repository possono evolvere nelle prossime fasi, in particolare per quanto riguarda:

- protocollo di consenso;
- gestione del leader;
- snapshot e backup;
- test distribuiti;
- configurazione Docker Compose;
- deployment su EC2.

---

## Note importanti

- Non usare `pip install -r requirements.txt`: il progetto non è Python.
- Le dipendenze Go sono gestite da `go.mod`.
- Il file `requirements.txt`, se presente, serve solo come riepilogo documentale dei requisiti di ambiente.
- Prima di eseguire build o test, usare `go mod tidy` per sincronizzare le dipendenze.

---

## Autore

**Enrico Barbatano**
