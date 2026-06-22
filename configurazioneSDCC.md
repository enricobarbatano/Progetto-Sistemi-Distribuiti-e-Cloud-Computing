# Configurazione ambiente progetto SDCC

Questo documento descrive il procedimento seguito per configurare l'ambiente di sviluppo del progetto di **Sistemi Distribuiti e Cloud Computing**.

Il progetto prevede la realizzazione di uno storage chiave-valore distribuito, replicato e fortemente consistente, implementato in **Go**, con comunicazione tramite **gRPC/Protocol Buffers**, containerizzazione tramite **Docker/Docker Compose** e deploy finale su **Amazon EC2**.

---

## 1. Cartella locale del progetto

La cartella locale utilizzata per il progetto è:

```text
C:\Users\User\OneDrive - Universita' degli Studi di Roma Tor Vergata\Desktop\Progetti_Uni\Progetto_SDCC
```

Poiché il percorso contiene spazi e apostrofi, nei comandi da terminale è consigliato racchiuderlo sempre tra virgolette:

```cmd
cd "C:\Users\User\OneDrive - Universita' degli Studi di Roma Tor Vergata\Desktop\Progetti_Uni\Progetto_SDCC"
```

---

## 2. Repository GitHub

È stato creato e collegato un repository GitHub con URL:

```text
https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
```

Il repository locale è stato sincronizzato con il repository remoto tramite Git.

### Verifica stato Git

```cmd
git status
```

Stato finale corretto:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

Durante la configurazione si è verificato un caso di divergenza tra branch locale e remoto, dovuto alla presenza di un commit remoto iniziale con README. La situazione è stata risolta tramite `git pull --rebase origin main`, successivo `git push` e rimozione manuale della cartella temporanea `.git\rebase-merge` quando Git era rimasto in stato di rebase.

Comando usato per ripulire lo stato temporaneo:

```cmd
rmdir /s /q .git\rebase-merge
```

---

## 3. Strumenti installati e verificati

Sono stati verificati i principali strumenti necessari per lo sviluppo.

### Go

Comando:

```cmd
go version
```

Output rilevato:

```text
go version go1.25.4 windows/amd64
```

Go sarà usato per implementare tutti i servizi principali:

- Consensus Node;
- Client Proxy Service;
- Snapshot & Backup Service;
- eventuale Benchmark Client.

---

### Git

Comando:

```cmd
git --version
```

Output rilevato:

```text
git version 2.51.1.windows.1
```

Git sarà usato per versionare il codice sorgente e sincronizzarlo con GitHub.

---

### Protocol Buffers Compiler

Comando:

```cmd
protoc --version
```

Output rilevato:

```text
libprotoc 33.0
```

`protoc` serve per compilare i file `.proto` e generare il codice Go necessario alla comunicazione gRPC.

---

### Plugin Go per Protobuf

Comando:

```cmd
protoc-gen-go --version
```

Output rilevato:

```text
protoc-gen-go v1.36.10
```

Questo plugin genera i file Go relativi ai messaggi Protobuf.

---

### Plugin Go per gRPC

Comando:

```cmd
protoc-gen-go-grpc --version
```

Output rilevato:

```text
protoc-gen-go-grpc 1.5.1
```

Questo plugin genera i file Go relativi ai servizi gRPC.

---

### WSL 2

Comando:

```cmd
wsl --version
```

Output rilevato:

```text
Versione WSL: 2.7.8.0
Versione kernel: 6.18.33.1-1
Versione WSLg: 1.0.73.2
Versione MSRDC: 1.2.6676
Versione Direct3D: 1.611.1-81528511
Versione DXCore: 10.0.26100.1-240331-1435.ge-release
Versione di Windows: 10.0.26200.8655
```

WSL 2 è utile perché Docker Desktop su Windows usa normalmente un backend Linux basato su WSL 2.

---

### Docker

Comando:

```cmd
docker --version
```

Output rilevato:

```text
Docker version 29.5.3, build d1c06ef
```

Docker servirà per containerizzare i servizi del progetto.

---

### Docker Compose

Comando:

```cmd
docker compose version
```

Output rilevato:

```text
Docker Compose version v5.1.4
```

Docker Compose servirà per orchestrare più container, ad esempio:

- `consensus-node-1`;
- `consensus-node-2`;
- `consensus-node-3`;
- `client-proxy`;
- `backup-service`.

---

### AWS CLI

Comando:

```cmd
aws --version
```

Output rilevato:

```text
aws-cli/2.35.9 Python/3.14.5 Windows/11 exe/AMD64
```

AWS CLI sarà usata più avanti per la fase di deploy su Amazon EC2.

Per ora non è necessario eseguire:

```cmd
aws configure
```

La configurazione delle credenziali AWS sarà fatta solo quando si passerà effettivamente alla fase cloud.

---

## 4. Inizializzazione modulo Go

Il modulo Go è stato inizializzato con il seguente comando corretto:

```cmd
go mod init github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing
```

Non bisogna usare l'URL completo con `https://` o il suffisso `.git`.

Esempio errato:

```cmd
go mod init https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
```

Questo produce errore perché `go mod init` richiede un module path in formato:

```text
github.com/utente/nome-repository
```

---

## 5. Contenuto attuale di `go.mod`

Il file `go.mod` contiene:

```go
module github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing

go 1.25.4

require (
	github.com/sony/gobreaker/v2 v2.4.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
```

Nota: le righe con hash del tipo:

```text
github.com/sony/gobreaker/v2 v2.4.0 h1:...
```

devono stare in `go.sum`, non in `go.mod`.

---

## 6. Dipendenze Go installate

Sono state installate le seguenti dipendenze principali:

```cmd
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go get github.com/sony/gobreaker/v2@latest
```

### Scopo delle dipendenze

#### `google.golang.org/grpc`

Serve per implementare la comunicazione RPC tra i servizi:

- nodi di consenso;
- proxy;
- servizio di backup.

#### `google.golang.org/protobuf`

Serve per usare Protocol Buffers come formato di definizione e serializzazione dei messaggi.

#### `github.com/sony/gobreaker/v2`

Serve per implementare il pattern **Circuit Breaker**, utile per evitare che il proxy o il servizio di backup continuino a inviare richieste verso nodi non disponibili o instabili.

---

## 7. Dipendenze da aggiungere più avanti

In una fase successiva potrà essere aggiunta la libreria:

```cmd
go get github.com/stretchr/testify@latest
```

Questa sarà utile per scrivere test unitari più leggibili.

Non è indispensabile aggiungerla subito, perché se non viene ancora usata nei file `.go`, un eventuale:

```cmd
go mod tidy
```

potrebbe rimuoverla automaticamente dal `go.mod`.

---

## 8. Struttura iniziale del progetto

È stata creata una struttura iniziale pensata per separare chiaramente i vari componenti.

Struttura prevista:

```text
Progetto_SDCC/
│
├── cmd/
│   ├── consensus-node/
│   │   └── main.go
│   ├── client-proxy/
│   │   └── main.go
│   ├── backup-service/
│   │   └── main.go
│   └── bench-client/
│       └── main.go
│
├── internal/
│   ├── consensus/
│   ├── storage/
│   ├── proxy/
│   ├── backup/
│   └── discovery/
│
├── proto/
│   ├── consensus.proto
│   ├── kv.proto
│   └── backup.proto
│
├── gen/
│   └── go/
│
├── deployments/
│
├── scripts/
│
├── reports/
│   └── results/
│
├── .gitignore
├── go.mod
└── go.sum
```

---

## 9. Comandi usati per creare le cartelle

In `cmd.exe` sono stati usati comandi di questo tipo:

```cmd
mkdir cmd
mkdir cmd\consensus-node
mkdir cmd\client-proxy
mkdir cmd\backup-service
mkdir cmd\bench-client

mkdir internal
mkdir internal\consensus
mkdir internal\storage
mkdir internal\proxy
mkdir internal\backup
mkdir internal\discovery

mkdir proto

mkdir gen
mkdir gen\go

mkdir deployments
mkdir scripts

mkdir reports
mkdir reports\results
```

---

## 10. Creazione di file vuoti da `cmd.exe`

Nel Prompt dei comandi di Windows non funziona il comando PowerShell `ni`.

Il comando:

```powershell
ni nome-file
```

è un alias PowerShell per `New-Item`, quindi in `cmd.exe` produce errore:

```text
"ni" non è riconosciuto come comando interno o esterno
```

In `cmd.exe` bisogna usare invece:

```cmd
type nul > cmd\consensus-node\main.go
type nul > cmd\client-proxy\main.go
type nul > cmd\backup-service\main.go
type nul > cmd\bench-client\main.go

type nul > proto\consensus.proto
type nul > proto\kv.proto
type nul > proto\backup.proto
```

---

## 11. File `main.go` vuoti

I file `.go` completamente vuoti non sono validi per il compilatore Go.

Ogni `main.go` iniziale dovrebbe contenere almeno:

```go
package main

func main() {
}
```

Questo permette a `go test ./...` di non fallire a causa di file Go vuoti.

---

## 12. Pattern architetturali scelti

Per mantenere il progetto fattibile ma coerente con i requisiti, la scelta consigliata è:

```text
Service Discovery custom + Circuit Breaker con gobreaker
```

### Service Discovery custom

Il proxy conoscerà una lista iniziale di nodi di consenso e li interrogherà tramite gRPC per scoprire quale nodo è il leader corrente.

In questo modo non è necessario introdurre subito strumenti esterni come Consul o etcd.

### Circuit Breaker

Il Circuit Breaker sarà usato nel Client Proxy o nel Backup Service per evitare chiamate ripetute verso nodi temporaneamente non disponibili.

La libreria scelta è:

```text
github.com/sony/gobreaker/v2
```

---

## 13. Prossimi passaggi tecnici

Dopo la configurazione dell'ambiente, i prossimi step saranno:

1. Scrivere i file `.proto`:
   - `proto/consensus.proto`;
   - `proto/kv.proto`;
   - `proto/backup.proto`.

2. Generare il codice Go dai file `.proto` tramite `protoc`.

3. Creare uno script:

```text
scripts/generate-proto.bat
```

4. Implementare un primo `consensus-node` minimale che espone alcune RPC gRPC.

5. Implementare un primo `client-proxy` minimale.

6. Creare i Dockerfile e il primo `docker-compose.yml` con tre nodi di consenso.

7. Implementare progressivamente:
   - elezione leader;
   - heartbeat;
   - log replication;
   - storage key-value;
   - snapshot;
   - benchmark;
   - test di tolleranza ai guasti.

---

## 14. Checklist ambiente finale

I seguenti comandi risultano funzionanti:

```cmd
go version
git --version
protoc --version
protoc-gen-go --version
protoc-gen-go-grpc --version
wsl --version
docker --version
docker compose version
aws --version
```

Output principali verificati:

```text
go version go1.25.4 windows/amd64
git version 2.51.1.windows.1
libprotoc 33.0
protoc-gen-go v1.36.10
protoc-gen-go-grpc 1.5.1
Docker version 29.5.3, build d1c06ef
Docker Compose version v5.1.4
aws-cli/2.35.9 Python/3.14.5 Windows/11 exe/AMD64
```

---

## 15. Comandi Git utili per il flusso di lavoro

Per controllare lo stato:

```cmd
git status
```

Per aggiungere modifiche:

```cmd
git add .
```

Per creare un commit:

```cmd
git commit -m "Messaggio del commit"
```

Per inviare su GitHub:

```cmd
git push
```

Per recuperare modifiche remote:

```cmd
git pull --rebase origin main
```

---

## 16. Nota su OneDrive

Il progetto si trova dentro una cartella sincronizzata con OneDrive.

Questo può andare bene, ma a volte OneDrive può bloccare file o cartelle, soprattutto durante operazioni Git come rebase, cancellazioni temporanee o modifiche alla cartella `.git`.

Se in futuro dovessero verificarsi blocchi simili, conviene:

1. chiudere VS Code;
2. chiudere Esplora file nella cartella del progetto;
3. attendere qualche secondo;
4. riprovare il comando Git;
5. eventualmente spostare il progetto in una cartella non sincronizzata, ad esempio:

```text
C:\Dev\Progetto_SDCC
```

---

## 17. Stato configurazione

La configurazione dell'ambiente di sviluppo è completata.

Restano da implementare i componenti applicativi del progetto.

Il prossimo passo operativo è la definizione dei file Protobuf per le API gRPC del sistema.
