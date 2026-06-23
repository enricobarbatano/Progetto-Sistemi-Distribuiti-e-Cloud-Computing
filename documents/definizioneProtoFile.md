# Definizione dei file `.proto` per il progetto SDCC

Questo documento descrive come sono state definite le interfacce gRPC del progetto di **Sistemi Distribuiti e Cloud Computing** e spiega il ruolo dei file `.proto` nella progettazione del sistema.

L'obiettivo di questa fase è definire il **contratto di comunicazione** tra i diversi componenti del sistema distribuito:

- **Consensus Node**, cioè i nodi stateful che partecipano al protocollo di consenso;
- **Client Proxy Service**, cioè il servizio stateless che riceve le richieste esterne;
- **Snapshot & Backup Service**, cioè il servizio stateless che gestisce snapshot e compattazione dei log.

I file `.proto` sono stati collocati nella cartella:

```text
proto/
```

I file previsti sono:

```text
proto/consensus.proto
proto/kv.proto
proto/backup.proto
```

---

## 1. Perché usare file `.proto`

I file `.proto` vengono usati come **Interface Definition Language**, cioè come linguaggio per descrivere in modo formale le interfacce tra servizi.

Nel progetto servono a definire:

- quali servizi gRPC esistono;
- quali metodi RPC espone ogni servizio;
- quali messaggi vengono inviati in richiesta;
- quali messaggi vengono restituiti in risposta;
- quali tipi di dato vengono usati nella comunicazione.

Questa scelta è importante perché il sistema è distribuito: i componenti non devono comunicare tramite chiamate locali, ma tramite messaggi inviati in rete.

In particolare, l'uso di gRPC e Protocol Buffers permette di avere:

- comunicazione fortemente tipizzata;
- generazione automatica del codice Go;
- separazione tra definizione dell'interfaccia e implementazione;
- maggiore chiarezza nella struttura delle RPC;
- maggiore controllo sulle interazioni tra nodi.

---

## 2. Organizzazione generale delle interfacce

Le interfacce sono state divise in tre file distinti per separare le responsabilità del sistema.

```text
consensus.proto -> RPC interne per il consenso distribuito
kv.proto        -> RPC per operazioni key-value e leader discovery
backup.proto    -> RPC per snapshot e backup
```

Questa separazione evita di avere un unico file `.proto` troppo grande e rende più chiaro il ruolo di ogni servizio.

---

## 3. File `consensus.proto`

Il file `consensus.proto` definisce le RPC usate dai **Consensus Node** per implementare le basi del protocollo Raft.

Questo file riguarda quindi la comunicazione interna al cluster di consenso.

Il servizio principale definito è:

```proto
service ConsensusService {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
}
```

---

### 3.1 `RequestVote`

La RPC `RequestVote` viene usata durante la fase di elezione del leader.

Quando un nodo non riceve heartbeat dal leader entro un certo timeout, passa allo stato di **Candidate** e chiede voti agli altri nodi del cluster.

La richiesta contiene:

```proto
message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}
```

I campi hanno questo significato:

- `term`: termine corrente del candidato;
- `candidate_id`: identificativo del nodo che richiede il voto;
- `last_log_index`: indice dell'ultima entry nel log del candidato;
- `last_log_term`: termine dell'ultima entry nel log del candidato.

La risposta contiene:

```proto
message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}
```

I campi hanno questo significato:

- `term`: termine corrente del nodo che risponde;
- `vote_granted`: indica se il voto è stato concesso.

Questa RPC è necessaria perché il sistema deve essere in grado di eleggere un leader senza dipendere da un coordinatore centralizzato.

---

### 3.2 `AppendEntries`

La RPC `AppendEntries` viene usata dal leader per due scopi:

1. inviare heartbeat periodici ai follower;
2. replicare nuove entry del log.

La richiesta contiene:

```proto
message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}
```

I campi principali sono:

- `term`: termine corrente del leader;
- `leader_id`: identificativo del leader;
- `prev_log_index`: indice della entry precedente a quelle da replicare;
- `prev_log_term`: termine della entry precedente;
- `entries`: lista di log entry da replicare;
- `leader_commit`: indice massimo che il leader considera committed.

La risposta contiene:

```proto
message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 match_index = 3;
}
```

I campi hanno questo significato:

- `term`: termine corrente del follower;
- `success`: indica se la replica è riuscita;
- `match_index`: indice dell'ultima entry replicata correttamente.

Questa RPC è centrale per garantire la forte consistenza: il leader confermerà una scrittura solo dopo che sarà stata replicata su una maggioranza di nodi.

---

### 3.3 `InstallSnapshot`

La RPC `InstallSnapshot` viene usata quando un follower è troppo indietro rispetto al leader e non possiede più alcune entry del log, perché sono state già compattate in uno snapshot.

La richiesta contiene:

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

I campi hanno questo significato:

- `term`: termine corrente del leader;
- `leader_id`: identificativo del leader;
- `last_included_index`: ultimo indice incluso nello snapshot;
- `last_included_term`: termine dell'ultimo indice incluso nello snapshot;
- `offset`: posizione del blocco di dati nello snapshot;
- `data`: dati raw dello snapshot;
- `done`: indica se questo è l'ultimo blocco dello snapshot.

La risposta contiene:

```proto
message InstallSnapshotResponse {
  uint64 term = 1;
  bool success = 2;
}
```

Questa RPC non è strettamente necessaria per una primissima versione minimale, ma è stata prevista perché il progetto richiede anche log compaction, snapshot e backup.

---

### 3.4 `LogEntry`

Nel file `consensus.proto` è stata definita anche la struttura del log replicato:

```proto
message LogEntry {
  uint64 index = 1;
  uint64 term = 2;
  LogOperation operation = 3;
  string key = 4;
  string value = 5;
}
```

Una `LogEntry` rappresenta un comando da applicare allo storage chiave-valore.

I campi indicano:

- `index`: posizione della entry nel log;
- `term`: termine in cui la entry è stata creata;
- `operation`: tipo di operazione;
- `key`: chiave interessata;
- `value`: valore associato, usato soprattutto per le operazioni `PUT`.

È stato definito anche l'enum:

```proto
enum LogOperation {
  LOG_OPERATION_UNSPECIFIED = 0;
  LOG_OPERATION_PUT = 1;
  LOG_OPERATION_DELETE = 2;
  LOG_OPERATION_NOOP = 3;
}
```

Questo permette di distinguere il tipo di comando contenuto nella entry.

---

## 4. File `kv.proto`

Il file `kv.proto` definisce le RPC legate allo storage chiave-valore.

Questo file rappresenta l'interfaccia usata dal **Client Proxy Service** per inoltrare operazioni al cluster.

Il servizio principale è:

```proto
service KeyValueService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc GetLeader(GetLeaderRequest) returns (GetLeaderResponse);
}
```

---

### 4.1 `Put`

La RPC `Put` serve per memorizzare o aggiornare una coppia chiave-valore.

Richiesta:

```proto
message PutRequest {
  string key = 1;
  string value = 2;
  string request_id = 3;
}
```

Risposta:

```proto
message PutResponse {
  bool success = 1;
  string error = 2;
  string leader_hint = 3;
}
```

La scrittura dovrebbe essere accettata solo dal leader. Se un follower riceve una richiesta `Put`, può restituire `success = false` e indicare il leader noto tramite `leader_hint`.

Il campo `request_id` è utile per tracciare la richiesta e, in una versione più evoluta, per evitare duplicazioni.

---

### 4.2 `Get`

La RPC `Get` serve per recuperare il valore associato a una chiave.

Richiesta:

```proto
message GetRequest {
  string key = 1;
}
```

Risposta:

```proto
message GetResponse {
  bool found = 1;
  string value = 2;
  string error = 3;
  string leader_hint = 4;
}
```

Il campo `found` indica se la chiave è presente nello storage.

Per mantenere una semantica fortemente consistente, nella prima versione si può scegliere di far passare anche le letture dal leader. In questo modo il proxy evita di leggere dati potenzialmente non aggiornati da un follower.

---

### 4.3 `Delete`

La RPC `Delete` serve per eliminare una chiave dallo storage.

Richiesta:

```proto
message DeleteRequest {
  string key = 1;
  string request_id = 2;
}
```

Risposta:

```proto
message DeleteResponse {
  bool success = 1;
  string error = 2;
  string leader_hint = 3;
}
```

Anche questa operazione modifica lo stato del sistema, quindi deve essere gestita dal leader e replicata tramite il log.

---

### 4.4 `GetLeader`

La RPC `GetLeader` serve per il pattern di **Service Discovery**.

Richiesta:

```proto
message GetLeaderRequest {
}
```

Risposta:

```proto
message GetLeaderResponse {
  bool has_leader = 1;
  string leader_id = 2;
  string leader_address = 3;
  uint64 term = 4;
}
```

Il proxy può interrogare uno o più nodi del cluster per scoprire quale nodo è il leader corrente.

Questa scelta evita di configurare staticamente il leader e rende il sistema più flessibile in caso di crash e rielezione.

---

## 5. File `backup.proto`

Il file `backup.proto` definisce le RPC del servizio di snapshot e backup.

Il servizio principale è:

```proto
service BackupService {
  rpc TriggerSnapshot(TriggerSnapshotRequest) returns (TriggerSnapshotResponse);
  rpc DownloadSnapshot(DownloadSnapshotRequest) returns (DownloadSnapshotResponse);
  rpc CompactLog(CompactLogRequest) returns (CompactLogResponse);
  rpc GetBackupStatus(GetBackupStatusRequest) returns (GetBackupStatusResponse);
}
```

---

### 5.1 `TriggerSnapshot`

La RPC `TriggerSnapshot` serve per avviare la creazione di uno snapshot.

Richiesta:

```proto
message TriggerSnapshotRequest {
  string requester_id = 1;
}
```

Risposta:

```proto
message TriggerSnapshotResponse {
  bool accepted = 1;
  string snapshot_id = 2;
  string error = 3;
}
```

Il campo `accepted` indica che la richiesta è stata accettata. Lo snapshot può essere creato in modo asincrono.

Questa scelta è coerente con il fatto che il Backup Service deve operare senza bloccare direttamente il normale funzionamento del cluster.

---

### 5.2 `DownloadSnapshot`

La RPC `DownloadSnapshot` serve per scaricare uno snapshot già creato.

Richiesta:

```proto
message DownloadSnapshotRequest {
  string snapshot_id = 1;
}
```

Risposta:

```proto
message DownloadSnapshotResponse {
  bool success = 1;
  string snapshot_id = 2;
  bytes snapshot_data = 3;
  uint64 last_included_index = 4;
  uint64 last_included_term = 5;
  string error = 6;
}
```

I campi `last_included_index` e `last_included_term` collegano lo snapshot allo stato del log Raft.

---

### 5.3 `CompactLog`

La RPC `CompactLog` serve per chiedere la compattazione del log fino a un certo indice.

Richiesta:

```proto
message CompactLogRequest {
  string snapshot_id = 1;
  uint64 up_to_index = 2;
}
```

Risposta:

```proto
message CompactLogResponse {
  bool success = 1;
  uint64 compacted_entries = 2;
  string error = 3;
}
```

Questa operazione permette di ridurre la dimensione dei log storici una volta che lo stato è stato salvato in uno snapshot stabile.

---

### 5.4 `GetBackupStatus`

La RPC `GetBackupStatus` serve per osservabilità e debug.

Richiesta:

```proto
message GetBackupStatusRequest {
}
```

Risposta:

```proto
message GetBackupStatusResponse {
  string service_id = 1;
  uint64 created_snapshots = 2;
  string last_snapshot_id = 3;
  string status = 4;
}
```

Questa RPC permette di sapere quanti snapshot sono stati creati, qual è l'ultimo snapshot e in che stato si trova il servizio.

---

## 6. Scelte progettuali adottate

### 6.1 Separazione delle responsabilità

Sono stati creati tre file `.proto` separati perché ogni file rappresenta una responsabilità distinta:

- `consensus.proto`: coordinamento interno e consenso distribuito;
- `kv.proto`: API logiche dello storage chiave-valore;
- `backup.proto`: gestione di snapshot, backup e compattazione.

Questa divisione rende il progetto più leggibile e più semplice da evolvere.

---

### 6.2 Uso di `uint64` per term e indici

I campi come `term`, `index`, `last_log_index`, `last_log_term` e `leader_commit` sono stati definiti come `uint64` perché rappresentano contatori monotoni non negativi.

Questo è coerente con il modello di Raft, dove termini e indici crescono nel tempo.

---

### 6.3 Uso di `string` per identificativi e indirizzi

Gli identificativi dei nodi e gli indirizzi sono stati definiti come `string`:

```proto
string leader_id = 2;
string leader_address = 3;
```

Questa scelta semplifica la configurazione iniziale, perché gli identificativi possono essere nomi logici come:

```text
node-1
node-2
node-3
```

e gli indirizzi possono essere valori come:

```text
consensus-node-1:50051
```

---

### 6.4 Uso di `bytes` per snapshot

I dati degli snapshot sono definiti come `bytes` perché uno snapshot può essere salvato in formato binario o serializzato.

Questo lascia libertà implementativa: lo snapshot potrà contenere JSON, Gob, dati compressi o un formato personalizzato.

---

### 6.5 Presenza di campi `error`

Molte risposte contengono un campo:

```proto
string error = ...;
```

Questo campo permette di restituire un messaggio leggibile in caso di errore applicativo.

Esempi:

- nodo non leader;
- chiave non valida;
- snapshot non trovato;
- replica fallita.

---

### 6.6 Presenza di `leader_hint`

Alcune risposte dello storage contengono:

```proto
string leader_hint = ...;
```

Questo campo serve al proxy per aggiornare rapidamente la propria conoscenza del leader senza dover interrogare sempre tutti i nodi.

È utile soprattutto dopo una rielezione o quando il proxy contatta accidentalmente un follower.

---

## 7. Generazione del codice Go

Dopo aver scritto i file `.proto`, il codice Go verrà generato usando `protoc` e i plugin:

- `protoc-gen-go`;
- `protoc-gen-go-grpc`.

Il comando previsto è:

```cmd
protoc --go_out=. --go_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing --go-grpc_out=. --go-grpc_opt=module=github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing proto\consensus.proto proto\kv.proto protoackup.proto
```

In alternativa verrà creato uno script:

```text
scripts/generate-proto.bat
```

per automatizzare la generazione.

---

## 8. Output atteso della generazione

La generazione dei file `.proto` produrrà codice Go nella cartella:

```text
gen/go/
```

Struttura attesa:

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

I file `.pb.go` conterranno le strutture Go generate dai messaggi Protobuf.

I file `_grpc.pb.go` conterranno invece le interfacce gRPC da implementare lato server e gli stub da usare lato client.

---

## 9. Stato finale del punto 1

Con questi tre file `.proto` viene completato il primo punto progettuale: la definizione delle interfacce gRPC.

A questo punto il sistema dispone di un contratto di comunicazione chiaro per:

- elezione del leader;
- heartbeat;
- replicazione dei log;
- installazione snapshot;
- operazioni key-value;
- discovery del leader;
- creazione e download di snapshot;
- compattazione dei log;
- controllo dello stato del backup service.

Il passo successivo sarà generare il codice Go e implementare un primo `consensus-node` minimale che esponga alcune di queste RPC.
