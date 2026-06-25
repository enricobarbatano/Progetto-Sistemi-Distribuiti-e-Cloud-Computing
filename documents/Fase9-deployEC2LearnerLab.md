# Fase 10 - Deploy su Amazon EC2 con AWS Academy Learner Lab

Questo documento descrive la fase di deployment cloud del progetto su **Amazon EC2**, usando l'ambiente vincolato di **AWS Academy Learner Lab**.

La fase parte da un sistema già containerizzato e validato localmente tramite Docker Compose e GitHub Actions. L'obiettivo è eseguire lo stesso cluster containerizzato in cloud, verificando che il sistema funzioni anche fuori dall'ambiente locale.

Il cluster distribuito comprende:

```text
3 Consensus Node
1 Client Proxy
1 Backup Service
```

I servizi vengono eseguiti su una singola istanza EC2 tramite Docker Compose.

---

## 1. Obiettivo della fase

L'obiettivo della fase è preparare e validare un ambiente cloud capace di ospitare il cluster distribuito containerizzato.

In particolare, la fase ha previsto:

```text
1. avvio di AWS Academy Learner Lab;
2. creazione di un'istanza EC2;
3. configurazione Security Group;
4. accesso SSH all'istanza;
5. installazione di Docker, Docker Compose, Buildx e Git;
6. clonazione del repository GitHub;
7. avvio del cluster tramite Docker Compose;
8. test remoti da macchina locale;
9. verifica backup e persistenza dei volumi;
10. arresto controllato dei container e del lab per preservare il budget.
```

---

## 2. Vincoli dell'ambiente AWS Academy Learner Lab

Il deployment è stato eseguito dentro AWS Academy Learner Lab, quindi con alcune limitazioni rispetto a un account AWS standard.

I vincoli principali considerati sono:

```text
Regioni consentite principali:
  us-east-1
  us-west-2

Key pair preconfigurata in us-east-1:
  vockey

Accesso SSH dal terminale browser:
  ~/.ssh/labsuser.pem

Instance type supportati:
  nano, micro, small, medium, large

Budget limitato:
  necessario fermare le risorse quando non vengono usate
```

Per ridurre problemi di accesso SSH e compatibilità con la key pair preconfigurata, è stata scelta la regione:

```text
us-east-1
```

---

## 3. Creazione dell'istanza EC2

L'istanza è stata creata dalla console AWS del Learner Lab.

Configurazione usata:

```text
Nome istanza:
  sdcc-docker-cluster

Regione:
  us-east-1

AMI:
  Amazon Linux 2023

Instance type:
  t3.small

Key pair:
  vockey
```

La scelta di `t3.small` è stata fatta perché il cluster esegue contemporaneamente cinque container e deve anche compilare le immagini Docker durante il primo `docker compose up --build`.

---

## 4. Security Group

È stato configurato un Security Group per permettere l'accesso SSH e il traffico verso i servizi principali del progetto.

Porte utilizzate:

```text
22/tcp    SSH verso EC2
8080/tcp  Client Proxy gRPC
8081/tcp  Client Proxy health endpoint
9090/tcp  Backup Service gRPC amministrativo
```

Nel progetto le porte sono così mappate:

```text
8080 -> richieste client verso il Proxy
8081 -> endpoint HTTP /health del Proxy
9090 -> Backup Service
```

Durante i test nel Learner Lab, alcune regole possono essere aperte temporaneamente verso:

```text
0.0.0.0/0
```

per semplificare il debug. In un ambiente non didattico, l'accesso andrebbe ristretto al proprio IP o a reti autorizzate.

---

## 5. Accesso SSH all'istanza

Dopo la creazione dell'istanza, è stato copiato il Public IPv4:

```text
54.159.53.117
```

Dal terminale browser del Learner Lab è stato usato il comando:

```bash
ssh -i ~/.ssh/labsuser.pem ec2-user@54.159.53.117
```

Durante il primo accesso SSH è comparsa la richiesta di conferma dell'host:

```text
The authenticity of host ... can't be established.
Are you sure you want to continue connecting (yes/no)?
```

È stato risposto:

```text
yes
```

Dopo il login, il prompt mostrava l'accesso all'istanza EC2:

```bash
[ec2-user@ip-172-31-19-118 ~]$
```

---

## 6. Verifica del sistema operativo

È stato eseguito:

```bash
cat /etc/os-release
```

Output rilevante:

```text
NAME="Amazon Linux"
VERSION="2023"
ID="amzn"
PRETTY_NAME="Amazon Linux 2023.12.20260622"
```

Questo ha confermato che l'istanza eseguiva correttamente Amazon Linux 2023.

---

## 7. Installazione Docker e Git

Dentro l'istanza EC2 sono stati eseguiti:

```bash
sudo dnf update -y
sudo dnf install -y docker git
```

L'installazione ha scaricato e installato Docker, Git e relative dipendenze.

Versioni poi verificate:

```text
Docker version 25.0.14, build 0bab007
Git version 2.50.1
```

---

## 8. Avvio del servizio Docker

Docker è stato avviato e abilitato al boot:

```bash
sudo systemctl start docker
sudo systemctl enable docker
```

Lo stato è stato verificato con:

```bash
sudo systemctl status docker
```

Output rilevante:

```text
Active: active (running)
```

Questo ha confermato che il daemon Docker era attivo.

---

## 9. Abilitazione di `ec2-user` al gruppo Docker

Per evitare di dover usare `sudo` per ogni comando Docker, l'utente `ec2-user` è stato aggiunto al gruppo Docker:

```bash
sudo usermod -aG docker ec2-user
```

Poi è stata chiusa e riaperta la sessione SSH:

```bash
exit
ssh -i ~/.ssh/labsuser.pem ec2-user@54.159.53.117
```

---

## 10. Installazione Docker Compose

Docker Compose è stato installato come plugin Docker CLI.

È stata creata la directory dei plugin:

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
```

Poi è stato scaricato il binario di Docker Compose:

```bash
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" -o /usr/local/lib/docker/cli-plugins/docker-compose
```

Infine è stato reso eseguibile:

```bash
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

La verifica ha prodotto:

```bash
docker compose version
```

Output:

```text
Docker Compose version v5.2.0
```

---

## 11. Verifica Docker con `hello-world`

È stato eseguito:

```bash
docker run hello-world
```

Output rilevante:

```text
Hello from Docker!
This message shows that your installation appears to be working correctly.
```

Questo ha confermato che:

```text
[OK] Docker client comunica con Docker daemon;
[OK] Docker riesce a scaricare immagini da Docker Hub;
[OK] Docker riesce ad avviare container;
[OK] Docker restituisce correttamente l'output del container.
```

---

## 12. Problema Buildx e correzione

Al primo tentativo di avvio del cluster è stato eseguito:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

L'errore restituito è stato:

```text
compose build requires buildx 0.17.0 or later
```

La versione Buildx presente inizialmente era:

```bash
docker buildx version
```

Output:

```text
github.com/docker/buildx 0.12.1 30feaa1a915b869ebc2eea6328624b49facd4bfb
```

Questa versione era inferiore al requisito minimo richiesto da Docker Compose.

È stato quindi installato Buildx `v0.35.0`:

```bash
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" -o /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
```

Verifica:

```bash
docker buildx version
```

Output:

```text
github.com/docker/buildx v0.35.0 a319e5b15052cf6557ceb666eb8ff6e32380b782
```

Dopo questa correzione, `docker compose up -d --build` ha funzionato correttamente.

---

## 13. Clonazione del repository

Dentro EC2 è stato clonato il repository del progetto:

```bash
git clone https://github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing.git
```

Output:

```text
Cloning into 'Progetto-Sistemi-Distribuiti-e-Cloud-Computing'...
Receiving objects: 100% (328/328), done.
Resolving deltas: 100% (139/139), done.
```

Poi è stato eseguito:

```bash
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
```

La directory conteneva:

```text
README.md
buf.yaml
cmd
deployments
documents
gen
go.mod
go.sum
internal
proto
```

---

## 14. Avvio del cluster su EC2

Il cluster è stato avviato con:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build
```

La build ha completato correttamente tutte le immagini:

```text
[+] up 15/15
 ✔ Image docker-node-2           Built
 ✔ Image docker-node-3           Built
 ✔ Image docker-client-proxy     Built
 ✔ Image docker-backup-service   Built
 ✔ Image docker-node-1           Built
 ✔ Network docker_sdcc-net       Created
 ✔ Volume docker_node1-data      Created
 ✔ Volume docker_node2-data      Created
 ✔ Volume docker_node3-data      Created
 ✔ Volume docker_backup-data     Created
 ✔ Container sdcc-node-1         Started
 ✔ Container sdcc-node-2         Started
 ✔ Container sdcc-node-3         Started
 ✔ Container sdcc-client-proxy   Started
 ✔ Container sdcc-backup-service Started
```

---

## 15. Stato dei container su EC2

È stato eseguito:

```bash
docker compose -f deployments/docker/docker-compose.yml ps
```

Output rilevante:

```text
sdcc-backup-service   Up              0.0.0.0:9090->9090/tcp
sdcc-client-proxy     Up (healthy)    0.0.0.0:8080-8081->8080-8081/tcp
sdcc-node-1           Up              0.0.0.0:50051->50051/tcp
sdcc-node-2           Up              0.0.0.0:50052->50051/tcp
sdcc-node-3           Up              0.0.0.0:50053->50051/tcp
```

Questo conferma che tutti i servizi sono partiti correttamente su EC2.

---

## 16. Verifica health del Proxy su EC2

Da dentro EC2 è stato eseguito:

```bash
curl http://localhost:8081/health
```

Output:

```json
{"status":"ok","timestamp":"2026-06-25T11:57:21.7475737Z"}
```

Questo conferma che il Client Proxy era operativo e marcato come healthy.

---

## 17. Test remoto: leader discovery

Dal PC locale è stato usato il Public IPv4 dell'istanza:

```text
54.159.53.117
```

Comando:

```cmd
set TARGET=54.159.53.117:8080
set OP=leader
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id=node-3 leader_address=node-3:50051 term=1
```

Questo conferma che:

```text
[OK] il Proxy è raggiungibile dall'esterno;
[OK] il Proxy comunica con i nodi nella rete Docker;
[OK] il cluster ha eletto un leader;
[OK] il leader corrente era node-3.
```

---

## 18. Test remoto: Put/Get

È stata eseguita una scrittura remota:

```cmd
set TARGET=54.159.53.117:8080
set OP=put
set KEY=ec2-key
set VALUE=ec2-value
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=ec2-key value=ec2-value error= leader_hint=
```

Poi è stata eseguita una lettura remota:

```cmd
set TARGET=54.159.53.117:8080
set OP=get
set KEY=ec2-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=ec2-key value=ec2-value error= leader_hint=node-3:50051
```

Questo valida il percorso completo:

```text
client locale -> IP pubblico EC2 -> Proxy container -> leader Raft -> state machine
```

---

## 19. Test remoto: Backup Service

È stato verificato lo stato iniziale del Backup Service:

```cmd
set TARGET=54.159.53.117:9090
set OP=status
go run .\cmd\backup-client
```

Output:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=0 downloaded_snapshots=0 last_backup_id= last_snapshot_id= last_error=
```

Poi è stato eseguito un backup remoto:

```cmd
set TARGET=54.159.53.117:9090
set OP=backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-client
```

Output:

```text
TriggerBackup response: accepted=true backup_id=backup_1782388868175539658 downloaded_snapshots=3 error=
```

Questo conferma che il Backup Service è raggiungibile dall'esterno e riesce a scaricare snapshot da tutti e tre i nodi.

---

## 20. Test backup parziale

Durante la fase di test è stato verificato anche un backup parziale.

Output:

```text
TriggerBackup response: accepted=true backup_id=backup_1782388938394046065 downloaded_snapshots=2 error=
```

Questo conferma che il Backup Service continua a funzionare anche quando un nodo non risponde o non è disponibile.

---

## 21. Test persistenza dei Consensus Node

È stata scritta una chiave persistente:

```cmd
set TARGET=54.159.53.117:8080
set OP=put
set KEY=ec2-persistent-key
set VALUE=ec2-persistent-value
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=ec2-persistent-key value=ec2-persistent-value error= leader_hint=
```

Poi il cluster è stato fermato e riavviato su EC2 con:

```bash
docker compose -f deployments/docker/docker-compose.yml down
docker compose -f deployments/docker/docker-compose.yml up -d
```

Non è stato usato `-v`, quindi i volumi Docker non sono stati cancellati.

Dopo il riavvio è stato eseguito:

```cmd
set TARGET=54.159.53.117:8080
set OP=get
set KEY=ec2-persistent-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=ec2-persistent-key value=ec2-persistent-value error= leader_hint=node-2:50051
```

Questo dimostra che la state machine dei nodi è persistita correttamente nei volumi Docker.

---

## 22. Backup post-restart

Dopo il riavvio del cluster, è stato lanciato un nuovo backup:

```cmd
set TARGET=54.159.53.117:9090
set OP=backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-client
```

Output:

```text
TriggerBackup response: accepted=true backup_id=backup_1782389315877748606 downloaded_snapshots=3 error=
```

Status successivo:

```text
GetBackupStatus response: service_id=backup-service-1 status=idle created_backups=1 downloaded_snapshots=3 last_backup_id=backup_1782389315877748606 last_snapshot_id=node-3_snapshot_3_term_11 last_error=
```

---

## 23. Verifica snapshot nel volume `backup-data`

Dentro EC2 è stato aperto il container del Backup Service:

```bash
docker exec -it sdcc-backup-service sh
```

Poi:

```sh
ls -l /backup-data
cat /backup-data/*.json
```

Sono stati trovati snapshot reali:

```text
node-1_node-1_snapshot_1_term_1_index_1_term_1.json
node-1_node-1_snapshot_2_term_1_index_2_term_1.json
node-2_node-2_snapshot_1_term_1_index_1_term_1.json
node-2_node-2_snapshot_2_term_1_index_2_term_1.json
node-2_node-2_snapshot_3_term_11_index_3_term_11.json
node-3_node-3_snapshot_1_term_1_index_1_term_1.json
node-3_node-3_snapshot_2_term_1_index_2_term_1.json
node-3_node-3_snapshot_3_term_11_index_3_term_11.json
```

Gli snapshot finali contenevano:

```json
{
  "last_included_index": 3,
  "last_included_term": 11,
  "data": {
    "ec2-failover-key": "ok",
    "ec2-key": "ec2-value",
    "ec2-persistent-key": "ec2-persistent-value"
  }
}
```

Questo conferma che anche il volume `backup-data` è persistente e contiene snapshot aggiornati.

---

## 24. Comportamento dello status del Backup Service dopo restart

Dopo `docker compose down` e `docker compose up -d`, lo status del Backup Service è tornato a:

```text
created_backups=0
downloaded_snapshots=0
```

Questo comportamento è corretto perché quei contatori sono mantenuti in memoria dal processo `backup-service`.

I file di backup, invece, sono rimasti nel volume:

```text
/backup-data
```

Quindi l'architettura è coerente:

```text
Backup Service:
  stateless per i contatori runtime

Volume backup-data:
  persistente per gli snapshot scaricati
```

---

## 25. Arresto controllato del cluster

Per fermare i container senza cancellare i volumi è stato usato:

```bash
docker compose -f deployments/docker/docker-compose.yml down
```

Questo rimuove:

```text
container
network Docker
```

ma conserva i volumi:

```text
node1-data
node2-data
node3-data
backup-data
```

Non è stato usato:

```bash
docker compose -f deployments/docker/docker-compose.yml down -v
```

perché `-v` cancellerebbe i volumi.

---

## 26. Chiusura del Learner Lab

Al termine dei test, il cluster è stato fermato con `docker compose down`.

Poi, per preservare il budget del Learner Lab, è stata fermata l'istanza EC2 dalla console AWS e successivamente è stato chiuso il lab con:

```text
End Lab
```

Questa procedura evita di lasciare risorse compute attive inutilmente.

---

## 27. Stato finale della fase

Checklist finale:

```text
[OK] Learner Lab avviato
[OK] EC2 creata in us-east-1
[OK] Amazon Linux 2023 verificato
[OK] SSH funzionante con labsuser.pem
[OK] Docker installato
[OK] Docker daemon attivo
[OK] Docker Compose installato
[OK] Buildx aggiornato a v0.35.0
[OK] Git installato
[OK] Repository clonato su EC2
[OK] Docker Compose build completata su EC2
[OK] Cluster avviato su EC2
[OK] Client Proxy healthy
[OK] Leader discovery remoto funzionante
[OK] Put/Get remoto funzionante
[OK] Backup Service remoto funzionante
[OK] Backup con 3 snapshot funzionante
[OK] Backup parziale funzionante
[OK] Persistenza state machine verificata
[OK] Persistenza snapshot backup verificata
[OK] Snapshot post-restart verificati
[OK] Cluster fermato senza cancellare volumi
[OK] Learner Lab chiuso correttamente
```

---

## 28. Comandi rapidi per una sessione successiva

Quando il Learner Lab viene riaperto, il Public IPv4 dell'istanza può cambiare. Prima di collegarsi bisogna quindi controllare il nuovo Public IPv4 nella console EC2.

### Connessione SSH

```bash
ssh -i ~/.ssh/labsuser.pem ec2-user@<NUOVO_PUBLIC_IP>
```

### Entrare nel repository

```bash
cd Progetto-Sistemi-Distribuiti-e-Cloud-Computing
```

### Avviare il cluster senza rebuild

```bash
docker compose -f deployments/docker/docker-compose.yml up -d
```

### Controllare i container

```bash
docker compose -f deployments/docker/docker-compose.yml ps
```

### Health locale su EC2

```bash
curl http://localhost:8081/health
```

### Fermare il cluster senza cancellare volumi

```bash
docker compose -f deployments/docker/docker-compose.yml down
```

---

## 29. Conclusione

La fase di deploy su EC2 ha dimostrato che il sistema è eseguibile in un ambiente cloud reale, anche con i vincoli del Learner Lab.

Il sistema è risultato:

```text
riproducibile
containerizzato
accessibile da remoto
persistente tramite Docker volumes
resiliente a indisponibilità parziale dei nodi
validato tramite test funzionali end-to-end
```

Con questa fase, il progetto soddisfa il requisito di esecuzione cloud del sistema distribuito e può essere presentato come architettura pronta per deployment su IaaS.
