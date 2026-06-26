# Fase 11 - Test sperimentali e benchmark su Amazon EC2

Questo documento descrive la fase di test sperimentale del progetto B5, eseguita dopo il deploy del sistema su **Amazon EC2** tramite **AWS Academy Learner Lab**.

L'obiettivo della fase è trasformare il deploy cloud in una valutazione empirica del comportamento del sistema distribuito, misurando:

```text
- scalabilità del cluster;
- latenza end-to-end delle RPC;
- throughput;
- tolleranza ai guasti;
- tempo di failover dopo crash del leader;
- funzionamento del Backup Service;
- snapshot e persistenza locale;
- resilienza del Backup Service con nodo non disponibile;
- limiti sperimentali dell'ambiente cloud usato.
```

I test sono stati eseguiti dal **PC locale** verso l'istanza EC2, usando l'indirizzo IPv4 pubblico dell'istanza come endpoint del Client Proxy e del Backup Service.

Il percorso misurato è quindi:

```text
PC locale -> Internet -> Security Group AWS -> EC2 -> Docker Compose -> Client Proxy -> Consensus Node
```

Questo rende le misure più realistiche rispetto a un benchmark su `localhost`, perché include anche la latenza WAN e l'accesso al sistema tramite rete pubblica.

---

## 1. Ambiente sperimentale

### 1.1 Infrastruttura cloud

I test sono stati eseguiti su:

```text
Cloud provider: AWS Academy Learner Lab
Servizio: Amazon EC2
Regione: us-east-1
AMI: Amazon Linux 2023
Instance type: t3.small
Deployment: Docker Compose
```

L'istanza EC2 esponeva i servizi principali tramite le seguenti porte:

```text
8080/tcp -> Client Proxy gRPC
8081/tcp -> endpoint HTTP /health del Client Proxy
9090/tcp -> Backup Service gRPC
```

### 1.2 Configurazione software

Il sistema è stato eseguito tramite Docker Compose con tre configurazioni diverse:

```text
3 Consensus Node
5 Consensus Node
7 Consensus Node
```

Sono stati usati i file Compose:

```text
deployments/docker/docker-compose.yml
deployments/docker/docker-compose-5nodes.yml
deployments/docker/docker-compose-7nodes.yml
```

I componenti applicativi coinvolti nei test sono:

```text
Consensus Node
Client Proxy
Backup Service
bench-client
perf-client
failover-client
backup-client
backup-benchmark-client
```

---

## 2. Metodologia generale

I test sono stati raccolti con strumenti dedicati aggiunti al repository:

```text
cmd/perf-client
cmd/failover-client
cmd/backup-benchmark-client
scripts/plot_results.py
scripts/plot_throughput.py
scripts/plot_failover.py
scripts/plot_backup_results.py
scripts/measure_node_storage.sh
```

I risultati sono stati salvati in:

```text
reports/raw/
reports/processed/
reports/figures/
```

### 2.1 Output prodotti

I CSV grezzi contengono una riga per operazione o trial:

```text
reports/raw/scalability_3nodes.csv
reports/raw/scalability_5nodes.csv
reports/raw/scalability_7nodes.csv
reports/raw/failover_trials.csv
reports/raw/backup_compaction_results.csv
reports/raw/node_storage_measurements.csv
```

I CSV aggregati contengono metriche sintetiche e percentili:

```text
reports/processed/scalability_summary.csv
reports/processed/throughput_summary.csv
reports/processed/failover_summary.csv
reports/processed/backup_compaction_summary.csv
```

I grafici generati sono:

```text
reports/figures/latency_avg_vs_cluster_size.png
reports/figures/latency_p95_vs_cluster_size.png
reports/figures/latency_p99_vs_cluster_size.png
reports/figures/success_rate_vs_cluster_size.png
reports/figures/throughput_vs_cluster_size.png
reports/figures/failover_downtime_histogram.png
reports/figures/failover_downtime_cdf.png
reports/figures/failover_failed_puts.png
reports/figures/backup_duration_vs_dataset_size.png
reports/figures/downloaded_snapshots_vs_dataset_size.png
```

---

## 3. Test di scalabilità

### 3.1 Obiettivo

Il test di scalabilità misura l'impatto dell'aumento del numero di nodi sui tempi di risposta del sistema.

Sono state provate tre configurazioni:

```text
3 nodi -> quorum 2
5 nodi -> quorum 3
7 nodi -> quorum 4
```

Per ogni configurazione sono state eseguite:

```text
300 Put
300 Get
```

precedute da una fase di warm-up non inclusa nei risultati:

```text
30 Put di warm-up
30 Get di warm-up
```

Le richieste sono state inviate dal PC locale verso:

```text
<EC2_PUBLIC_IP>:8080
```

### 3.2 Risultati aggregati

Risultati raccolti:

```csv
cluster_size,operation,count,success_rate,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,min_latency_ms,max_latency_ms
3,get,300,1.0,107.048,106.98,108.396,109.455,105.193,111.189
5,get,300,1.0,121.612,116.804,118.322,274.555,114.464,559.113
7,get,300,1.0,106.289,106.041,107.926,112.142,104.254,118.061
3,put,300,1.0,109.808,109.027,112.972,124.457,106.402,170.372
5,put,300,1.0,123.159,120.589,128.707,152.662,117.101,553.493
7,put,300,1.0,110.883,110.448,115.491,119.133,106.63,123.936
```

### 3.3 Analisi latenza media

La latenza media delle `Put` è risultata:

```text
3 nodi -> 109.808 ms
5 nodi -> 123.159 ms
7 nodi -> 110.883 ms
```

La latenza media delle `Get` è risultata:

```text
3 nodi -> 107.048 ms
5 nodi -> 121.612 ms
7 nodi -> 106.289 ms
```

La configurazione a 5 nodi presenta una latenza media superiore rispetto alle altre configurazioni. Questo comportamento è coerente con gli outlier osservati nel test e con la maggiore variabilità dell'ambiente cloud.

Le `Put` risultano leggermente più sensibili alla dimensione del cluster, poiché richiedono coordinamento e replica su quorum. Le `Get`, invece, rimangono complessivamente stabili.

### 3.4 Analisi P95 e P99

Il P95 delle `Put` è:

```text
3 nodi -> 112.972 ms
5 nodi -> 128.707 ms
7 nodi -> 115.491 ms
```

Il P99 delle `Put` è:

```text
3 nodi -> 124.457 ms
5 nodi -> 152.662 ms
7 nodi -> 119.133 ms
```

Il P99 delle `Get` mostra un outlier nella configurazione a 5 nodi:

```text
3 nodi -> 109.455 ms
5 nodi -> 274.555 ms
7 nodi -> 112.142 ms
```

Anche i valori massimi confermano la presenza di outlier nella configurazione a 5 nodi:

```text
GET 5 nodi max -> 559.113 ms
PUT 5 nodi max -> 553.493 ms
```

Gli outlier non compromettono la correttezza del sistema, poiché il success rate è pari al 100% in tutte le configurazioni.

### 3.5 Success rate

Il success rate è risultato:

```text
3 nodi Put/Get -> 100%
5 nodi Put/Get -> 100%
7 nodi Put/Get -> 100%
```

Questo dimostra che il sistema resta funzionalmente corretto anche aumentando il numero di nodi.

---

## 4. Test di throughput

### 4.1 Obiettivo

Il throughput misura quante operazioni al secondo il sistema riesce a completare.

La metrica è stata calcolata a partire dai CSV raw dei benchmark di scalabilità:

```text
throughput_ops_sec = operazioni riuscite / durata della finestra di test
```

### 4.2 Risultati throughput

```csv
cluster_size,operation,count,success_count,success_rate,duration_seconds,throughput_ops_sec
3,get,300,300,1.0,32.014371,9.370791636043702
5,get,300,300,1.0,36.372311,8.248032411248214
7,get,300,300,1.0,31.783608,9.438827712700206
3,put,300,300,1.0,32.836923,9.136056992916176
5,put,300,300,1.0,36.831877,8.145118425542092
7,put,300,300,1.0,33.160703,9.046852836624122
```

### 4.3 Analisi throughput

Per le `Put`:

```text
3 nodi -> 9.14 ops/sec
5 nodi -> 8.15 ops/sec
7 nodi -> 9.05 ops/sec
```

Per le `Get`:

```text
3 nodi -> 9.37 ops/sec
5 nodi -> 8.25 ops/sec
7 nodi -> 9.44 ops/sec
```

Il throughput resta sostanzialmente stabile tra 3 e 7 nodi. La configurazione a 5 nodi presenta una riduzione temporanea, coerente con gli outlier osservati nelle latenze.

Il risultato complessivo è positivo: il sistema mantiene un throughput vicino a 9 operazioni al secondo anche passando da 3 a 7 nodi e mantiene un success rate pari al 100%.

---

## 5. Test di tolleranza ai guasti

### 5.1 Obiettivo

Il test di tolleranza ai guasti misura il comportamento del cluster dopo il crash del leader.

Il test è stato eseguito su un cluster da 5 nodi.

Per ogni trial:

```text
1. il client identifica il leader corrente;
2. il container Docker del leader viene fermato manualmente su EC2;
3. il client misura il tempo fino al nuovo leader;
4. il client misura il tempo fino alla prima Put nuovamente accettata;
5. il client conta le Put fallite durante la finestra di failover;
6. il nodo fermato viene riavviato prima del trial successivo.
```

Sono stati eseguiti 10 trial.

### 5.2 Risultati raw

```csv
timestamp_utc,trial,cluster_size,old_leader,new_leader,new_leader_time_ms,first_successful_put_ms,downtime_ms,failed_puts,successful_puts,leader_polls,notes
2026-06-26T15:58:19.8964978Z,1,5,node-1,node-4,1912,2036,2036,1,1,2,
2026-06-26T16:00:08.0146066Z,2,5,node-4,node-2,1111,1235,1235,1,1,2,
2026-06-26T16:00:31.9214763Z,3,5,node-2,node-3,1109,1231,1231,1,1,2,
2026-06-26T16:00:52.9696339Z,4,5,node-3,node-5,1110,1233,1233,1,1,2,
2026-06-26T16:01:14.0189314Z,5,5,node-5,node-4,1110,1235,1235,1,1,2,
2026-06-26T16:01:33.9282337Z,6,5,node-4,node-3,1112,1235,1235,1,1,2,
2026-06-26T16:01:52.2555022Z,7,5,node-3,node-1,1109,1233,1233,1,1,2,
2026-06-26T16:02:11.1979344Z,8,5,node-1,node-4,1912,7034,7034,5,1,6,
2026-06-26T16:02:46.7351806Z,9,5,node-4,node-2,1110,1234,1234,1,1,2,
2026-06-26T16:03:09.687809Z,10,5,node-2,node-1,1108,1230,1230,1,1,2,
```

### 5.3 Metriche sintetiche

Tempo fino al nuovo leader:

```text
media   -> 1270.3 ms
mediana -> 1110.0 ms
min     -> 1108 ms
max     -> 1912 ms
```

Downtime percepito dal client:

```text
media   -> 1893.6 ms
mediana -> 1234.5 ms
min     -> 1230 ms
max     -> 7034 ms
```

Put fallite durante il failover:

```text
media   -> 1.4
mediana -> 1
min     -> 1
max     -> 5
```

### 5.4 Analisi

Tutti i 10 trial hanno recuperato correttamente un nuovo leader. In tutti i trial il sistema è tornato a servire richieste `Put`.

Il comportamento tipico è stabile: 9 trial su 10 hanno un downtime compreso tra circa 1.23 secondi e 2.04 secondi. Il trial 8 è un outlier, con downtime pari a 7.034 secondi e 5 Put fallite.

Il test distingue due metriche diverse:

```text
new_leader_time_ms -> tempo di convergenza del cluster;
downtime_ms        -> tempo percepito dal client fino alla prima Put riuscita.
```

Nel trial 8 il cluster ha rilevato il nuovo leader in 1912 ms, ma il client ha recuperato la prima Put riuscita dopo 7034 ms. Questo indica che il sistema aveva già convergito a livello di leader election, ma il percorso client/proxy ha osservato un ritardo maggiore prima di tornare a servire scritture.

La CDF del downtime mostra che:

```text
80% dei trial recupera entro circa 1235 ms;
90% dei trial recupera entro circa 2036 ms;
100% dei trial recupera entro circa 7034 ms.
```

Il risultato dimostra che il cluster è resiliente alla perdita del leader e riesce a ristabilire automaticamente la disponibilità.

---

## 6. Backup Service, snapshot e compaction

### 6.1 Obiettivo

Il test valida il comportamento del Backup Service:

```text
- inserimento di dataset crescenti;
- creazione forzata di snapshot;
- download degli snapshot da tutti i nodi;
- salvataggio degli snapshot in /backup-data;
- misura dei file persistenti dei nodi.
```

Il test è stato eseguito su un cluster da 5 nodi.

### 6.2 Risultati TriggerBackup

```csv
timestamp_utc,dataset_size,put_successes,put_failures,put_duration_ms,backup_accepted,backup_id,downloaded_snapshots,backup_duration_ms,error
2026-06-26T16:28:29.7444255Z,100,100,0,11748,True,backup_1782491321413802575,5,462,
2026-06-26T16:28:41.9589519Z,300,300,0,34660,True,backup_1782491356262561036,5,143,
2026-06-26T16:29:16.7689754Z,600,600,0,73690,True,backup_1782491430098637544,5,179,
```

Per tutte le dimensioni del dataset:

```text
backup_accepted = true
downloaded_snapshots = 5
put_failures = 0
```

### 6.3 Analisi durata backup

Durate di TriggerBackup:

```text
100 chiavi -> 462 ms
300 chiavi -> 143 ms
600 chiavi -> 179 ms
```

Il primo backup è più lento, probabilmente a causa del warm-up del servizio e dell'inizializzazione delle operazioni di snapshot/download.

Dopo il primo backup, la durata resta bassa anche aumentando il dataset.

### 6.4 Snapshot scaricati

Nel container del Backup Service, la directory `/backup-data` contiene snapshot per tutti i 5 nodi.

Sono presenti tre livelli di snapshot:

```text
snapshot index 118
snapshot index 418
snapshot index 1018
```

Le dimensioni crescono con il dataset:

```text
snapshot_118  ->  7,742 byte
snapshot_418  -> 28,142 byte
snapshot_1018 -> 68,943 byte
```

Per ogni indice sono presenti 5 file, uno per ciascun nodo.

Questo dimostra che il Backup Service scarica realmente lo stato serializzato dai Consensus Node e lo preserva nel volume persistente `backup-data`.

### 6.5 Misura dei file persistenti dei nodi

Dopo backup e snapshot, ogni nodo presenta file locali persistenti.

Esempio di dimensioni misurate:

```text
node-1_wal.log        -> 7,571,988 byte
node-1_snapshot.json  ->    75,067 byte
node-1_state.json     ->    75,175 byte
```

Dimensioni simili sono state rilevate anche sugli altri nodi:

```text
node-2_wal.log -> 7,570,844 byte
node-3_wal.log -> 7,572,259 byte
node-4_wal.log -> 7,572,476 byte
node-5_wal.log -> 7,571,588 byte
```

Questo conferma la persistenza locale di:

```text
WAL
snapshot
state machine
```

### 6.6 Nota sulla compaction

Il test conferma che il sistema produce snapshot e che il Backup Service scarica snapshot persistenti da tutti i nodi.

Tuttavia, il file WAL fisico rimane intorno a 7.2 MB per nodo anche dopo `CompactLog`.

Questo suggerisce che l'implementazione corrente:

```text
- esegue snapshot logico;
- mantiene state e snapshot persistenti;
- scarica correttamente gli snapshot tramite Backup Service;
- non effettua una truncation fisica del file WAL nel test osservato.
```

Quindi la compaction deve essere interpretata come consolidamento logico dello stato tramite snapshot, non come riduzione fisica immediata del file WAL.

---

## 7. Test Circuit Breaker e backup parziale

### 7.1 Obiettivo

Il test verifica che il Backup Service non si blocchi se un nodo è indisponibile.

Procedura:

```text
1. il cluster da 5 nodi è attivo;
2. viene fermato manualmente node-5;
3. viene invocato TriggerBackup dal PC locale;
4. viene verificato che il backup venga accettato anche con un nodo down;
5. node-5 viene riavviato.
```

### 7.2 Risultato

Output del client:

```text
TriggerBackup response: accepted=true backup_id=backup_1782492559401113015 downloaded_snapshots=4 error=
```

Con un nodo fermo, il Backup Service ha scaricato snapshot da 4 nodi disponibili su 5.

### 7.3 Analisi

Il risultato dimostra che l'indisponibilità di un nodo non blocca l'intera procedura di backup.

Il Backup Service continua a lavorare sui nodi disponibili e completa l'operazione senza errore applicativo.

Questo comportamento valida il meccanismo di resilienza basato su Circuit Breaker e fault isolation.

---

## 8. Osservabilità infrastrutturale e limiti sperimentali

I test sono stati eseguiti su AWS Academy Learner Lab con una singola istanza EC2 `t3.small`.

Non è stato usato Amazon CloudWatch come sorgente primaria per la raccolta dei risultati, perché il progetto richiede principalmente metriche applicative:

```text
latenza RPC
throughput
tempo di failover
success rate
backup e snapshot
```

CloudWatch resta un possibile strumento di estensione per correlare eventuali outlier con metriche infrastrutturali come:

```text
CPUUtilization
NetworkIn
NetworkOut
CPU credits
status check istanza
```

Gli outlier osservati, soprattutto nei test a 5 nodi, devono essere interpretati tenendo conto di:

```text
latenza Internet tra PC locale ed EC2;
jitter di rete;
scheduling dei container Docker;
limiti dell'istanza t3.small;
ambiente condiviso del Learner Lab.
```

Queste limitazioni non invalidano i risultati, perché tutte le metriche funzionali mostrano correttezza e recupero:

```text
success rate 100% nei test di scalabilità;
10/10 failover recuperati;
backup accettati e completati;
snapshot scaricati da tutti i nodi disponibili.
```

---

## 9. Conclusioni della fase test

La fase sperimentale conferma che il sistema soddisfa i requisiti principali del progetto B5.

### 9.1 Scalabilità

Il sistema è stato testato con 3, 5 e 7 Consensus Node.

Tutte le configurazioni hanno raggiunto un success rate del 100%.

La latenza media resta nell'ordine di circa 100-125 ms nel test remoto da PC verso EC2.

### 9.2 Throughput

Il throughput resta vicino a 9 operazioni al secondo nelle configurazioni da 3 e 7 nodi.

Il calo a 5 nodi è coerente con gli outlier di latenza osservati e non indica perdita di correttezza.

### 9.3 Fault tolerance

Il cluster recupera correttamente dopo il crash del leader.

In 10 trial su 10 viene eletto un nuovo leader e il sistema torna a servire scritture.

Il downtime mediano percepito dal client è circa 1234.5 ms.

### 9.4 Backup e snapshot

Il Backup Service scarica gli snapshot da tutti i nodi disponibili.

Gli snapshot salvati in `/backup-data` crescono coerentemente con la dimensione del dataset.

I nodi mantengono file persistenti per WAL, snapshot e state.

### 9.5 Circuit Breaker

Con un nodo fermo, il Backup Service completa comunque il backup scaricando 4 snapshot su 5.

Questo dimostra isolamento del guasto e resilienza rispetto a nodi non disponibili.

---

## 10. Checklist finale

```text
[OK] Benchmark remoti da PC locale verso EC2
[OK] Scalabilità 3/5/7 nodi
[OK] Latenza media, P95, P99
[OK] Success rate
[OK] Throughput
[OK] Failover leader
[OK] Downtime percepito dal client
[OK] CDF downtime
[OK] Backup Service
[OK] Snapshot scaricati
[OK] File persistenti nei nodi
[OK] Backup parziale con nodo down
[OK] Circuit Breaker / fault isolation
[OK] Limiti sperimentali documentati
```

Questa fase conclude la validazione empirica del sistema distribuito su ambiente cloud.
