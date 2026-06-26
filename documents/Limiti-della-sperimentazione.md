# Limiti della sperimentazione

Questo documento raccoglie e analizza i principali limiti della sperimentazione svolta nel progetto B5, considerando tutte le fasi implementative e sperimentali completate: sviluppo del Consensus Node, replica, Client Proxy, persistenza, snapshot, Backup Service, containerizzazione Docker Compose, CI/CD GitHub Actions, deploy su Amazon EC2 e benchmark finali.

Lo scopo del documento è chiarire quali fattori possono aver influenzato i risultati sperimentali e quali aspetti devono essere considerati nell'interpretazione dei dati raccolti.

---

## 1. Sintesi delle fasi completate

Il progetto è stato sviluppato in modo incrementale attraverso più fasi.

Le principali fasi completate sono:

```text
Fase 1  - Configurazione iniziale del progetto Go e CI di base
Fase 2  - Definizione dei file Protobuf e generazione stub gRPC
Fase 3  - Implementazione della struttura base del Consensus Node
Fase 4  - Algoritmo di elezione del leader
Fase 5  - Replica atomica dei log
Fase 5b - Refactor SRP del Consensus Node
Fase 6  - Client Proxy Service
Fase 6b - Risanamento debito tecnico pre-Docker
Fase 8  - Snapshot e Backup Service
Fase 9  - Containerizzazione con Docker Compose e CI Docker
Fase 10 - Deploy su Amazon EC2 con AWS Academy Learner Lab
Fase 11 - Test sperimentali e benchmark su EC2
```

Durante la fase sperimentale sono stati raccolti dati su:

```text
- latenza media RPC;
- latenza P95 e P99;
- throughput;
- success rate;
- failover dopo crash del leader;
- downtime percepito dal client;
- Backup Service;
- snapshot scaricati;
- persistenza di WAL, snapshot e state;
- backup parziale con nodo non disponibile.
```

I test finali sono stati eseguiti dal PC locale verso l'istanza EC2, quindi misurano il comportamento end-to-end del sistema in uno scenario cloud reale.

---

## 2. Limiti dell'ambiente AWS Academy Learner Lab

La sperimentazione è stata eseguita all'interno di AWS Academy Learner Lab, non in un account AWS produttivo.

Questo comporta alcuni limiti:

```text
- budget limitato;
- durata limitata della sessione di laboratorio;
- necessità di fermare e riavviare manualmente il lab;
- possibilità di cambio dell'indirizzo IPv4 pubblico dopo stop/start dell'istanza;
- numero limitato di regioni e tipi di istanza utilizzabili;
- strumenti e permessi potenzialmente ridotti rispetto a un account AWS completo.
```

Questi vincoli non invalidano i test, ma influenzano la riproducibilità perfetta dell'esperimento. In particolare, dopo il riavvio del Learner Lab l'IP pubblico dell'istanza EC2 può cambiare, richiedendo l'aggiornamento manuale del target nei client di benchmark.

---

## 3. Limiti dell'istanza EC2 t3.small

I test sono stati eseguiti su una singola istanza EC2 di tipo:

```text
t3.small
```

Questo tipo di istanza è adatto a un ambiente didattico e a test di progetto, ma presenta limiti rispetto a una macchina dedicata o a un cluster reale multi-host.

I principali limiti sono:

```text
- risorse CPU e RAM limitate;
- possibile variabilità delle prestazioni;
- esecuzione di tutti i container sulla stessa macchina fisica/virtuale;
- contesa di CPU tra Consensus Node, Client Proxy, Backup Service e Docker daemon;
- possibile impatto dei burst CPU credit tipici delle istanze T3;
- possibile interferenza tra fase di build Docker e fase di test se eseguite nella stessa sessione.
```

Questi fattori possono influenzare soprattutto:

```text
- outlier sulle latenze P99;
- picchi di downtime percepito;
- durata del backup;
- tempi di build Docker;
- stabilità del throughput.
```

Per questo motivo, i risultati devono essere interpretati come misure sperimentali su ambiente cloud didattico, non come benchmark assoluti di produzione.

---

## 4. Limite della singola istanza EC2

Il sistema è distribuito a livello logico, perché ogni Consensus Node è un processo/container separato e comunica via gRPC.

Tuttavia, nella sperimentazione tutti i nodi sono stati eseguiti sulla stessa istanza EC2 tramite Docker Compose.

Questo implica che:

```text
- non esiste vera distribuzione geografica tra i nodi;
- non esiste latenza di rete reale tra i Consensus Node;
- non vengono simulate failure fisiche di macchine diverse;
- CPU, RAM, disco e rete sono condivisi tra tutti i container;
- un guasto dell'istanza EC2 comprometterebbe l'intero cluster.
```

Quindi il test valida correttamente:

```text
- coordinamento logico tra nodi;
- replica tramite RPC;
- election del leader;
- comportamento del Proxy;
- Backup Service;
- containerizzazione;
- deploy cloud su EC2.
```

Non valida invece completamente uno scenario multi-macchina come:

```text
node-1 su EC2-A
node-2 su EC2-B
node-3 su EC2-C
```

Un'estensione naturale sarebbe distribuire i nodi su più istanze EC2 o su un orchestratore come Kubernetes.

---

## 5. Latenza WAN tra client locale ed EC2

I benchmark finali sono stati eseguiti dal PC locale verso l'istanza EC2 tramite IP pubblico.

Questo è positivo perché misura il percorso completo:

```text
PC locale -> Internet -> Security Group AWS -> EC2 -> Docker -> Client Proxy -> Consensus Node
```

Tuttavia, questo introduce anche variabilità esterna:

```text
- latenza della rete domestica/universitaria;
- instradamento Internet verso AWS;
- jitter di rete;
- congestione temporanea;
- differenze tra run eseguiti in orari diversi;
- eventuale influenza di Wi-Fi, ISP o VPN.
```

Questi fattori possono spiegare perché alcune configurazioni mostrano outlier, in particolare nei risultati a 5 nodi.

Esempi osservati:

```text
GET 5 nodi max -> 559.113 ms
PUT 5 nodi max -> 553.493 ms
GET 5 nodi P99 -> 274.555 ms
```

Questi valori sono stati trattati come outlier infrastrutturali o di rete, perché il success rate è rimasto pari al 100%.

---

## 6. Confronto tra benchmark locali e benchmark EC2

Durante lo sviluppo sono stati eseguiti anche test locali con Docker Desktop e target `localhost`.

Questi test sono stati utili per verificare:

```text
- correttezza del perf-client;
- correttezza dei CSV;
- correttezza dei calcoli P50/P95/P99;
- funzionamento dei file Compose 3/5/7 nodi;
- funzionamento del cluster prima del deploy remoto.
```

Tuttavia, i risultati locali non sono stati usati come risultati principali della relazione finale, perché non includono:

```text
- rete Internet;
- Security Group;
- IP pubblico EC2;
- deploy cloud reale;
- latenza WAN;
- comportamento del sistema in ambiente AWS.
```

I risultati principali della fase test sono quindi quelli raccolti dal PC locale verso EC2.

---

## 7. Dimensione limitata dei dataset sperimentali

I benchmark di scalabilità sono stati eseguiti con:

```text
300 Put
300 Get
30 Put di warm-up
30 Get di warm-up
```

I test Backup Service sono stati eseguiti con dataset crescenti di:

```text
100 chiavi
300 chiavi
600 chiavi
```

Queste dimensioni sono adeguate per un progetto universitario e per un ambiente Learner Lab, ma non rappresentano carichi di produzione.

I limiti sono:

```text
- numero di operazioni relativamente contenuto;
- dataset piccoli rispetto a sistemi reali;
- assenza di test molto lunghi nel tempo;
- assenza di stress test su migliaia o milioni di chiavi;
- assenza di concorrenza elevata.
```

Per evitare saturazione dell'istanza t3.small, è stata usata una configurazione prudente.

Un'estensione possibile sarebbe ripetere i benchmark con:

```text
- 1000, 5000 o 10000 operazioni;
- dataset più grandi;
- test ripetuti in momenti diversi;
- maggiore concorrenza client;
- più istanze EC2.
```

---

## 8. Concorrenza limitata del client

I benchmark di scalabilità sono stati eseguiti con:

```text
CONCURRENCY=1
```

Questa scelta permette di misurare in modo più chiaro la latenza per singola richiesta, evitando che la pressione del client saturi il sistema.

Il limite è che non misura pienamente il comportamento sotto carico concorrente elevato.

Non vengono quindi esplorati in profondità:

```text
- code di richieste nel Proxy;
- contesa tra più client simultanei;
- saturazione del leader;
- throughput massimo sostenibile;
- degradazione progressiva sotto carico.
```

Il throughput calcolato rappresenta quindi la capacità osservata nella configurazione sperimentale scelta, non il massimo teorico del sistema.

---

## 9. Numero limitato di trial per il failover

Il test di failover è stato eseguito con:

```text
10 crash del leader
```

Questo numero è sufficiente per mostrare il comportamento del sistema e produrre un istogramma/CDF, ma resta limitato dal punto di vista statistico.

Con 10 trial:

```text
- è possibile osservare outlier;
- la media può essere influenzata da un singolo caso anomalo;
- non è possibile stimare con alta precisione percentili estremi;
- la CDF è utile ma ha granularità limitata.
```

Nel test è stato osservato un outlier:

```text
trial 8 -> downtime 7034 ms
```

Mentre il comportamento tipico è risultato più stabile:

```text
mediana downtime -> 1234.5 ms
```

Un'estensione possibile sarebbe eseguire:

```text
30 o 50 trial di failover
```

in sessioni differenti per ridurre l'impatto del singolo outlier.

---

## 10. Stop manuale del leader nei test di failover

Il crash del leader è stato simulato tramite comando manuale:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml stop node-X
```

Questo approccio è semplice e controllabile, ma introduce un limite:

```text
il tempo esatto di crash non è misurato automaticamente dal sistema di benchmark.
```

Il failover-client avvia il timer dopo la conferma manuale dell'operatore.

Quindi la misura può includere una piccola imprecisione umana tra:

```text
- momento effettivo dello stop del container;
- pressione di INVIO nel client locale.
```

Questa imprecisione è ridotta perché la procedura è stata eseguita rapidamente, ma non è nulla.

Un'estensione più rigorosa sarebbe automatizzare il crash via SSH o Docker API, facendo partire il timer nello stesso processo che invoca lo stop del container.

---

## 11. Misura della rielezione tramite polling

Il failover-client misura il nuovo leader tramite polling sul Client Proxy.

Parametri usati:

```text
POLL_INTERVAL_MS=100
PUT_INTERVAL_MS=100
```

Questo significa che la risoluzione temporale della misura è limitata a circa 100 ms.

Il tempo reale di elezione potrebbe essere leggermente inferiore rispetto a quello osservato.

Questo limite riguarda:

```text
new_leader_time_ms
downtime_ms
first_successful_put_ms
```

Per aumentare la precisione si potrebbe ridurre il polling interval, ma questo aumenterebbe anche il carico sul Proxy durante la fase di failover.

---

## 12. Compaction logica e file WAL fisico

Il test Backup Service ha confermato che:

```text
- gli snapshot vengono creati;
- gli snapshot vengono scaricati dal Backup Service;
- i file snapshot locali esistono;
- lo state file locale esiste;
- il WAL locale esiste.
```

Tuttavia, il file WAL fisico resta circa:

```text
7.2 MB per nodo
```

anche dopo backup e richiesta di compaction.

Questo indica che l'implementazione corrente produce snapshot logici e mantiene stato persistente, ma non tronca fisicamente il file WAL nel test osservato.

Questa è una limitazione importante da dichiarare:

```text
la compaction è validata come consolidamento logico tramite snapshot, ma non come riduzione fisica immediata del file WAL.
```

Possibili cause:

```text
- WAL implementato come file append-only;
- CompactLog aggiorna lo stato logico ma non riscrive/tronca il file;
- truncation fisica non ancora implementata;
- scelta implementativa orientata alla semplicità e recuperabilità.
```

Un'estensione futura sarebbe implementare la riscrittura del WAL dopo compaction, mantenendo solo le entry successive all'ultimo snapshot incluso.

---

## 13. Backup Service: durata misurata su dataset piccoli

Il test Backup Service ha usato dataset di:

```text
100, 300, 600 chiavi
```

La durata di backup osservata è stata:

```text
100 chiavi -> 462 ms
300 chiavi -> 143 ms
600 chiavi -> 179 ms
```

Il primo valore è più alto probabilmente per warm-up e inizializzazione.

Il limite è che non è stata misurata la durata del backup su stati molto grandi.

Quindi non è possibile concludere come il Backup Service si comporterebbe con:

```text
- decine di migliaia di chiavi;
- snapshot di molti MB;
- rete congestionata;
- nodi distribuiti su istanze diverse;
- backup concorrenti.
```

Il test dimostra correttamente il funzionamento del meccanismo, ma non ne esplora il limite massimo.

---

## 14. Circuit Breaker validato in scenario semplice

Il comportamento del Circuit Breaker è stato validato tramite backup parziale:

```text
cluster da 5 nodi
node-5 fermato
TriggerBackup eseguito
risultato: downloaded_snapshots=4
```

Questo dimostra che un nodo indisponibile non blocca l'intero Backup Service.

Il limite è che il test copre un caso semplice:

```text
nodo completamente fermo
```

Non sono stati testati scenari più complessi come:

```text
- nodo molto lento ma non down;
- risposte intermittenti;
- network partition parziale;
- latenza artificiale su un solo nodo;
- apertura e chiusura ripetuta del circuito;
- verifica quantitativa dello stato interno del Circuit Breaker.
```

Quindi il test dimostra fault isolation a livello applicativo, ma non una caratterizzazione completa del Circuit Breaker sotto tutte le condizioni.

---

## 15. Assenza di CloudWatch come sorgente sperimentale primaria

Amazon CloudWatch non è stato usato come sorgente primaria dei dati sperimentali.

I dati principali sono stati raccolti a livello applicativo tramite client dedicati:

```text
perf-client
failover-client
backup-benchmark-client
backup-client
```

Questo è coerente con gli obiettivi del progetto, perché i requisiti sperimentali principali riguardano:

```text
latenza RPC
throughput
failover
backup
success rate
correttezza funzionale
```

Tuttavia, senza CloudWatch non è possibile correlare con precisione gli outlier a metriche infrastrutturali come:

```text
CPUUtilization
NetworkIn
NetworkOut
CPU credits
utilizzo disco
status check istanza
```

Gli outlier osservati sono quindi interpretati qualitativamente, non correlati quantitativamente a metriche EC2.

CloudWatch resta una possibile estensione per aumentare l'osservabilità dell'esperimento.

---

## 16. Security Group e accesso pubblico

Durante i test, alcune porte dell'istanza EC2 sono state aperte per permettere accesso dal PC locale:

```text
8080/tcp -> Client Proxy
8081/tcp -> health endpoint
9090/tcp -> Backup Service
```

Questo è necessario per eseguire benchmark remoti, ma rappresenta una configurazione semplificata rispetto a un ambiente produttivo.

In produzione sarebbe opportuno:

```text
- restringere le regole al solo IP autorizzato;
- usare TLS per gRPC;
- evitare esposizione diretta del Backup Service;
- usare autenticazione/autorizzazione;
- inserire un load balancer o API gateway;
- configurare VPC/subnet/security group in modo più rigoroso.
```

Nel progetto, la configurazione è adeguata allo scenario didattico e sperimentale.

---

## 17. Assenza di cifratura e autenticazione sulle RPC

Le RPC gRPC del progetto sono state eseguite in modalità non cifrata nell'ambiente di test.

Questo semplifica lo sviluppo e la valutazione, ma non è una configurazione adatta a un ambiente produttivo esposto su Internet.

Limiti:

```text
- assenza di TLS;
- assenza di autenticazione client;
- assenza di autorizzazione sulle operazioni amministrative;
- Backup Service esposto direttamente su porta 9090;
- possibile manipolazione delle richieste in un ambiente non protetto.
```

Per un deployment reale sarebbe necessario introdurre:

```text
- TLS/mTLS;
- token o credenziali applicative;
- policy di accesso;
- restrizione delle porte amministrative;
- log e audit delle operazioni sensibili.
```

---

## 18. Persistenza validata ma non sottoposta a test distruttivi estesi

Il progetto ha validato la persistenza tramite:

```text
- Docker volumes;
- restart dei container;
- verifica di chiavi dopo down/up;
- presenza di WAL, snapshot e state file;
- snapshot salvati in backup-data.
```

Tuttavia non sono stati eseguiti test distruttivi estesi come:

```text
- corruzione manuale del WAL;
- cancellazione parziale di snapshot;
- crash durante scrittura WAL;
- crash durante snapshot;
- crash durante CompactLog;
- recupero dopo riavvio dell'intera istanza EC2;
- restore completo da backup remoto.
```

Quindi la persistenza è validata per scenari ordinari di restart e backup, ma non per scenari estremi di corruzione o crash durante operazioni critiche.

---

## 19. CI/CD e test automatici

Il progetto include workflow GitHub Actions per:

```text
- go test;
- gofmt;
- go vet;
- build;
- Protobuf checks;
- Docker build;
- Docker Compose integration;
- scan immagini.
```

Questo aumenta la qualità del repository.

Tuttavia, i benchmark EC2 non sono eseguiti automaticamente in CI, perché richiedono:

```text
- istanza EC2 attiva;
- IP pubblico aggiornato;
- Security Group configurato;
- Docker Compose avviato su EC2;
- budget Learner Lab disponibile.
```

I test sperimentali sono quindi riproducibili tramite script e documentazione, ma non automatizzati end-to-end in GitHub Actions.

---

## 20. Interpretazione generale dei risultati

Nonostante i limiti descritti, i risultati sono solidi rispetto agli obiettivi del progetto.

Il sistema ha dimostrato:

```text
- correttezza funzionale in tutte le configurazioni;
- success rate del 100% nei benchmark di scalabilità;
- throughput stabile intorno a 9 ops/sec nel test remoto;
- recupero automatico dopo crash del leader;
- downtime mediano intorno a 1.23 secondi;
- Backup Service funzionante;
- snapshot persistenti e crescenti con il dataset;
- fault isolation con nodo non disponibile;
- capacità di deploy su EC2 con Docker Compose.
```

I limiti principali riguardano invece:

```text
- ambiente single-instance;
- risorse limitate della t3.small;
- jitter WAN;
- dataset contenuti;
- concorrenza limitata;
- stop manuale nei failover;
- assenza di CloudWatch nella correlazione degli outlier;
- compaction fisica del WAL non osservata.
```

---

## 21. Possibili sviluppi futuri

Per migliorare ulteriormente la sperimentazione si potrebbero introdurre:

```text
1. cluster multi-istanza EC2;
2. test con più client concorrenti;
3. dataset più grandi;
4. test di lunga durata;
5. automazione SSH del crash leader;
6. raccolta metriche CloudWatch;
7. TLS/mTLS per gRPC;
8. autenticazione sul Backup Service;
9. truncation fisica del WAL dopo compaction;
10. test di restore completo da snapshot;
11. chaos testing con latenza o perdita di pacchetti;
12. test di Circuit Breaker su nodi lenti, non solo spenti.
```

Queste estensioni non sono necessarie per soddisfare i requisiti del progetto, ma rappresentano evoluzioni naturali verso un sistema più vicino alla produzione.

---

## 22. Conclusione

La sperimentazione condotta è sufficiente per valutare il comportamento del sistema rispetto ai requisiti del progetto B5.

I limiti individuati non compromettono la validità dei risultati, ma aiutano a interpretarli correttamente.

In particolare:

```text
- gli outlier di latenza sono plausibili in un ambiente EC2 t3.small raggiunto via Internet;
- la singola istanza limita la validazione di fault fisici multi-host;
- i dataset contenuti sono adeguati al Learner Lab ma non rappresentano carichi produttivi;
- la compaction produce snapshot logici ma non riduce fisicamente il WAL nel test osservato;
- CloudWatch sarebbe utile come estensione, ma non è necessario per dimostrare i requisiti principali.
```

Il progetto dimostra quindi in modo convincente il funzionamento end-to-end di un sistema distribuito containerizzato, resiliente e deployabile su cloud, pur restando consapevole dei limiti sperimentali dell'ambiente usato.
