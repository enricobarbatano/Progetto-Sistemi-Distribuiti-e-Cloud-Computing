# Fase 6 - Client Proxy Service

Questo documento descrive la **Fase 6** del progetto, dedicata allo sviluppo del **Client Proxy Service**.

Il Client Proxy Service è il punto di ingresso unico per i client esterni. Il suo scopo è nascondere ai client la complessità del cluster di Consensus Node, occupandosi di:

- scoprire dinamicamente il leader;
- inoltrare `Put`, `Get` e `Delete` al leader corretto;
- usare `leader_hint` per aggiornare rapidamente la vista del leader;
- mantenere la forte consistenza facendo passare anche le letture dal leader;
- proteggere le chiamate verso i nodi tramite Circuit Breaker;
- mantenere una struttura interna modulare, evitando una nuova God Class.

---

## 1. Obiettivo della fase

Prima della Fase 6, il client di test comunicava direttamente con i Consensus Node tramite la variabile:

```cmd
set TARGET=localhost:50051
```

oppure:

```cmd
set TARGET=localhost:50052
set TARGET=localhost:50053
```

Questo obbligava il client a conoscere direttamente la topologia del cluster e, in alcuni casi, a dover sapere quale nodo fosse leader.

Con la Fase 6 viene introdotto un nuovo componente:

```text
Client Proxy Service
```

Il client ora comunica solo con il Proxy:

```cmd
set TARGET=localhost:8080
```

Il Proxy si occupa internamente di trovare il leader e inoltrare la richiesta al nodo corretto.

---

## 2. Requisiti architetturali

Il Proxy è stato progettato come componente **stateless rispetto ai dati applicativi**.

Questo significa che il Proxy:

- non salva coppie chiave-valore;
- non mantiene una propria state machine;
- non partecipa al protocollo Raft;
- non replica log;
- non decide commit;
- non applica entry.

Il Proxy mantiene solo stato operativo leggero:

```text
leader cache
connessioni gRPC verso nodi
circuit breaker per nodo
configurazione runtime
```

Questo stato serve solo per instradare meglio le richieste.

---

## 3. Struttura finale della Fase 6

Per evitare una nuova God Class, la logica del Proxy è stata divisa in più file:

```text
internal/
  proxy/
    config.go
    leader_cache.go
    node_client.go
    circuit_breaker.go
    router.go
    service.go

cmd/
  client-proxy/
    main.go
```

Ogni file ha una responsabilità specifica.

---

## 4. `internal/proxy/config.go`

Il file `config.go` contiene la configurazione del Proxy.

Responsabilità principali:

- leggere le variabili d'ambiente;
- validare la lista dei Consensus Node;
- configurare timeout RPC;
- configurare retry;
- configurare backoff;
- configurare la porta del Proxy.

Variabili d'ambiente supportate:

```cmd
set PROXY_PORT=8080
set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
set RPC_TIMEOUT_MS=800
set MAX_RETRIES=3
set BACKOFF_MS=100
```

La struct principale è:

```go
type Config struct {
    Port           string
    ConsensusNodes []string
    RPCTimeout     time.Duration
    MaxRetries     int
    Backoff        time.Duration
}
```

Il metodo principale è:

```go
func LoadConfigFromEnv() (Config, error)
```

Questo file non fa routing e non apre connessioni gRPC.

---

## 5. `internal/proxy/leader_cache.go`

Il file `leader_cache.go` contiene una cache thread-safe del leader noto.

Responsabilità principali:

- conservare l'indirizzo del leader conosciuto;
- aggiornare il leader tramite discovery;
- aggiornare il leader tramite `leader_hint`;
- cancellare il leader dalla cache se una richiesta fallisce;
- proteggere gli accessi concorrenti con mutex.

La struct principale è:

```go
type LeaderCache struct {
    mu            sync.RWMutex
    leaderAddress string
}
```

Metodi principali:

```go
func NewLeaderCache() *LeaderCache
func (c *LeaderCache) Get() (string, bool)
func (c *LeaderCache) Set(leaderAddress string)
func (c *LeaderCache) Clear()
func (c *LeaderCache) UpdateFromHint(leaderHint string)
```

Questa struct non esegue RPC e non decide da sola chi sia il leader.

---

## 6. `internal/proxy/node_client.go`

Il file `node_client.go` contiene il client gRPC interno usato dal Proxy per comunicare con i Consensus Node.

Responsabilità principali:

- creare connessioni gRPC verso i nodi;
- riusare connessioni esistenti;
- invocare `GetLeader` su un nodo specifico;
- invocare `Put` su un nodo specifico;
- invocare `Get` su un nodo specifico;
- invocare `Delete` su un nodo specifico;
- chiudere le connessioni in fase di shutdown.

La struct principale è:

```go
type NodeClient struct {
    mu      sync.Mutex
    conns   map[string]*grpc.ClientConn
    clients map[string]kvpb.KeyValueServiceClient
}
```

Metodi principali:

```go
func NewNodeClient() *NodeClient
func (c *NodeClient) GetLeader(ctx context.Context, address string) (*kvpb.GetLeaderResponse, error)
func (c *NodeClient) Put(ctx context.Context, address string, key string, value string) (*kvpb.PutResponse, error)
func (c *NodeClient) Get(ctx context.Context, address string, key string) (*kvpb.GetResponse, error)
func (c *NodeClient) Delete(ctx context.Context, address string, key string) (*kvpb.DeleteResponse, error)
func (c *NodeClient) Close()
```

`NodeClient` non contiene logica di retry, circuit breaker o discovery. Queste responsabilità sono delegate al Router.

---

## 7. `internal/proxy/circuit_breaker.go`

Il file `circuit_breaker.go` contiene il `CircuitBreakerManager`.

Responsabilità principali:

- mantenere un Circuit Breaker per ogni Consensus Node;
- evitare chiamate ripetute verso nodi che stanno fallendo;
- permettere recovery tramite stato half-open;
- loggare i cambi di stato del breaker.

La struct principale è:

```go
type CircuitBreakerManager struct {
    mu       sync.Mutex
    breakers map[string]*gobreaker.CircuitBreaker[any]
}
```

Metodo principale:

```go
func (m *CircuitBreakerManager) Execute(address string, req func() (any, error)) (any, error)
```

Ogni nodo ha un circuito indipendente:

```text
localhost:50051 -> breaker dedicato
localhost:50052 -> breaker dedicato
localhost:50053 -> breaker dedicato
```

In questo modo, se un nodo non è raggiungibile, il Proxy può evitare di continuare a chiamarlo senza bloccare automaticamente tutto il cluster.

---

## 8. `internal/proxy/router.go`

Il file `router.go` contiene la logica centrale di instradamento.

Responsabilità principali:

- scoprire il leader interrogando i seed nodes;
- usare la cache del leader;
- usare `leader_hint` per aggiornare la cache;
- inoltrare `Put`, `Get` e `Delete` al leader;
- usare `NodeClient` per le chiamate gRPC;
- usare `CircuitBreakerManager` per proteggere le chiamate;
- applicare retry e backoff.

La struct principale è:

```go
type Router struct {
    config   Config
    cache    *LeaderCache
    client   *NodeClient
    breakers *CircuitBreakerManager
}
```

Metodi principali:

```go
func NewRouter(config Config, cache *LeaderCache, client *NodeClient, breakers *CircuitBreakerManager) *Router
func (r *Router) DiscoverLeader(ctx context.Context) (string, error)
func (r *Router) Put(ctx context.Context, key string, value string) (*kvpb.PutResponse, error)
func (r *Router) Get(ctx context.Context, key string) (*kvpb.GetResponse, error)
func (r *Router) Delete(ctx context.Context, key string) (*kvpb.DeleteResponse, error)
```

Il Router è il componente che coordina gli altri componenti del Proxy, ma non espone direttamente un server gRPC.

---

## 9. `internal/proxy/service.go`

Il file `service.go` contiene l'implementazione gRPC esterna del Proxy.

Responsabilità principali:

- implementare `kvpb.KeyValueServiceServer`;
- ricevere richieste client;
- delegare tutto al Router.

La struct principale è:

```go
type ProxyService struct {
    kvpb.UnimplementedKeyValueServiceServer
    router *Router
}
```

Metodi principali:

```go
func NewProxyService(router *Router) *ProxyService
func (s *ProxyService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error)
func (s *ProxyService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error)
func (s *ProxyService) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error)
func (s *ProxyService) GetLeader(ctx context.Context, req *kvpb.GetLeaderRequest) (*kvpb.GetLeaderResponse, error)
```

`ProxyService` non contiene logica di discovery, retry o circuit breaker. Si limita a ricevere RPC esterne e delegare.

---

## 10. `cmd/client-proxy/main.go`

Il file `cmd/client-proxy/main.go` è il bootstrap applicativo del Proxy.

Responsabilità principali:

- leggere la configurazione;
- creare `LeaderCache`;
- creare `NodeClient`;
- creare `CircuitBreakerManager`;
- creare `Router`;
- creare `ProxyService`;
- avviare il server gRPC sulla porta configurata;
- registrare `KeyValueServiceServer`.

Il `main.go` rimane intenzionalmente piccolo. Non contiene la logica del Proxy.

Avvio tipico:

```cmd
set PROXY_PORT=8080
set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
set RPC_TIMEOUT_MS=800
set MAX_RETRIES=3
set BACKOFF_MS=100
go run .\cmd\client-proxy
```

Output atteso:

```text
client proxy listening on port 8080 with consensus nodes [localhost:50051 localhost:50052 localhost:50053]
```

---

## 11. Flusso di una Put via Proxy

Il flusso di una scrittura è:

```text
bench-client
  -> Client Proxy localhost:8080
    -> Router
      -> LeaderCache
      -> se leader sconosciuto: DiscoverLeader
      -> NodeClient.Put verso il leader
      -> CircuitBreakerManager protegge la chiamata
        -> Consensus Node leader
          -> append log
          -> replica quorum
          -> commit
          -> apply su KVStore
        <- risposta success=true
    <- ProxyService
<- bench-client
```

Il client non deve conoscere il leader e non deve conoscere la lista dei nodi.

---

## 12. Forte consistenza

Il Proxy inoltra sia scritture sia letture al leader.

Regole:

```text
Put    -> leader
Delete -> leader
Get    -> leader
```

Questo mantiene il comportamento già scelto nella Fase 5: evitare letture stale dai follower.

---

## 13. Uso di leader_hint

Se il Proxy invia una richiesta a un follower, il Consensus Node risponde con:

```text
error=node is not leader
leader_hint=<leader-address>
```

Il Router usa `leader_hint` per aggiornare la cache:

```go
r.cache.UpdateFromHint(resp.LeaderHint)
```

Poi ritenta la richiesta verso il nuovo leader.

Questo riduce il numero di discovery complete e rende il Proxy più reattivo ai cambi di leader.

---

## 14. Test di compilazione

Dopo l'implementazione del Proxy è stato eseguito:

```cmd
gofmt -w cmd\client-proxy\main.go internal\proxy\config.go internal\proxy\leader_cache.go internal\proxy\node_client.go internal\proxy\circuit_breaker.go internal\proxy\router.go internal\proxy\service.go
```

Poi:

```cmd
go test ./...
```

L'output conferma che tutti i package compilano correttamente, incluso:

```text
internal/proxy [no test files]
cmd/client-proxy [no test files]
```

---

## 15. Test runtime: discovery leader via Proxy

Il client è stato configurato per parlare con il Proxy:

```cmd
set TARGET=localhost:8080
set OP=leader
go run .\cmd\bench-client
```

Output:

```text
GetLeader response: has_leader=true leader_id= leader_address=localhost:50051 term=0
```

Il leader effettivo era `node-1`, come mostrato dai log del nodo:

```text
node node-1 became leader for term 35
```

Il Proxy ha quindi scoperto correttamente il leader e ha restituito l'indirizzo:

```text
localhost:50051
```

Nota: in questa prima versione il Proxy conserva principalmente `leader_address`, quindi `leader_id` e `term` non sono ancora valorizzati nella risposta del Proxy.

---

## 16. Test runtime: Put via Proxy

È stata eseguita una scrittura passando solo dal Proxy:

```cmd
set TARGET=localhost:8080
set OP=put
set KEY=proxy-key
set VALUE=proxy-value
go run .\cmd\bench-client
```

Output:

```text
Put response: success=true key=proxy-key value=proxy-value error= leader_hint=
```

Questo conferma che il Proxy ha inoltrato la richiesta al leader e che il cluster ha confermato la scrittura dopo il quorum.

---

## 17. Test runtime: Get via Proxy

Dopo la `Put`, è stata eseguita una lettura passando solo dal Proxy:

```cmd
set TARGET=localhost:8080
set OP=get
set KEY=proxy-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=true key=proxy-key value=proxy-value error= leader_hint=localhost:50051
```

Questo conferma che:

- il valore è stato scritto;
- il Proxy ha inoltrato la lettura al leader;
- il leader ha letto il valore dalla state machine;
- la risposta è tornata al client tramite Proxy.

---

## 18. Test runtime: Delete via Proxy

È stata poi eseguita una cancellazione passando solo dal Proxy:

```cmd
set TARGET=localhost:8080
set OP=delete
set KEY=proxy-key
go run .\cmd\bench-client
```

Output:

```text
Delete response: success=true key=proxy-key error= leader_hint=
```

Questo conferma che il Proxy inoltra correttamente anche le cancellazioni al leader.

---

## 19. Test runtime: Get dopo Delete via Proxy

Dopo la cancellazione, è stata eseguita una nuova `Get`:

```cmd
set TARGET=localhost:8080
set OP=get
set KEY=proxy-key
go run .\cmd\bench-client
```

Output:

```text
Get response: found=false key=proxy-key value= error= leader_hint=localhost:50051
```

Questo conferma che la `Delete` è stata applicata correttamente alla state machine del leader.

---

## 20. Stato finale della Fase 6

Checklist:

```text
[OK] Config del Proxy da variabili d'ambiente
[OK] Proxy stateless rispetto ai dati applicativi
[OK] LeaderCache thread-safe
[OK] NodeClient con connessioni gRPC riusabili
[OK] CircuitBreakerManager per nodo
[OK] Router con discovery leader
[OK] Router con leader_hint
[OK] Router con retry e backoff
[OK] ProxyService gRPC separato
[OK] main.go minimale
[OK] GetLeader via Proxy
[OK] Put via Proxy
[OK] Get via Proxy
[OK] Delete via Proxy
[OK] Get dopo Delete via Proxy
[OK] client esterno comunica solo con localhost:8080
```

---

## 21. Limiti attuali

La Fase 6 implementa il Proxy base, ma restano alcuni miglioramenti possibili:

- `GetLeader` del Proxy restituisce `leader_address`, ma non ancora `leader_id` e `term` reali;
- il retry usa un backoff semplice lineare, non ancora exponential backoff completo;
- non sono ancora presenti test automatici per failover leader tramite Proxy;
- il Circuit Breaker è presente ma non è ancora stato stressato con test automatici;
- il Proxy non espone ancora metriche o health check;
- non è ancora implementata una configurazione da file, solo da variabili d'ambiente.

---

## 22. Prossimi step

Dopo la Fase 6, i prossimi obiettivi consigliati sono:

```text
1. migliorare LeaderCache per conservare anche leader_id e term;
2. aggiungere test di failover leader via Proxy;
3. testare Circuit Breaker spegnendo nodi del cluster;
4. aggiungere health check del Proxy;
5. preparare il Backup Service e la gestione snapshot remoti;
6. evolvere WAL e snapshot verso log compaction reale.
```
