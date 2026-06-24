// Package proxy contiene la logica interna del Client Proxy Service.
//
// Il Proxy è il punto di ingresso unico per i client esterni. Non salva dati
// applicativi e non partecipa al consenso: il suo compito è scoprire il leader
// del cluster, inoltrare le richieste al nodo corretto e nascondere ai client
// la complessità del cluster Raft.
//
// Questo file contiene solo la configurazione del Proxy. La configurazione viene
// letta da variabili d'ambiente, così il codice resta indipendente da indirizzi
// hardcoded e può essere usato facilmente in test locali o deployment diversi.
package proxy

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultProxyPort     = "8080"
	defaultHealthPort    = "8081"
	defaultRPCTimeoutMS  = 800
	defaultMaxRetries    = 3
	defaultBackoffMS     = 100
	defaultMaxBackoffMS  = 1500
	defaultJitterRatioMS = 100
)

// Config raccoglie tutti i parametri configurabili del Client Proxy.
//
// La struct non contiene logica di routing o chiamate gRPC. Serve solo a
// trasportare in modo ordinato i parametri letti dall'ambiente.
type Config struct {
	// Port è la porta gRPC su cui il Proxy espone il servizio KeyValueService.
	Port string

	// HealthPort è la porta HTTP su cui il Proxy espone /health.
	HealthPort string

	// ConsensusNodes contiene gli indirizzi dei Consensus Node usati come seed.
	// Il Proxy li interroga con GetLeader per scoprire il leader corrente.
	ConsensusNodes []string

	// RPCTimeout è il timeout massimo per una singola chiamata gRPC verso un nodo.
	RPCTimeout time.Duration

	// MaxRetries indica quante volte il Proxy può ritentare una richiesta
	// quando riceve errori temporanei o leader_hint.
	MaxRetries int

	// Backoff indica la pausa iniziale tra un retry e il successivo.
	// Il Router la usa come base per exponential backoff.
	Backoff time.Duration

	// MaxBackoff limita il tempo massimo di attesa tra due retry.
	MaxBackoff time.Duration

	// JitterRatio indica la percentuale massima di jitter applicata al backoff.
	// Esempio: 100 significa jitter fino al 100% del delay calcolato.
	JitterRatio int
}

// LoadConfigFromEnv costruisce la configurazione del Proxy leggendo variabili
// d'ambiente.
//
// Variabili supportate:
//
//   - PROXY_PORT: porta gRPC del Proxy, default 8080
//   - PROXY_HEALTH_PORT: porta HTTP per /health, default 8081
//   - CONSENSUS_NODES: lista separata da virgole dei nodi seed
//   - RPC_TIMEOUT_MS: timeout RPC in millisecondi, default 800
//   - MAX_RETRIES: numero massimo retry, default 3
//   - BACKOFF_MS: backoff iniziale in millisecondi, default 100
//   - MAX_BACKOFF_MS: backoff massimo in millisecondi, default 1500
//   - JITTER_RATIO: percentuale jitter, default 100
//
// Esempio:
//
//	set PROXY_PORT=8080
//	set PROXY_HEALTH_PORT=8081
//	set CONSENSUS_NODES=localhost:50051,localhost:50052,localhost:50053
//	set RPC_TIMEOUT_MS=800
//	set MAX_RETRIES=3
//	set BACKOFF_MS=100
//	set MAX_BACKOFF_MS=1500
//	set JITTER_RATIO=100
func LoadConfigFromEnv() (Config, error) {
	nodes := readStringListEnv("CONSENSUS_NODES")
	if len(nodes) == 0 {
		return Config{}, errors.New("CONSENSUS_NODES cannot be empty")
	}

	return Config{
		Port:           readStringEnv("PROXY_PORT", defaultProxyPort),
		HealthPort:     readStringEnv("PROXY_HEALTH_PORT", defaultHealthPort),
		ConsensusNodes: nodes,
		RPCTimeout:     time.Duration(readIntEnv("RPC_TIMEOUT_MS", defaultRPCTimeoutMS)) * time.Millisecond,
		MaxRetries:     readIntEnv("MAX_RETRIES", defaultMaxRetries),
		Backoff:        time.Duration(readIntEnv("BACKOFF_MS", defaultBackoffMS)) * time.Millisecond,
		MaxBackoff:     time.Duration(readIntEnv("MAX_BACKOFF_MS", defaultMaxBackoffMS)) * time.Millisecond,
		JitterRatio:    readIntEnv("JITTER_RATIO", defaultJitterRatioMS),
	}, nil
}

func readStringEnv(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

func readIntEnv(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultValue
	}

	return parsed
}

func readStringListEnv(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}

	rawParts := strings.Split(value, ",")
	result := make([]string, 0, len(rawParts))

	for _, part := range rawParts {
		cleanPart := strings.TrimSpace(part)
		if cleanPart != "" {
			result = append(result, cleanPart)
		}
	}

	return result
}
