// Questo file contiene la gestione dei Circuit Breaker usati dal Proxy.
//
// Ogni Consensus Node è protetto da un circuito indipendente. In questo modo,
// se un nodo diventa non disponibile, il Proxy evita di continuare a inviare
// richieste inutili verso quel nodo e preserva risorse per i nodi sani.
package proxy

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
)

// CircuitBreakerManager gestisce i Circuit Breaker associati ai nodi.
//
// Il manager mantiene un circuito per nodo o indirizzo. Questa granularità
// consente fault isolation: il guasto di un nodo non impedisce al Proxy di
// contattare gli altri nodi del cluster.
type CircuitBreakerManager struct {
	mu sync.Mutex

	// breakers associa ogni nodo al proprio Circuit Breaker.
	//
	// Circuiti separati evitano che gli errori di un peer influenzino le richieste
	// verso peer ancora disponibili.
	breakers map[string]*gobreaker.CircuitBreaker[any]
}

// NewCircuitBreakerManager crea un nuovo manager vuoto.
//
// I circuiti vengono creati in modo lazy, cioè solo quando il Proxy deve
// eseguire la prima richiesta verso uno specifico nodo.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*gobreaker.CircuitBreaker[any]),
	}
}

// Execute esegue una RPC protetta dal Circuit Breaker del nodo.
//
// Se il circuito è Closed, la chiamata viene eseguita normalmente. Se gli errori
// superano la soglia configurata, il circuito può passare a Open e rifiutare
// temporaneamente nuove chiamate verso quel nodo. Dopo il timeout, lo stato
// Half-Open consente richieste di prova.
//
// Questo meccanismo evita cascading failure: il Proxy non continua a consumare
// tempo e risorse su nodi notoriamente non disponibili.
func (m *CircuitBreakerManager) Execute(address string, req func() (any, error)) (any, error) {
	if address == "" {
		return nil, fmt.Errorf("circuit breaker address cannot be empty")
	}

	breaker := m.breakerForAddress(address)

	result, err := breaker.Execute(req)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// breakerForAddress restituisce il Circuit Breaker associato a un nodo.
//
// Se non esiste ancora, lo crea con una configurazione prudente per test locali:
// dopo alcune failure consecutive il circuito si apre, resta aperto per qualche
// secondo e poi passa in half-open per provare a recuperare.
func (m *CircuitBreakerManager) breakerForAddress(address string) *gobreaker.CircuitBreaker[any] {
	m.mu.Lock()
	defer m.mu.Unlock()

	if breaker, ok := m.breakers[address]; ok {
		return breaker
	}

	settings := gobreaker.Settings{
		Name:        fmt.Sprintf("consensus-node-%s", address),
		MaxRequests: 2,
		Interval:    30 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("circuit breaker %s changed state from %s to %s", name, from.String(), to.String())
		},
	}

	breaker := gobreaker.NewCircuitBreaker[any](settings)
	m.breakers[address] = breaker

	return breaker
}
