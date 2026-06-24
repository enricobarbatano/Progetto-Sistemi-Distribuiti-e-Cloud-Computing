// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene CircuitBreakerManager, il componente responsabile
// della protezione delle chiamate verso i Consensus Node tramite Circuit Breaker.
//
// Il Circuit Breaker evita che il Proxy continui a inviare richieste a un nodo
// che sta fallendo ripetutamente. In questo modo si riduce il rischio di
// accumulare timeout, bloccare richieste client o peggiorare un guasto già in corso.
//
// Il manager mantiene un circuito separato per ogni nodo del cluster:
// se node-1 è irraggiungibile, il circuito di node-1 può aprirsi senza bloccare
// le chiamate verso node-2 o node-3.
package proxy

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
)

// CircuitBreakerManager mantiene un Circuit Breaker per ogni Consensus Node.
//
// La chiave della mappa è l'indirizzo del nodo, ad esempio:
//
//	localhost:50051
//
// Ogni nodo ha un circuito indipendente, così un nodo guasto non rende
// automaticamente indisponibile tutto il cluster.
type CircuitBreakerManager struct {
	mu       sync.Mutex
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

// Execute esegue una richiesta protetta dal Circuit Breaker del nodo indicato.
//
// Se il circuito è closed, la richiesta viene eseguita normalmente.
// Se il circuito è open, gobreaker restituisce subito errore senza chiamare req.
// Se il circuito è half-open, lascia passare un numero limitato di richieste
// di prova per capire se il nodo è tornato sano.
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
