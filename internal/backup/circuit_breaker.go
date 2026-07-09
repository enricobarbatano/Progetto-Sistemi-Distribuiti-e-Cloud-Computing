// Package backup implementa la logica interna del Backup Service.
//
// Questo file contiene la gestione dei Circuit Breaker usati durante le RPC
// amministrative verso i Consensus Node. Ogni nodo ha un circuito indipendente,
// così un nodo non disponibile non blocca l'intero ciclo di backup.
//
// Il Circuit Breaker non rende disponibile un nodo guasto, ma evita che il
// Backup Service continui a consumare tempo e risorse su chiamate destinate a
// fallire, permettendo backup parziali sui nodi ancora raggiungibili.

package backup

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
)

// CircuitBreakerManager gestisce i Circuit Breaker associati ai Consensus Node.
//
// Il manager mantiene un circuito per indirizzo di nodo. Questa granularità
// permette fault isolation: l'indisponibilità di un nodo non influenza
// direttamente le chiamate verso gli altri nodi del cluster.
type CircuitBreakerManager struct {
	// mu protegge la mappa dei circuiti durante accessi concorrenti.
	mu sync.Mutex

	// breakers associa ogni indirizzo di nodo al proprio Circuit Breaker.
	//
	// Circuiti separati evitano che gli errori di un nodo compromettano le RPC
	// amministrative verso nodi ancora disponibili.
	breakers map[string]*gobreaker.CircuitBreaker[any]
}

// NewCircuitBreakerManager crea un gestore di Circuit Breaker vuoto pronto
// ad essere usato per proteggere le RPC verso i Consensus Node del cluster.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*gobreaker.CircuitBreaker[any]),
	}
}

// Execute esegue una RPC amministrativa protetta dal Circuit Breaker del nodo.
//
// Se il circuito è Closed, la chiamata viene eseguita normalmente. Dopo più
// fallimenti consecutivi, il circuito può passare a Open e rifiutare nuove
// chiamate verso quel nodo per un intervallo di tempo. Lo stato Half-Open
// consente poi alcune richieste di prova.
//
// Questo meccanismo permette al Backup Service di proseguire sui nodi sani
// quando un nodo è offline o temporaneamente irraggiungibile.
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

// breakerForAddress restituisce il Circuit Breaker associato a un indirizzo.
//
// Se il circuito non esiste ancora, viene creato con una configurazione
// prudente: dopo tre fallimenti consecutivi il circuito si apre, resta aperto
// per alcuni secondi e poi passa in Half-Open per verificare se il nodo è
// tornato disponibile.
func (m *CircuitBreakerManager) breakerForAddress(address string) *gobreaker.CircuitBreaker[any] {
	m.mu.Lock()
	defer m.mu.Unlock()

	if breaker, ok := m.breakers[address]; ok {
		return breaker
	}

	settings := gobreaker.Settings{
		Name:        fmt.Sprintf("backup-node-%s", address),
		MaxRequests: 2,
		Interval:    30 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("backup circuit breaker %s changed state from %s to %s", name, from.String(), to.String())
		},
	}

	breaker := gobreaker.NewCircuitBreaker[any](settings)
	m.breakers[address] = breaker

	return breaker
}
