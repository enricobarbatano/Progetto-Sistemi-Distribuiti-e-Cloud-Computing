// Package backup contiene la logica interna del Backup Service.
//
// Questo file contiene un CircuitBreakerManager simile a quello del Proxy.
// Ogni Consensus Node ha un circuito indipendente, così un nodo guasto non
// blocca l'intero ciclo di backup.
package backup

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
)

type CircuitBreakerManager struct {
	mu       sync.Mutex
	breakers map[string]*gobreaker.CircuitBreaker[any]
}

func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*gobreaker.CircuitBreaker[any]),
	}
}

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
