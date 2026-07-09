// Package backup contiene la logica interna del Backup Service.
//
// LogCompactor invia richieste CompactLog ai Consensus Node. Non scarica
// snapshot e non mantiene stato del ciclo di backup.
package backup

import (
	"context"
	"fmt"
	"time"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
)

// LogCompactor invia la richiesta di compattazione del log ai nodi remoti dopo
// un backup riuscito, senza dover gestire direttamente lo storage locale.
type LogCompactor struct {
	config   Config
	client   *NodeClient
	breakers *CircuitBreakerManager
}

// NewLogCompactor crea un compattatore pronto a inviare richieste di compaction
// ai Consensus Node del cluster.
func NewLogCompactor(config Config, client *NodeClient, breakers *CircuitBreakerManager) *LogCompactor {
	return &LogCompactor{
		config:   config,
		client:   client,
		breakers: breakers,
	}
}

// Compact richiede la compattazione del log di un nodo fino a un indice
// specifico, usando retry e Circuit Breaker per isolare nodi indisponibili.
func (c *LogCompactor) Compact(ctx context.Context, address string, snapshotID string, upToIndex uint64) (*backuppb.CompactLogResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, c.config.RPCTimeout)

		result, err := c.breakers.Execute(address, func() (any, error) {
			return c.client.CompactLog(callCtx, address, c.config.ServiceID, snapshotID, upToIndex)
		})

		cancel()

		if err == nil {
			resp, ok := result.(*backuppb.CompactLogResponse)
			if !ok {
				return nil, fmt.Errorf("unexpected CompactLog response type from %s", address)
			}

			return resp, nil
		}

		lastErr = err
		c.sleepBeforeRetry(attempt)
	}

	return nil, lastErr
}

func (c *LogCompactor) sleepBeforeRetry(attempt int) {
	if c.config.Backoff <= 0 || attempt >= c.config.MaxRetries {
		return
	}

	time.Sleep(time.Duration(attempt+1) * c.config.Backoff)
}
