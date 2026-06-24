// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene un endpoint HTTP minimale di health check.
// È pensato per Docker Compose o orchestratori che devono verificare se il
// processo del Proxy è vivo e pronto a ricevere richieste.
//
// L'health server non parla con i Consensus Node e non esegue routing.
// Controlla solo che il processo Proxy sia in esecuzione.
package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// HealthResponse rappresenta la risposta JSON dell'endpoint /health.
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// StartHealthServer avvia un server HTTP minimale per l'endpoint /health.
//
// La funzione avvia il server in una goroutine e restituisce subito il server
// creato. Il chiamante può usarlo per shutdown controllato in futuro.
func StartHealthServer(port string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%s", port),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	go func() {
		log.Printf("client proxy health endpoint listening on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server stopped with error: %v", err)
		}
	}()

	return server
}

func handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := HealthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("cannot encode health response: %v", err)
	}
}
