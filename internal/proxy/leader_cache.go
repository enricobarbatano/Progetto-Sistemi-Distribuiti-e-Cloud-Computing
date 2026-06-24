// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene la cache thread-safe del leader conosciuto dal Proxy.
// La cache non effettua chiamate gRPC e non decide da sola chi sia il leader:
// viene aggiornata dal Router quando una chiamata GetLeader ha successo oppure
// quando un follower restituisce un leader_hint.
package proxy

import "sync"

// LeaderCache mantiene l'indirizzo del leader attualmente conosciuto.
//
// gRPC gestisce le richieste in goroutine separate, quindi più client possono
// arrivare contemporaneamente al Proxy. Per questo motivo l'accesso alla cache
// deve essere protetto da mutex.
//
// Questa struct non contiene logica di discovery: conserva solo il risultato
// più recente scoperto da altri componenti.
type LeaderCache struct {
	mu            sync.RWMutex
	leaderAddress string
}

// NewLeaderCache crea una cache leader vuota.
//
// All'avvio il Proxy potrebbe non conoscere ancora il leader. In quel caso il
// Router dovrà interrogare i seed nodes tramite GetLeader.
func NewLeaderCache() *LeaderCache {
	return &LeaderCache{}
}

// Get restituisce l'indirizzo del leader noto.
//
// Il secondo valore di ritorno indica se il leader è presente.
// Se non è ancora stato scoperto, found sarà false.
func (c *LeaderCache) Get() (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.leaderAddress == "" {
		return "", false
	}

	return c.leaderAddress, true
}

// Set aggiorna l'indirizzo del leader noto.
//
// Se viene passato un indirizzo vuoto, la funzione non modifica la cache.
// Questo evita di cancellare accidentalmente un leader valido con un hint vuoto.
func (c *LeaderCache) Set(leaderAddress string) {
	if leaderAddress == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.leaderAddress = leaderAddress
}

// Clear rimuove il leader attualmente noto.
//
// Viene usata quando il Proxy rileva che il leader in cache non è più
// raggiungibile o non è più leader.
func (c *LeaderCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leaderAddress = ""
}

// UpdateFromHint aggiorna la cache usando un leader_hint ricevuto da un follower.
//
// È semanticamente uguale a Set, ma mantiene più chiaro il punto del codice in
// cui l'indirizzo arriva da una risposta del cluster.
func (c *LeaderCache) UpdateFromHint(leaderHint string) {
	c.Set(leaderHint)
}
