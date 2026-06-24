// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene la cache thread-safe del leader conosciuto dal Proxy.
// La cache non effettua chiamate gRPC e non decide da sola chi sia il leader:
// viene aggiornata dal Router quando una chiamata GetLeader ha successo oppure
// quando un follower restituisce un leader_hint.
package proxy

import "sync"

// LeaderInfo rappresenta il leader noto al Proxy.
//
// Address è l'informazione necessaria per il routing.
// ID e Term migliorano osservabilità e debug, perché permettono al Proxy di
// restituire una risposta GetLeader più completa al client.
type LeaderInfo struct {
	ID      string
	Address string
	Term    uint64
}

// LeaderCache mantiene il leader attualmente conosciuto.
//
// gRPC gestisce le richieste in goroutine separate, quindi più client possono
// arrivare contemporaneamente al Proxy. Per questo motivo l'accesso alla cache
// deve essere protetto da mutex.
//
// Questa struct non contiene logica di discovery: conserva solo il risultato
// più recente scoperto da altri componenti.
type LeaderCache struct {
	mu     sync.RWMutex
	leader LeaderInfo
}

// NewLeaderCache crea una cache leader vuota.
//
// All'avvio il Proxy potrebbe non conoscere ancora il leader. In quel caso il
// Router dovrà interrogare i seed nodes tramite GetLeader.
func NewLeaderCache() *LeaderCache {
	return &LeaderCache{}
}

// Get restituisce il leader noto.
//
// Il secondo valore di ritorno indica se il leader è presente.
// Se non è ancora stato scoperto, found sarà false.
func (c *LeaderCache) Get() (LeaderInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.leader.Address == "" {
		return LeaderInfo{}, false
	}

	return c.leader, true
}

// Set aggiorna il leader noto.
//
// Se viene passato un indirizzo vuoto, la funzione non modifica la cache.
// Questo evita di cancellare accidentalmente un leader valido con un hint vuoto.
func (c *LeaderCache) Set(leader LeaderInfo) {
	if leader.Address == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.leader = leader
}

// Clear rimuove il leader attualmente noto.
//
// Viene usata quando il Proxy rileva che il leader in cache non è più
// raggiungibile o non è più leader.
func (c *LeaderCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leader = LeaderInfo{}
}

// UpdateFromHint aggiorna la cache usando un leader_hint ricevuto da un follower.
//
// Il leader_hint contiene solo l'indirizzo, quindi ID e Term vengono lasciati
// vuoti. Alla successiva discovery completa verranno valorizzati di nuovo.
func (c *LeaderCache) UpdateFromHint(leaderHint string) {
	if leaderHint == "" {
		return
	}

	c.Set(LeaderInfo{
		Address: leaderHint,
	})
}
