// Questo file contiene la cache thread-safe del leader conosciuto dal Proxy.
//
// La cache è un'ottimizzazione della Client-side Service Discovery: consente al
// Proxy di riutilizzare l'ultimo leader noto invece di interrogare tutti i nodi
// a ogni richiesta. La cache non è una fonte di verità permanente e può essere
// invalidata dopo errori, cambio di leader o leader_hint ricevuti dai follower.

package proxy

import "sync"

// LeaderInfo descrive il leader conosciuto dal Proxy.
//
// Include identificativo del nodo, indirizzo gRPC e termine associato. Il
// termine aiuta a distinguere informazioni recenti da informazioni
// potenzialmente obsolete dopo una nuova elezione.

type LeaderInfo struct {
	ID      string
	Address string
	Term    uint64
}

// LeaderCache memorizza in modo thread-safe l'ultimo leader noto.
//
// Il Proxy può servire più richieste concorrenti; per questo l'accesso alla
// cache è protetto da mutex. La cache non è una fonte di verità definitiva:
// può essere invalidata o aggiornata tramite GetLeader e leader_hint.
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

// Get restituisce il leader attualmente memorizzato nella cache.
//
// Se la cache è vuota, found=false indica al Router che deve eseguire una
// discovery completa.
func (c *LeaderCache) Get() (LeaderInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.leader.Address == "" {
		return LeaderInfo{}, false
	}

	return c.leader, true
}

// Set aggiorna la cache con un nuovo leader noto.
//
// Viene chiamato dopo una discovery riuscita o dopo aver ricevuto un leader_hint
// da un follower.
func (c *LeaderCache) Set(leader LeaderInfo) {
	if leader.Address == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.leader = leader
}

// Clear invalida la cache del leader.
//
// Viene usato quando il leader memorizzato non risponde più, rifiuta la richiesta
// o sembra non essere più valido. La richiesta successiva dovrà eseguire una
// nuova discovery.
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
