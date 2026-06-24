// Package storage contiene la macchina a stati key-value del sistema.
//
// Questo package non conosce nulla del protocollo Raft, dei leader,
// delle elezioni o della replicazione. Si occupa solo di mantenere
// una mappa chiave-valore locale e di applicare operazioni già committed.
//
// In pratica, il ConsensusNode decide quando una LogEntry è sicura
// e committed; KVStore decide solo come quella entry modifica lo stato.
package storage

import (
	"sync"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// KVStore rappresenta la macchina a stati key-value locale.
//
// Questa struct incapsula la map[string]string che prima era dentro
// ConsensusNode. In questo modo il nodo di consenso non modifica più
// direttamente la mappa dei dati, ma delega allo storage l'applicazione
// delle entry committed.
//
// La presenza del mutex rende KVStore utilizzabile anche in modo autonomo.
// Nel progetto attuale molte chiamate arrivano già mentre ConsensusNode
// possiede il proprio lock, ma mantenere il lock qui rende il componente
// più robusto e più facile da testare separatamente.
type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

// NewKVStore crea una nuova macchina a stati key-value vuota.
//
// Viene chiamata dal ConsensusNode durante l'inizializzazione del nodo.
// Dopo il recovery, lo stato può essere ripristinato usando Restore.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply applica una LogEntry già committed alla mappa key-value.
//
// Questa funzione non deve essere chiamata su entry non committed.
// Il consenso e il quorum restano responsabilità del package consensus.
// Qui viene solo eseguita l'operazione concreta sulla mappa locale.
func (s *KVStore) Apply(entry *consensuspb.LogEntry) {
	if entry == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch entry.Operation {
	case consensuspb.LogOperation_LOG_OPERATION_PUT:
		s.data[entry.Key] = entry.Value

	case consensuspb.LogOperation_LOG_OPERATION_DELETE:
		delete(s.data, entry.Key)

	case consensuspb.LogOperation_LOG_OPERATION_NOOP:
		// NOOP non modifica la macchina a stati.

	default:
		// Operazioni non riconosciute vengono ignorate in questa fase.
	}
}

// Get restituisce il valore associato a una chiave.
//
// Il metodo non effettua controlli di consistenza Raft.
// Se una Get può essere servita oppure no viene deciso dal ConsensusNode,
// che in questa fase consente letture solo dal leader.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	return value, ok
}

// Snapshot restituisce una copia dello stato key-value corrente.
//
// Viene usato dal livello di persistenza per salvare state.json,
// WAL snapshot-based e snapshot locale. Restituisce sempre una copia,
// così il chiamante non può modificare direttamente la mappa interna.
func (s *KVStore) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyData := make(map[string]string, len(s.data))
	for key, value := range s.data {
		copyData[key] = value
	}

	return copyData
}

// Restore sostituisce lo stato key-value corrente con quello fornito.
//
// Viene usato durante il recovery da state.json, WAL o snapshot.
// Anche qui viene fatta una copia, così la mappa interna dello store
// resta isolata dalla mappa passata dal chiamante.
func (s *KVStore) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]string, len(data))
	for key, value := range data {
		s.data[key] = value
	}
}

// Reset svuota completamente la macchina a stati.
//
// Serve quando il ConsensusNode deve ricostruire la data map applicando
// da capo le entry committed del log.
func (s *KVStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]string)
}
