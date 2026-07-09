// Package storage implementa la macchina a stati key-value locale.
//
// Il package non contiene logica di consenso, elezione del leader o
// comunicazione di rete. La responsabilità di storage è limitata
// all'applicazione deterministica delle entry già committed e alla
// produzione/ripristino dello stato della mappa.
//
// Nel modello a Replicated State Machine, il package consensus decide
// quando una LogEntry è sicura perché replicata su quorum; questo package
// definisce invece come quella entry modifica lo stato applicativo locale.
package storage

import (
	"sync"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// KVStore rappresenta la state machine key-value locale di un Consensus Node.
//
// Lo store incapsula una map[string]string e applica in modo deterministico
// le operazioni del log già committed. Se due nodi applicano la stessa sequenza
// di LogEntry nello stesso ordine, ottengono lo stesso stato finale.
//
// Il mutex protegge la mappa da accessi concorrenti. Anche se molte chiamate
// arrivano già sincronizzate dal ConsensusNode, mantenere il lock dentro KVStore
// rende il componente più robusto e testabile in isolamento.
type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

// NewKVStore crea una nuova state machine key-value vuota.
//
// Viene usata durante l'inizializzazione del ConsensusNode. Dopo un crash o un
// restart, lo stato può essere ripristinato tramite Restore usando dati letti
// da state file, WAL o snapshot.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply applica una LogEntry già committed alla state machine locale.
//
// Questo metodo non esegue controlli di consenso e non verifica il quorum:
// queste responsabilità appartengono al package consensus. KVStore assume che
// l'entry ricevuta sia già stata considerata sicura dal ConsensusNode.
//
// L'applicazione è deterministica: PUT assegna il valore alla chiave, DELETE
// rimuove la chiave e NOOP non modifica lo stato. Questa proprietà è essenziale
// perché tutte le repliche convergano verso la stessa mappa key-value.
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

// Get restituisce il valore associato a una chiave locale.
//
// Il metodo legge solo la mappa in memoria e non decide se la lettura sia
// semanticamente valida rispetto al protocollo di consenso. Nel sistema attuale
// questa decisione viene presa dal ConsensusNode, che serve le letture dal leader
// per evitare dati obsoleti.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, ok := s.data[key]
	return value, ok
}

// Snapshot restituisce una copia dello stato key-value corrente.
//
// Il metodo viene usato dal livello di persistenza e dal meccanismo di snapshot
// per ottenere una rappresentazione stabile della state machine. Viene restituita
// una copia della mappa, non la mappa interna, così il chiamante non può modificare
// direttamente lo stato dello store.
func (s *KVStore) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyData := make(map[string]string, len(s.data))
	for key, value := range s.data {
		copyData[key] = value
	}

	return copyData
}

// Restore sostituisce lo stato corrente con i dati forniti.
//
// Questo metodo viene usato durante il recovery da state file, WAL o snapshot.
// La mappa ricevuta viene copiata per mantenere l'incapsulamento dello store:
// eventuali modifiche successive alla mappa passata dal chiamante non influenzano
// lo stato interno di KVStore.
func (s *KVStore) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]string, len(data))
	for key, value := range data {
		s.data[key] = value
	}
}

// Reset svuota completamente la state machine.
//
// Viene usato quando il ConsensusNode deve ricostruire lo stato applicando da
// capo le entry committed del log o quando un test richiede uno store pulito.
func (s *KVStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]string)
}
