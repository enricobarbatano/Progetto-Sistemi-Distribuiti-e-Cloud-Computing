// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la parte di ConsensusNode legata allo stato persistente,
// al recovery e all'applicazione delle entry committed alla state machine.
//
// La persistenza fisica dei file è delegata a internal/persistence, mentre la
// mappa key-value è delegata a internal/storage. Questo file resta nel package
// consensus perché coordina questi componenti con gli indici Raft commitIndex
// e lastApplied.
package consensus

import (
	"log"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// cloneDataLocked restituisce una copia dello stato key-value corrente.
//
// Il ConsensusNode non accede direttamente alla mappa dati, ma chiede allo
// store uno snapshot della state machine. Deve essere chiamata quando lo stato
// del nodo è già protetto dal lock del ConsensusNode.
func (n *ConsensusNode) cloneDataLocked() map[string]string {
	return n.store.Snapshot()
}

// persistentStateLocked costruisce lo stato persistente corrente del nodo.
//
// Questa funzione traduce i campi interni di ConsensusNode nella struct
// persistence.State, che poi viene salvata dal Persistence Manager.
func (n *ConsensusNode) persistentStateLocked() persistence.State {
	return persistence.State{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		Data:        n.cloneDataLocked(),
	}
}

// persistLocked salva lo stato corrente del nodo.
//
// ConsensusNode decide quando salvare, ma non conosce più i dettagli di
// state.json, WAL e file temporanei. Questi dettagli sono responsabilità del
// persistence.Manager.
func (n *ConsensusNode) persistLocked() error {
	return n.persistenceManager.Save(n.persistentStateLocked())
}

// saveSnapshotLocked salva uno snapshot locale della state machine.
//
// Lo snapshot contiene l'ultimo indice applicato, il termine corrispondente e
// una copia della mappa key-value. Per ora non esegue log compaction.
func (n *ConsensusNode) saveSnapshotLocked() error {
	if n.lastApplied == 0 {
		return nil
	}

	lastIncludedTerm, _ := n.logTermAtIndexLocked(n.lastApplied)

	return n.persistenceManager.SaveSnapshot(persistence.Snapshot{
		LastIncludedIndex: n.lastApplied,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              n.cloneDataLocked(),
	})
}

// maybeSaveSnapshotLocked decide se salvare uno snapshot locale.
//
// Lo snapshot viene creato solo quando lastApplied è multiplo della soglia
// configurata. Nei test si può usare SNAPSHOT_THRESHOLD=1 per forzare la
// creazione immediata dello snapshot.
func (n *ConsensusNode) maybeSaveSnapshotLocked() {
	if n.lastApplied == 0 || n.snapshotThreshold == 0 {
		return
	}

	if n.lastApplied%n.snapshotThreshold != 0 {
		return
	}

	if err := n.saveSnapshotLocked(); err != nil {
		log.Printf("node %s cannot save snapshot: %v", n.id, err)
	}
}

// loadPersistentState carica lo stato persistente del nodo all'avvio.
//
// L'ordine è:
//  1. carica state.json oppure fallback dall'ultimo record WAL valido;
//  2. carica uno snapshot locale se più recente;
//  3. applica eventuali entry committed non ancora applicate;
//  4. se necessario, risalva lo stato compatto.
func (n *ConsensusNode) loadPersistentState() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	state, stateFound, err := n.persistenceManager.Load()
	if err != nil {
		return err
	}

	if stateFound {
		n.restorePersistentStateLocked(state)
	}

	snapshot, snapshotFound, err := n.persistenceManager.LoadSnapshot()
	if err != nil {
		return err
	}

	snapshotApplied := false
	if snapshotFound {
		snapshotApplied = n.restoreSnapshotIfNewerLocked(snapshot)
	}

	committedEntriesApplied := false
	if n.commitIndex > n.lastApplied {
		previousLastApplied := n.lastApplied
		n.applyCommittedEntriesLocked()
		committedEntriesApplied = n.lastApplied != previousLastApplied
	}

	if !stateFound || snapshotApplied || committedEntriesApplied {
		return n.persistLocked()
	}

	return nil
}

// restoreSnapshotIfNewerLocked applica lo snapshot solo se è più recente.
//
// Questo evita di sovrascrivere una state machine già più aggiornata caricata
// da state.json o dal WAL.
func (n *ConsensusNode) restoreSnapshotIfNewerLocked(snapshot persistence.Snapshot) bool {
	if snapshot.Data == nil {
		return false
	}

	if snapshot.LastIncludedIndex <= n.lastApplied {
		return false
	}

	n.store.Restore(snapshot.Data)
	n.lastApplied = snapshot.LastIncludedIndex

	if snapshot.LastIncludedIndex > n.commitIndex {
		n.commitIndex = snapshot.LastIncludedIndex
	}

	return true
}

// restorePersistentStateLocked ripristina i campi persistenti del nodo.
//
// Ripristina currentTerm, votedFor, log, commitIndex, lastApplied e la state
// machine. Se lo stato non contiene ancora Data, ricostruisce la mappa
// applicando le entry committed presenti nel log.
func (n *ConsensusNode) restorePersistentStateLocked(state persistence.State) {
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor

	if state.Log == nil {
		n.log = make([]*consensuspb.LogEntry, 0)
	} else {
		n.log = state.Log
	}

	n.commitIndex = state.CommitIndex
	if n.commitIndex > n.lastLogIndexLocked() {
		n.commitIndex = n.lastLogIndexLocked()
	}

	if state.Data != nil {
		n.store.Restore(state.Data)
		n.lastApplied = state.LastApplied
		if n.lastApplied > n.commitIndex {
			n.lastApplied = n.commitIndex
		}
		return
	}

	n.rebuildStateMachineFromCommittedLogLocked()
}

// rebuildStateMachineFromCommittedLogLocked ricostruisce la state machine.
//
// Svuota lo store e riapplica solo le entry committed, cioè quelle con indice
// minore o uguale a commitIndex.
func (n *ConsensusNode) rebuildStateMachineFromCommittedLogLocked() {
	n.store.Reset()
	n.lastApplied = 0
	n.applyCommittedEntriesLocked()
}

// applyCommittedEntriesLocked applica alla state machine le entry committed
// ma non ancora applicate.
//
// Il consenso decide quando un'entry è committed. Lo storage decide come
// applicarla alla mappa key-value.
func (n *ConsensusNode) applyCommittedEntriesLocked() {
	for n.lastApplied < n.commitIndex {
		nextIndex := n.lastApplied + 1
		entry := n.entryByIndexLocked(nextIndex)
		if entry == nil {
			break
		}

		n.store.Apply(entry)
		n.lastApplied = nextIndex
	}

	n.maybeSaveSnapshotLocked()
}
