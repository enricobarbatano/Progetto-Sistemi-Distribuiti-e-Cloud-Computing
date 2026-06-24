// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la parte di ConsensusNode legata allo stato persistente,
// al recovery, agli snapshot locali e all'applicazione delle entry committed.
//
// La persistenza fisica dei file è delegata a internal/persistence, mentre la
// mappa key-value è delegata a internal/storage.
package consensus

import (
	"log"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// cloneDataLocked restituisce una copia dello stato key-value corrente.
func (n *ConsensusNode) cloneDataLocked() map[string]string {
	return n.store.Snapshot()
}

// persistentStateLocked costruisce lo stato persistente corrente del nodo.
func (n *ConsensusNode) persistentStateLocked() persistence.State {
	return persistence.State{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Log:               n.log,
		CommitIndex:       n.commitIndex,
		LastApplied:       n.lastApplied,
		Data:              n.cloneDataLocked(),
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
	}
}

// persistLocked salva lo stato corrente del nodo.
func (n *ConsensusNode) persistLocked() error {
	return n.persistenceManager.Save(n.persistentStateLocked())
}

// saveSnapshotLocked salva uno snapshot locale e compatta il log.
//
// Lo snapshot diventa il checkpoint reale della state machine fino a
// lastApplied. Dopo il salvataggio, le entry con indice <= lastApplied vengono
// rimosse dal log fisico.
func (n *ConsensusNode) saveSnapshotLocked() error {
	if n.lastApplied == 0 {
		return nil
	}

	if n.lastApplied <= n.lastIncludedIndex {
		return nil
	}

	lastIncludedTerm, ok := n.logTermAtIndexLocked(n.lastApplied)
	if !ok {
		return nil
	}

	snapshot := persistence.Snapshot{
		LastIncludedIndex: n.lastApplied,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              n.cloneDataLocked(),
	}

	if err := n.persistenceManager.SaveSnapshot(snapshot); err != nil {
		return err
	}

	n.lastIncludedIndex = snapshot.LastIncludedIndex
	n.lastIncludedTerm = snapshot.LastIncludedTerm
	n.compactLogUpToLocked(snapshot.LastIncludedIndex)

	return nil
}

// maybeSaveSnapshotLocked decide se salvare uno snapshot locale.
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

// restoreSnapshotIfNewerLocked applica uno snapshot solo se è più recente.
func (n *ConsensusNode) restoreSnapshotIfNewerLocked(snapshot persistence.Snapshot) bool {
	if snapshot.Data == nil {
		return false
	}

	if snapshot.LastIncludedIndex <= n.lastIncludedIndex {
		return false
	}

	n.store.Restore(snapshot.Data)

	n.lastIncludedIndex = snapshot.LastIncludedIndex
	n.lastIncludedTerm = snapshot.LastIncludedTerm

	if snapshot.LastIncludedIndex > n.lastApplied {
		n.lastApplied = snapshot.LastIncludedIndex
	}

	if snapshot.LastIncludedIndex > n.commitIndex {
		n.commitIndex = snapshot.LastIncludedIndex
	}

	n.compactLogUpToLocked(snapshot.LastIncludedIndex)

	return true
}

// restorePersistentStateLocked ripristina i campi persistenti del nodo.
func (n *ConsensusNode) restorePersistentStateLocked(state persistence.State) {
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.lastIncludedIndex = state.LastIncludedIndex
	n.lastIncludedTerm = state.LastIncludedTerm

	if state.Log == nil {
		n.log = make([]*consensuspb.LogEntry, 0)
	} else {
		n.log = state.Log
	}

	n.compactLogUpToLocked(n.lastIncludedIndex)

	n.commitIndex = state.CommitIndex
	if n.commitIndex > n.lastLogIndexLocked() {
		n.commitIndex = n.lastLogIndexLocked()
	}

	if n.commitIndex < n.lastIncludedIndex {
		n.commitIndex = n.lastIncludedIndex
	}

	if state.Data != nil {
		n.store.Restore(state.Data)
		n.lastApplied = state.LastApplied

		if n.lastApplied > n.commitIndex {
			n.lastApplied = n.commitIndex
		}

		if n.lastApplied < n.lastIncludedIndex {
			n.lastApplied = n.lastIncludedIndex
		}

		return
	}

	n.rebuildStateMachineFromCommittedLogLocked()
}

// rebuildStateMachineFromCommittedLogLocked ricostruisce la state machine.
func (n *ConsensusNode) rebuildStateMachineFromCommittedLogLocked() {
	n.store.Reset()
	n.lastApplied = n.lastIncludedIndex
	n.applyCommittedEntriesLocked()
}

// applyCommittedEntriesLocked applica alla state machine le entry committed.
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
