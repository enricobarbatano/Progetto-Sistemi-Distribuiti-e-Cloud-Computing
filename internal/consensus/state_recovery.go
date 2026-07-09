// Package consensus contiene anche la gestione dello stato persistente del nodo.
//
// Questo file raccoglie la logica di collegamento tra ConsensusNode,
// internal/persistence e internal/storage: costruzione dello stato persistente,
// recovery all'avvio, salvataggio snapshot, compattazione logica e applicazione
// delle entry committed alla state machine.
//
// La persistenza fisica dei file è delegata a internal/persistence, mentre la
// mappa key-value è delegata a internal/storage. ConsensusNode mantiene solo la
// logica che decide quando salvare, ripristinare o applicare entry.

package consensus

import (
	"log"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// cloneDataLocked restituisce una copia dello stato key-value corrente.
//
// Deve essere chiamata quando il chiamante possiede n.mu. La copia evita che il
// livello di persistenza possa modificare direttamente la state machine.
func (n *ConsensusNode) cloneDataLocked() map[string]string {
	return n.store.Snapshot()
}

// persistentStateLocked costruisce lo stato persistente corrente del nodo.
//
// Include termine, voto, log, commit index, last applied, dati della state
// machine e bordo dello snapshot. Questo oggetto viene poi salvato dal
// persistenceManager come checkpoint compatto.
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

// persistLocked salva lo stato corrente del nodo tramite persistenceManager.
//
// Deve essere chiamata con n.mu già acquisito. Il metodo centralizza il passaggio
// da stato in memoria a stato persistente, lasciando al package persistence i
// dettagli di JSON, WAL e scrittura atomica.
func (n *ConsensusNode) persistLocked() error {
	return n.persistenceManager.Save(n.persistentStateLocked())
}

// saveSnapshotLocked salva uno snapshot locale e applica compattazione logica.
//
// Lo snapshot diventa il checkpoint della state machine fino a lastApplied.
// Dopo il salvataggio, le entry fino a LastIncludedIndex vengono rimosse dal log
// in memoria del nodo. Il file WAL fisico resta append-only in questa versione
// ed è discusso tra le limitazioni del progetto.
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

// maybeSaveSnapshotLocked decide se creare uno snapshot locale.
//
// La soglia è basata su snapshotThreshold: quando lastApplied raggiunge un
// multiplo della soglia, il nodo prova a salvare uno snapshot. Questo evita di
// produrre snapshot troppo frequentemente durante la replica.
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
// Il recovery combina tre fonti: state.json, snapshot locale e log committed.
// Se esiste stato persistente, viene ripristinato; se esiste uno snapshot più
// recente, viene applicato; infine vengono applicate alla state machine eventuali
// entry committed ma non ancora applied.
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
//
// Uno snapshot vecchio o già incluso viene ignorato. Se lo snapshot è valido,
// la state machine viene ripristinata, lastIncludedIndex e lastIncludedTerm sono
// aggiornati e il log in memoria viene compattato logicamente.
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
//
// Vengono ripristinati termine, voto, log, indici di snapshot e commit index.
// Se lo stato contiene già una copia della state machine, viene usata quella;
// altrimenti la state machine viene ricostruita applicando le entry committed
// disponibili nel log.
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
//
// Lo store viene svuotato e vengono riapplicate solo le entry committed. Questo
// evita di rendere visibili operazioni presenti nel log ma non ancora confermate
// da quorum.
func (n *ConsensusNode) rebuildStateMachineFromCommittedLogLocked() {
	n.store.Reset()
	n.lastApplied = n.lastIncludedIndex
	n.applyCommittedEntriesLocked()
}

// applyCommittedEntriesLocked applica alla state machine le entry committed.
//
// Il metodo avanza lastApplied fino a commitIndex, applicando ogni entry tramite
// KVStore. Solo le entry committed vengono applicate, preservando la coerenza
// tra log replicato e stato key-value esposto ai client.
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
