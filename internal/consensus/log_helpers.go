// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie gli helper interni usati per lavorare sul log Raft.
// Dopo la log compaction, il log fisico può non contenere più le entry iniziali.
// Per questo gli helper tengono conto di lastIncludedIndex e lastIncludedTerm.
package consensus

import consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"

// entryByIndexLocked cerca una entry del log tramite indice logico.
//
// Se l'indice è già coperto dallo snapshot, l'entry non è più presente nel log
// fisico e viene restituito nil.
func (n *ConsensusNode) entryByIndexLocked(index uint64) *consensuspb.LogEntry {
	if index <= n.lastIncludedIndex {
		return nil
	}

	for _, entry := range n.log {
		if entry.Index == index {
			return entry
		}
	}

	return nil
}

// lastLogIndexLocked restituisce l'ultimo indice logico noto.
//
// Se il log fisico è vuoto, l'ultimo indice noto è quello coperto dallo snapshot.
func (n *ConsensusNode) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedIndex
	}

	return n.log[len(n.log)-1].Index
}

// lastLogTermLocked restituisce il termine dell'ultima entry logica nota.
//
// Se il log fisico è vuoto, il termine è quello dell'ultimo indice incluso
// nello snapshot.
func (n *ConsensusNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return n.lastIncludedTerm
	}

	return n.log[len(n.log)-1].Term
}

// logTermAtIndexLocked restituisce il termine associato a un indice logico.
//
// Se l'indice coincide con lastIncludedIndex, il termine viene letto dallo
// snapshot. Se l'indice è precedente allo snapshot, non è più disponibile.
func (n *ConsensusNode) logTermAtIndexLocked(index uint64) (uint64, bool) {
	if index == 0 && n.lastIncludedIndex == 0 {
		return 0, true
	}

	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm, true
	}

	if index < n.lastIncludedIndex {
		return 0, false
	}

	for _, entry := range n.log {
		if entry.Index == index {
			return entry.Term, true
		}
	}

	return 0, false
}

// hasLogEntryLocked verifica se il nodo possiede o rappresenta una entry.
//
// L'entry può essere fisicamente nel log oppure rappresentata dal bordo dello
// snapshot tramite lastIncludedIndex/lastIncludedTerm.
func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool {
	localTerm, ok := n.logTermAtIndexLocked(index)
	if !ok {
		return false
	}

	return localTerm == term
}

// truncateLogFromIndexLocked elimina tutte le entry con indice >= index.
//
// Se index è già coperto dallo snapshot, il log fisico viene svuotato.
func (n *ConsensusNode) truncateLogFromIndexLocked(index uint64) {
	if index <= n.lastIncludedIndex {
		n.log = make([]*consensuspb.LogEntry, 0)

		if n.commitIndex < n.lastIncludedIndex {
			n.commitIndex = n.lastIncludedIndex
		}

		if n.lastApplied < n.lastIncludedIndex {
			n.lastApplied = n.lastIncludedIndex
		}

		return
	}

	truncated := make([]*consensuspb.LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry.Index < index {
			truncated = append(truncated, entry)
		}
	}

	n.log = truncated

	if n.commitIndex >= index {
		n.commitIndex = index - 1
	}

	if n.commitIndex < n.lastIncludedIndex {
		n.commitIndex = n.lastIncludedIndex
	}

	if n.lastApplied > n.commitIndex {
		n.rebuildStateMachineFromCommittedLogLocked()
	}
}

// compactLogUpToLocked elimina dal log fisico tutte le entry già incluse nello
// snapshot locale.
func (n *ConsensusNode) compactLogUpToLocked(lastIncludedIndex uint64) {
	if lastIncludedIndex == 0 {
		return
	}

	compacted := make([]*consensuspb.LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry.Index > lastIncludedIndex {
			compacted = append(compacted, entry)
		}
	}

	n.log = compacted
}

// isCandidateLogUpToDateLocked verifica se il log di un candidato è aggiornato.
//
// Il confronto usa l'ultimo indice/termine logico noto, considerando anche lo
// snapshot se il log fisico è stato compattato.
func (n *ConsensusNode) isCandidateLogUpToDateLocked(candidateLastLogIndex uint64, candidateLastLogTerm uint64) bool {
	localLastLogTerm := n.lastLogTermLocked()
	localLastLogIndex := n.lastLogIndexLocked()

	if candidateLastLogTerm != localLastLogTerm {
		return candidateLastLogTerm > localLastLogTerm
	}

	return candidateLastLogIndex >= localLastLogIndex
}

// majority restituisce il numero minimo di voti necessari per il quorum.
func (n *ConsensusNode) majority() int {
	clusterSize := len(n.peers) + 1
	return (clusterSize / 2) + 1
}
