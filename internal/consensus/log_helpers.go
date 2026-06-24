// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie gli helper interni usati per lavorare sul log Raft:
// ricerca di entry per indice, lettura dell'ultimo indice/termine, controllo
// di coerenza prevLogIndex/prevLogTerm, troncamento dei conflitti e calcolo
// della maggioranza.
//
// La separazione da node.go serve a mantenere il nodo principale più snello:
// ConsensusNode resta l'orchestratore, mentre qui vivono le piccole operazioni
// di supporto sul log.
package consensus

import consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"

// entryByIndexLocked cerca una entry del log tramite il suo indice.
//
// La funzione effettua una ricerca lineare perché in questa fase il log è una
// slice semplice. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) entryByIndexLocked(index uint64) *consensuspb.LogEntry {
	for _, entry := range n.log {
		if entry.Index == index {
			return entry
		}
	}

	return nil
}

// lastLogIndexLocked restituisce l'indice dell'ultima entry del log.
//
// Se il log è vuoto, restituisce 0. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Index
}

// lastLogTermLocked restituisce il termine dell'ultima entry del log.
//
// Se il log è vuoto, restituisce 0. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Term
}

// logTermAtIndexLocked restituisce il termine della entry con l'indice dato.
//
// Se index è 0, restituisce termine 0, perché l'indice 0 rappresenta il punto
// precedente all'inizio del log. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) logTermAtIndexLocked(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}

	for _, entry := range n.log {
		if entry.Index == index {
			return entry.Term, true
		}
	}

	return 0, false
}

// hasLogEntryLocked verifica se il nodo possiede una entry con indice e termine dati.
//
// Questo è il controllo usato da AppendEntries per validare prevLogIndex e
// prevLogTerm. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool {
	localTerm, ok := n.logTermAtIndexLocked(index)
	if !ok {
		return false
	}

	return localTerm == term
}

// truncateLogFromIndexLocked rimuove tutte le entry con indice maggiore o uguale
// a quello specificato.
//
// Viene usata quando un follower trova una entry locale in conflitto con quella
// ricevuta dal leader. Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) truncateLogFromIndexLocked(index uint64) {
	truncated := make([]*consensuspb.LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		if entry.Index < index {
			truncated = append(truncated, entry)
		}
	}

	n.log = truncated

	if n.commitIndex >= index {
		if index == 0 {
			n.commitIndex = 0
		} else {
			n.commitIndex = index - 1
		}
	}

	if n.lastApplied > n.commitIndex {
		n.rebuildStateMachineFromCommittedLogLocked()
	}
}

// isCandidateLogUpToDateLocked verifica se il log di un candidato è almeno
// aggiornato quanto quello locale.
//
// Raft concede il voto solo a candidati con un log non più vecchio del proprio.
// Il confronto avviene prima sul termine dell'ultima entry e poi sull'indice.
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) isCandidateLogUpToDateLocked(candidateLastLogIndex uint64, candidateLastLogTerm uint64) bool {
	localLastLogTerm := n.lastLogTermLocked()
	localLastLogIndex := n.lastLogIndexLocked()

	if candidateLastLogTerm != localLastLogTerm {
		return candidateLastLogTerm > localLastLogTerm
	}

	return candidateLastLogIndex >= localLastLogIndex
}

// majority restituisce il numero minimo di voti necessari per ottenere il quorum.
//
// Il cluster include il nodo corrente più tutti i peer conosciuti.
func (n *ConsensusNode) majority() int {
	clusterSize := len(n.peers) + 1
	return (clusterSize / 2) + 1
}
