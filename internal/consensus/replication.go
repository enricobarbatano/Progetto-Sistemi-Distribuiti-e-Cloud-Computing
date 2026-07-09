// Package consensus contiene anche la replica del log.
//
// Questo file raccoglie la parte del protocollo relativa a heartbeat,
// AppendEntries, replica delle entry verso i follower, commit su quorum e
// invio di snapshot ai follower troppo arretrati.
//
// Una scrittura Put/Delete diventa visibile alla state machine solo dopo che il
// leader l'ha replicata su una maggioranza del cluster. Questo è il punto in cui
// il sistema collega la replica del log alla linearizzabilità delle operazioni
// completate.
package consensus

import (
	"context"
	"log"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// appendEntriesFromLeaderLocked integra nel log locale le entry ricevute dal leader.
//
// Se un'entry in arrivo ha lo stesso indice ma termine diverso da quella locale,
// il follower tronca il log dal punto di conflitto e accetta il suffix del leader.
// Questa regola mantiene i log dei follower allineati al leader corrente.
func (n *ConsensusNode) appendEntriesFromLeaderLocked(entries []*consensuspb.LogEntry) bool {
	if len(entries) == 0 {
		return false
	}

	for i, incomingEntry := range entries {
		if incomingEntry.Index <= n.lastIncludedIndex {
			continue
		}

		localEntry := n.entryByIndexLocked(incomingEntry.Index)
		if localEntry == nil {
			n.log = append(n.log, entries[i:]...)
			return true
		}

		if localEntry.Term != incomingEntry.Term {
			n.truncateLogFromIndexLocked(incomingEntry.Index)
			n.log = append(n.log, entries[i:]...)
			return true
		}
	}

	return false
}

// entriesFromIndexLocked restituisce le entry a partire da index.
//
// Se index è già coperto dallo snapshot locale, la ricerca parte dalla prima
// entry non inclusa nello snapshot. Questo evita di richiedere entry che non
// esistono più nel log in memoria dopo la compattazione logica.
func (n *ConsensusNode) entriesFromIndexLocked(index uint64) []*consensuspb.LogEntry {
	if index <= n.lastIncludedIndex {
		index = n.lastIncludedIndex + 1
	}

	entries := make([]*consensuspb.LogEntry, 0)
	for _, entry := range n.log {
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}

	return entries
}

// heartbeatLoop invia heartbeat periodici quando il nodo è leader.
//
// Gli heartbeat sono AppendEntries vuoti: non replicano nuove operazioni, ma
// mantengono l'autorità del leader e impediscono ai follower di avviare elezioni
// inutili finché il leader è raggiungibile.
func (n *ConsensusNode) heartbeatLoop() {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.role == consensuspb.NodeRole_NODE_ROLE_LEADER
			n.mu.Unlock()

			if isLeader {
				n.sendHeartbeats()
			}
		}
	}
}

// sendHeartbeats invia AppendEntries vuoti a tutti i peer.
//
// Oltre a mantenere viva la leadership, le risposte degli heartbeat permettono
// al leader di osservare termini più recenti e fare step-down se un altro nodo
// ha già avanzato il termine.
func (n *ConsensusNode) sendHeartbeats() {
	n.mu.Lock()

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex

	peers := make(map[string]string, len(n.peers))
	for peerID, peerAddress := range n.peers {
		peers[peerID] = peerAddress
	}
	n.mu.Unlock()

	for peerID, peerAddress := range peers {
		go func(peerID string, peerAddress string) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			n.mu.Lock()

			nextIndex := n.nextIndexForPeerLocked(peerID)
			prevLogIndex := nextIndex - 1
			prevLogTerm, _ := n.logTermAtIndexLocked(prevLogIndex)

			client, err := n.getPeerClientLocked(peerID, peerAddress)
			if err != nil {
				n.markPeerOfflineLocked(peerID, err)
				n.mu.Unlock()
				return
			}

			n.mu.Unlock()

			resp, err := client.AppendEntries(ctx, &consensuspb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			})
			if err != nil {
				n.mu.Lock()
				n.markPeerOfflineLocked(peerID, err)
				n.mu.Unlock()
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			n.markPeerOnlineLocked(peerID)

			if resp.Term > n.currentTerm {
				n.stepDownToTermLocked(resp.Term)

				if err := n.persistLocked(); err != nil {
					log.Printf("node %s cannot persist higher term from heartbeat response: %v", n.id, err)
				}

				n.resetElectionTimer()
				return
			}

			if !resp.Success {
				n.updateNextIndexFromConflictLocked(peerID, resp)
			}
		}(peerID, peerAddress)
	}
}

// replicateLogToPeer prova a portare un follower almeno fino a targetIndex.
//
// Il leader usa nextIndex e matchIndex per decidere quali entry inviare. Se il
// follower è troppo indietro e le entry necessarie sono già incluse nello
// snapshot locale, il leader invia InstallSnapshot prima di riprendere la replica.
//
// La funzione restituisce true solo quando il follower conferma di avere almeno
// targetIndex.
func (n *ConsensusNode) replicateLogToPeer(ctx context.Context, peerID string, peerAddress string, targetIndex uint64) bool {
	for {
		select {
		case <-ctx.Done():
			return false

		default:
		}

		n.mu.Lock()

		if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
			n.mu.Unlock()
			return false
		}

		nextIndex := n.nextIndex[peerID]
		if nextIndex == 0 {
			nextIndex = n.lastLogIndexLocked() + 1
			n.nextIndex[peerID] = nextIndex
		}

		if nextIndex <= n.lastIncludedIndex {
			n.mu.Unlock()

			if !n.sendSnapshotToPeer(ctx, peerID, peerAddress) {
				return false
			}

			continue
		}

		prevLogIndex := nextIndex - 1
		prevLogTerm, _ := n.logTermAtIndexLocked(prevLogIndex)
		entries := n.entriesFromIndexLocked(nextIndex)
		term := n.currentTerm
		leaderID := n.id
		leaderCommit := n.commitIndex

		client, err := n.getPeerClientLocked(peerID, peerAddress)
		if err != nil {
			n.markPeerOfflineLocked(peerID, err)
			n.mu.Unlock()
			return false
		}

		n.mu.Unlock()

		resp, err := client.AppendEntries(ctx, &consensuspb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     leaderID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		})
		if err != nil {
			n.mu.Lock()
			n.markPeerOfflineLocked(peerID, err)
			n.mu.Unlock()
			return false
		}

		n.mu.Lock()

		n.markPeerOnlineLocked(peerID)

		if resp.Term > n.currentTerm {
			n.stepDownToTermLocked(resp.Term)

			if err := n.persistLocked(); err != nil {
				log.Printf("node %s cannot persist higher term during replication: %v", n.id, err)
			}

			n.resetElectionTimer()
			n.mu.Unlock()
			return false
		}

		if resp.Success {
			n.matchIndex[peerID] = resp.MatchIndex
			n.nextIndex[peerID] = resp.MatchIndex + 1
			replicated := resp.MatchIndex >= targetIndex
			n.mu.Unlock()

			return replicated
		}

		n.updateNextIndexFromConflictLocked(peerID, resp)
		n.mu.Unlock()
	}
}

// sendSnapshotToPeer invia lo snapshot locale a un follower troppo arretrato.
//
// Questo caso si verifica quando il follower richiede entry che il leader ha già
// compattato logicamente nello snapshot. In questa versione lo snapshot viene
// inviato in un unico chunk; il proto contiene comunque offset/data/done, quindi
// il trasferimento potrà essere spezzato in blocchi in una versione futura.
func (n *ConsensusNode) sendSnapshotToPeer(ctx context.Context, peerID string, peerAddress string) bool {
	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		n.mu.Unlock()
		return false
	}

	if n.lastIncludedIndex == 0 {
		n.mu.Unlock()
		return false
	}

	snapshot := persistence.Snapshot{
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Data:              n.cloneDataLocked(),
	}

	snapshotData, err := persistence.EncodeSnapshot(snapshot)
	if err != nil {
		log.Printf("node %s cannot encode snapshot for peer %s: %v", n.id, peerID, err)
		n.mu.Unlock()
		return false
	}

	term := n.currentTerm
	leaderID := n.id

	client, err := n.getPeerClientLocked(peerID, peerAddress)
	if err != nil {
		n.markPeerOfflineLocked(peerID, err)
		n.mu.Unlock()
		return false
	}

	n.mu.Unlock()

	resp, err := client.InstallSnapshot(ctx, &consensuspb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          leaderID,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Offset:            0,
		Data:              snapshotData,
		Done:              true,
	})
	if err != nil {
		n.mu.Lock()
		n.markPeerOfflineLocked(peerID, err)
		n.mu.Unlock()
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.markPeerOnlineLocked(peerID)

	if resp.Term > n.currentTerm {
		n.stepDownToTermLocked(resp.Term)

		if err := n.persistLocked(); err != nil {
			log.Printf("node %s cannot persist higher term from InstallSnapshot response: %v", n.id, err)
		}

		n.resetElectionTimer()
		return false
	}

	if !resp.Success {
		return false
	}

	n.matchIndex[peerID] = snapshot.LastIncludedIndex
	n.nextIndex[peerID] = snapshot.LastIncludedIndex + 1

	return true
}

// replicateEntryToQuorum replica una entry fino alla maggioranza del cluster.
//
// Il leader conta sé stesso come prima replica valida e invia la entry ai peer.
// La scrittura viene considerata sicura solo quando replicatedCount raggiunge
// majority(), cioè N/2 + 1. Questo impedisce che una scrittura confermata venga
// persa se il leader crasha subito dopo il commit.
func (n *ConsensusNode) replicateEntryToQuorum(ctx context.Context, entryIndex uint64) bool {
	n.mu.Lock()

	neededVotes := n.majority()
	if neededVotes <= 1 {
		n.mu.Unlock()
		return true
	}

	peers := make(map[string]string, len(n.peers))
	for peerID, peerAddress := range n.peers {
		peers[peerID] = peerAddress
	}

	n.mu.Unlock()

	replicationCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resultCh := make(chan bool, len(peers))
	for peerID, peerAddress := range peers {
		go func(peerID string, peerAddress string) {
			resultCh <- n.replicateLogToPeer(replicationCtx, peerID, peerAddress, entryIndex)
		}(peerID, peerAddress)
	}

	replicatedCount := 1
	responses := 0

	for responses < len(peers) {
		select {
		case success := <-resultCh:
			responses++
			if success {
				replicatedCount++
				if replicatedCount >= neededVotes {
					return true
				}
			}

		case <-replicationCtx.Done():
			return false
		}
	}

	return replicatedCount >= neededVotes
}

// handleWriteOperation gestisce una scrittura Put/Delete sul leader.
//
// Se il nodo non è leader, restituisce errore e leader hint al chiamante. Se è
// leader, crea una nuova LogEntry, la persiste, la replica fino al quorum e solo
// dopo aggiorna commitIndex e applica l'entry alla state machine.
//
// Questo ordine è centrale per la correttezza: una scrittura viene resa visibile
// solo dopo essere stata replicata sulla maggioranza.
func (n *ConsensusNode) handleWriteOperation(ctx context.Context, operation consensuspb.LogOperation, key string, value string) (bool, string, string, error) {
	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		leaderHint := n.leaderAddress
		n.mu.Unlock()
		return false, "node is not leader", leaderHint, nil
	}

	entry := &consensuspb.LogEntry{
		Index:     n.lastLogIndexLocked() + 1,
		Term:      n.currentTerm,
		Operation: operation,
		Key:       key,
		Value:     value,
	}

	n.log = append(n.log, entry)

	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return false, "", "", err
	}

	n.mu.Unlock()

	if !n.replicateEntryToQuorum(ctx, entry.Index) {
		n.mu.Lock()
		leaderHint := n.leaderAddress
		n.mu.Unlock()

		return false, "failed to replicate entry to quorum", leaderHint, nil
	}

	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER || entry.Term != n.currentTerm {
		leaderHint := n.leaderAddress
		n.mu.Unlock()

		return false, "leadership changed before commit", leaderHint, nil
	}

	if entry.Index > n.commitIndex {
		n.commitIndex = entry.Index
		n.applyCommittedEntriesLocked()

		if err := n.persistLocked(); err != nil {
			n.mu.Unlock()
			return false, "", "", err
		}
	}

	n.mu.Unlock()

	go n.sendHeartbeats()

	return true, "", "", nil
}

// AppendEntries gestisce heartbeat e replica log dal leader verso un follower.
//
// La RPC svolge due funzioni:
//   - heartbeat, quando Entries è vuoto;
//   - replica effettiva, quando contiene nuove LogEntry.
//
// Il follower accetta la richiesta solo se il termine è valido e se prevLogIndex
// e prevLogTerm sono coerenti con il proprio log o con il bordo dello snapshot.
// In caso di conflitto, restituisce informazioni utili al leader per ridurre
// rapidamente nextIndex.
func (n *ConsensusNode) AppendEntries(ctx context.Context, req *consensuspb.AppendEntriesRequest) (*consensuspb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			MatchIndex:    n.lastLogIndexLocked(),
			ConflictIndex: n.lastLogIndexLocked() + 1,
			ConflictTerm:  0,
		}, nil
	}

	stateChanged := false
	// Messaggi provenienti da termini vecchi vengono rifiutati.
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		stateChanged = true
	}

	n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
	n.leaderID = req.LeaderId

	if leaderAddress, ok := n.peers[req.LeaderId]; ok {
		n.leaderAddress = leaderAddress
	} else if req.LeaderId == n.id {
		n.leaderAddress = n.address
	}

	if req.PrevLogIndex < n.lastIncludedIndex {
		if stateChanged {
			if err := n.persistLocked(); err != nil {
				return nil, err
			}
		}

		n.resetElectionTimer()

		return &consensuspb.AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			MatchIndex:    n.lastLogIndexLocked(),
			ConflictIndex: n.lastIncludedIndex + 1,
			ConflictTerm:  n.lastIncludedTerm,
		}, nil
	}
	// Regola di coerenza del log: il follower accetta nuove entry solo se il
	// prefisso indicato dal leader coincide con il proprio log locale.
	if !n.hasLogEntryLocked(req.PrevLogIndex, req.PrevLogTerm) {
		conflictIndex, conflictTerm := n.conflictInfoLocked(req.PrevLogIndex)

		if stateChanged {
			if err := n.persistLocked(); err != nil {
				return nil, err
			}
		}

		n.resetElectionTimer()

		return &consensuspb.AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			MatchIndex:    n.lastLogIndexLocked(),
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}, nil
	}

	if n.appendEntriesFromLeaderLocked(req.Entries) {
		stateChanged = true
	}

	// Il follower avanza il commit index seguendo il leader e applica alla state
	// machine tutte le entry che diventano committed.
	if req.LeaderCommit > n.commitIndex {
		lastIndex := n.lastLogIndexLocked()

		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
		}

		if n.commitIndex < n.lastIncludedIndex {
			n.commitIndex = n.lastIncludedIndex
		}

		n.applyCommittedEntriesLocked()
		stateChanged = true
	}

	if stateChanged {
		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	n.resetElectionTimer()

	return &consensuspb.AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: n.lastLogIndexLocked(),
	}, nil
}

// nextIndexForPeerLocked restituisce il nextIndex valido per un peer.
func (n *ConsensusNode) nextIndexForPeerLocked(peerID string) uint64 {
	nextIndex := n.nextIndex[peerID]
	if nextIndex == 0 {
		nextIndex = n.lastLogIndexLocked() + 1
	}

	minimumNextIndex := n.lastIncludedIndex + 1
	if minimumNextIndex == 0 {
		minimumNextIndex = 1
	}

	if nextIndex < minimumNextIndex {
		nextIndex = minimumNextIndex
	}

	n.nextIndex[peerID] = nextIndex

	return nextIndex
}

// updateNextIndexFromConflictLocked aggiorna nextIndex dopo un rifiuto.
//
// Usa le informazioni di conflitto restituite dal follower per fare fast back-off
// invece di decrementare nextIndex una posizione alla volta. Questo riduce il
// numero di round RPC necessari per riallineare log divergenti.
func (n *ConsensusNode) updateNextIndexFromConflictLocked(peerID string, resp *consensuspb.AppendEntriesResponse) {
	nextIndex := n.nextIndex[peerID]
	if nextIndex == 0 {
		nextIndex = n.lastLogIndexLocked() + 1
	}

	if resp.ConflictIndex > 0 {
		nextIndex = resp.ConflictIndex

		if resp.ConflictTerm > 0 {
			if lastIndex, ok := n.lastIndexForTermLocked(resp.ConflictTerm); ok {
				nextIndex = lastIndex + 1
			}
		}
	} else if nextIndex > 1 {
		nextIndex--
	} else {
		nextIndex = 1
	}

	if nextIndex == 0 {
		nextIndex = 1
	}

	n.nextIndex[peerID] = nextIndex
}

// conflictInfoLocked calcola le informazioni di conflitto per il leader.
//
// Quando AppendEntries fallisce per incoerenza del log, il follower restituisce
// il primo indice del termine in conflitto. Il leader usa queste informazioni
// per aggiornare nextIndex in modo più efficiente.
func (n *ConsensusNode) conflictInfoLocked(prevLogIndex uint64) (uint64, uint64) {
	if prevLogIndex > n.lastLogIndexLocked() {
		return n.lastLogIndexLocked() + 1, 0
	}

	localTerm, ok := n.logTermAtIndexLocked(prevLogIndex)
	if !ok {
		return n.lastLogIndexLocked() + 1, 0
	}

	firstIndex := prevLogIndex
	for _, entry := range n.log {
		if entry.Term == localTerm && entry.Index < firstIndex {
			firstIndex = entry.Index
		}
	}

	if localTerm == n.lastIncludedTerm && n.lastIncludedIndex < firstIndex {
		firstIndex = n.lastIncludedIndex
	}

	return firstIndex, localTerm
}

// lastIndexForTermLocked cerca l'ultima entry locale con il termine dato.
func (n *ConsensusNode) lastIndexForTermLocked(term uint64) (uint64, bool) {
	var lastIndex uint64
	found := false

	if n.lastIncludedIndex > 0 && n.lastIncludedTerm == term {
		lastIndex = n.lastIncludedIndex
		found = true
	}

	for _, entry := range n.log {
		if entry.Term == term {
			lastIndex = entry.Index
			found = true
		}
	}

	return lastIndex, found
}

// stepDownToTermLocked forza il nodo a diventare follower in un termine più alto.
//
// Un nodo che osserva un termine maggiore perde immediatamente eventuale ruolo di
// leader o candidate. Questo evita che un leader obsoleto continui a servire
// richieste dopo la presenza di un termine più recente.
func (n *ConsensusNode) stepDownToTermLocked(term uint64) {
	n.currentTerm = term
	n.votedFor = ""
	n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
	n.leaderID = ""
	n.leaderAddress = ""
}

// confirmLeadershipWithQuorum verifica che il leader abbia ancora contatto con
// la maggioranza del cluster prima di servire una lettura.
//
// Questa è una versione semplificata del meccanismo ReadIndex: invece di
// introdurre una nuova RPC, il leader invia AppendEntries vuoti ai peer e conta
// le risposte valide. Se raggiunge la maggioranza nello stesso termine, può
// rispondere alla Get riducendo il rischio di letture stale.
//
// Il nodo leader conta sé stesso come prima conferma valida.
func (n *ConsensusNode) confirmLeadershipWithQuorum(ctx context.Context) bool {
	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		n.mu.Unlock()
		return false
	}

	neededVotes := n.majority()
	if neededVotes <= 1 {
		n.mu.Unlock()
		return true
	}

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex

	peers := make(map[string]string, len(n.peers))
	for peerID, peerAddress := range n.peers {
		peers[peerID] = peerAddress
	}

	n.mu.Unlock()

	quorumCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()

	resultCh := make(chan bool, len(peers))

	for peerID, peerAddress := range peers {
		go func(peerID string, peerAddress string) {
			resultCh <- n.confirmLeadershipWithPeer(quorumCtx, peerID, peerAddress, term, leaderID, leaderCommit)
		}(peerID, peerAddress)
	}

	successCount := 1
	responses := 0

	for responses < len(peers) {
		select {
		case success := <-resultCh:
			responses++

			if success {
				successCount++
				if successCount >= neededVotes {
					return true
				}
			}

		case <-quorumCtx.Done():
			return false
		}
	}

	return successCount >= neededVotes
}

// confirmLeadershipWithPeer invia un heartbeat AppendEntries vuoto a un peer.
//
// Se il peer risponde con un termine maggiore, il nodo corrente perde la
// leadership e passa a follower. Se il peer accetta l'heartbeat nello stesso
// termine, la risposta conta come conferma valida per la lettura linearizzabile.
func (n *ConsensusNode) confirmLeadershipWithPeer(ctx context.Context, peerID string, peerAddress string, term uint64, leaderID string, leaderCommit uint64) bool {
	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}

	nextIndex := n.nextIndexForPeerLocked(peerID)
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := n.logTermAtIndexLocked(prevLogIndex)

	client, err := n.getPeerClientLocked(peerID, peerAddress)
	if err != nil {
		n.markPeerOfflineLocked(peerID, err)
		n.mu.Unlock()
		return false
	}

	n.mu.Unlock()

	resp, err := client.AppendEntries(ctx, &consensuspb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		n.mu.Lock()
		n.markPeerOfflineLocked(peerID, err)
		n.mu.Unlock()
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.markPeerOnlineLocked(peerID)

	if resp.Term > n.currentTerm {
		n.stepDownToTermLocked(resp.Term)

		if err := n.persistLocked(); err != nil {
			log.Printf("node %s cannot persist higher term during read quorum check: %v", n.id, err)
		}

		n.resetElectionTimer()
		return false
	}

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER || n.currentTerm != term {
		return false
	}

	if !resp.Success {
		n.updateNextIndexFromConflictLocked(peerID, resp)
		return false
	}

	return true
}
