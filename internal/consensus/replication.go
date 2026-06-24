// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la parte di Raft relativa alla replicazione del log:
// heartbeat del leader, AppendEntries, replica verso i peer, raggiungimento
// del quorum e gestione delle scritture Put/Delete dopo il commit.
//
// La separazione da node.go serve a mantenere più chiara la responsabilità
// principale del ConsensusNode: il nodo coordina il protocollo, mentre questo
// file contiene il comportamento specifico della log replication.
package consensus

import (
	"context"
	"log"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// appendEntriesFromLeaderLocked appende al log locale le entry ricevute dal leader.
//
// Se trova una entry con lo stesso indice ma termine diverso, tronca il log da
// quell'indice e appende da lì le entry del leader.
//
// Deve essere chiamata con n.mu già acquisito.
// Restituisce true se il log locale è stato modificato.
func (n *ConsensusNode) appendEntriesFromLeaderLocked(entries []*consensuspb.LogEntry) bool {
	if len(entries) == 0 {
		return false
	}

	for i, incomingEntry := range entries {
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

// entriesFromIndexLocked restituisce tutte le entry del log a partire
// dall'indice specificato.
//
// Viene usata dal leader per inviare a un follower le entry mancanti a partire
// da nextIndex[peer].
//
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) entriesFromIndexLocked(index uint64) []*consensuspb.LogEntry {
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
// Gli heartbeat sono AppendEntries vuoti. Servono a mantenere l'autorità del
// leader e a propagare ai follower il commitIndex più recente.
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

// sendHeartbeats invia AppendEntries vuote a tutti i peer.
//
// Anche se non invia nuove entry, include il LeaderCommit corrente, così i
// follower possono avanzare il proprio commitIndex e applicare entry già
// presenti nel log locale.
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

			nextIndex := n.nextIndex[peerID]
			if nextIndex == 0 {
				nextIndex = n.lastLogIndexLocked() + 1
				n.nextIndex[peerID] = nextIndex
			}

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
				n.currentTerm = resp.Term
				n.votedFor = ""
				n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
				n.leaderID = ""
				n.leaderAddress = ""

				if err := n.persistLocked(); err != nil {
					log.Printf("node %s cannot persist higher term from heartbeat response: %v", n.id, err)
				}

				n.resetElectionTimer()
			}
		}(peerID, peerAddress)
	}
}

// replicateLogToPeer prova a replicare il log verso un singolo follower.
//
// Usa nextIndex e matchIndex per capire da quale entry partire. Se il follower
// rifiuta AppendEntries per log inconsistente, il leader decrementa nextIndex
// e riprova fino a trovare un punto comune oppure fino alla scadenza del contesto.
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
			n.currentTerm = resp.Term
			n.votedFor = ""
			n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
			n.leaderID = ""
			n.leaderAddress = ""

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

		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		} else {
			n.nextIndex[peerID] = 1
		}

		n.mu.Unlock()
	}
}

// replicateEntryToQuorum replica una specifica entry fino al raggiungimento
// della maggioranza del cluster.
//
// Il leader conta sé stesso come primo nodo che possiede la entry. Con tre nodi,
// quindi, basta una conferma positiva da un follower per raggiungere il quorum.
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
// La funzione crea una LogEntry, la persiste localmente, la replica sui follower,
// aspetta il quorum, poi aggiorna commitIndex e applica la entry alla state
// machine. Il client riceve success=true solo dopo il commit locale.
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
// La RPC implementa il controllo prevLogIndex/prevLogTerm, la risoluzione dei
// conflitti, l'append delle entry e l'aggiornamento del commitIndex comunicato
// dal leader.
func (n *ConsensusNode) AppendEntries(ctx context.Context, req *consensuspb.AppendEntriesRequest) (*consensuspb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.AppendEntriesResponse{
			Term:       n.currentTerm,
			Success:    false,
			MatchIndex: n.lastLogIndexLocked(),
		}, nil
	}

	stateChanged := false

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

	if !n.hasLogEntryLocked(req.PrevLogIndex, req.PrevLogTerm) {
		if stateChanged {
			if err := n.persistLocked(); err != nil {
				return nil, err
			}
		}

		n.resetElectionTimer()

		return &consensuspb.AppendEntriesResponse{
			Term:       n.currentTerm,
			Success:    false,
			MatchIndex: n.lastLogIndexLocked(),
		}, nil
	}

	if n.appendEntriesFromLeaderLocked(req.Entries) {
		stateChanged = true
	}

	if req.LeaderCommit > n.commitIndex {
		lastIndex := n.lastLogIndexLocked()

		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
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
