// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la parte di Raft relativa alla replicazione del log:
// heartbeat del leader, AppendEntries, replica verso i peer, raggiungimento
// del quorum e gestione delle scritture Put/Delete dopo il commit.
package consensus

import (
	"context"
	"log"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// appendEntriesFromLeaderLocked appende al log locale le entry ricevute dal leader.
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

// entriesFromIndexLocked restituisce tutte le entry del log a partire da index.
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

			if nextIndex <= n.lastIncludedIndex {
				nextIndex = n.lastIncludedIndex + 1
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
			nextIndex = n.lastIncludedIndex + 1
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

		minimumNextIndex := n.lastIncludedIndex + 1
		if minimumNextIndex == 0 {
			minimumNextIndex = 1
		}

		if n.nextIndex[peerID] > minimumNextIndex {
			n.nextIndex[peerID]--
		} else {
			n.nextIndex[peerID] = minimumNextIndex
		}

		n.mu.Unlock()
	}
}

// replicateEntryToQuorum replica una specifica entry fino al quorum.
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

	if req.PrevLogIndex < n.lastIncludedIndex {
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
