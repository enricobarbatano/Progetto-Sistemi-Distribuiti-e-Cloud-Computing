// Package consensus contiene la logica interna del Consensus Node.
//
// Il package definisce un nodo di consenso stateful basato su una versione
// incrementale del protocollo Raft. Il nodo espone sia le RPC interne di
// consenso sia le RPC key-value usate dal client/proxy.
//
// Questa versione include:
//
//   - elezione del leader;
//   - heartbeat periodici;
//   - riuso delle connessioni gRPC verso i peer;
//   - tracking peer online/offline;
//   - persistenza su state.json e WAL append-only;
//   - recovery robusto da state.json e fallback dall'ultimo record WAL valido;
//   - snapshot locale minimale della state machine;
//   - ricostruzione della state machine applicando solo entry committed;
//   - controllo di coerenza del log lato follower;
//   - replicazione delle entry su quorum per Put e Delete;
//   - letture Get servite solo dal leader per evitare letture stale.
package consensus

import (
	"context"

	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"

	"strconv"
	"sync"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultSnapshotThreshold uint64 = 1000

type ConsensusNode struct {
	consensuspb.UnimplementedConsensusServiceServer
	kvpb.UnimplementedKeyValueServiceServer

	mu sync.Mutex

	id      string
	address string
	peers   map[string]string

	currentTerm uint64
	votedFor    string
	log         []*consensuspb.LogEntry

	commitIndex uint64
	lastApplied uint64
	role        consensuspb.NodeRole

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// store rappresenta la macchina a stati key-value locale.
	//
	// ConsensusNode non modifica più direttamente una map[string]string,
	// ma applica le entry committed delegando allo storage.
	store *storage.KVStore

	// persistenceManager gestisce state.json, WAL e snapshot locali.
	//
	// ConsensusNode decide quando salvare o caricare lo stato, ma non conosce più
	// i dettagli dei file JSON o WAL.
	persistenceManager *persistence.Manager

	snapshotThreshold uint64

	electionResetCh chan struct{}
	stopCh          chan struct{}
	stopOnce        sync.Once

	heartbeatInterval time.Duration

	leaderID      string
	leaderAddress string

	peerConns   map[string]*grpc.ClientConn
	peerClients map[string]consensuspb.ConsensusServiceClient
	peerOnline  map[string]bool
}

func NewConsensusNode(id string, address string, peers map[string]string, dataDir string) (*ConsensusNode, error) {
	if id == "" {
		return nil, errors.New("node id cannot be empty")
	}

	if dataDir == "" {
		dataDir = "data"
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create data directory: %w", err)
	}

	node := &ConsensusNode{
		id:      id,
		address: address,
		peers:   peers,

		currentTerm: 0,
		votedFor:    "",
		log:         make([]*consensuspb.LogEntry, 0),

		commitIndex: 0,
		lastApplied: 0,
		role:        consensuspb.NodeRole_NODE_ROLE_FOLLOWER,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		store: storage.NewKVStore(),

		persistenceManager: persistence.NewManager(id, dataDir),
		snapshotThreshold:  readUint64Env("SNAPSHOT_THRESHOLD", defaultSnapshotThreshold),

		electionResetCh: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),

		heartbeatInterval: 300 * time.Millisecond,

		leaderID:      "",
		leaderAddress: "",

		peerConns:   make(map[string]*grpc.ClientConn),
		peerClients: make(map[string]consensuspb.ConsensusServiceClient),
		peerOnline:  make(map[string]bool),
	}

	for peerID := range peers {
		node.peerOnline[peerID] = true
	}

	if err := node.loadPersistentState(); err != nil {
		return nil, err
	}

	return node, nil
}

func readUint64Env(key string, defaultValue uint64) uint64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return defaultValue
	}

	return parsed
}

func (n *ConsensusNode) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

func (n *ConsensusNode) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)

		n.mu.Lock()
		defer n.mu.Unlock()

		for peerID, conn := range n.peerConns {
			if err := conn.Close(); err != nil {
				log.Printf("node %s cannot close connection to peer %s: %v", n.id, peerID, err)
			}
		}
	})
}

func (n *ConsensusNode) getPeerClientLocked(peerID string, peerAddress string) (consensuspb.ConsensusServiceClient, error) {
	if client, ok := n.peerClients[peerID]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		peerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := consensuspb.NewConsensusServiceClient(conn)
	n.peerConns[peerID] = conn
	n.peerClients[peerID] = client

	return client, nil
}

func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || wasOnline {
		log.Printf("node %s marked peer %s as offline: %v", n.id, peerID, err)
	}

	n.peerOnline[peerID] = false
}

func (n *ConsensusNode) markPeerOnlineLocked(peerID string) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || !wasOnline {
		log.Printf("node %s marked peer %s as online", n.id, peerID)
	}

	n.peerOnline[peerID] = true
}

func (n *ConsensusNode) cloneDataLocked() map[string]string {
	return n.store.Snapshot()
}

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

// refactoring manager.go
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

func (n *ConsensusNode) persistLocked() error {
	return n.persistenceManager.Save(n.persistentStateLocked())
}

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

func (n *ConsensusNode) rebuildStateMachineFromCommittedLogLocked() {
	n.store.Reset()
	n.lastApplied = 0
	n.applyCommittedEntriesLocked()
}

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

func (n *ConsensusNode) entryByIndexLocked(index uint64) *consensuspb.LogEntry {
	for _, entry := range n.log {
		if entry.Index == index {
			return entry
		}
	}

	return nil
}

func (n *ConsensusNode) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Index
}

func (n *ConsensusNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Term
}

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

func (n *ConsensusNode) hasLogEntryLocked(index uint64, term uint64) bool {
	localTerm, ok := n.logTermAtIndexLocked(index)
	if !ok {
		return false
	}

	return localTerm == term
}

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

func (n *ConsensusNode) entriesFromIndexLocked(index uint64) []*consensuspb.LogEntry {
	entries := make([]*consensuspb.LogEntry, 0)
	for _, entry := range n.log {
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}

	return entries
}

func (n *ConsensusNode) isCandidateLogUpToDateLocked(candidateLastLogIndex uint64, candidateLastLogTerm uint64) bool {
	localLastLogTerm := n.lastLogTermLocked()
	localLastLogIndex := n.lastLogIndexLocked()

	if candidateLastLogTerm != localLastLogTerm {
		return candidateLastLogTerm > localLastLogTerm
	}

	return candidateLastLogIndex >= localLastLogIndex
}

func (n *ConsensusNode) majority() int {
	clusterSize := len(n.peers) + 1
	return (clusterSize / 2) + 1
}

func randomElectionTimeout() time.Duration {
	minTimeout := 1500
	maxTimeout := 3000
	randomMillis := minTimeout + rand.Intn(maxTimeout-minTimeout+1)

	return time.Duration(randomMillis) * time.Millisecond
}

func (n *ConsensusNode) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

func (n *ConsensusNode) electionLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(randomElectionTimeout())

		case <-timer.C:
			n.mu.Lock()
			isLeader := n.role == consensuspb.NodeRole_NODE_ROLE_LEADER
			n.mu.Unlock()

			if !isLeader {
				n.startElection()
			}

			timer.Reset(randomElectionTimeout())
		}
	}
}

func (n *ConsensusNode) startElection() {
	n.mu.Lock()
	if n.role == consensuspb.NodeRole_NODE_ROLE_LEADER {
		n.mu.Unlock()
		return
	}

	n.role = consensuspb.NodeRole_NODE_ROLE_CANDIDATE
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.leaderAddress = ""

	termStarted := n.currentTerm
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	votes := 1
	neededVotes := n.majority()

	if err := n.persistLocked(); err != nil {
		log.Printf("node %s cannot persist state before election: %v", n.id, err)
		n.mu.Unlock()
		return
	}

	log.Printf("node %s became candidate for term %d", n.id, termStarted)

	if votes >= neededVotes {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		go n.sendHeartbeats()
		return
	}

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
			client, err := n.getPeerClientLocked(peerID, peerAddress)
			if err != nil {
				n.markPeerOfflineLocked(peerID, err)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			resp, err := client.RequestVote(ctx, &consensuspb.RequestVoteRequest{
				Term:         termStarted,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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

			if n.role != consensuspb.NodeRole_NODE_ROLE_CANDIDATE || n.currentTerm != termStarted {
				return
			}

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.votedFor = ""
				n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
				n.leaderID = ""
				n.leaderAddress = ""

				if err := n.persistLocked(); err != nil {
					log.Printf("node %s cannot persist higher term: %v", n.id, err)
				}

				n.resetElectionTimer()
				return
			}

			if resp.VoteGranted {
				votes++

				if votes >= neededVotes && n.role == consensuspb.NodeRole_NODE_ROLE_CANDIDATE {
					n.becomeLeaderLocked()
					go n.sendHeartbeats()
				}
			}
		}(peerID, peerAddress)
	}
}

func (n *ConsensusNode) becomeLeaderLocked() {
	n.role = consensuspb.NodeRole_NODE_ROLE_LEADER
	n.leaderID = n.id
	n.leaderAddress = n.address

	nextIndex := n.lastLogIndexLocked() + 1
	for peerID := range n.peers {
		n.nextIndex[peerID] = nextIndex
		n.matchIndex[peerID] = 0
	}

	log.Printf("node %s became leader for term %d", n.id, n.currentTerm)
	n.resetElectionTimer()
}

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

func (n *ConsensusNode) RequestVote(ctx context.Context, req *consensuspb.RequestVoteRequest) (*consensuspb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}, nil
	}

	stateChanged := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
		n.leaderID = ""
		n.leaderAddress = ""
		stateChanged = true
	}

	voteGranted := false
	canVoteForCandidate := n.votedFor == "" || n.votedFor == req.CandidateId
	logIsUpToDate := n.isCandidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	if canVoteForCandidate && logIsUpToDate {
		n.votedFor = req.CandidateId
		voteGranted = true
		stateChanged = true
	}

	if stateChanged {
		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	if voteGranted {
		n.resetElectionTimer()
	}

	return &consensuspb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}, nil
}

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

func (n *ConsensusNode) InstallSnapshot(ctx context.Context, req *consensuspb.InstallSnapshotRequest) (*consensuspb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
		n.leaderID = req.LeaderId

		if leaderAddress, ok := n.peers[req.LeaderId]; ok {
			n.leaderAddress = leaderAddress
		}

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	n.resetElectionTimer()

	return &consensuspb.InstallSnapshotResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}

func (n *ConsensusNode) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	success, errorMessage, leaderHint, err := n.handleWriteOperation(
		ctx,
		consensuspb.LogOperation_LOG_OPERATION_PUT,
		req.Key,
		req.Value,
	)
	if err != nil {
		return nil, err
	}

	return &kvpb.PutResponse{
		Success:    success,
		Error:      errorMessage,
		LeaderHint: leaderHint,
	}, nil
}

func (n *ConsensusNode) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.GetResponse{
			Found:      false,
			Error:      "node is not leader",
			LeaderHint: n.leaderAddress,
		}, nil
	}

	value, ok := n.store.Get(req.Key)
	if !ok {
		return &kvpb.GetResponse{
			Found:      false,
			LeaderHint: n.leaderAddress,
		}, nil
	}

	return &kvpb.GetResponse{
		Found:      true,
		Value:      value,
		LeaderHint: n.leaderAddress,
	}, nil
}

func (n *ConsensusNode) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	success, errorMessage, leaderHint, err := n.handleWriteOperation(
		ctx,
		consensuspb.LogOperation_LOG_OPERATION_DELETE,
		req.Key,
		"",
	)
	if err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{
		Success:    success,
		Error:      errorMessage,
		LeaderHint: leaderHint,
	}, nil
}

func (n *ConsensusNode) GetLeader(ctx context.Context, req *kvpb.GetLeaderRequest) (*kvpb.GetLeaderResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.GetLeaderResponse{
			HasLeader:     true,
			LeaderId:      n.id,
			LeaderAddress: n.address,
			Term:          n.currentTerm,
		}, nil
	}

	if n.leaderID != "" {
		return &kvpb.GetLeaderResponse{
			HasLeader:     true,
			LeaderId:      n.leaderID,
			LeaderAddress: n.leaderAddress,
			Term:          n.currentTerm,
		}, nil
	}

	return &kvpb.GetLeaderResponse{
		HasLeader: false,
		Term:      n.currentTerm,
	}, nil
}
