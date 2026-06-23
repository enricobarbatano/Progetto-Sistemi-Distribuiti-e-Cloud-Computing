package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

type persistentState struct {
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`
}

type ConsensusNode struct {
	consensuspb.UnimplementedConsensusServiceServer
	kvpb.UnimplementedKeyValueServiceServer

	mu sync.Mutex

	id      string
	address string
	peers   map[string]string

	// Stato Raft persistente.
	currentTerm uint64
	votedFor    string
	log         []*consensuspb.LogEntry

	// Stato Raft volatile.
	commitIndex uint64
	lastApplied uint64
	role        consensuspb.NodeRole

	// Stato volatile usato solo dal leader.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Macchina a stati key-value locale.
	data map[string]string

	// File usato per salvare lo stato persistente.
	stateFile string
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
		id:          id,
		address:     address,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*consensuspb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		role:        consensuspb.NodeRole_NODE_ROLE_FOLLOWER,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		data:        make(map[string]string),
		stateFile:   filepath.Join(dataDir, fmt.Sprintf("%s_state.json", id)),
	}

	if err := node.loadPersistentState(); err != nil {
		return nil, err
	}

	return node, nil
}

func (n *ConsensusNode) persistLocked() error {
	state := persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal persistent state: %w", err)
	}

	tmpFile := n.stateFile + ".tmp"

	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		return fmt.Errorf("cannot write temporary state file: %w", err)
	}

	if err := os.Rename(tmpFile, n.stateFile); err != nil {
		return fmt.Errorf("cannot replace state file: %w", err)
	}

	return nil
}

func (n *ConsensusNode) loadPersistentState() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	content, err := os.ReadFile(n.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return n.persistLocked()
		}

		return fmt.Errorf("cannot read persistent state: %w", err)
	}

	var state persistentState
	if err := json.Unmarshal(content, &state); err != nil {
		return fmt.Errorf("cannot unmarshal persistent state: %w", err)
	}

	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor

	if state.Log == nil {
		n.log = make([]*consensuspb.LogEntry, 0)
	} else {
		n.log = state.Log
	}

	return nil
}

func (n *ConsensusNode) applyCommittedEntriesLocked() {
	for n.lastApplied < n.commitIndex {
		nextIndex := n.lastApplied + 1
		entry := n.entryByIndexLocked(nextIndex)

		if entry == nil {
			break
		}

		switch entry.Operation {
		case consensuspb.LogOperation_LOG_OPERATION_PUT:
			n.data[entry.Key] = entry.Value

		case consensuspb.LogOperation_LOG_OPERATION_DELETE:
			delete(n.data, entry.Key)

		case consensuspb.LogOperation_LOG_OPERATION_NOOP:
			// Nessuna operazione da applicare.

		default:
			// Per ora ignoriamo operazioni non riconosciute.
		}

		n.lastApplied = nextIndex
	}
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

func (n *ConsensusNode) RequestVote(ctx context.Context, req *consensuspb.RequestVoteRequest) (*consensuspb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	voteGranted := false

	if req.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == req.CandidateId) {
		n.votedFor = req.CandidateId
		voteGranted = true

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
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

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	if len(req.Entries) > 0 {
		n.log = append(n.log, req.Entries...)

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastIndex := n.lastLogIndexLocked()

		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
		}

		n.applyCommittedEntriesLocked()
	}

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

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	return &consensuspb.InstallSnapshotResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}

func (n *ConsensusNode) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.PutResponse{
			Success:    false,
			Error:      "node is not leader",
			LeaderHint: "",
		}, nil
	}

	entry := &consensuspb.LogEntry{
		Index:     n.lastLogIndexLocked() + 1,
		Term:      n.currentTerm,
		Operation: consensuspb.LogOperation_LOG_OPERATION_PUT,
		Key:       req.Key,
		Value:     req.Value,
	}

	n.log = append(n.log, entry)
	n.commitIndex = entry.Index
	n.applyCommittedEntriesLocked()

	if err := n.persistLocked(); err != nil {
		return nil, err
	}

	return &kvpb.PutResponse{
		Success: true,
	}, nil
}

func (n *ConsensusNode) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	value, ok := n.data[req.Key]
	if !ok {
		return &kvpb.GetResponse{
			Found: false,
		}, nil
	}

	return &kvpb.GetResponse{
		Found: true,
		Value: value,
	}, nil
}

func (n *ConsensusNode) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.DeleteResponse{
			Success:    false,
			Error:      "node is not leader",
			LeaderHint: "",
		}, nil
	}

	entry := &consensuspb.LogEntry{
		Index:     n.lastLogIndexLocked() + 1,
		Term:      n.currentTerm,
		Operation: consensuspb.LogOperation_LOG_OPERATION_DELETE,
		Key:       req.Key,
	}

	n.log = append(n.log, entry)
	n.commitIndex = entry.Index
	n.applyCommittedEntriesLocked()

	if err := n.persistLocked(); err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{
		Success: true,
	}, nil
}

func (n *ConsensusNode) GetLeader(ctx context.Context, req *kvpb.GetLeaderRequest) (*kvpb.GetLeaderResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.GetLeaderResponse{
			HasLeader: false,
			Term:      n.currentTerm,
		}, nil
	}

	return &kvpb.GetLeaderResponse{
		HasLeader:     true,
		LeaderId:      n.id,
		LeaderAddress: n.address,
		Term:          n.currentTerm,
	}, nil
}
