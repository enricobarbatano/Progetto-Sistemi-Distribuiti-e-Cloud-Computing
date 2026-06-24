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
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/storage"
	"google.golang.org/grpc"
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
