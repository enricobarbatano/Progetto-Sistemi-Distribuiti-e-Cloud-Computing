// Package consensus implementa la logica del nodo di consenso.
//
// Il package contiene il cuore del sistema distribuito: elezione del leader,
// replica del log, commit su quorum, persistenza dello stato e applicazione
// delle entry committed alla state machine key-value.
//
// L'implementazione è basata su un protocollo con log replicato ispirato a Raft.
// Il nodo può assumere i ruoli di follower, candidate o leader. Le scritture
// vengono accettate dal leader e applicate solo dopo replica sulla maggioranza
// del cluster, così da mantenere una semantica linearizzabile per le operazioni
// completate.
//
// Il package espone anche le RPC key-value e backup perché il ConsensusNode è
// il componente che possiede materialmente log, state machine, WAL e snapshot.
package consensus

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/storage"
	"google.golang.org/grpc"
)

const defaultSnapshotThreshold uint64 = 1000

// ConsensusNode rappresenta un nodo stateful del cluster.
//
// Ogni nodo mantiene lo stato persistente richiesto dal protocollo di consenso:
// termine corrente, voto espresso, log, commit index, last applied, snapshot e
// state machine locale. Il nodo può cambiare ruolo durante l'esecuzione passando
// tra follower, candidate e leader.
//
// La struct contiene anche i client gRPC verso i peer, lo stato online/offline
// dei nodi remoti e i componenti locali di storage e persistenza. In questo modo
// il nodo può coordinare consenso, replica e recovery senza delegare ad altri
// servizi la responsabilità dello stato replicato.
type ConsensusNode struct {
	consensuspb.UnimplementedConsensusServiceServer
	kvpb.UnimplementedKeyValueServiceServer
	backuppb.UnimplementedBackupNodeServiceServer

	// mu protegge lo stato interno del nodo.
	//
	// Le RPC gRPC possono arrivare in concorrenza: RequestVote, AppendEntries,
	// Put/Get e InstallSnapshot possono accedere a termine, ruolo, log e commit
	// index. Il mutex evita race condition e garantisce che ogni transizione di
	// stato del nodo sia atomica dal punto di vista del processo locale.
	mu sync.Mutex

	// id identifica stabilmente il nodo nel cluster.
	// address è l'indirizzo gRPC su cui il nodo è raggiungibile.
	// peers contiene gli altri nodi del cluster, indicizzati per nodeID.
	id      string
	address string
	peers   map[string]string

	// currentTerm è il termine più alto osservato dal nodo.
	// Cresce in modo monotono e funziona come clock logico per scartare messaggi
	// obsoleti provenienti da termini precedenti.
	currentTerm uint64

	// votedFor memorizza il candidato votato nel termine corrente.
	// Serve a garantire che un nodo conceda al massimo un voto per termine.
	votedFor string

	// log contiene le entry replicate.
	// Ogni entry rappresenta un comando della state machine, come PUT, DELETE o NOOP.
	log []*consensuspb.LogEntry

	// commitIndex è l'indice più alto noto come committed.
	// Una entry diventa committed quando il leader la replica su un quorum.
	commitIndex uint64

	// lastApplied è l'indice più alto già applicato alla state machine locale.
	//Può essere minore di commitIndex se ci sono entry committed non ancora applicate.
	lastApplied uint64

	//role rappresenta il ruolo corrente del nodo: follower, candidate o leader.
	role consensuspb.NodeRole

	// lastIncludedIndex e lastIncludedTerm rappresentano il prefisso del log
	// già coperto dallo snapshot locale.
	//
	// Dopo la log compaction, il log fisico può non contenere più le entry
	// iniziali. Questi due campi permettono comunque di validare prevLogIndex
	// e prevLogTerm quando l'indice coincide con il bordo dello snapshot.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	// nextIndex indica, per ogni follower, la prossima entry che il leader proverà
	// a inviare. matchIndex indica invece l'ultima entry che il leader sa essere
	// replicata con successo su quel follower.
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

	// peerConns e peerClients mantengono connessioni gRPC riutilizzabili verso i peer.
	// Riutilizzare le connessioni evita di aprire un nuovo canale per ogni RPC.
	peerConns   map[string]*grpc.ClientConn
	peerClients map[string]consensuspb.ConsensusServiceClient

	// peerOnline traccia lo stato osservato dei peer.
	// Questo campo non decide il consenso, ma aiuta logging, diagnostica e gestione
	// di nodi temporaneamente non raggiungibili.
	peerOnline map[string]bool
}

// NewConsensusNode inizializza un nuovo nodo di consenso.
//
// La funzione prepara lo stato in memoria, crea lo storage locale, inizializza
// il manager di persistenza e prova a caricare eventuale stato precedente da
// disco. Questo permette a un nodo di ripartire dopo un crash mantenendo termine,
// voto, log, snapshot e state machine persistenti.
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

		lastIncludedIndex: 0,
		lastIncludedTerm:  0,

		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		store: storage.NewKVStore(),

		persistenceManager: persistence.NewManager(id, dataDir),

		snapshotThreshold: readUint64Env("SNAPSHOT_THRESHOLD", defaultSnapshotThreshold),

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

// readUint64Env legge una variabile d'ambiente uint64.
// Viene usata per rendere configurabile la soglia di snapshot senza modificare
// il codice. Se il valore non esiste, non è valido o è zero, viene usato il
// default passato dal chiamante.
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

// Start avvia i loop principali del nodo.
//
// Il loop di elezione gestisce timeout e transizioni a candidate; il loop degli
// heartbeat invia periodicamente AppendEntries vuoti quando il nodo è leader.
func (n *ConsensusNode) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

// Stop arresta il nodo e chiude le connessioni gRPC verso i peer.
//
// stopOnce garantisce che la chiusura avvenga una sola volta, evitando panic su
// chiusure multiple del canale stopCh.
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
