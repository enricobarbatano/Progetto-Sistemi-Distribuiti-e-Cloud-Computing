// Package consensus contiene la logica interna del Consensus Node.
//
// In questa fase del progetto il package definisce la struttura base di un nodo
// di consenso stateful. Il nodo implementa gli stub gRPC generati dai file
// .proto e mantiene lo stato necessario per una futura implementazione completa
// di Raft.
//
// Questo file include una prima implementazione dell'elezione del leader:
//
//   - timer di elezione randomizzato;
//   - transizione da Follower a Candidate;
//   - invio parallelo di RequestVote ai peer;
//   - conteggio dei voti ricevuti;
//   - transizione da Candidate a Leader al raggiungimento della maggioranza;
//   - invio periodico di heartbeat tramite AppendEntries vuote.
//
// In più, prima della fase di replicazione dei log, sono state aggiunte alcune
// migliorie infrastrutturali:
//
//   - riuso delle connessioni gRPC verso i peer;
//   - gestione dello stato online/offline dei peer;
//   - logging dei peer offline solo al cambio di stato.
//
// La logica di replicazione completa dei log non è ancora implementata.
// Verrà aggiunta nelle fasi successive del progetto.
package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// persistentState rappresenta la parte dello stato Raft che deve sopravvivere
// a un riavvio del nodo.
//
// Secondo Raft, currentTerm, votedFor e log sono informazioni persistenti:
// se il nodo crasha e riparte, deve poter recuperare questi valori da storage
// stabile prima di tornare a partecipare al cluster.
type persistentState struct {
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`
}

// ConsensusNode rappresenta un nodo stateful del cluster di consenso.
//
// La struct implementa sia ConsensusServiceServer sia KeyValueServiceServer.
// Questo significa che lo stesso processo gRPC può ricevere sia chiamate interne
// al protocollo di consenso, sia richieste legate allo storage chiave-valore.
//
// In questa fase il nodo mantiene già lo stato principale richiesto da Raft.
// La parte di elezione del leader è implementata in forma base; la replicazione
// atomica dei log verrà completata nella fase successiva.
type ConsensusNode struct {
	// Embedding degli stub non implementati generati da protoc.
	// Questo permette alla struct di soddisfare le interfacce gRPC anche se
	// in futuro vengono aggiunti nuovi metodi ai servizi.
	consensuspb.UnimplementedConsensusServiceServer
	kvpb.UnimplementedKeyValueServiceServer

	// Mutex usato per proteggere tutto lo stato interno del nodo.
	// Le RPC gRPC possono essere gestite in goroutine diverse, quindi senza
	// lock ci sarebbero race condition su term, log, ruolo e mappa key-value.
	mu sync.Mutex

	// Identificativo logico del nodo, indirizzo gRPC locale e lista dei peer.
	id      string
	address string
	peers   map[string]string

	// Stato Raft persistente.
	// Questi campi vengono salvati su disco perché devono sopravvivere ai crash.
	currentTerm uint64
	votedFor    string
	log         []*consensuspb.LogEntry

	// Stato Raft volatile.
	// Questi campi esistono solo in memoria e vengono ricostruiti durante
	// l'esecuzione del nodo.
	commitIndex uint64
	lastApplied uint64
	role        consensuspb.NodeRole

	// Stato volatile usato solo dal leader.
	// nextIndex indica, per ogni follower, la prossima log entry da inviare.
	// matchIndex indica, per ogni follower, l'ultima entry nota come replicata.
	// Per ora sono inizializzati e saranno usati nella fase di log replication.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Macchina a stati key-value locale.
	// Le entry committed del log vengono applicate a questa mappa.
	data map[string]string

	// Percorso del file usato per salvare lo stato persistente del nodo.
	stateFile string

	// Canale usato per notificare al loop di elezione che il timer va resettato.
	// Viene usato quando arriva un heartbeat valido oppure quando il nodo
	// concede un voto.
	electionResetCh chan struct{}

	// Canale usato per fermare le goroutine interne del nodo.
	stopCh chan struct{}

	// Evita di chiudere stopCh più di una volta.
	stopOnce sync.Once

	// Intervallo con cui un leader invia heartbeat ai follower.
	heartbeatInterval time.Duration

	// Informazioni sul leader attualmente conosciuto.
	// Un follower aggiorna questi campi quando riceve AppendEntries valido.
	leaderID      string
	leaderAddress string

	// Connessioni e client gRPC persistenti verso i peer.
	// Questo evita di aprire una nuova connessione a ogni RequestVote o heartbeat.
	peerConns   map[string]*grpc.ClientConn
	peerClients map[string]consensuspb.ConsensusServiceClient

	// Stato di raggiungibilità dei peer.
	// Serve per loggare solo quando un peer cambia stato da online a offline
	// o da offline a online.
	peerOnline map[string]bool
}

// NewConsensusNode costruisce e inizializza un nuovo ConsensusNode.
//
// Il costruttore prepara lo stato iniziale del nodo, crea la directory dei dati,
// inizializza le mappe interne e prova a caricare da disco lo stato persistente.
// Se non esiste ancora un file di stato, ne viene creato uno nuovo.
func NewConsensusNode(id string, address string, peers map[string]string, dataDir string) (*ConsensusNode, error) {
	if id == "" {
		return nil, errors.New("node id cannot be empty")
	}

	// Se non viene specificata una directory dati, viene usata "data".
	if dataDir == "" {
		dataDir = "data"
	}

	// La directory dati deve esistere prima di poter salvare lo stato persistente.
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create data directory: %w", err)
	}

	node := &ConsensusNode{
		id:      id,
		address: address,
		peers:   peers,

		// Stato persistente iniziale.
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*consensuspb.LogEntry, 0),

		// Stato volatile iniziale.
		commitIndex: 0,
		lastApplied: 0,
		role:        consensuspb.NodeRole_NODE_ROLE_FOLLOWER,

		// Strutture dati che saranno usate dal leader durante la replicazione.
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),

		// Macchina a stati key-value locale.
		data: make(map[string]string),

		// File JSON in cui viene salvato lo stato persistente del nodo.
		stateFile: filepath.Join(dataDir, fmt.Sprintf("%s_state.json", id)),

		// Canale bufferizzato usato per resettare il timer di elezione.
		electionResetCh: make(chan struct{}, 1),

		// Canale usato per fermare i loop interni.
		stopCh: make(chan struct{}),

		// Intervallo con cui il leader invia heartbeat ai follower.
		heartbeatInterval: 300 * time.Millisecond,

		// All'avvio il nodo non conosce ancora un leader.
		leaderID:      "",
		leaderAddress: "",

		// Client gRPC persistenti verso i peer.
		peerConns:   make(map[string]*grpc.ClientConn),
		peerClients: make(map[string]consensuspb.ConsensusServiceClient),

		// Stato iniziale dei peer.
		// Li consideriamo online all'inizio, così il primo fallimento viene loggato
		// come transizione online -> offline.
		peerOnline: make(map[string]bool),
	}

	for peerID := range peers {
		node.peerOnline[peerID] = true
	}

	// Se esiste uno stato precedente su disco, viene caricato.
	// Altrimenti viene creato un nuovo file di stato iniziale.
	if err := node.loadPersistentState(); err != nil {
		return nil, err
	}

	return node, nil
}

// Start avvia le goroutine interne del nodo.
//
// In particolare avvia:
//   - il loop di elezione, che controlla la scadenza del timeout;
//   - il loop degli heartbeat, che invia AppendEntries vuote quando il nodo è leader.
func (n *ConsensusNode) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

// Stop ferma le goroutine interne del nodo e chiude le connessioni gRPC
// persistenti verso i peer.
//
// La sync.Once evita il panic che si avrebbe chiudendo più volte lo stesso canale.
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

// getPeerClientLocked restituisce un client gRPC persistente verso un peer.
//
// Deve essere chiamata con n.mu già acquisito.
// Se il client esiste già, viene riutilizzato.
// Se non esiste, viene creata una nuova connessione gRPC e salvata nella struct.
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

// markPeerOfflineLocked marca un peer come non raggiungibile.
//
// Il log viene scritto solo quando il peer passa da online a offline.
// Questo evita di stampare lo stesso errore a ogni heartbeat fallito.
func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || wasOnline {
		log.Printf("node %s marked peer %s as offline: %v", n.id, peerID, err)
	}

	n.peerOnline[peerID] = false
}

// markPeerOnlineLocked marca un peer come raggiungibile.
//
// Il log viene scritto solo quando il peer passa da offline a online.
func (n *ConsensusNode) markPeerOnlineLocked(peerID string) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || !wasOnline {
		log.Printf("node %s marked peer %s as online", n.id, peerID)
	}

	n.peerOnline[peerID] = true
}

// persistLocked salva su disco lo stato persistente del nodo.
//
// Il suffisso "Locked" indica che questa funzione deve essere chiamata solo
// quando il mutex n.mu è già stato acquisito dal chiamante.
//
// Il salvataggio avviene scrivendo prima su un file temporaneo e poi sostituendo
// il file definitivo tramite rename. Questo riduce il rischio di lasciare un
// file di stato corrotto in caso di errore durante la scrittura.
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

// loadPersistentState carica da disco lo stato persistente del nodo.
//
// Se il file di stato non esiste ancora, viene creato un nuovo file con lo
// stato iniziale. In questo modo il nodo risulta stateful fin dal primo avvio.
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

// applyCommittedEntriesLocked applica alla macchina a stati locale tutte le
// entry committed ma non ancora applicate.
//
// In Raft, una entry diventa committed quando è stata confermata secondo le
// regole del protocollo. Una volta committed, viene applicata alla state machine.
// In questo progetto la state machine è una semplice map[string]string.
//
// Anche questa funzione richiede che il lock sia già acquisito.
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
			// NOOP non modifica la macchina a stati.

		default:
			// Operazioni non riconosciute vengono ignorate in questa fase.
		}

		n.lastApplied = nextIndex
	}
}

// entryByIndexLocked cerca una entry del log tramite il suo indice.
//
// La funzione effettua una ricerca lineare perché in questa fase il log è ancora
// una slice semplice. Più avanti si potrà ottimizzare se necessario.
//
// Richiede che il lock sia già acquisito.
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
// Se il log è vuoto, restituisce 0. Anche questa funzione richiede che il lock
// sia già acquisito.
func (n *ConsensusNode) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Index
}

// lastLogTermLocked restituisce il termine dell'ultima entry del log.
//
// Se il log è vuoto, restituisce 0.
// La funzione richiede che il lock n.mu sia già stato acquisito.
func (n *ConsensusNode) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}

	return n.log[len(n.log)-1].Term
}

// isCandidateLogUpToDateLocked verifica se il log di un candidato è almeno
// aggiornato quanto quello locale.
//
// Raft concede il voto solo a candidati con un log non più vecchio del proprio.
// Il confronto avviene prima sul termine dell'ultima entry e poi sull'indice.
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

// randomElectionTimeout genera un timeout di elezione casuale.
//
// L'intervallo scelto è volutamente più largo rispetto ai valori classici di Raft,
// perché i test locali su Windows con più processi avviati manualmente tramite
// go run possono introdurre latenza e jitter.
func randomElectionTimeout() time.Duration {
	minTimeout := 1500
	maxTimeout := 3000

	randomMillis := minTimeout + rand.Intn(maxTimeout-minTimeout+1)

	return time.Duration(randomMillis) * time.Millisecond
}

// resetElectionTimer notifica al loop di elezione che il timer deve essere resettato.
//
// Il canale è bufferizzato e l'invio è non bloccante: se c'è già un reset in
// attesa, non serve accodarne un altro.
func (n *ConsensusNode) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

// electionLoop gestisce il timer di elezione del nodo.
//
// Se il timer scade e il nodo non è leader, viene avviata una nuova elezione.
// Il timer viene resettato quando il nodo riceve un heartbeat valido o concede
// un voto a un candidato.
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

// startElection avvia una nuova elezione Raft.
//
// Il nodo passa a Candidate, incrementa il termine, vota per se stesso e invia
// RequestVote in parallelo a tutti i peer conosciuti.
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

// becomeLeaderLocked trasforma il nodo corrente in leader.
//
// Questa funzione deve essere chiamata con n.mu già acquisito.
// Inizializza le strutture nextIndex e matchIndex per ogni follower.
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

// heartbeatLoop invia heartbeat periodici quando il nodo è leader.
//
// Gli heartbeat sono AppendEntries vuoti. Servono a mantenere l'autorità del
// leader e a impedire che i follower avviino nuove elezioni.
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
// In questa fase gli heartbeat non replicano ancora log entry.
// Servono solo per stabilizzare l'elezione del leader.
func (n *ConsensusNode) sendHeartbeats() {
	n.mu.Lock()

	term := n.currentTerm
	leaderID := n.id
	leaderCommit := n.commitIndex
	prevLogIndex := n.lastLogIndexLocked()
	prevLogTerm := n.lastLogTermLocked()

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

// RequestVote gestisce la RPC usata da un Candidate per richiedere un voto.
//
// Un voto viene concesso solo se:
//   - il termine del candidato non è più vecchio del termine locale;
//   - il nodo non ha già votato per un altro candidato nello stesso termine;
//   - il log del candidato è almeno aggiornato quanto quello locale.
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

// AppendEntries gestisce la RPC usata dal leader per heartbeat e replicazione.
//
// In questa fase viene usata soprattutto come heartbeat. Quando un follower
// riceve un AppendEntries valido, aggiorna il leader noto e resetta il timer
// di elezione.
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

	if len(req.Entries) > 0 {
		n.log = append(n.log, req.Entries...)
		stateChanged = true
	}

	if stateChanged {
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

	n.resetElectionTimer()

	return &consensuspb.AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: n.lastLogIndexLocked(),
	}, nil
}

// InstallSnapshot gestisce la RPC usata per installare uno snapshot.
//
// In una versione completa, questa RPC permetterà al leader di inviare a un
// follower uno snapshot quando il follower è troppo indietro rispetto al log.
// Per ora il metodo aggiorna solo il termine se necessario e restituisce una
// risposta coerente.
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

// Put gestisce una richiesta di scrittura sullo storage key-value.
//
// In Raft, una scrittura deve essere accettata dal leader, inserita nel log,
// replicata sui follower e applicata solo dopo il commit. In questa versione
// semplificata, se il nodo è leader, la entry viene aggiunta al log locale e
// applicata subito.
//
// La replicazione su quorum verrà aggiunta nelle fasi successive.
func (n *ConsensusNode) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.PutResponse{
			Success:    false,
			Error:      "node is not leader",
			LeaderHint: n.leaderAddress,
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

// Get gestisce una richiesta di lettura dallo storage key-value locale.
//
// Per ora legge direttamente dalla mappa locale. In una versione completa,
// per garantire consistenza forte, il proxy dovrà preferire letture dal leader
// oppure usare un meccanismo equivalente al read-index.
func (n *ConsensusNode) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	value, ok := n.data[req.Key]
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

// Delete gestisce una richiesta di cancellazione dallo storage key-value.
//
// Come Put, anche Delete modifica lo stato del sistema, quindi in una versione
// completa dovrà essere accettata solo dal leader e replicata su quorum prima
// di essere applicata alla macchina a stati.
func (n *ConsensusNode) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.DeleteResponse{
			Success:    false,
			Error:      "node is not leader",
			LeaderHint: n.leaderAddress,
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

// GetLeader restituisce informazioni sul leader noto.
//
// Se il nodo corrente è leader, restituisce se stesso.
// Se il nodo è follower ma ha ricevuto heartbeat validi, restituisce il leader
// conosciuto. Se non conosce alcun leader, risponde con HasLeader=false.
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
