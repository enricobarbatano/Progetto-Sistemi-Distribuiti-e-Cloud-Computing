// Package consensus contiene la logica interna del Consensus Node.
//
// In questa fase del progetto il package definisce la struttura base di un nodo
// di consenso stateful. Il nodo implementa gli stub gRPC generati dai file
// .proto e mantiene lo stato necessario per una futura implementazione completa
// di Raft.
//
// La logica qui presente non è ancora un Raft completo: rappresenta
// l'ossatura iniziale su cui verranno poi aggiunti elezione del leader,
// heartbeat, replicazione su quorum e gestione completa dei log.
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
// In questa fase il nodo mantiene già tutto lo stato principale richiesto da
// Raft, ma molte operazioni sono ancora stub o implementazioni semplificate.
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
	// Per ora sono inizializzati ma non ancora usati pienamente.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Macchina a stati key-value locale.
	// Le entry committed del log vengono applicate a questa mappa.
	data map[string]string

	// Percorso del file usato per salvare lo stato persistente del nodo.
	stateFile string
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
	}

	// Se esiste uno stato precedente su disco, viene caricato.
	// Altrimenti viene creato un nuovo file di stato iniziale.
	if err := node.loadPersistentState(); err != nil {
		return nil, err
	}

	return node, nil
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

		// Se l'entry non esiste nel log, interrompiamo l'applicazione.
		// Questo evita accessi non validi in una fase in cui la gestione del
		// log non è ancora completa.
		if entry == nil {
			break
		}

		switch entry.Operation {
		case consensuspb.LogOperation_LOG_OPERATION_PUT:
			// Applica una scrittura o un aggiornamento.
			n.data[entry.Key] = entry.Value

		case consensuspb.LogOperation_LOG_OPERATION_DELETE:
			// Applica una cancellazione.
			delete(n.data, entry.Key)

		case consensuspb.LogOperation_LOG_OPERATION_NOOP:
			// NOOP non modifica la macchina a stati.
			// Potrà essere utile più avanti per alcune fasi del consenso.

		default:
			// Operazioni non riconosciute vengono ignorate in questa fase.
			// In una versione più completa si potrebbe restituire errore o loggare.
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

// RequestVote gestisce la RPC usata da un Candidate per richiedere un voto.
//
// Questa implementazione è ancora semplificata: aggiorna il termine se riceve
// un termine più alto e concede il voto se il nodo non ha già votato nello stesso
// termine oppure se aveva già votato per lo stesso candidato.
//
// La verifica completa della freschezza del log del candidato verrà aggiunta
// nella fase di implementazione completa dell'elezione leader.
func (n *ConsensusNode) RequestVote(ctx context.Context, req *consensuspb.RequestVoteRequest) (*consensuspb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Se il candidato ha un termine più recente, il nodo aggiorna il proprio
	// termine, dimentica il voto precedente e torna follower.
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	voteGranted := false

	// Il voto viene concesso solo se il termine coincide e il nodo non ha già
	// votato per un altro candidato nello stesso termine.
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

// AppendEntries gestisce la RPC usata dal leader per heartbeat e replicazione.
//
// In Raft questa RPC ha due ruoli principali:
//   - inviare heartbeat periodici ai follower;
//   - replicare nuove entry del log.
//
// Questa versione è ancora semplificata: controlla il termine, accoda eventuali
// entry ricevute e aggiorna commitIndex in base al valore comunicato dal leader.
// Il controllo completo di coerenza con prevLogIndex e prevLogTerm sarà aggiunto
// nella fase successiva.
func (n *ConsensusNode) AppendEntries(ctx context.Context, req *consensuspb.AppendEntriesRequest) (*consensuspb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Se il termine del leader è vecchio, la richiesta viene rifiutata.
	if req.Term < n.currentTerm {
		return &consensuspb.AppendEntriesResponse{
			Term:       n.currentTerm,
			Success:    false,
			MatchIndex: n.lastLogIndexLocked(),
		}, nil
	}

	// Se il leader ha un termine più recente, il nodo aggiorna il proprio stato
	// e torna follower.
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	// In questa prima versione le entry vengono semplicemente aggiunte al log.
	// La gestione completa dei conflitti verrà implementata più avanti.
	if len(req.Entries) > 0 {
		n.log = append(n.log, req.Entries...)

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	// Aggiorna commitIndex in base a quanto comunicato dal leader.
	// Non può superare l'ultimo indice realmente presente nel log locale.
	if req.LeaderCommit > n.commitIndex {
		lastIndex := n.lastLogIndexLocked()

		if req.LeaderCommit < lastIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastIndex
		}

		// Applica alla mappa key-value le entry diventate committed.
		n.applyCommittedEntriesLocked()
	}

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

	// Snapshot proveniente da un termine vecchio: richiesta rifiutata.
	if req.Term < n.currentTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	// Se lo snapshot arriva da un leader con termine più nuovo, il nodo aggiorna
	// il proprio termine e torna follower.
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

	// Solo il leader può accettare scritture.
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

	// Inserimento della nuova operazione nel log locale.
	n.log = append(n.log, entry)

	// Per ora la entry viene considerata committed immediatamente.
	// Questo verrà sostituito dalla logica di quorum.
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
			Found: false,
		}, nil
	}

	return &kvpb.GetResponse{
		Found: true,
		Value: value,
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

	// Solo il leader può accettare modifiche allo stato.
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

	// Inserimento della cancellazione nel log.
	n.log = append(n.log, entry)

	// Commit immediato solo per questa fase iniziale.
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
// In questa fase il nodo risponde positivamente solo se è lui stesso leader.
// Non esiste ancora una propagazione completa dell'informazione sul leader
// corrente, che verrà aggiunta insieme all'elezione e agli heartbeat.
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
