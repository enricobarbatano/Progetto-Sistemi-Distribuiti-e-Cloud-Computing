// Package persistence contiene la gestione dello stato persistente dei nodi.
//
// Questo package non conosce il protocollo Raft nel dettaglio operativo:
// non decide quando una entry è committed, non replica log e non gestisce
// elezioni. Si occupa solo di salvare e caricare da disco:
//
//   - lo stato compatto state.json;
//   - il WAL append-only wal.log;
//   - lo snapshot locale snapshot.json.
//
// In questo modo ConsensusNode può concentrarsi sul consenso, mentre
// PersistenceManager gestisce solo i file.
package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// State rappresenta lo stato persistente completo del nodo.
//
// Contiene sia lo stato Raft persistente classico, quindi currentTerm,
// votedFor e log, sia alcuni campi utili per il recovery robusto del progetto:
// commitIndex, lastApplied e data map.
//
// In una implementazione Raft stretta commitIndex e lastApplied sono volatile,
// ma in questo progetto vengono salvati per rendere il recovery della state
// machine più semplice e robusto.
type State struct {
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`

	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	Data        map[string]string `json:"data"`
}

// Snapshot rappresenta un checkpoint locale della state machine.
//
// Per ora lo snapshot non implementa ancora la log compaction reale e non viene
// inviato ai follower tramite InstallSnapshot. Serve però come base stabile per
// recovery e per preparare il sistema al Backup Service.
type Snapshot struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}

// walRecord rappresenta una riga append-only del WAL.
//
// In questa fase il WAL è ancora snapshot-based: ogni record contiene una copia
// dello stato persistente corrente. Più avanti potrà essere reso event-based,
// con record più granulari come log_appended, vote_granted, commit_advanced.
type walRecord struct {
	Timestamp   string                  `json:"timestamp"`
	NodeID      string                  `json:"node_id"`
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`

	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	Data        map[string]string `json:"data"`
}

// Manager gestisce tutti i file persistenti di un singolo nodo.
//
// Questa struct è il componente che sostituisce la logica di persistenza che
// prima era dentro ConsensusNode. Il nodo usa Manager per salvare o caricare
// lo stato, ma non si occupa più direttamente di JSON, WAL o snapshot file.
type Manager struct {
	nodeID       string
	stateFile    string
	walFile      string
	snapshotFile string
}

// NewManager crea un nuovo manager di persistenza per un nodo.
//
// dataDir deve essere la directory dati specifica del nodo, ad esempio:
//
//	data/node-1
//
// I file gestiti saranno:
//
//	node-1_state.json
//	node-1_wal.log
//	node-1_snapshot.json
func NewManager(nodeID string, dataDir string) *Manager {
	return &Manager{
		nodeID:       nodeID,
		stateFile:    filepath.Join(dataDir, fmt.Sprintf("%s_state.json", nodeID)),
		walFile:      filepath.Join(dataDir, fmt.Sprintf("%s_wal.log", nodeID)),
		snapshotFile: filepath.Join(dataDir, fmt.Sprintf("%s_snapshot.json", nodeID)),
	}
}

// Save salva lo stato persistente del nodo.
//
// La scrittura segue il principio del Write-Ahead Logging:
//
//  1. prima viene aggiunto un record append-only al WAL;
//  2. poi viene aggiornato atomicamente state.json.
//
// Il chiamante deve passare uno State già coerente.
func (m *Manager) Save(state State) error {
	state.Data = cloneData(state.Data)

	if err := m.appendWAL(state); err != nil {
		return err
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal persistent state: %w", err)
	}

	tmpFile := m.stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		return fmt.Errorf("cannot write temporary state file: %w", err)
	}

	if err := os.Rename(tmpFile, m.stateFile); err != nil {
		return fmt.Errorf("cannot replace state file: %w", err)
	}

	return nil
}

// Load carica lo stato persistente del nodo.
//
// L'ordine di recovery è:
//
//  1. prova a caricare state.json;
//  2. se state.json non esiste, prova a recuperare l'ultimo record valido dal WAL;
//  3. se non esiste nulla, restituisce found=false.
//
// Questo permette al ConsensusNode di decidere se inizializzare uno stato nuovo.
func (m *Manager) Load() (State, bool, error) {
	state, err := m.readStateFile()
	if err == nil {
		return state, true, nil
	}

	if !os.IsNotExist(err) {
		return State{}, false, err
	}

	walState, walErr := m.readLatestStateFromWAL()
	if walErr == nil {
		return walState, true, nil
	}

	if os.IsNotExist(walErr) {
		return State{}, false, nil
	}

	return State{}, false, walErr
}

// SaveSnapshot salva uno snapshot locale della state machine.
//
// Lo snapshot viene scritto in modo atomico con il pattern:
//
//	write tmp -> rename
//
// Per ora non effettua log compaction: salva solo un checkpoint della data map.
func (m *Manager) SaveSnapshot(snapshot Snapshot) error {
	if snapshot.LastIncludedIndex == 0 {
		return nil
	}

	snapshot.Data = cloneData(snapshot.Data)

	content, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal snapshot: %w", err)
	}

	tmpFile := m.snapshotFile + ".tmp"
	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		return fmt.Errorf("cannot write temporary snapshot file: %w", err)
	}

	if err := os.Rename(tmpFile, m.snapshotFile); err != nil {
		return fmt.Errorf("cannot replace snapshot file: %w", err)
	}

	return nil
}

// LoadSnapshot carica lo snapshot locale, se esiste.
//
// Se il file non esiste restituisce found=false.
// Se esiste, restituisce lo snapshot caricato e found=true.
func (m *Manager) LoadSnapshot() (Snapshot, bool, error) {
	content, err := os.ReadFile(m.snapshotFile)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, false, nil
		}

		return Snapshot{}, false, fmt.Errorf("cannot read snapshot file: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		return Snapshot{}, false, fmt.Errorf("cannot unmarshal snapshot: %w", err)
	}

	snapshot.Data = cloneData(snapshot.Data)

	return snapshot, true, nil
}

func (m *Manager) appendWAL(state State) error {
	record := walRecord{
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		NodeID:      m.nodeID,
		CurrentTerm: state.CurrentTerm,
		VotedFor:    state.VotedFor,
		Log:         state.Log,
		CommitIndex: state.CommitIndex,
		LastApplied: state.LastApplied,
		Data:        cloneData(state.Data),
	}

	file, err := os.OpenFile(m.walFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("cannot open wal file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(record); err != nil {
		return fmt.Errorf("cannot append wal record: %w", err)
	}

	return nil
}

func (m *Manager) readStateFile() (State, error) {
	content, err := os.ReadFile(m.stateFile)
	if err != nil {
		return State{}, err
	}

	var state State
	if err := json.Unmarshal(content, &state); err != nil {
		return State{}, fmt.Errorf("cannot unmarshal persistent state: %w", err)
	}

	state.Data = cloneData(state.Data)

	return state, nil
}

func (m *Manager) readLatestStateFromWAL() (State, error) {
	file, err := os.Open(m.walFile)
	if err != nil {
		return State{}, err
	}
	defer file.Close()

	var latest *State

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record walRecord
		if err := json.Unmarshal(line, &record); err != nil {
			continue
		}

		latest = &State{
			CurrentTerm: record.CurrentTerm,
			VotedFor:    record.VotedFor,
			Log:         record.Log,
			CommitIndex: record.CommitIndex,
			LastApplied: record.LastApplied,
			Data:        cloneData(record.Data),
		}
	}

	if err := scanner.Err(); err != nil {
		return State{}, fmt.Errorf("cannot scan wal file: %w", err)
	}

	if latest == nil {
		return State{}, os.ErrNotExist
	}

	return *latest, nil
}

func cloneData(data map[string]string) map[string]string {
	if data == nil {
		return nil
	}

	copyData := make(map[string]string, len(data))
	for key, value := range data {
		copyData[key] = value
	}

	return copyData
}
