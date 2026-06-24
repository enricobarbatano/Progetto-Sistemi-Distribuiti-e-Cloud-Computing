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
// Il WAL è event-based: ogni riga rappresenta un evento persistente, per
// esempio un cambio termine, un voto, una entry aggiunta al log o un avanzamento
// del commit index. Lo state.json resta un checkpoint compatto per accelerare
// il recovery.
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

const (
	walEventTermUpdated    = "term_updated"
	walEventVoteUpdated    = "vote_updated"
	walEventLogAppended    = "log_appended"
	walEventLogTruncated   = "log_truncated"
	walEventCommitAdvanced = "commit_advanced"
	walEventStateApplied   = "state_applied"
	walEventSnapshotSaved  = "snapshot_saved"
)

// State rappresenta lo stato persistente completo del nodo.
//
// Contiene lo stato Raft persistente classico, cioè currentTerm, votedFor e log,
// più alcuni campi utili al recovery robusto del progetto.
//
// LastIncludedIndex e LastIncludedTerm rappresentano il bordo dello snapshot
// locale. Quando il log viene compattato, le entry fino a LastIncludedIndex non
// sono più presenti fisicamente nel log, ma restano rappresentate dallo snapshot.
type State struct {
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`

	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	Data        map[string]string `json:"data"`

	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// Snapshot rappresenta un checkpoint locale della state machine.
//
// Lo snapshot contiene la mappa key-value applicata fino a LastIncludedIndex.
// LastIncludedTerm serve a mantenere la coerenza del log Raft anche dopo la
// compaction locale.
type Snapshot struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}

// walEvent rappresenta un singolo evento append-only del WAL.
//
// Il campo Type indica quale evento è stato registrato. Gli altri campi vengono
// valorizzati solo quando servono per quello specifico evento.
type walEvent struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	NodeID    string `json:"node_id"`

	Term     uint64 `json:"term,omitempty"`
	VotedFor string `json:"voted_for,omitempty"`

	Entry        *consensuspb.LogEntry `json:"entry,omitempty"`
	TruncateFrom uint64                `json:"truncate_from,omitempty"`

	CommitIndex uint64 `json:"commit_index,omitempty"`
	LastApplied uint64 `json:"last_applied,omitempty"`

	LastIncludedIndex uint64            `json:"last_included_index,omitempty"`
	LastIncludedTerm  uint64            `json:"last_included_term,omitempty"`
	Data              map[string]string `json:"data,omitempty"`
}

// legacyWalRecord rappresenta il vecchio formato snapshot-based del WAL.
//
// Viene mantenuto solo per compatibilità con file wal.log già esistenti.
// Le nuove scritture usano walEvent.
type legacyWalRecord struct {
	Timestamp   string                  `json:"timestamp"`
	NodeID      string                  `json:"node_id"`
	CurrentTerm uint64                  `json:"current_term"`
	VotedFor    string                  `json:"voted_for"`
	Log         []*consensuspb.LogEntry `json:"log"`

	CommitIndex uint64            `json:"commit_index"`
	LastApplied uint64            `json:"last_applied"`
	Data        map[string]string `json:"data"`

	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// Manager gestisce tutti i file persistenti di un singolo nodo.
//
// Questa struct è il componente che sostituisce la logica di persistenza che
// prima era dentro ConsensusNode. Il nodo usa Manager per salvare o caricare
// lo stato, ma non si occupa direttamente di JSON, WAL o snapshot file.
type Manager struct {
	nodeID       string
	stateFile    string
	walFile      string
	snapshotFile string

	lastState *State
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
// La scrittura segue questo ordine:
//
//  1. calcola gli eventi WAL rispetto all'ultimo stato noto;
//  2. appende gli eventi al wal.log;
//  3. aggiorna atomicamente state.json come checkpoint compatto.
//
// Il WAL non contiene più uno snapshot completo a ogni salvataggio, ma eventi
// granulari. state.json rimane disponibile per recovery veloce.
func (m *Manager) Save(state State) error {
	state = cloneState(state)

	events := m.eventsFromState(state)
	if len(events) > 0 {
		if err := m.appendWALEvents(events); err != nil {
			return err
		}
	}

	if err := m.writeStateFile(state); err != nil {
		return err
	}

	m.lastState = cloneStatePtr(state)

	return nil
}

// Load carica lo stato persistente del nodo.
//
// L'ordine di recovery è:
//
//  1. prova a caricare state.json;
//  2. se state.json non esiste, esegue il replay del WAL event-based;
//  3. se non esiste nulla, restituisce found=false.
func (m *Manager) Load() (State, bool, error) {
	state, err := m.readStateFile()
	if err == nil {
		m.lastState = cloneStatePtr(state)
		return state, true, nil
	}

	if !os.IsNotExist(err) {
		return State{}, false, err
	}

	walState, walErr := m.replayWAL()
	if walErr == nil {
		m.lastState = cloneStatePtr(walState)
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
// Dopo il salvataggio viene registrato anche un evento snapshot_saved nel WAL.
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

	event := m.newWALEvent(walEventSnapshotSaved)
	event.LastIncludedIndex = snapshot.LastIncludedIndex
	event.LastIncludedTerm = snapshot.LastIncludedTerm
	event.Data = cloneData(snapshot.Data)

	return m.appendWALEvents([]walEvent{event})
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

// EncodeSnapshot serializza uno snapshot in JSON.
//
// Viene usata dal leader per inserire lo snapshot nel campo bytes della RPC
// InstallSnapshot. La funzione vive nel package persistence perché conosce la
// struttura persistente dello snapshot, ma non conosce la logica Raft.
func EncodeSnapshot(snapshot Snapshot) ([]byte, error) {
	snapshot.Data = cloneData(snapshot.Data)

	content, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("cannot encode snapshot: %w", err)
	}

	return content, nil
}

// DecodeSnapshot deserializza uno snapshot ricevuto tramite InstallSnapshot.
//
// Viene usata dal follower quando riceve uno snapshot remoto dal leader.
func DecodeSnapshot(data []byte) (Snapshot, error) {
	if len(data) == 0 {
		return Snapshot{}, fmt.Errorf("snapshot data cannot be empty")
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("cannot decode snapshot: %w", err)
	}

	snapshot.Data = cloneData(snapshot.Data)

	return snapshot, nil
}

func (m *Manager) eventsFromState(state State) []walEvent {
	if m.lastState == nil {
		return m.initialEvents(state)
	}

	previous := *m.lastState
	events := make([]walEvent, 0)

	if state.CurrentTerm != previous.CurrentTerm {
		event := m.newWALEvent(walEventTermUpdated)
		event.Term = state.CurrentTerm
		events = append(events, event)
	}

	if state.VotedFor != previous.VotedFor {
		event := m.newWALEvent(walEventVoteUpdated)
		event.VotedFor = state.VotedFor
		events = append(events, event)
	}

	events = append(events, m.logDeltaEvents(previous.Log, state.Log)...)

	if state.CommitIndex != previous.CommitIndex {
		event := m.newWALEvent(walEventCommitAdvanced)
		event.CommitIndex = state.CommitIndex
		events = append(events, event)
	}

	if state.LastApplied != previous.LastApplied {
		event := m.newWALEvent(walEventStateApplied)
		event.LastApplied = state.LastApplied
		events = append(events, event)
	}

	return events
}

func (m *Manager) initialEvents(state State) []walEvent {
	events := make([]walEvent, 0)

	if state.CurrentTerm != 0 {
		event := m.newWALEvent(walEventTermUpdated)
		event.Term = state.CurrentTerm
		events = append(events, event)
	}

	if state.VotedFor != "" {
		event := m.newWALEvent(walEventVoteUpdated)
		event.VotedFor = state.VotedFor
		events = append(events, event)
	}

	for _, entry := range state.Log {
		event := m.newWALEvent(walEventLogAppended)
		event.Entry = cloneLogEntry(entry)
		events = append(events, event)
	}

	if state.LastIncludedIndex != 0 {
		event := m.newWALEvent(walEventSnapshotSaved)
		event.LastIncludedIndex = state.LastIncludedIndex
		event.LastIncludedTerm = state.LastIncludedTerm
		event.Data = cloneData(state.Data)
		events = append(events, event)
	}

	if state.CommitIndex != 0 {
		event := m.newWALEvent(walEventCommitAdvanced)
		event.CommitIndex = state.CommitIndex
		events = append(events, event)
	}

	if state.LastApplied != 0 {
		event := m.newWALEvent(walEventStateApplied)
		event.LastApplied = state.LastApplied
		events = append(events, event)
	}

	return events
}

func (m *Manager) logDeltaEvents(previousLog []*consensuspb.LogEntry, currentLog []*consensuspb.LogEntry) []walEvent {
	commonPrefixLength := commonLogPrefixLength(previousLog, currentLog)
	events := make([]walEvent, 0)

	if commonPrefixLength < len(previousLog) {
		event := m.newWALEvent(walEventLogTruncated)
		event.TruncateFrom = truncateFromIndex(currentLog, commonPrefixLength)
		events = append(events, event)
	}

	for _, entry := range currentLog[commonPrefixLength:] {
		event := m.newWALEvent(walEventLogAppended)
		event.Entry = cloneLogEntry(entry)
		events = append(events, event)
	}

	return events
}

func commonLogPrefixLength(left []*consensuspb.LogEntry, right []*consensuspb.LogEntry) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}

	for index := 0; index < limit; index++ {
		if !sameLogEntry(left[index], right[index]) {
			return index
		}
	}

	return limit
}

func truncateFromIndex(log []*consensuspb.LogEntry, commonPrefixLength int) uint64 {
	if commonPrefixLength < len(log) {
		return log[commonPrefixLength].Index
	}

	if len(log) == 0 {
		return 1
	}

	return log[len(log)-1].Index + 1
}

func sameLogEntry(left *consensuspb.LogEntry, right *consensuspb.LogEntry) bool {
	if left == nil || right == nil {
		return left == right
	}

	return left.Index == right.Index &&
		left.Term == right.Term &&
		left.Operation == right.Operation &&
		left.Key == right.Key &&
		left.Value == right.Value
}

func (m *Manager) newWALEvent(eventType string) walEvent {
	return walEvent{
		Type:      eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		NodeID:    m.nodeID,
	}
}

func (m *Manager) appendWALEvents(events []walEvent) error {
	if len(events) == 0 {
		return nil
	}

	file, err := os.OpenFile(m.walFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("cannot open wal file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return fmt.Errorf("cannot append wal event: %w", err)
		}
	}

	return nil
}

func (m *Manager) writeStateFile(state State) error {
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

func (m *Manager) readStateFile() (State, error) {
	content, err := os.ReadFile(m.stateFile)
	if err != nil {
		return State{}, err
	}

	var state State
	if err := json.Unmarshal(content, &state); err != nil {
		return State{}, fmt.Errorf("cannot unmarshal persistent state: %w", err)
	}

	return cloneState(state), nil
}

func (m *Manager) replayWAL() (State, error) {
	file, err := os.Open(m.walFile)
	if err != nil {
		return State{}, err
	}
	defer file.Close()

	state := State{
		Log: make([]*consensuspb.LogEntry, 0),
	}

	foundRecord := false
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		applied, err := applyWALLine(&state, line)
		if err != nil {
			continue
		}

		if applied {
			foundRecord = true
		}
	}

	if err := scanner.Err(); err != nil {
		return State{}, fmt.Errorf("cannot scan wal file: %w", err)
	}

	if !foundRecord {
		return State{}, os.ErrNotExist
	}

	return cloneState(state), nil
}

func applyWALLine(state *State, line []byte) (bool, error) {
	var event walEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return false, err
	}

	if event.Type == "" {
		return applyLegacyWALLine(state, line)
	}

	applyWALEvent(state, event)
	return true, nil
}

func applyLegacyWALLine(state *State, line []byte) (bool, error) {
	var legacy legacyWalRecord
	if err := json.Unmarshal(line, &legacy); err != nil {
		return false, err
	}

	*state = State{
		CurrentTerm:       legacy.CurrentTerm,
		VotedFor:          legacy.VotedFor,
		Log:               cloneLog(legacy.Log),
		CommitIndex:       legacy.CommitIndex,
		LastApplied:       legacy.LastApplied,
		Data:              cloneData(legacy.Data),
		LastIncludedIndex: legacy.LastIncludedIndex,
		LastIncludedTerm:  legacy.LastIncludedTerm,
	}

	return true, nil
}

func applyWALEvent(state *State, event walEvent) {
	switch event.Type {
	case walEventTermUpdated:
		state.CurrentTerm = event.Term

	case walEventVoteUpdated:
		state.VotedFor = event.VotedFor

	case walEventLogAppended:
		appendOrReplaceLogEntry(state, event.Entry)

	case walEventLogTruncated:
		truncateLogFromIndex(state, event.TruncateFrom)

	case walEventCommitAdvanced:
		state.CommitIndex = event.CommitIndex

	case walEventStateApplied:
		state.LastApplied = event.LastApplied

	case walEventSnapshotSaved:
		state.LastIncludedIndex = event.LastIncludedIndex
		state.LastIncludedTerm = event.LastIncludedTerm

		if event.Data != nil {
			state.Data = cloneData(event.Data)
		}

		truncateLogThroughIndex(state, event.LastIncludedIndex)

		if event.LastIncludedIndex > state.CommitIndex {
			state.CommitIndex = event.LastIncludedIndex
		}

		if event.LastIncludedIndex > state.LastApplied {
			state.LastApplied = event.LastIncludedIndex
		}
	}
}

func appendOrReplaceLogEntry(state *State, entry *consensuspb.LogEntry) {
	if entry == nil {
		return
	}

	for index, localEntry := range state.Log {
		if localEntry.Index == entry.Index {
			state.Log[index] = cloneLogEntry(entry)
			return
		}
	}

	state.Log = append(state.Log, cloneLogEntry(entry))
}

func truncateLogFromIndex(state *State, truncateFrom uint64) {
	if truncateFrom == 0 {
		return
	}

	truncated := make([]*consensuspb.LogEntry, 0, len(state.Log))
	for _, entry := range state.Log {
		if entry.Index < truncateFrom {
			truncated = append(truncated, cloneLogEntry(entry))
		}
	}

	state.Log = truncated

	if state.CommitIndex >= truncateFrom {
		state.CommitIndex = truncateFrom - 1
	}

	if state.LastApplied > state.CommitIndex {
		state.LastApplied = state.CommitIndex
	}
}

func truncateLogThroughIndex(state *State, lastIncludedIndex uint64) {
	if lastIncludedIndex == 0 {
		return
	}

	remaining := make([]*consensuspb.LogEntry, 0, len(state.Log))
	for _, entry := range state.Log {
		if entry.Index > lastIncludedIndex {
			remaining = append(remaining, cloneLogEntry(entry))
		}
	}

	state.Log = remaining
}

func cloneState(state State) State {
	return State{
		CurrentTerm:       state.CurrentTerm,
		VotedFor:          state.VotedFor,
		Log:               cloneLog(state.Log),
		CommitIndex:       state.CommitIndex,
		LastApplied:       state.LastApplied,
		Data:              cloneData(state.Data),
		LastIncludedIndex: state.LastIncludedIndex,
		LastIncludedTerm:  state.LastIncludedTerm,
	}
}

func cloneStatePtr(state State) *State {
	cloned := cloneState(state)
	return &cloned
}

func cloneLog(logEntries []*consensuspb.LogEntry) []*consensuspb.LogEntry {
	if logEntries == nil {
		return nil
	}

	cloned := make([]*consensuspb.LogEntry, 0, len(logEntries))
	for _, entry := range logEntries {
		cloned = append(cloned, cloneLogEntry(entry))
	}

	return cloned
}

func cloneLogEntry(entry *consensuspb.LogEntry) *consensuspb.LogEntry {
	if entry == nil {
		return nil
	}

	return &consensuspb.LogEntry{
		Index:     entry.Index,
		Term:      entry.Term,
		Operation: entry.Operation,
		Key:       entry.Key,
		Value:     entry.Value,
	}
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
