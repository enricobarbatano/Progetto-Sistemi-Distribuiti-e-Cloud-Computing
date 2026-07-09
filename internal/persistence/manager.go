// Package persistence gestisce la durabilità su disco dei Consensus Node.
//
// Il package non implementa direttamente il protocollo di consenso: non decide
// quando una entry è committed, non replica log e non gestisce elezioni.
// La sua responsabilità è salvare e ricostruire lo stato persistente del nodo.
//
// La persistenza è basata su tre file principali:
//
//   - state.json: checkpoint compatto dello stato persistente;
//   - wal.log: Write-Ahead Log event-based e append-only;
//   - snapshot.json: checkpoint locale della state machine.
//
// Il WAL registra eventi persistenti, come aggiornamenti di termine, voto,
// append del log, avanzamento del commit index e salvataggio snapshot.
// In questa versione la compattazione è logica: lo stato viene consolidato
// tramite snapshot, ma il file WAL fisico resta append-only.
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

// State rappresenta lo stato persistente completo di un Consensus Node.
//
// Contiene lo stato persistente richiesto dal protocollo di consenso, come
// currentTerm, votedFor e log, insieme a commit index, last applied e dati della
// state machine. Questo oggetto viene scritto in state.json come checkpoint
// compatto per velocizzare il recovery dopo un riavvio.
//
// LastIncludedIndex e LastIncludedTerm rappresentano il bordo dello snapshot
// locale: le entry fino a LastIncludedIndex possono essere considerate incluse
// nello snapshot logico.
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
// LastIncludedTerm permette di mantenere la coerenza con il log replicato anche
// dopo la compattazione logica. Il Backup Service scarica questi snapshot per
// conservare copie persistenti dello stato dei nodi.
type Snapshot struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}

// walEvent rappresenta un singolo evento append-only del WAL.
//
// Ogni riga del wal.log contiene un evento JSON indipendente. Questo approccio
// event-based evita di riscrivere l'intero stato a ogni modifica e consente,
// in assenza di state.json, di ricostruire lo stato tramite replay sequenziale.
//
// Il campo Type identifica l'evento; gli altri campi sono valorizzati solo se
// necessari per quel tipo di evento.
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
// Viene mantenuto solo per compatibilità con file wal.log generati da versioni
// precedenti del progetto. Le nuove scritture usano walEvent, che registra
// eventi granulari invece di serializzare ogni volta lo stato completo.
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

// Manager coordina le operazioni di persistenza di un singolo Consensus Node.
//
// Il ConsensusNode usa Manager per salvare e caricare stato persistente, WAL e
// snapshot, senza conoscere i dettagli di serializzazione JSON o gestione file.
// In Docker, i file gestiti da Manager risiedono su volumi persistenti, così lo
// stato sopravvive al ciclo di vita dei container.
type Manager struct {
	nodeID       string
	stateFile    string
	walFile      string
	snapshotFile string

	lastState *State
}

// NewManager crea un manager di persistenza per un nodo.
//
// dataDir indica la directory dati associata al nodo. Per ogni nodeID vengono
// creati percorsi separati per state file, WAL e snapshot, ad esempio:
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
// La scrittura segue due passaggi:
//  1. calcola e appende al WAL gli eventi cambiati rispetto all'ultimo stato noto;
//  2. aggiorna state.json come checkpoint compatto.
//
// Il WAL viene scritto prima del checkpoint compatto, seguendo il principio del
// Write-Ahead Logging: le modifiche vengono registrate in modo append-only prima
// di essere considerate stabilmente recuperabili. state.json serve invece a
// velocizzare il recovery senza dover sempre rileggere l'intero WAL.
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
// Il recovery segue un ordine semplice:
//  1. prova a caricare state.json;
//  2. se state.json non esiste, ricostruisce lo stato tramite replay del WAL;
//  3. se non esiste nessun file persistente, restituisce found=false.
//
// Questo permette al nodo di ripartire rapidamente quando il checkpoint esiste,
// ma mantiene comunque il WAL come fallback di recovery.
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
// Lo snapshot viene scritto con il pattern write-to-temp-and-rename: prima viene
// creato un file temporaneo, poi os.Rename sostituisce il file definitivo.
// Questo riduce il rischio di lasciare snapshot parzialmente scritti in caso di
// crash durante la scrittura.
//
// Dopo il salvataggio viene registrato anche un evento snapshot_saved nel WAL.
// In questa versione lo snapshot abilita la compattazione logica dello stato,
// ma non comporta la truncation fisica immediata del file wal.log.
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

// LoadSnapshot carica lo snapshot locale, se presente.
//
// Restituisce found=false se il file non esiste. Quando lo snapshot viene
// caricato correttamente, la mappa Data viene copiata per evitare che il chiamante
// possa modificare direttamente lo stato interno della struttura caricata.
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
// Viene usata quando uno snapshot deve essere trasportato tramite RPC, ad esempio
// durante InstallSnapshot o durante il download da parte del Backup Service.
// La funzione vive nel package persistence perché conosce il formato persistente,
// ma non contiene logica di consenso.
func EncodeSnapshot(snapshot Snapshot) ([]byte, error) {
	snapshot.Data = cloneData(snapshot.Data)

	content, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("cannot encode snapshot: %w", err)
	}

	return content, nil
}

// DecodeSnapshot deserializza uno snapshot ricevuto come sequenza di byte.
//
// Viene usata dai nodi che ricevono uno snapshot remoto. Restituisce errore se
// il payload è vuoto o non rappresenta uno snapshot JSON valido.
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

// eventsFromState confronta lo stato corrente con l'ultimo stato salvato.
//
// Invece di riscrivere sempre tutto lo stato nel WAL, genera solo gli eventi
// necessari a rappresentare le modifiche avvenute: cambio termine, voto,
// append/truncation del log, commit index e last applied.
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

// initialEvents genera gli eventi WAL necessari quando non esiste uno stato
// precedente noto.
//
// Questo caso si verifica al primo salvataggio dopo l'avvio del Manager.
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

// logDeltaEvents calcola le modifiche tra il log precedente e quello corrente.
//
// Se il prefisso comune si interrompe, viene registrato un evento di truncation.
// Le entry successive vengono poi registrate come append. Questo modello consente
// di rappresentare sia la crescita del log sia eventuali correzioni dovute alla
// replica.
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

// appendWALEvents aggiunge eventi al WAL in modalità append-only.
//
// Ogni evento viene codificato come una riga JSON indipendente. Il file fisico
// non viene troncato in questa versione: la compattazione del WAL resta logica,
// mentre la truncation fisica è una possibile evoluzione futura.
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

// writeStateFile scrive state.json in modo atomico.
//
// Il metodo usa un file temporaneo e poi os.Rename per sostituire il file
// definitivo. Questo pattern evita, per quanto possibile, di lasciare un file
// state.json corrotto o incompleto in caso di crash durante la scrittura.
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

// replayWAL ricostruisce lo stato persistente rileggendo il WAL.
//
// Viene usato come fallback quando state.json non è disponibile. Il replay
// applica gli eventi nell'ordine in cui sono stati scritti, ricostruendo termine,
// voto, log, commit index, last applied e stato snapshot-based.
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

// applyWALEvent applica un singolo evento WAL allo stato ricostruito.
//
// Questa funzione è usata durante il replay del WAL e deve rimanere deterministica:
// a parità di sequenza di eventi, il risultato del recovery deve essere sempre
// lo stesso.
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
