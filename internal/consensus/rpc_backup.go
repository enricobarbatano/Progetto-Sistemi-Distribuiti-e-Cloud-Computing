// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file contiene le RPC che ogni Consensus Node espone al Backup Service.
//
// Il Backup Service non deve accedere direttamente allo stato interno del nodo.
// Per questo motivo il nodo espone operazioni esplicite per:
//   - forzare la creazione di uno snapshot locale;
//   - scaricare lo snapshot locale;
//   - richiedere la compattazione del log fino a un certo indice.
//
// Queste RPC sono operative sul singolo nodo, non orchestrano l'intero cluster.
package consensus

import (
	"context"
	"fmt"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// TriggerSnapshot forza il nodo a creare uno snapshot locale.
//
// Lo snapshot viene creato solo se esiste almeno una entry applicata alla state
// machine. Dopo la creazione dello snapshot, il nodo salva anche lo stato
// persistente, così state.json resta coerente con lastIncludedIndex e
// lastIncludedTerm.
func (n *ConsensusNode) TriggerSnapshot(ctx context.Context, req *backuppb.TriggerSnapshotRequest) (*backuppb.TriggerSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.lastApplied == 0 {
		return &backuppb.TriggerSnapshotResponse{
			Accepted: false,
			NodeId:   n.id,
			Error:    "no applied entry available for snapshot",
		}, nil
	}

	if err := n.saveSnapshotLocked(); err != nil {
		return nil, err
	}

	if err := n.persistLocked(); err != nil {
		return nil, err
	}

	snapshot, found, err := n.persistenceManager.LoadSnapshot()
	if err != nil {
		return nil, err
	}

	if !found {
		return &backuppb.TriggerSnapshotResponse{
			Accepted: false,
			NodeId:   n.id,
			Error:    "snapshot was not created",
		}, nil
	}

	snapshotID := buildSnapshotID(n.id, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)

	return &backuppb.TriggerSnapshotResponse{
		Accepted:          true,
		NodeId:            n.id,
		SnapshotId:        snapshotID,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
	}, nil
}

// DownloadSnapshot restituisce lo snapshot locale più recente del nodo.
//
// Se snapshot_id è vuoto, viene restituito l'ultimo snapshot disponibile.
// In questa fase non manteniamo ancora uno storico di più snapshot per nodo:
// il file snapshot.json rappresenta sempre l'ultimo checkpoint locale.
func (n *ConsensusNode) DownloadSnapshot(ctx context.Context, req *backuppb.DownloadSnapshotRequest) (*backuppb.DownloadSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	snapshot, found, err := n.persistenceManager.LoadSnapshot()
	if err != nil {
		return nil, err
	}

	if !found {
		return &backuppb.DownloadSnapshotResponse{
			Success: false,
			NodeId:  n.id,
			Error:   "snapshot not found",
		}, nil
	}

	snapshotID := buildSnapshotID(n.id, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
	if req.SnapshotId != "" && req.SnapshotId != snapshotID {
		return &backuppb.DownloadSnapshotResponse{
			Success:    false,
			NodeId:     n.id,
			SnapshotId: req.SnapshotId,
			Error:      "requested snapshot_id does not match latest local snapshot",
		}, nil
	}

	data, err := persistence.EncodeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	return &backuppb.DownloadSnapshotResponse{
		Success:           true,
		NodeId:            n.id,
		SnapshotId:        snapshotID,
		SnapshotData:      data,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
	}, nil
}

// CompactLog compatta il log locale fino all'indice richiesto.
//
// Per sicurezza il nodo permette la compattazione remota solo fino all'indice
// già coperto dal proprio snapshot locale. Questo evita di eliminare entry non
// ancora rappresentate in modo sicuro da uno snapshot.
func (n *ConsensusNode) CompactLog(ctx context.Context, req *backuppb.CompactLogRequest) (*backuppb.CompactLogResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.UpToIndex == 0 {
		return &backuppb.CompactLogResponse{
			Success: false,
			NodeId:  n.id,
			Error:   "up_to_index cannot be zero",
		}, nil
	}

	if req.UpToIndex > n.lastIncludedIndex {
		return &backuppb.CompactLogResponse{
			Success: false,
			NodeId:  n.id,
			Error:   "cannot compact beyond local snapshot boundary",
		}, nil
	}

	compactedEntries := n.countCompactableEntriesLocked(req.UpToIndex)
	n.compactLogUpToLocked(req.UpToIndex)

	if err := n.persistLocked(); err != nil {
		return nil, err
	}

	return &backuppb.CompactLogResponse{
		Success:          true,
		NodeId:           n.id,
		CompactedEntries: compactedEntries,
	}, nil
}

func (n *ConsensusNode) countCompactableEntriesLocked(upToIndex uint64) uint64 {
	var count uint64

	for _, entry := range n.log {
		if entry.Index <= upToIndex {
			count++
		}
	}

	return count
}

func buildSnapshotID(nodeID string, lastIncludedIndex uint64, lastIncludedTerm uint64) string {
	return fmt.Sprintf("%s_snapshot_%d_term_%d", nodeID, lastIncludedIndex, lastIncludedTerm)
}
