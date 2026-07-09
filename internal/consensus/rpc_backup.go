// Package consensus contiene anche le RPC amministrative esposte al Backup Service.
//
// Questo file definisce le operazioni che un singolo Consensus Node rende
// disponibili per snapshot, download e compattazione logica.
//
// Il Backup Service non accede direttamente ai campi interni del nodo: usa RPC
// esplicite per forzare snapshot, scaricare lo stato e richiedere compattazione.
// Queste RPC operano sul singolo nodo; l'orchestrazione dell'intero backup del
// cluster rimane nel package internal/backup.

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
// machine. Dopo il salvataggio dello snapshot, il nodo persiste anche lo stato
// corrente, così state.json resta coerente con lastIncludedIndex e
// lastIncludedTerm.
//
// Questa RPC è usata dal Backup Service prima di scaricare lo stato del nodo.
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
// Se SnapshotId è vuoto, viene restituito l'ultimo snapshot disponibile. In
// questa versione il nodo mantiene un solo snapshot locale corrente:
// snapshot.json rappresenta l'ultimo checkpoint della state machine.
//
// Il payload restituito è lo snapshot serializzato dal package persistence.
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

// CompactLog applica la compattazione logica del log locale fino all'indice richiesto.
//
// Per sicurezza, il nodo accetta la compattazione solo fino a un indice già
// coperto dallo snapshot locale. Questo evita di eliminare entry che non sono
// ancora rappresentate in modo sicuro da un checkpoint.
//
// Nota: la compattazione è logica. Il log in memoria può essere ridotto, ma il
// file WAL fisico resta append-only in questa versione.
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

// countCompactableEntriesLocked conta quante entry del log possono essere
// considerate coperte dallo snapshot fino a upToIndex.
//
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) countCompactableEntriesLocked(upToIndex uint64) uint64 {
	var count uint64

	for _, entry := range n.log {
		if entry.Index <= upToIndex {
			count++
		}
	}

	return count
}

// buildSnapshotID costruisce un identificativo leggibile dello snapshot.
//
// L'identificativo include nodeID, lastIncludedIndex e lastIncludedTerm, così il
// Backup Service può salvare file con nome informativo e verificabile.
func buildSnapshotID(nodeID string, lastIncludedIndex uint64, lastIncludedTerm uint64) string {
	return fmt.Sprintf("%s_snapshot_%d_term_%d", nodeID, lastIncludedIndex, lastIncludedTerm)
}
