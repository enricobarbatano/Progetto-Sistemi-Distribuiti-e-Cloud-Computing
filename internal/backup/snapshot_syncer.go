// Package backup contiene la logica interna del Backup Service.
//
// SnapshotSyncer si occupa solo di salvare su disco gli snapshot scaricati dai
// Consensus Node. Non decide quando scaricarli e non invia comandi di
// compattazione.
package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
)

type SnapshotSyncer struct {
	backupDir string
}

type SnapshotFile struct {
	NodeID            string
	SnapshotID        string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Path              string
}

func NewSnapshotSyncer(backupDir string) *SnapshotSyncer {
	return &SnapshotSyncer{
		backupDir: backupDir,
	}
}

func (s *SnapshotSyncer) SaveSnapshot(resp *backuppb.DownloadSnapshotResponse) (SnapshotFile, error) {
	if resp == nil {
		return SnapshotFile{}, fmt.Errorf("download snapshot response cannot be nil")
	}

	if !resp.Success {
		return SnapshotFile{}, fmt.Errorf("cannot save unsuccessful snapshot response: %s", resp.Error)
	}

	if len(resp.SnapshotData) == 0 {
		return SnapshotFile{}, fmt.Errorf("snapshot data cannot be empty")
	}

	if err := os.MkdirAll(s.backupDir, 0755); err != nil {
		return SnapshotFile{}, fmt.Errorf("cannot create backup dir: %w", err)
	}

	nodeID := sanitizeFilePart(resp.NodeId)
	snapshotID := sanitizeFilePart(resp.SnapshotId)

	fileName := fmt.Sprintf(
		"%s_%s_index_%d_term_%d.json",
		nodeID,
		snapshotID,
		resp.LastIncludedIndex,
		resp.LastIncludedTerm,
	)

	finalPath := filepath.Join(s.backupDir, fileName)
	tmpPath := finalPath + ".tmp"

	if err := os.WriteFile(tmpPath, resp.SnapshotData, 0644); err != nil {
		return SnapshotFile{}, fmt.Errorf("cannot write temporary snapshot backup: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return SnapshotFile{}, fmt.Errorf("cannot finalize snapshot backup: %w", err)
	}

	return SnapshotFile{
		NodeID:            resp.NodeId,
		SnapshotID:        resp.SnapshotId,
		LastIncludedIndex: resp.LastIncludedIndex,
		LastIncludedTerm:  resp.LastIncludedTerm,
		Path:              finalPath,
	}, nil
}

func sanitizeFilePart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}

	replacer := strings.NewReplacer(
		":", "_",
		"/", "_",
		"\\", "_",
		" ", "_",
	)

	return replacer.Replace(value)
}
