// Package backup contiene la logica interna del Backup Service.
//
// BackupManager coordina un ciclo completo di backup:
//   - opzionalmente chiede ai nodi di creare uno snapshot;
//   - scarica gli snapshot disponibili;
//   - salva gli snapshot in BACKUP_DIR;
//   - opzionalmente chiede ai nodi di compattare il log.
package backup

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
)

type BackupManager struct {
	config    Config
	client    *NodeClient
	breakers  *CircuitBreakerManager
	syncer    *SnapshotSyncer
	compactor *LogCompactor

	mu                  sync.Mutex
	status              string
	createdBackups      uint64
	downloadedSnapshots uint64
	lastBackupID        string
	lastSnapshotID      string
	lastError           string
}

func NewBackupManager(
	config Config,
	client *NodeClient,
	breakers *CircuitBreakerManager,
	syncer *SnapshotSyncer,
	compactor *LogCompactor,
) *BackupManager {
	return &BackupManager{
		config:    config,
		client:    client,
		breakers:  breakers,
		syncer:    syncer,
		compactor: compactor,
		status:    "idle",
	}
}

func (m *BackupManager) StartPeriodic(ctx context.Context) {
	if m.config.BackupInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(m.config.BackupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				_, _, err := m.RunBackup(ctx, true, m.config.CompactAfterDownload)
				if err != nil {
					log.Printf("periodic backup failed: %v", err)
				}
			}
		}
	}()
}

func (m *BackupManager) RunBackup(ctx context.Context, forceSnapshot bool, compactAfterDownload bool) (string, uint64, error) {
	backupID := fmt.Sprintf("backup_%d", time.Now().UTC().UnixNano())

	m.setRunning(backupID)

	var downloaded uint64
	var lastErr error

	for _, address := range m.config.ConsensusNodes {
		snapshotID := ""

		if forceSnapshot {
			resp, err := m.triggerSnapshot(ctx, address)
			if err != nil {
				lastErr = err
				m.setLastError(err)
				continue
			}

			if !resp.Accepted {
				lastErr = fmt.Errorf("node %s rejected TriggerSnapshot: %s", address, resp.Error)
				m.setLastError(lastErr)
				continue
			}

			snapshotID = resp.SnapshotId
		}

		downloadResp, err := m.downloadSnapshot(ctx, address, snapshotID)
		if err != nil {
			lastErr = err
			m.setLastError(err)
			continue
		}

		if !downloadResp.Success {
			lastErr = fmt.Errorf("node %s DownloadSnapshot failed: %s", address, downloadResp.Error)
			m.setLastError(lastErr)
			continue
		}

		fileInfo, err := m.syncer.SaveSnapshot(downloadResp)
		if err != nil {
			lastErr = err
			m.setLastError(err)
			continue
		}

		log.Printf("backup saved snapshot from node %s to %s", fileInfo.NodeID, fileInfo.Path)

		downloaded++
		m.setLastSnapshotID(fileInfo.SnapshotID)

		if compactAfterDownload {
			compactResp, err := m.compactor.Compact(ctx, address, fileInfo.SnapshotID, fileInfo.LastIncludedIndex)
			if err != nil {
				lastErr = err
				m.setLastError(err)
				continue
			}

			if !compactResp.Success {
				lastErr = fmt.Errorf("node %s CompactLog failed: %s", address, compactResp.Error)
				m.setLastError(lastErr)
				continue
			}
		}
	}

	m.mu.Lock()
	m.downloadedSnapshots += downloaded
	if downloaded > 0 {
		m.createdBackups++
		m.status = "idle"
		m.lastError = ""
	} else {
		m.status = "degraded"
	}
	m.mu.Unlock()

	if downloaded == 0 && lastErr != nil {
		return backupID, downloaded, lastErr
	}

	return backupID, downloaded, nil
}

func (m *BackupManager) StatusResponse() *backuppb.GetBackupStatusResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	return &backuppb.GetBackupStatusResponse{
		ServiceId:           m.config.ServiceID,
		Status:              m.status,
		CreatedBackups:      m.createdBackups,
		DownloadedSnapshots: m.downloadedSnapshots,
		LastBackupId:        m.lastBackupID,
		LastSnapshotId:      m.lastSnapshotID,
		LastError:           m.lastError,
	}
}

func (m *BackupManager) triggerSnapshot(ctx context.Context, address string) (*backuppb.TriggerSnapshotResponse, error) {
	result, err := m.executeWithRetry(ctx, address, func(callCtx context.Context) (any, error) {
		return m.client.TriggerSnapshot(callCtx, address, m.config.ServiceID)
	})
	if err != nil {
		return nil, err
	}

	resp, ok := result.(*backuppb.TriggerSnapshotResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected TriggerSnapshot response type from %s", address)
	}

	return resp, nil
}

func (m *BackupManager) downloadSnapshot(ctx context.Context, address string, snapshotID string) (*backuppb.DownloadSnapshotResponse, error) {
	result, err := m.executeWithRetry(ctx, address, func(callCtx context.Context) (any, error) {
		return m.client.DownloadSnapshot(callCtx, address, m.config.ServiceID, snapshotID)
	})
	if err != nil {
		return nil, err
	}

	resp, ok := result.(*backuppb.DownloadSnapshotResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected DownloadSnapshot response type from %s", address)
	}

	return resp, nil
}

func (m *BackupManager) executeWithRetry(ctx context.Context, address string, call func(callCtx context.Context) (any, error)) (any, error) {
	var lastErr error

	for attempt := 0; attempt <= m.config.MaxRetries; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, m.config.RPCTimeout)

		result, err := m.breakers.Execute(address, func() (any, error) {
			return call(callCtx)
		})

		cancel()

		if err == nil {
			return result, nil
		}

		lastErr = err
		m.sleepBeforeRetry(attempt)
	}

	return nil, lastErr
}

func (m *BackupManager) sleepBeforeRetry(attempt int) {
	if m.config.Backoff <= 0 || attempt >= m.config.MaxRetries {
		return
	}

	time.Sleep(time.Duration(attempt+1) * m.config.Backoff)
}

func (m *BackupManager) setRunning(backupID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.status = "running"
	m.lastBackupID = backupID
}

func (m *BackupManager) setLastSnapshotID(snapshotID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastSnapshotID = snapshotID
}

func (m *BackupManager) setLastError(err error) {
	if err == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastError = err.Error()
}
