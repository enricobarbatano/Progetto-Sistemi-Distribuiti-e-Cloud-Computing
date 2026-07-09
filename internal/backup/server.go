// Package backup contiene la logica interna del Backup Service.
//
// Server implementa il servizio gRPC BackupService esposto verso operator o
// client amministrativi. Non parla direttamente con i nodi: delega al Manager.
package backup

import (
	"context"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
)

// Server espone le RPC amministrative del Backup Service e delega l'esecuzione
// del lavoro reale al BackupManager.
type Server struct {
	backuppb.UnimplementedBackupServiceServer

	manager *BackupManager
}

// NewServer crea il server gRPC che espone TriggerBackup e GetBackupStatus.
func NewServer(manager *BackupManager) *Server {
	return &Server{
		manager: manager,
	}
}

// TriggerBackup avvia un nuovo ciclo di backup remoto e restituisce un risultato
// amministrativo con l'ID del backup e il numero di snapshot scaricati.
func (s *Server) TriggerBackup(ctx context.Context, req *backuppb.TriggerBackupRequest) (*backuppb.TriggerBackupResponse, error) {
	backupID, downloaded, err := s.manager.RunBackup(ctx, req.ForceSnapshot, req.CompactAfterDownload)
	if err != nil {
		return &backuppb.TriggerBackupResponse{
			Accepted:            false,
			BackupId:            backupID,
			DownloadedSnapshots: downloaded,
			Error:               err.Error(),
		}, nil
	}

	return &backuppb.TriggerBackupResponse{
		Accepted:            true,
		BackupId:            backupID,
		DownloadedSnapshots: downloaded,
	}, nil
}

// GetBackupStatus restituisce lo stato aggregato corrente del servizio di backup.
func (s *Server) GetBackupStatus(ctx context.Context, req *backuppb.GetBackupStatusRequest) (*backuppb.GetBackupStatusResponse, error) {
	return s.manager.StatusResponse(), nil
}
