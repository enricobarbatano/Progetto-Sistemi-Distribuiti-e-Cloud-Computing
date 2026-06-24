// Package backup contiene la logica interna del Backup Service.
//
// Server implementa il servizio gRPC BackupService esposto verso operator o
// client amministrativi. Non parla direttamente con i nodi: delega al Manager.
package backup

import (
	"context"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
)

type Server struct {
	backuppb.UnimplementedBackupServiceServer

	manager *BackupManager
}

func NewServer(manager *BackupManager) *Server {
	return &Server{
		manager: manager,
	}
}

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

func (s *Server) GetBackupStatus(ctx context.Context, req *backuppb.GetBackupStatusRequest) (*backuppb.GetBackupStatusResponse, error) {
	return s.manager.StatusResponse(), nil
}
