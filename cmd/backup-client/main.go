// Package main contiene un piccolo client amministrativo per il Backup Service.
//
// Questo client non esegue operazioni key-value. Serve solo per testare
// manualmente le RPC esposte dal backup-service:
//   - TriggerBackup
//   - GetBackupStatus
//
// In questo modo bench-client resta dedicato alle operazioni Put/Get/Delete,
// mentre backup-client viene usato per la parte amministrativa della Fase 8.
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTarget = "localhost:9090"
	defaultOp     = "status"
)

func main() {
	target := readStringEnv("TARGET", defaultTarget)
	op := strings.ToLower(readStringEnv("OP", defaultOp))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("cannot create backup client connection to %s: %v", target, err)
	}
	defer conn.Close()

	client := backuppb.NewBackupServiceClient(conn)

	log.Printf("backup-client target=%s op=%s", target, op)

	switch op {
	case "backup":
		runTriggerBackup(ctx, client)

	case "status":
		runGetBackupStatus(ctx, client)

	default:
		log.Fatalf("unsupported OP=%s, use OP=backup or OP=status", op)
	}
}

func runTriggerBackup(ctx context.Context, client backuppb.BackupServiceClient) {
	forceSnapshot := readBoolEnv("FORCE_SNAPSHOT", true)
	compactAfterDownload := readBoolEnv("COMPACT_AFTER_DOWNLOAD", true)
	requesterID := readStringEnv("REQUESTER_ID", "backup-client")

	resp, err := client.TriggerBackup(ctx, &backuppb.TriggerBackupRequest{
		RequesterId:          requesterID,
		ForceSnapshot:        forceSnapshot,
		CompactAfterDownload: compactAfterDownload,
	})
	if err != nil {
		log.Fatalf("TriggerBackup failed: %v", err)
	}

	log.Printf(
		"TriggerBackup response: accepted=%v backup_id=%s downloaded_snapshots=%d error=%s",
		resp.Accepted,
		resp.BackupId,
		resp.DownloadedSnapshots,
		resp.Error,
	)
}

func runGetBackupStatus(ctx context.Context, client backuppb.BackupServiceClient) {
	resp, err := client.GetBackupStatus(ctx, &backuppb.GetBackupStatusRequest{})
	if err != nil {
		log.Fatalf("GetBackupStatus failed: %v", err)
	}

	log.Printf(
		"GetBackupStatus response: service_id=%s status=%s created_backups=%d downloaded_snapshots=%d last_backup_id=%s last_snapshot_id=%s last_error=%s",
		resp.ServiceId,
		resp.Status,
		resp.CreatedBackups,
		resp.DownloadedSnapshots,
		resp.LastBackupId,
		resp.LastSnapshotId,
		resp.LastError,
	)
}

func readStringEnv(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

func readBoolEnv(key string, defaultValue bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return defaultValue
	}

	switch value {
	case "true", "1", "yes", "y":
		return true

	case "false", "0", "no", "n":
		return false

	default:
		return defaultValue
	}
}
