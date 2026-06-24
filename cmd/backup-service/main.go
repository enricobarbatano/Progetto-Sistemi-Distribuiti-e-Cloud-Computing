// Package main contiene il punto di ingresso del Backup Service.
//
// Il Backup Service è un componente stateless rispetto al consenso: non
// partecipa a Raft, non decide commit e non mantiene una state machine.
// Il suo compito è orchestrare snapshot, download e compaction remota dei log
// usando le RPC BackupNodeService esposte dai Consensus Node.
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/backup"

	"google.golang.org/grpc"
)

func main() {
	config, err := backup.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("cannot load backup config: %v", err)
	}

	nodeClient := backup.NewNodeClient()
	defer nodeClient.Close()

	breakers := backup.NewCircuitBreakerManager()
	syncer := backup.NewSnapshotSyncer(config.BackupDir)
	compactor := backup.NewLogCompactor(config, nodeClient, breakers)

	manager := backup.NewBackupManager(
		config,
		nodeClient,
		breakers,
		syncer,
		compactor,
	)

	manager.StartPeriodic(context.Background())

	listenAddress := fmt.Sprintf(":%s", config.Port)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", listenAddress, err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()

	backupServer := backup.NewServer(manager)
	backuppb.RegisterBackupServiceServer(grpcServer, backupServer)

	log.Printf(
		"backup service %s listening on port %s with consensus nodes %v",
		config.ServiceID,
		config.Port,
		config.ConsensusNodes,
	)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("backup service stopped with error: %v", err)
	}
}
