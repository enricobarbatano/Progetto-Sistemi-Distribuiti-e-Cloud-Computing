// Package backup contiene la logica interna del Backup Service.
//
// NodeClient gestisce le connessioni gRPC verso i Consensus Node e invoca le
// RPC del BackupNodeService esposte dai nodi.
package backup

import (
	"context"
	"fmt"
	"sync"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeClient incapsula la connessione gRPC verso i nodi del cluster e espone
// metodi semplici per invocare le RPC di snapshot e compattazione.
type NodeClient struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]backuppb.BackupNodeServiceClient
}

// NewNodeClient crea un client di backup vuoto con una cache di connessioni
// pronta ad essere riempita al primo contatto con un nodo.
func NewNodeClient() *NodeClient {
	return &NodeClient{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]backuppb.BackupNodeServiceClient),
	}
}

// TriggerSnapshot richiede al nodo remoto di creare uno snapshot locale del
// proprio stato, così il Backup Service può salvarlo in modo affidabile.
func (c *NodeClient) TriggerSnapshot(ctx context.Context, address string, requesterID string) (*backuppb.TriggerSnapshotResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.TriggerSnapshot(ctx, &backuppb.TriggerSnapshotRequest{
		RequesterId: requesterID,
	})
}

// DownloadSnapshot scarica lo snapshot remoto dal nodo specificato, restituendo
// il payload binario da salvare sul disco di backup.
func (c *NodeClient) DownloadSnapshot(ctx context.Context, address string, requesterID string, snapshotID string) (*backuppb.DownloadSnapshotResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.DownloadSnapshot(ctx, &backuppb.DownloadSnapshotRequest{
		RequesterId: requesterID,
		SnapshotId:  snapshotID,
	})
}

// CompactLog richiede al nodo remoto di compattare il proprio log fino a un
// indice di sicurezza già coperto dallo snapshot locale.
func (c *NodeClient) CompactLog(ctx context.Context, address string, requesterID string, snapshotID string, upToIndex uint64) (*backuppb.CompactLogResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.CompactLog(ctx, &backuppb.CompactLogRequest{
		RequesterId: requesterID,
		SnapshotId:  snapshotID,
		UpToIndex:   upToIndex,
	})
}

// Close chiude tutte le connessioni gRPC aperte verso i nodi del cluster.
func (c *NodeClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for address, conn := range c.conns {
		if err := conn.Close(); err != nil {
			fmt.Printf("cannot close backup connection to node %s: %v\n", address, err)
		}
	}

	c.conns = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]backuppb.BackupNodeServiceClient)
}

func (c *NodeClient) clientForAddress(address string) (backuppb.BackupNodeServiceClient, error) {
	if address == "" {
		return nil, fmt.Errorf("backup node address cannot be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[address]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create backup node client for %s: %w", address, err)
	}

	client := backuppb.NewBackupNodeServiceClient(conn)

	c.conns[address] = conn
	c.clients[address] = client

	return client, nil
}
