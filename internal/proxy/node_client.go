// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene NodeClient, il componente responsabile delle chiamate
// gRPC verso i Consensus Node.
//
// NodeClient non decide quale nodo sia il leader e non implementa retry,
// backoff o circuit breaker. La sua responsabilità è più semplice:
// mantenere connessioni gRPC riutilizzabili verso i nodi e offrire metodi
// per invocare Put, Get, Delete e GetLeader su uno specifico indirizzo.
package proxy

import (
	"context"
	"fmt"
	"sync"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeClient gestisce i client gRPC verso i Consensus Node.
//
// Il Proxy deve poter parlare con più nodi del cluster. Per evitare di aprire
// una nuova connessione a ogni richiesta, NodeClient mantiene una cache di
// connessioni e client gRPC indicizzati per indirizzo.
//
// La struct è thread-safe perché il Proxy riceverà richieste concorrenti.
type NodeClient struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]kvpb.KeyValueServiceClient
}

// NewNodeClient crea un nuovo client manager per i Consensus Node.
//
// All'inizio non apre connessioni. Le connessioni vengono create in modo lazy,
// cioè solo quando viene inviata la prima richiesta verso uno specifico nodo.
func NewNodeClient() *NodeClient {
	return &NodeClient{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]kvpb.KeyValueServiceClient),
	}
}

// GetLeader invoca la RPC GetLeader sul nodo indicato.
//
// Il Router userà questo metodo durante la fase di discovery per interrogare
// i seed nodes e trovare il leader corrente del cluster.
func (c *NodeClient) GetLeader(ctx context.Context, address string) (*kvpb.GetLeaderResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.GetLeader(ctx, &kvpb.GetLeaderRequest{})
}

// Put invoca la RPC Put sul nodo indicato.
//
// NodeClient non verifica se address è davvero il leader. Se viene chiamato un
// follower, sarà il Consensus Node a rispondere con success=false e leader_hint.
// Il Router userà poi quell'hint per aggiornare la propria cache.
func (c *NodeClient) Put(ctx context.Context, address string, key string, value string) (*kvpb.PutResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.Put(ctx, &kvpb.PutRequest{
		Key:   key,
		Value: value,
	})
}

// Get invoca la RPC Get sul nodo indicato.
//
// Nel progetto attuale le letture devono passare dal leader. NodeClient però
// non contiene questa regola: si limita a chiamare il nodo richiesto.
func (c *NodeClient) Get(ctx context.Context, address string, key string) (*kvpb.GetResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.Get(ctx, &kvpb.GetRequest{
		Key: key,
	})
}

// Delete invoca la RPC Delete sul nodo indicato.
//
// Come per Put, NodeClient non decide se il nodo è leader. L'eventuale redirect
// tramite leader_hint sarà gestito dal Router.
func (c *NodeClient) Delete(ctx context.Context, address string, key string) (*kvpb.DeleteResponse, error) {
	client, err := c.clientForAddress(address)
	if err != nil {
		return nil, err
	}

	return client.Delete(ctx, &kvpb.DeleteRequest{
		Key: key,
	})
}

// Close chiude tutte le connessioni gRPC aperte verso i Consensus Node.
//
// Verrà chiamata dal main del Proxy durante lo shutdown del servizio.
func (c *NodeClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for address, conn := range c.conns {
		if err := conn.Close(); err != nil {
			fmt.Printf("cannot close connection to consensus node %s: %v\n", address, err)
		}
	}

	c.conns = make(map[string]*grpc.ClientConn)
	c.clients = make(map[string]kvpb.KeyValueServiceClient)
}

// clientForAddress restituisce un client gRPC verso l'indirizzo indicato.
//
// Se esiste già una connessione, viene riutilizzata.
// Se non esiste, viene creata e salvata nella cache interna.
func (c *NodeClient) clientForAddress(address string) (kvpb.KeyValueServiceClient, error) {
	if address == "" {
		return nil, fmt.Errorf("consensus node address cannot be empty")
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
		return nil, fmt.Errorf("cannot create grpc client for consensus node %s: %w", address, err)
	}

	client := kvpb.NewKeyValueServiceClient(conn)

	c.conns[address] = conn
	c.clients[address] = client

	return client, nil
}
