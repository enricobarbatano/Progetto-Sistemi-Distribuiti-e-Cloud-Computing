// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la gestione dei peer del cluster: creazione e riuso
// dei client gRPC verso gli altri nodi, e tracking dello stato online/offline.
//
// La logica è separata da node.go per mantenere ConsensusNode più leggibile:
// il nodo continua a coordinare il protocollo Raft, mentre questo file contiene
// solo il codice di supporto per comunicare con gli altri nodi.
package consensus

import (
	"log"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// getPeerClientLocked restituisce un client gRPC persistente verso un peer.
//
// Deve essere chiamata con n.mu già acquisito.
// Se il client esiste già, viene riutilizzato.
// Se non esiste, viene creata una nuova connessione gRPC e salvata nella struct.
//
// Questo evita di aprire una nuova connessione per ogni RequestVote,
// AppendEntries o heartbeat.
func (n *ConsensusNode) getPeerClientLocked(peerID string, peerAddress string) (consensuspb.ConsensusServiceClient, error) {
	if client, ok := n.peerClients[peerID]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(
		peerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := consensuspb.NewConsensusServiceClient(conn)
	n.peerConns[peerID] = conn
	n.peerClients[peerID] = client

	return client, nil
}

// markPeerOfflineLocked marca un peer come non raggiungibile.
//
// Il log viene scritto solo quando il peer passa da online a offline.
// Questo evita di stampare lo stesso errore a ogni heartbeat fallito.
//
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || wasOnline {
		log.Printf("node %s marked peer %s as offline: %v", n.id, peerID, err)
	}

	n.peerOnline[peerID] = false
}

// markPeerOnlineLocked marca un peer come raggiungibile.
//
// Il log viene scritto solo quando il peer passa da offline a online.
//
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) markPeerOnlineLocked(peerID string) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || !wasOnline {
		log.Printf("node %s marked peer %s as online", n.id, peerID)
	}

	n.peerOnline[peerID] = true
}
