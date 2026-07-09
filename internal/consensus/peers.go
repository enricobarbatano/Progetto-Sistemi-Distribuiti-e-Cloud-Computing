// Package consensus contiene anche la gestione dei peer del cluster.
//
// Questo file isola la logica di supporto per comunicare con gli altri
// Consensus Node: creazione e riuso dei client gRPC, chiusura delle connessioni
// e tracking dello stato online/offline osservato.
//
// La separazione da node.go mantiene più leggibile la logica del consenso:
// election e replication usano questi helper, ma non gestiscono direttamente i
// dettagli di dial gRPC e diagnostica dei peer.

package consensus

import (
	"log"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// getPeerClientLocked restituisce un client gRPC riutilizzabile verso un peer.
//
// Deve essere chiamata con n.mu già acquisito. Se il client esiste già, viene
// riutilizzato; altrimenti viene creata una nuova connessione e salvata nella
// struct del nodo.
//
// Il riuso delle connessioni evita di aprire un nuovo canale gRPC per ogni
// RequestVote, AppendEntries o heartbeat, riducendo overhead e latenza nelle
// fasi di elezione e replica.
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
// Questo stato non modifica direttamente il risultato del consenso, che resta
// basato su quorum, ma aiuta logging, diagnostica e gestione dei tentativi verso
// nodi temporaneamente non disponibili.
//
// Il log viene scritto solo quando il peer passa da online a offline, evitando
// messaggi ripetuti a ogni heartbeat fallito. Deve essere chiamata con n.mu già
// acquisito.
func (n *ConsensusNode) markPeerOfflineLocked(peerID string, err error) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || wasOnline {
		log.Printf("node %s marked peer %s as offline: %v", n.id, peerID, err)
	}

	n.peerOnline[peerID] = false
}

// markPeerOnlineLocked marca un peer come nuovamente raggiungibile.
//
// Il log viene scritto solo quando il peer passa da offline a online. Questo
// rende più leggibili i log durante test di fault tolerance, crash leader o
// arresto temporaneo di un container.
//
// Deve essere chiamata con n.mu già acquisito.
func (n *ConsensusNode) markPeerOnlineLocked(peerID string) {
	wasOnline, exists := n.peerOnline[peerID]
	if !exists || !wasOnline {
		log.Printf("node %s marked peer %s as online", n.id, peerID)
	}

	n.peerOnline[peerID] = true
}
