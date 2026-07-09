// Package consensus contiene anche le RPC key-value esposte dal ConsensusNode.
//
// Questo file definisce il punto di ingresso applicativo del nodo per le
// operazioni Put, Get, Delete e GetLeader.
//
// Le scritture Put e Delete non modificano direttamente la state machine:
// vengono prima inserite nel log replicato e diventano visibili solo dopo il
// commit su quorum.
//
// Le letture Get vengono servite dal leader e, prima di rispondere, il leader
// verifica di avere ancora contatto con la maggioranza del cluster. Questo
// riduce il rischio di letture stale da un leader isolato e supporta la
// semantica linearizzabile descritta nella relazione.
//
// GetLeader permette al Client Proxy di implementare la discovery dinamica del
// leader senza conoscere in anticipo la topologia effettiva del cluster.

package consensus

import (
	"context"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

// Put gestisce una richiesta di scrittura sullo storage key-value.
//
// La richiesta viene accettata solo se il nodo corrente è leader. Il leader
// crea una LogEntry, la persiste, la replica su quorum e solo dopo il commit la
// applica alla state machine locale.
//
// Se il nodo non è leader, la risposta contiene leader_hint, usato dal Client
// Proxy per aggiornare la cache del leader e ridurre i tentativi verso nodi
// sbagliati.
func (n *ConsensusNode) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	success, errorMessage, leaderHint, err := n.handleWriteOperation(
		ctx,
		consensuspb.LogOperation_LOG_OPERATION_PUT,
		req.Key,
		req.Value,
	)
	if err != nil {
		return nil, err
	}

	return &kvpb.PutResponse{
		Success:    success,
		Error:      errorMessage,
		LeaderHint: leaderHint,
	}, nil
}

// Get gestisce una richiesta di lettura dallo storage key-value.
//
// La lettura viene servita solo dal leader. Prima di leggere dalla state machine,
// il leader verifica di avere ancora contatto con la maggioranza del cluster
// tramite confirmLeadershipWithQuorum.
//
// Questa verifica è una forma semplificata di ReadIndex: non introduce una nuova
// RPC, ma riusa heartbeat AppendEntries vuoti. Lo scopo è evitare che un leader
// isolato continui a servire valori potenzialmente obsoleti.
func (n *ConsensusNode) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	n.mu.Lock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		leaderHint := n.leaderAddress
		n.mu.Unlock()

		return &kvpb.GetResponse{
			Found:      false,
			Error:      "node is not leader",
			LeaderHint: leaderHint,
		}, nil
	}

	leaderHint := n.leaderAddress
	n.mu.Unlock()

	// Prima di rispondere a una lettura, il leader conferma di poter ancora
	// raggiungere il quorum. Se il quorum non è disponibile, la lettura viene rifiutata.
	if !n.confirmLeadershipWithQuorum(ctx) {
		n.mu.Lock()
		leaderHint = n.leaderAddress
		n.mu.Unlock()

		return &kvpb.GetResponse{
			Found:      false,
			Error:      "leader quorum unavailable",
			LeaderHint: leaderHint,
		}, nil
	}

	value, ok := n.store.Get(req.Key)
	if !ok {
		return &kvpb.GetResponse{
			Found:      false,
			LeaderHint: leaderHint,
		}, nil
	}

	return &kvpb.GetResponse{
		Found:      true,
		Value:      value,
		LeaderHint: leaderHint,
	}, nil
}

// Delete gestisce una richiesta di cancellazione dallo storage key-value.
//
// Come Put, anche Delete passa dal log replicato. La cancellazione diventa
// visibile solo dopo essere stata replicata su quorum e applicata alla state
// machine. Se il nodo non è leader, viene restituito leader_hint.
func (n *ConsensusNode) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	success, errorMessage, leaderHint, err := n.handleWriteOperation(
		ctx,
		consensuspb.LogOperation_LOG_OPERATION_DELETE,
		req.Key,
		"",
	)
	if err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{
		Success:    success,
		Error:      errorMessage,
		LeaderHint: leaderHint,
	}, nil
}

// GetLeader restituisce il leader noto dal nodo corrente.
//
// Se il nodo corrente è leader, restituisce sé stesso. Se è follower e ha
// ricevuto heartbeat validi, restituisce l'ultimo leader noto. Se non conosce
// ancora un leader, risponde con HasLeader=false.
//
// Questa RPC è usata dal Client Proxy per implementare Client-side Service
// Discovery.
func (n *ConsensusNode) GetLeader(ctx context.Context, req *kvpb.GetLeaderRequest) (*kvpb.GetLeaderResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.GetLeaderResponse{
			HasLeader:     true,
			LeaderId:      n.id,
			LeaderAddress: n.address,
			Term:          n.currentTerm,
		}, nil
	}

	if n.leaderID != "" {
		return &kvpb.GetLeaderResponse{
			HasLeader:     true,
			LeaderId:      n.leaderID,
			LeaderAddress: n.leaderAddress,
			Term:          n.currentTerm,
		}, nil
	}

	return &kvpb.GetLeaderResponse{
		HasLeader: false,
		Term:      n.currentTerm,
	}, nil
}
