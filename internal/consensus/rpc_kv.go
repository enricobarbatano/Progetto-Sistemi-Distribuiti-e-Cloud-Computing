// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie le RPC key-value esposte dal ConsensusNode.
//
// Le RPC Put e Delete non modificano direttamente la state machine: passano
// prima dal log Raft e vengono applicate solo dopo replica su quorum.
// La RPC Get viene servita solo dal leader, così si evitano letture stale dai
// follower. GetLeader invece permette al client o al proxy di scoprire il
// leader attualmente noto.
package consensus

import (
	"context"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

// Put gestisce una richiesta di scrittura sullo storage key-value.
//
// La scrittura viene accettata solo dal leader. Il leader crea una LogEntry,
// la replica su quorum, la committa e solo dopo la applica alla state machine.
// Se il nodo non è leader, restituisce leader_hint.
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
// In questa fase le letture vengono servite solo dal leader per evitare che un
// follower non ancora aggiornato restituisca dati stale. Se il nodo non è leader,
// risponde con leader_hint.
func (n *ConsensusNode) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != consensuspb.NodeRole_NODE_ROLE_LEADER {
		return &kvpb.GetResponse{
			Found:      false,
			Error:      "node is not leader",
			LeaderHint: n.leaderAddress,
		}, nil
	}

	value, ok := n.store.Get(req.Key)
	if !ok {
		return &kvpb.GetResponse{
			Found:      false,
			LeaderHint: n.leaderAddress,
		}, nil
	}

	return &kvpb.GetResponse{
		Found:      true,
		Value:      value,
		LeaderHint: n.leaderAddress,
	}, nil
}

// Delete gestisce una richiesta di cancellazione dallo storage key-value.
//
// Come Put, anche Delete passa dal log Raft e viene confermata solo dopo replica
// su quorum. Se il nodo non è leader, restituisce leader_hint.
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

// GetLeader restituisce informazioni sul leader noto.
//
// Se il nodo corrente è leader, restituisce sé stesso.
// Se è follower ma ha ricevuto heartbeat validi, restituisce il leader noto.
// Se non conosce ancora un leader, risponde con HasLeader=false.
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
