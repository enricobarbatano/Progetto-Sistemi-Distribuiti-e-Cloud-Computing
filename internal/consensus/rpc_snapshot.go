// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la RPC InstallSnapshot.
//
// In questa fase InstallSnapshot è ancora minimale: aggiorna il termine,
// riconosce il leader e resetta il timer di elezione. La logica completa di
// installazione snapshot leader -> follower verrà implementata nelle fasi
// successive, quando verranno introdotte log compaction e snapshot remoti.
package consensus

import (
	"context"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// InstallSnapshot gestisce la RPC usata dal leader per installare uno snapshot.
//
// In questa fase la funzione non applica ancora uno snapshot remoto reale.
// Mantiene però il comportamento Raft minimo: rifiuta termini vecchi, aggiorna
// il termine se necessario, riconosce il leader e resetta il timer di elezione.
func (n *ConsensusNode) InstallSnapshot(ctx context.Context, req *consensuspb.InstallSnapshotRequest) (*consensuspb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
		n.leaderID = req.LeaderId

		if leaderAddress, ok := n.peers[req.LeaderId]; ok {
			n.leaderAddress = leaderAddress
		}

		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	n.resetElectionTimer()

	return &consensuspb.InstallSnapshotResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}
