// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la RPC InstallSnapshot.
//
// InstallSnapshot viene usata dal leader quando un follower è troppo indietro
// e le entry necessarie non sono più disponibili nel log, perché sono già state
// incluse in uno snapshot locale del leader.
//
// In questa versione il protocollo supporta già offset/data/done nel proto,
// ma l'implementazione usa un singolo chunk: offset=0 e done=true.
package consensus

import (
	"context"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// InstallSnapshot gestisce la RPC usata dal leader per installare uno snapshot.
//
// Il follower:
//   - rifiuta snapshot con termine vecchio;
//   - aggiorna il proprio termine se necessario;
//   - riconosce il leader;
//   - decodifica lo snapshot;
//   - ripristina la state machine;
//   - aggiorna lastIncludedIndex e lastIncludedTerm;
//   - scarta le entry obsolete dal log;
//   - persiste snapshot e stato corrente;
//   - resetta il timer di elezione.
func (n *ConsensusNode) InstallSnapshot(ctx context.Context, req *consensuspb.InstallSnapshotRequest) (*consensuspb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	stateChanged := false

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		stateChanged = true
	}

	n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
	n.leaderID = req.LeaderId

	if leaderAddress, ok := n.peers[req.LeaderId]; ok {
		n.leaderAddress = leaderAddress
	} else if req.LeaderId == n.id {
		n.leaderAddress = n.address
	}

	// In questa fase gestiamo snapshot in un singolo chunk.
	// Il proto è già pronto per i chunk multipli, ma non li usiamo ancora.
	if req.Offset != 0 || !req.Done {
		if stateChanged {
			if err := n.persistLocked(); err != nil {
				return nil, err
			}
		}

		n.resetElectionTimer()

		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	// Se lo snapshot è vecchio o già applicato, non serve reinstallarlo.
	if req.LastIncludedIndex <= n.lastIncludedIndex {
		if stateChanged {
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

	snapshot, err := persistence.DecodeSnapshot(req.Data)
	if err != nil {
		return nil, err
	}

	// Verifica di coerenza tra metadati RPC e contenuto serializzato.
	if snapshot.LastIncludedIndex != req.LastIncludedIndex || snapshot.LastIncludedTerm != req.LastIncludedTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	n.store.Restore(snapshot.Data)
	n.lastIncludedIndex = snapshot.LastIncludedIndex
	n.lastIncludedTerm = snapshot.LastIncludedTerm

	if n.commitIndex < snapshot.LastIncludedIndex {
		n.commitIndex = snapshot.LastIncludedIndex
	}

	if n.lastApplied < snapshot.LastIncludedIndex {
		n.lastApplied = snapshot.LastIncludedIndex
	}

	n.compactLogUpToLocked(snapshot.LastIncludedIndex)

	if err := n.persistenceManager.SaveSnapshot(snapshot); err != nil {
		return nil, err
	}

	if err := n.persistLocked(); err != nil {
		return nil, err
	}

	n.resetElectionTimer()

	return &consensuspb.InstallSnapshotResponse{
		Term:    n.currentTerm,
		Success: true,
	}, nil
}
