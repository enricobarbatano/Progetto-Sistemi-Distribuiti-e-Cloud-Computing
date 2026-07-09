// Package consensus contiene anche la RPC InstallSnapshot.
//
// InstallSnapshot viene usata dal leader quando un follower è troppo indietro
// rispetto al log corrente e le entry necessarie sono già incluse nello snapshot
// locale del leader.
//
// In questa versione il proto supporta offset/data/done, quindi il protocollo è
// predisposto per snapshot a chunk multipli. L'implementazione attuale invia
// però lo snapshot in un singolo chunk: offset=0 e done=true.

package consensus

import (
	"context"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/persistence"
)

// InstallSnapshot installa sul follower uno snapshot inviato dal leader.
//
// Il follower rifiuta snapshot provenienti da termini vecchi. Se il termine
// della richiesta è più recente, il nodo aggiorna il proprio termine e passa a
// follower. Successivamente decodifica lo snapshot, ripristina la state machine,
// aggiorna lastIncludedIndex e lastIncludedTerm, scarta le entry obsolete dal log
// e persiste snapshot e stato corrente.
//
// Questa RPC permette a un follower molto arretrato di riallinearsi senza dover
// ricevere tutte le entry storiche una per una.
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

	// L'implementazione corrente accetta solo snapshot completi in un singolo chunk.
	// Richieste parziali vengono rifiutate, anche se il contratto proto è già pronto
	// per supportarle in futuro.
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

	// Se lo snapshot è vecchio o già coperto dallo snapshot locale, non serve
	// reinstallarlo. La risposta positiva rende l'operazione idempotente rispetto a
	// retry del leader.
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

	// Verifica che i metadati dichiarati nella RPC coincidano con il contenuto
	// serializzato. Questo evita di installare snapshot incoerenti.
	if snapshot.LastIncludedIndex != req.LastIncludedIndex || snapshot.LastIncludedTerm != req.LastIncludedTerm {
		return &consensuspb.InstallSnapshotResponse{
			Term:    n.currentTerm,
			Success: false,
		}, nil
	}

	// Lo snapshot sostituisce lo stato applicativo locale fino a LastIncludedIndex.
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
