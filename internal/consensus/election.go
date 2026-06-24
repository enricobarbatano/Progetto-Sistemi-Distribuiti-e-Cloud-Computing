// Package consensus contiene la logica del protocollo di consenso.
//
// Questo file raccoglie la parte di Raft relativa all'elezione del leader:
// timeout di elezione, transizione a Candidate, invio delle RequestVote,
// conteggio dei voti e transizione a Leader.
//
// La separazione da node.go serve a ridurre la complessità della struct
// principale: ConsensusNode resta l'orchestratore del nodo, mentre qui viene
// isolato il comportamento legato alla leadership.
package consensus

import (
	"context"
	"log"
	"math/rand"
	"time"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
)

// randomElectionTimeout genera un timeout di elezione casuale.
//
// L'intervallo è volutamente abbastanza largo per rendere più stabili i test
// locali con più processi avviati manualmente tramite go run.
func randomElectionTimeout() time.Duration {
	minTimeout := 1500
	maxTimeout := 3000
	randomMillis := minTimeout + rand.Intn(maxTimeout-minTimeout+1)

	return time.Duration(randomMillis) * time.Millisecond
}

// resetElectionTimer notifica al loop di elezione che il timer deve essere resettato.
//
// Il canale è bufferizzato e l'invio è non bloccante: se esiste già un reset
// pendente, non serve accodarne un altro.
func (n *ConsensusNode) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

// electionLoop gestisce il timer di elezione del nodo.
//
// Se il timer scade e il nodo non è leader, viene avviata una nuova elezione.
// Il timer viene resettato quando il nodo riceve heartbeat validi o concede un voto.
func (n *ConsensusNode) electionLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(randomElectionTimeout())

		case <-timer.C:
			n.mu.Lock()
			isLeader := n.role == consensuspb.NodeRole_NODE_ROLE_LEADER
			n.mu.Unlock()

			if !isLeader {
				n.startElection()
			}

			timer.Reset(randomElectionTimeout())
		}
	}
}

// startElection avvia una nuova elezione Raft.
//
// Il nodo passa a Candidate, incrementa il termine, vota per sé stesso e invia
// RequestVote in parallelo a tutti i peer conosciuti.
func (n *ConsensusNode) startElection() {
	n.mu.Lock()
	if n.role == consensuspb.NodeRole_NODE_ROLE_LEADER {
		n.mu.Unlock()
		return
	}

	n.role = consensuspb.NodeRole_NODE_ROLE_CANDIDATE
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.leaderAddress = ""

	termStarted := n.currentTerm
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	votes := 1
	neededVotes := n.majority()

	if err := n.persistLocked(); err != nil {
		log.Printf("node %s cannot persist state before election: %v", n.id, err)
		n.mu.Unlock()
		return
	}

	log.Printf("node %s became candidate for term %d", n.id, termStarted)

	if votes >= neededVotes {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		go n.sendHeartbeats()
		return
	}

	peers := make(map[string]string, len(n.peers))
	for peerID, peerAddress := range n.peers {
		peers[peerID] = peerAddress
	}
	n.mu.Unlock()

	for peerID, peerAddress := range peers {
		go func(peerID string, peerAddress string) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			n.mu.Lock()
			client, err := n.getPeerClientLocked(peerID, peerAddress)
			if err != nil {
				n.markPeerOfflineLocked(peerID, err)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			resp, err := client.RequestVote(ctx, &consensuspb.RequestVoteRequest{
				Term:         termStarted,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				n.mu.Lock()
				n.markPeerOfflineLocked(peerID, err)
				n.mu.Unlock()
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			n.markPeerOnlineLocked(peerID)

			if n.role != consensuspb.NodeRole_NODE_ROLE_CANDIDATE || n.currentTerm != termStarted {
				return
			}

			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.votedFor = ""
				n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
				n.leaderID = ""
				n.leaderAddress = ""

				if err := n.persistLocked(); err != nil {
					log.Printf("node %s cannot persist higher term: %v", n.id, err)
				}

				n.resetElectionTimer()
				return
			}

			if resp.VoteGranted {
				votes++

				if votes >= neededVotes && n.role == consensuspb.NodeRole_NODE_ROLE_CANDIDATE {
					n.becomeLeaderLocked()
					go n.sendHeartbeats()
				}
			}
		}(peerID, peerAddress)
	}
}

// becomeLeaderLocked trasforma il nodo corrente in leader.
//
// Deve essere chiamata con n.mu già acquisito.
// Inizializza nextIndex e matchIndex per ogni follower.
func (n *ConsensusNode) becomeLeaderLocked() {
	n.role = consensuspb.NodeRole_NODE_ROLE_LEADER
	n.leaderID = n.id
	n.leaderAddress = n.address

	nextIndex := n.lastLogIndexLocked() + 1
	for peerID := range n.peers {
		n.nextIndex[peerID] = nextIndex
		n.matchIndex[peerID] = 0
	}

	log.Printf("node %s became leader for term %d", n.id, n.currentTerm)
	n.resetElectionTimer()
}

// RequestVote gestisce la RPC usata da un Candidate per richiedere un voto.
//
// Un voto viene concesso solo se:
//   - il termine del candidato non è più vecchio del termine locale;
//   - il nodo non ha già votato per un altro candidato nello stesso termine;
//   - il log del candidato è almeno aggiornato quanto quello locale.
func (n *ConsensusNode) RequestVote(ctx context.Context, req *consensuspb.RequestVoteRequest) (*consensuspb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &consensuspb.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}, nil
	}

	stateChanged := false
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.role = consensuspb.NodeRole_NODE_ROLE_FOLLOWER
		n.leaderID = ""
		n.leaderAddress = ""
		stateChanged = true
	}

	voteGranted := false
	canVoteForCandidate := n.votedFor == "" || n.votedFor == req.CandidateId
	logIsUpToDate := n.isCandidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	if canVoteForCandidate && logIsUpToDate {
		n.votedFor = req.CandidateId
		voteGranted = true
		stateChanged = true
	}

	if stateChanged {
		if err := n.persistLocked(); err != nil {
			return nil, err
		}
	}

	if voteGranted {
		n.resetElectionTimer()
	}

	return &consensuspb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}, nil
}
