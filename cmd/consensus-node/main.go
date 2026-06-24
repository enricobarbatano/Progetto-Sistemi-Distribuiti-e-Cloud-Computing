// Package main contiene il punto di ingresso del servizio consensus-node.
//
// Questo eseguibile avvia un nodo di consenso stateful del sistema distribuito.
// Il nodo espone tramite gRPC tre servizi principali:
//
//   - ConsensusService: usato per le RPC interne del protocollo di consenso,
//     come RequestVote, AppendEntries e InstallSnapshot.
//
//   - KeyValueService: usato per ricevere operazioni sullo storage chiave-valore,
//     come Put, Get, Delete e GetLeader.
//
//   - BackupNodeService: usato dal Backup Service per forzare snapshot,
//     scaricare snapshot locali e richiedere compattazione del log.
//
// In questa fase del progetto il file si occupa solo della configurazione e
// dell'avvio del server gRPC. La logica interna del nodo è implementata nel
// package internal/consensus.
package main

import (
	"log"
	"net"
	"os"
	"strings"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/consensus"

	"google.golang.org/grpc"
)

// main configura e avvia un singolo Consensus Node.
//
// La configurazione del nodo viene letta da variabili d'ambiente, in modo da
// poter avviare più nodi con parametri diversi, sia localmente sia tramite
// Docker Compose.
//
// Variabili d'ambiente usate:
//
//   - NODE_ID: identificativo logico del nodo.
//   - NODE_ADDRESS: indirizzo gRPC con cui gli altri nodi possono raggiungerlo.
//   - PORT: porta TCP locale su cui il server gRPC resta in ascolto.
//   - DATA_DIR: directory in cui salvare lo stato persistente del nodo.
//   - PEERS: lista degli altri nodi del cluster nel formato
//     node-2=localhost:50052,node-3=localhost:50053.
func main() {
	nodeID := getEnv("NODE_ID", "node-1")
	nodeAddress := getEnv("NODE_ADDRESS", "localhost:50051")
	port := getEnv("PORT", "50051")
	dataDir := getEnv("DATA_DIR", "data")

	peers := parsePeers(os.Getenv("PEERS"))

	node, err := consensus.NewConsensusNode(nodeID, nodeAddress, peers, dataDir)
	if err != nil {
		log.Fatalf("cannot create consensus node: %v", err)
	}

	node.Start()
	defer node.Stop()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("cannot listen on port %s: %v", port, err)
	}

	server := grpc.NewServer()

	// Servizio Raft interno.
	consensuspb.RegisterConsensusServiceServer(server, node)

	// Servizio key-value usato da client e proxy.
	kvpb.RegisterKeyValueServiceServer(server, node)

	// Servizio usato dal Backup Service.
	// Espone TriggerSnapshot, DownloadSnapshot e CompactLog sul singolo nodo.
	backuppb.RegisterBackupNodeServiceServer(server, node)

	log.Printf("consensus node %s listening on port %s", nodeID, port)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("grpc server stopped: %v", err)
	}
}

// getEnv restituisce il valore di una variabile d'ambiente.
//
// Se la variabile non è definita o è vuota, viene restituito il valore di
// default passato come parametro.
func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}

// parsePeers converte la lista testuale dei peer in una mappa.
//
// Il formato atteso della stringa raw è:
//
//	node-2=localhost:50052,node-3=localhost:50053
//
// Il risultato sarà una mappa del tipo:
//
//	node-2 -> localhost:50052
//	node-3 -> localhost:50053
//
// Le entry malformate vengono ignorate.
func parsePeers(raw string) map[string]string {
	peers := make(map[string]string)

	if raw == "" {
		return peers
	}

	parts := strings.Split(raw, ",")
	for _, part := range parts {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			continue
		}

		id := strings.TrimSpace(pair[0])
		address := strings.TrimSpace(pair[1])

		if id != "" && address != "" {
			peers[id] = address
		}
	}

	return peers
}
