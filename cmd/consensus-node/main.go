package main

import (
	"log"
	"net"
	"os"
	"strings"

	consensuspb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/consensuspb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/consensus"

	"google.golang.org/grpc"
)

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

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("cannot listen on port %s: %v", port, err)
	}

	server := grpc.NewServer()

	consensuspb.RegisterConsensusServiceServer(server, node)
	kvpb.RegisterKeyValueServiceServer(server, node)

	log.Printf("consensus node %s listening on port %s", nodeID, port)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("grpc server stopped: %v", err)
	}
}

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}

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
