package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/internal/proxy"

	"google.golang.org/grpc"
)

func main() {
	config, err := proxy.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("cannot load proxy config: %v", err)
	}

	healthServer := proxy.StartHealthServer(config.HealthPort)
	defer shutdownHealthServer(healthServer)

	listenAddress := fmt.Sprintf(":%s", config.Port)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", listenAddress, err)
	}
	defer listener.Close()

	leaderCache := proxy.NewLeaderCache()
	nodeClient := proxy.NewNodeClient()
	defer nodeClient.Close()

	circuitBreakers := proxy.NewCircuitBreakerManager()

	router := proxy.NewRouter(
		config,
		leaderCache,
		nodeClient,
		circuitBreakers,
	)

	proxyService := proxy.NewProxyService(router)

	grpcServer := grpc.NewServer()
	kvpb.RegisterKeyValueServiceServer(grpcServer, proxyService)

	log.Printf(
		"client proxy listening on port %s with consensus nodes %v",
		config.Port,
		config.ConsensusNodes,
	)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("client proxy stopped with error: %v", err)
	}
}

func shutdownHealthServer(server interface {
	Shutdown(ctx context.Context) error
}) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("cannot shutdown health server cleanly: %v", err)
	}
}
