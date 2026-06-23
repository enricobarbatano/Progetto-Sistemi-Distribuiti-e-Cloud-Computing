package main

//client minimale che serve solo per testare il collegamento client-server
import (
	"context"
	"log"
	"os"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := getEnv("TARGET", "localhost:50051")

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("cannot create grpc client: %v", err)
	}
	defer conn.Close()

	client := kvpb.NewKeyValueServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	leaderResp, err := client.GetLeader(ctx, &kvpb.GetLeaderRequest{})
	if err != nil {
		log.Fatalf("GetLeader failed: %v", err)
	}

	log.Printf(
		"GetLeader response: has_leader=%v leader_id=%s leader_address=%s term=%d",
		leaderResp.HasLeader,
		leaderResp.LeaderId,
		leaderResp.LeaderAddress,
		leaderResp.Term,
	)

	getResp, err := client.Get(ctx, &kvpb.GetRequest{
		Key: "test-key",
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}

	log.Printf(
		"Get response: found=%v value=%s error=%s leader_hint=%s",
		getResp.Found,
		getResp.Value,
		getResp.Error,
		getResp.LeaderHint,
	)
}

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}
