package main

// key-value esposto dai consensus node.
//
// Il comportamento è controllato tramite variabili d'ambiente:
//
//   TARGET=localhost:50051   nodo gRPC da contattare
//   OP=leader                operazione da eseguire: leader, get, put, delete, all
//   KEY=test-key             chiave usata da get/put/delete
//   VALUE=test-value         valore usato da put
//
// Esempi:
//
//   set TARGET=localhost:50052
//   set OP=put
//   set KEY=name
//   set VALUE=enrico
//   go run .\cmd\bench-client
//
//   set TARGET=localhost:50052
//   set OP=get
//   set KEY=name
//   go run .\cmd\bench-client
import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := getEnv("TARGET", "localhost:50051")
	op := strings.ToLower(getEnv("OP", "all"))
	key := getEnv("KEY", "test-key")
	value := getEnv("VALUE", "test-value")

	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("cannot create grpc client: %v", err)
	}
	defer conn.Close()

	client := kvpb.NewKeyValueServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Printf("bench-client target=%s op=%s key=%s value=%s", target, op, key, value)

	switch op {
	case "leader":
		runGetLeader(ctx, client)

	case "get":
		runGet(ctx, client, key)

	case "put":
		runPut(ctx, client, key, value)

	case "delete":
		runDelete(ctx, client, key)

	case "all":
		// Modalità compatibile con la vecchia versione: prima scopre il leader,
		// poi esegue una Get sulla chiave indicata.
		runGetLeader(ctx, client)
		runGet(ctx, client, key)

	default:
		log.Fatalf("unknown OP=%s. Supported values: leader, get, put, delete, all", op)
	}
}

func runGetLeader(ctx context.Context, client kvpb.KeyValueServiceClient) {
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
}

func runGet(ctx context.Context, client kvpb.KeyValueServiceClient, key string) {
	getResp, err := client.Get(ctx, &kvpb.GetRequest{
		Key: key,
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}

	log.Printf(
		"Get response: found=%v key=%s value=%s error=%s leader_hint=%s",
		getResp.Found,
		key,
		getResp.Value,
		getResp.Error,
		getResp.LeaderHint,
	)
}

func runPut(ctx context.Context, client kvpb.KeyValueServiceClient, key string, value string) {
	putResp, err := client.Put(ctx, &kvpb.PutRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	log.Printf(
		"Put response: success=%v key=%s value=%s error=%s leader_hint=%s",
		putResp.Success,
		key,
		value,
		putResp.Error,
		putResp.LeaderHint,
	)
}

func runDelete(ctx context.Context, client kvpb.KeyValueServiceClient, key string) {
	deleteResp, err := client.Delete(ctx, &kvpb.DeleteRequest{
		Key: key,
	})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	log.Printf(
		"Delete response: success=%v key=%s error=%s leader_hint=%s",
		deleteResp.Success,
		key,
		deleteResp.Error,
		deleteResp.LeaderHint,
	)
}

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}
