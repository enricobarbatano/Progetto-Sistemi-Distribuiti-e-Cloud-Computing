package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Target         string
	ClusterSize    int
	Trials         int
	TrialStart     int
	CSVOut         string
	KeyPrefix      string
	TimeoutMS      int
	PollIntervalMS int
	PutIntervalMS  int
	MaxWaitMS      int
	ComposeFile    string
}

type TrialResult struct {
	TimestampUTC         string
	Trial                int
	ClusterSize          int
	OldLeader            string
	NewLeader            string
	NewLeaderTimeMS      int64
	FirstSuccessfulPutMS int64
	DowntimeMS           int64
	FailedPuts           int
	SuccessfulPuts       int
	LeaderPolls          int
	Notes                string
}

func main() {
	cfg := loadConfig()
	log.Printf("failover-client target=%s cluster_size=%d trials=%d csv_out=%s", cfg.Target, cfg.ClusterSize, cfg.Trials, cfg.CSVOut)

	conn, err := grpc.NewClient(cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	defer conn.Close()

	client := kvpb.NewKeyValueServiceClient(conn)
	reader := bufio.NewReader(os.Stdin)

	if err := ensureCSV(cfg.CSVOut); err != nil {
		log.Fatalf("failed to prepare csv: %v", err)
	}

	for offset := 0; offset < cfg.Trials; offset++ {
		trial := cfg.TrialStart + offset
		result := runTrial(cfg, client, reader, trial)
		if err := appendResult(cfg.CSVOut, result); err != nil {
			log.Fatalf("failed to write failover result: %v", err)
		}

		log.Printf("trial=%d old_leader=%s new_leader=%s new_leader_ms=%d downtime_ms=%d first_successful_put_ms=%d failed_puts=%d successful_puts=%d notes=%s",
			result.Trial,
			result.OldLeader,
			result.NewLeader,
			result.NewLeaderTimeMS,
			result.DowntimeMS,
			result.FirstSuccessfulPutMS,
			result.FailedPuts,
			result.SuccessfulPuts,
			result.Notes,
		)

		if offset < cfg.Trials-1 {
			fmt.Println()
			fmt.Printf("Riavvia il nodo fermato su EC2 con:\n")
			fmt.Printf("  docker compose -f %s start %s\n", cfg.ComposeFile, result.OldLeader)
			fmt.Println("Attendi 10-15 secondi, poi premi INVIO per continuare con il trial successivo.")
			waitEnter(reader)
		}
	}
}

func runTrial(cfg Config, client kvpb.KeyValueServiceClient, reader *bufio.Reader, trial int) TrialResult {
	oldLeader, err := waitForLeader(cfg, client, "")
	if err != nil {
		return TrialResult{
			TimestampUTC: time.Now().UTC().Format(time.RFC3339Nano),
			Trial:        trial,
			ClusterSize:  cfg.ClusterSize,
			Notes:        "failed_to_find_initial_leader: " + err.Error(),
		}
	}

	fmt.Println()
	fmt.Printf("Trial %d - leader corrente: %s\n", trial, oldLeader)
	fmt.Println("Nel terminale SSH su EC2 esegui ORA:")
	fmt.Printf("  docker compose -f %s stop %s\n", cfg.ComposeFile, oldLeader)
	fmt.Println("Subito dopo lo stop, premi INVIO qui per far partire la misura del downtime percepito dal client.")
	waitEnter(reader)

	crashTime := time.Now()
	deadline := crashTime.Add(time.Duration(cfg.MaxWaitMS) * time.Millisecond)

	result := TrialResult{
		TimestampUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Trial:        trial,
		ClusterSize:  cfg.ClusterSize,
		OldLeader:    oldLeader,
		NewLeader:    "",
		Notes:        "",
	}

	var firstSuccessfulPutAt time.Time
	var newLeaderAt time.Time

	lastPutAttempt := time.Time{}
	lastLeaderPoll := time.Time{}

	for time.Now().Before(deadline) {
		now := time.Now()

		if lastPutAttempt.IsZero() || now.Sub(lastPutAttempt) >= time.Duration(cfg.PutIntervalMS)*time.Millisecond {
			lastPutAttempt = now
			key := fmt.Sprintf("%s-trial-%02d-%d", cfg.KeyPrefix, trial, now.UnixNano())
			success := tryPut(cfg, client, key, "ok")
			if success {
				result.SuccessfulPuts++
				if firstSuccessfulPutAt.IsZero() {
					firstSuccessfulPutAt = time.Now()
				}
			} else {
				result.FailedPuts++
			}
		}

		if lastLeaderPoll.IsZero() || now.Sub(lastLeaderPoll) >= time.Duration(cfg.PollIntervalMS)*time.Millisecond {
			lastLeaderPoll = now
			result.LeaderPolls++
			leader, err := getLeader(cfg, client)
			if err == nil && leader != "" && leader != oldLeader {
				if newLeaderAt.IsZero() {
					newLeaderAt = time.Now()
					result.NewLeader = leader
				}
			}
		}

		if !firstSuccessfulPutAt.IsZero() && !newLeaderAt.IsZero() {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !newLeaderAt.IsZero() {
		result.NewLeaderTimeMS = newLeaderAt.Sub(crashTime).Milliseconds()
	} else {
		result.NewLeaderTimeMS = -1
		result.Notes = appendNote(result.Notes, "new_leader_not_detected")
	}

	if !firstSuccessfulPutAt.IsZero() {
		result.FirstSuccessfulPutMS = firstSuccessfulPutAt.Sub(crashTime).Milliseconds()
		result.DowntimeMS = result.FirstSuccessfulPutMS
	} else {
		result.FirstSuccessfulPutMS = -1
		result.DowntimeMS = -1
		result.Notes = appendNote(result.Notes, "put_did_not_recover")
	}

	return result
}

func loadConfig() Config {
	clusterSize := getEnvInt("CLUSTER_SIZE", 5)
	return Config{
		Target:         getEnv("TARGET", "localhost:8080"),
		ClusterSize:    clusterSize,
		Trials:         getEnvInt("TRIALS", 1),
		TrialStart:     getEnvInt("TRIAL_START", 1),
		CSVOut:         getEnv("CSV_OUT", "reports/raw/failover_trials.csv"),
		KeyPrefix:      getEnv("KEY_PREFIX", "failover"),
		TimeoutMS:      getEnvInt("TIMEOUT_MS", 1000),
		PollIntervalMS: getEnvInt("POLL_INTERVAL_MS", 100),
		PutIntervalMS:  getEnvInt("PUT_INTERVAL_MS", 100),
		MaxWaitMS:      getEnvInt("MAX_WAIT_MS", 15000),
		ComposeFile:    getEnv("COMPOSE_FILE", "deployments/docker/docker-compose-5nodes.yml"),
	}
}

func waitForLeader(cfg Config, client kvpb.KeyValueServiceClient, previous string) (string, error) {
	deadline := time.Now().Add(time.Duration(cfg.MaxWaitMS) * time.Millisecond)
	var lastErr error

	for time.Now().Before(deadline) {
		leader, err := getLeader(cfg, client)
		if err == nil && leader != "" && leader != previous {
			return leader, nil
		}
		lastErr = err
		time.Sleep(time.Duration(cfg.PollIntervalMS) * time.Millisecond)
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("leader not found before deadline")
}

func getLeader(cfg Config, client kvpb.KeyValueServiceClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
	defer cancel()

	resp, err := client.GetLeader(ctx, &kvpb.GetLeaderRequest{})
	if err != nil {
		return "", err
	}
	if !resp.HasLeader {
		return "", nil
	}
	return resp.LeaderId, nil
}

func tryPut(cfg Config, client kvpb.KeyValueServiceClient, key string, value string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
	defer cancel()

	resp, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
	if err != nil {
		return false
	}
	return resp.Success
}

func ensureCSV(path string) error {
	if err := ensureParent(path); err != nil {
		return err
	}

	if _, err := os.Stat(path); err == nil {
		return nil
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	return writer.Write([]string{
		"timestamp_utc",
		"trial",
		"cluster_size",
		"old_leader",
		"new_leader",
		"new_leader_time_ms",
		"first_successful_put_ms",
		"downtime_ms",
		"failed_puts",
		"successful_puts",
		"leader_polls",
		"notes",
	})
}

func appendResult(path string, result TrialResult) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	return writer.Write([]string{
		result.TimestampUTC,
		strconv.Itoa(result.Trial),
		strconv.Itoa(result.ClusterSize),
		result.OldLeader,
		result.NewLeader,
		strconv.FormatInt(result.NewLeaderTimeMS, 10),
		strconv.FormatInt(result.FirstSuccessfulPutMS, 10),
		strconv.FormatInt(result.DowntimeMS, 10),
		strconv.Itoa(result.FailedPuts),
		strconv.Itoa(result.SuccessfulPuts),
		strconv.Itoa(result.LeaderPolls),
		result.Notes,
	})
}

func ensureParent(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func waitEnter(reader *bufio.Reader) {
	_, _ = reader.ReadString('\n')
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func appendNote(current string, note string) string {
	if strings.TrimSpace(current) == "" {
		return note
	}
	return current + ";" + note
}
