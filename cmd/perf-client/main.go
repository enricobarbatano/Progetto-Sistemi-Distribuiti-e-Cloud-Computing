package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Target      string
	ClusterSize int
	Puts        int
	Gets        int
	Concurrency int
	KeyPrefix   string
	CSVOut      string
	SummaryOut  string
	TimeoutMS   int
}

type Result struct {
	Timestamp   string
	ClusterSize int
	Operation   string
	Index       int
	Success     bool
	LatencyMS   float64
	Error       string
}

type Summary struct {
	ClusterSize int
	Operation   string
	Count       int
	SuccessRate float64
	Avg         float64
	P50         float64
	P95         float64
	P99         float64
	Min         float64
	Max         float64
}

func main() {
	cfg := loadConfig()
	log.Printf("perf-client target=%s cluster_size=%d puts=%d gets=%d concurrency=%d key_prefix=%s csv_out=%s summary_out=%s",
		cfg.Target, cfg.ClusterSize, cfg.Puts, cfg.Gets, cfg.Concurrency, cfg.KeyPrefix, cfg.CSVOut, cfg.SummaryOut)

	conn, err := grpc.NewClient(cfg.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create grpc client: %v", err)
	}
	defer conn.Close()

	client := kvpb.NewKeyValueServiceClient(conn)

	results := make([]Result, 0, cfg.Puts+cfg.Gets)

	putResults := runOperations(cfg, "put", cfg.Puts, func(ctx context.Context, index int) (bool, string) {
		key := fmt.Sprintf("%s-key-%06d", cfg.KeyPrefix, index)
		value := fmt.Sprintf("%s-value-%06d", cfg.KeyPrefix, index)
		resp, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		if err != nil {
			return false, err.Error()
		}
		if !resp.Success {
			if resp.LeaderHint != "" {
				return false, fmt.Sprintf("%s leader_hint=%s", resp.Error, resp.LeaderHint)
			}
			return false, resp.Error
		}
		return true, ""
	})
	results = append(results, putResults...)

	getResults := runOperations(cfg, "get", cfg.Gets, func(ctx context.Context, index int) (bool, string) {
		key := fmt.Sprintf("%s-key-%06d", cfg.KeyPrefix, index)
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return false, err.Error()
		}
		if !resp.Found {
			if resp.Error != "" {
				return false, resp.Error
			}
			return false, "key not found"
		}
		return true, ""
	})
	results = append(results, getResults...)

	if err := writeRawCSV(cfg.CSVOut, results); err != nil {
		log.Fatalf("failed to write raw csv: %v", err)
	}

	summaries := []Summary{
		buildSummary(cfg.ClusterSize, "put", putResults),
		buildSummary(cfg.ClusterSize, "get", getResults),
	}

	if err := writeSummaryCSV(cfg.SummaryOut, summaries); err != nil {
		log.Fatalf("failed to write summary csv: %v", err)
	}

	for _, s := range summaries {
		log.Printf("summary cluster_size=%d operation=%s count=%d success_rate=%.4f avg_ms=%.3f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f min_ms=%.3f max_ms=%.3f",
			s.ClusterSize, s.Operation, s.Count, s.SuccessRate, s.Avg, s.P50, s.P95, s.P99, s.Min, s.Max)
	}
}

func loadConfig() Config {
	clusterSize := getEnvInt("CLUSTER_SIZE", 3)
	return Config{
		Target:      getEnv("TARGET", "localhost:8080"),
		ClusterSize: clusterSize,
		Puts:        getEnvInt("PUTS", 300),
		Gets:        getEnvInt("GETS", 300),
		Concurrency: max(1, getEnvInt("CONCURRENCY", 1)),
		KeyPrefix:   getEnv("KEY_PREFIX", fmt.Sprintf("perf-%dnodes", clusterSize)),
		CSVOut:      getEnv("CSV_OUT", fmt.Sprintf("reports/raw/scalability_%dnodes.csv", clusterSize)),
		SummaryOut:  getEnv("SUMMARY_OUT", fmt.Sprintf("reports/processed/scalability_%dnodes_summary.csv", clusterSize)),
		TimeoutMS:   getEnvInt("TIMEOUT_MS", 3000),
	}
}

func runOperations(cfg Config, operation string, count int, fn func(context.Context, int) (bool, string)) []Result {
	if count <= 0 {
		return nil
	}

	results := make([]Result, count)
	jobs := make(chan int)
	var wg sync.WaitGroup
	var completed int64

	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
				started := time.Now()
				success, errText := fn(ctx, index)
				elapsed := time.Since(started)
				cancel()

				results[index-1] = Result{
					Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
					ClusterSize: cfg.ClusterSize,
					Operation:   operation,
					Index:       index,
					Success:     success,
					LatencyMS:   float64(elapsed.Microseconds()) / 1000.0,
					Error:       errText,
				}

				current := atomic.AddInt64(&completed, 1)
				if current%100 == 0 || current == int64(count) {
					log.Printf("operation=%s completed=%d/%d", operation, current, count)
				}
			}
		}()
	}

	for index := 1; index <= count; index++ {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	return results
}

func writeRawCSV(path string, results []Result) error {
	if err := ensureParent(path); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"timestamp", "cluster_size", "operation", "index", "success", "latency_ms", "error"}); err != nil {
		return err
	}

	for _, r := range results {
		if err := writer.Write([]string{
			r.Timestamp,
			strconv.Itoa(r.ClusterSize),
			r.Operation,
			strconv.Itoa(r.Index),
			strconv.FormatBool(r.Success),
			fmt.Sprintf("%.3f", r.LatencyMS),
			r.Error,
		}); err != nil {
			return err
		}
	}

	return writer.Error()
}

func writeSummaryCSV(path string, summaries []Summary) error {
	if err := ensureParent(path); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"cluster_size", "operation", "count", "success_rate",
		"avg_latency_ms", "p50_latency_ms", "p95_latency_ms", "p99_latency_ms",
		"min_latency_ms", "max_latency_ms",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	for _, s := range summaries {
		if err := writer.Write([]string{
			strconv.Itoa(s.ClusterSize),
			s.Operation,
			strconv.Itoa(s.Count),
			fmt.Sprintf("%.6f", s.SuccessRate),
			fmt.Sprintf("%.3f", s.Avg),
			fmt.Sprintf("%.3f", s.P50),
			fmt.Sprintf("%.3f", s.P95),
			fmt.Sprintf("%.3f", s.P99),
			fmt.Sprintf("%.3f", s.Min),
			fmt.Sprintf("%.3f", s.Max),
		}); err != nil {
			return err
		}
	}

	return writer.Error()
}

func buildSummary(clusterSize int, operation string, results []Result) Summary {
	latencies := make([]float64, 0, len(results))
	successes := 0

	for _, r := range results {
		if r.Success {
			successes++
			latencies = append(latencies, r.LatencyMS)
		}
	}

	if len(results) == 0 || len(latencies) == 0 {
		return Summary{ClusterSize: clusterSize, Operation: operation, Count: len(results)}
	}

	sort.Float64s(latencies)

	var sum float64
	for _, latency := range latencies {
		sum += latency
	}

	return Summary{
		ClusterSize: clusterSize,
		Operation:   operation,
		Count:       len(results),
		SuccessRate: float64(successes) / float64(len(results)),
		Avg:         sum / float64(len(latencies)),
		P50:         percentile(latencies, 50),
		P95:         percentile(latencies, 95),
		P99:         percentile(latencies, 99),
		Min:         latencies[0],
		Max:         latencies[len(latencies)-1],
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	rank := (p / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sorted[lower]
	}

	weight := rank - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func ensureParent(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
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
