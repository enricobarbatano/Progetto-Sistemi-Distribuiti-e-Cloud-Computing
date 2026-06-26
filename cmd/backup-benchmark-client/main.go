package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	backuppb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/backuppb"
	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	KVTarget             string
	BackupTarget         string
	DatasetSizes         []int
	KeyPrefix            string
	CSVOut               string
	TimeoutMS            int
	ForceSnapshot        bool
	CompactAfterDownload bool
}

type Result struct {
	TimestampUTC        string
	DatasetSize         int
	PutSuccesses        int
	PutFailures         int
	PutDurationMS       int64
	BackupAccepted      bool
	BackupID            string
	DownloadedSnapshots uint64
	BackupDurationMS    int64
	Error               string
}

func main() {
	cfg := loadConfig()
	log.Printf("backup-benchmark kv_target=%s backup_target=%s dataset_sizes=%v csv_out=%s force_snapshot=%t compact_after_download=%t",
		cfg.KVTarget, cfg.BackupTarget, cfg.DatasetSizes, cfg.CSVOut, cfg.ForceSnapshot, cfg.CompactAfterDownload)

	kvConn, err := grpc.NewClient(cfg.KVTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create kv grpc client: %v", err)
	}
	defer kvConn.Close()
	kvClient := kvpb.NewKeyValueServiceClient(kvConn)

	backupConn, err := grpc.NewClient(cfg.BackupTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create backup grpc client: %v", err)
	}
	defer backupConn.Close()
	backupClient := backuppb.NewBackupServiceClient(backupConn)

	if err := ensureCSV(cfg.CSVOut); err != nil {
		log.Fatalf("failed to prepare csv: %v", err)
	}

	for _, datasetSize := range cfg.DatasetSizes {
		result := runCase(cfg, kvClient, backupClient, datasetSize)
		if err := appendResult(cfg.CSVOut, result); err != nil {
			log.Fatalf("failed to append result: %v", err)
		}
		log.Printf("dataset_size=%d put_successes=%d put_failures=%d put_ms=%d backup_accepted=%t downloaded_snapshots=%d backup_ms=%d backup_id=%s error=%s",
			result.DatasetSize,
			result.PutSuccesses,
			result.PutFailures,
			result.PutDurationMS,
			result.BackupAccepted,
			result.DownloadedSnapshots,
			result.BackupDurationMS,
			result.BackupID,
			result.Error,
		)
	}
}

func runCase(cfg Config, kvClient kvpb.KeyValueServiceClient, backupClient backuppb.BackupServiceClient, datasetSize int) Result {
	result := Result{
		TimestampUTC: time.Now().UTC().Format(time.RFC3339Nano),
		DatasetSize:  datasetSize,
	}

	putStart := time.Now()
	for i := 1; i <= datasetSize; i++ {
		key := fmt.Sprintf("%s-size-%d-key-%06d", cfg.KeyPrefix, datasetSize, i)
		value := fmt.Sprintf("%s-size-%d-value-%06d", cfg.KeyPrefix, datasetSize, i)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
		resp, err := kvClient.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
		cancel()

		if err != nil || !resp.Success {
			result.PutFailures++
			continue
		}
		result.PutSuccesses++
	}
	result.PutDurationMS = time.Since(putStart).Milliseconds()

	backupStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
	resp, err := backupClient.TriggerBackup(ctx, &backuppb.TriggerBackupRequest{
		ForceSnapshot:        cfg.ForceSnapshot,
		CompactAfterDownload: cfg.CompactAfterDownload,
	})
	cancel()
	result.BackupDurationMS = time.Since(backupStart).Milliseconds()

	if err != nil {
		result.Error = err.Error()
		return result
	}

	result.BackupAccepted = resp.Accepted
	result.BackupID = resp.BackupId
	result.DownloadedSnapshots = resp.DownloadedSnapshots
	result.Error = resp.Error
	return result
}

func loadConfig() Config {
	return Config{
		KVTarget:             getEnv("KV_TARGET", getEnv("TARGET", "localhost:8080")),
		BackupTarget:         getEnv("BACKUP_TARGET", "localhost:9090"),
		DatasetSizes:         parseSizes(getEnv("DATASET_SIZES", "100,300,600")),
		KeyPrefix:            getEnv("KEY_PREFIX", "backup-bench"),
		CSVOut:               getEnv("CSV_OUT", "reports/raw/backup_compaction_results.csv"),
		TimeoutMS:            getEnvInt("TIMEOUT_MS", 15000),
		ForceSnapshot:        getEnvBool("FORCE_SNAPSHOT", true),
		CompactAfterDownload: getEnvBool("COMPACT_AFTER_DOWNLOAD", true),
	}
}

func parseSizes(raw string) []int {
	parts := strings.Split(raw, ",")
	sizes := make([]int, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		value, err := strconv.Atoi(trimmed)
		if err != nil || value <= 0 {
			continue
		}
		sizes = append(sizes, value)
	}
	if len(sizes) == 0 {
		return []int{100, 300, 600}
	}
	return sizes
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
		"dataset_size",
		"put_successes",
		"put_failures",
		"put_duration_ms",
		"backup_accepted",
		"backup_id",
		"downloaded_snapshots",
		"backup_duration_ms",
		"error",
	})
}

func appendResult(path string, result Result) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	return writer.Write([]string{
		result.TimestampUTC,
		strconv.Itoa(result.DatasetSize),
		strconv.Itoa(result.PutSuccesses),
		strconv.Itoa(result.PutFailures),
		strconv.FormatInt(result.PutDurationMS, 10),
		strconv.FormatBool(result.BackupAccepted),
		result.BackupID,
		strconv.FormatUint(result.DownloadedSnapshots, 10),
		strconv.FormatInt(result.BackupDurationMS, 10),
		result.Error,
	})
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

func getEnvBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "true" || value == "1" || value == "yes"
}
