// Package backup contiene la logica interna del Backup Service.
//
// Questo file contiene la configurazione del servizio. I parametri vengono
// letti da variabili d'ambiente, così il Backup Service può essere avviato sia
// localmente sia dentro Docker Compose senza indirizzi hardcoded.
package backup

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultServiceID       = "backup-service"
	defaultBackupPort      = "9090"
	defaultBackupDir       = "backup-data"
	defaultRPCTimeoutMS    = 1000
	defaultMaxRetries      = 3
	defaultBackoffMS       = 200
	defaultBackupInterval  = 0
	defaultCompactAfterRun = true
)

type Config struct {
	ServiceID            string
	Port                 string
	ConsensusNodes       []string
	BackupDir            string
	RPCTimeout           time.Duration
	MaxRetries           int
	Backoff              time.Duration
	BackupInterval       time.Duration
	CompactAfterDownload bool
}

func LoadConfigFromEnv() (Config, error) {
	nodes := readStringListEnv("CONSENSUS_NODES")
	if len(nodes) == 0 {
		return Config{}, errors.New("CONSENSUS_NODES cannot be empty")
	}

	return Config{
		ServiceID:            readStringEnv("BACKUP_SERVICE_ID", defaultServiceID),
		Port:                 readStringEnv("BACKUP_PORT", defaultBackupPort),
		ConsensusNodes:       nodes,
		BackupDir:            readStringEnv("BACKUP_DIR", defaultBackupDir),
		RPCTimeout:           time.Duration(readIntEnv("RPC_TIMEOUT_MS", defaultRPCTimeoutMS)) * time.Millisecond,
		MaxRetries:           readIntEnv("MAX_RETRIES", defaultMaxRetries),
		Backoff:              time.Duration(readIntEnv("BACKOFF_MS", defaultBackoffMS)) * time.Millisecond,
		BackupInterval:       time.Duration(readIntEnv("BACKUP_INTERVAL_MS", defaultBackupInterval)) * time.Millisecond,
		CompactAfterDownload: readBoolEnv("COMPACT_AFTER_DOWNLOAD", defaultCompactAfterRun),
	}, nil
}

func readStringEnv(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

func readIntEnv(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return defaultValue
	}

	return parsed
}

func readBoolEnv(key string, defaultValue bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return defaultValue
	}

	switch value {
	case "true", "1", "yes", "y":
		return true
	case "false", "0", "no", "n":
		return false
	default:
		return defaultValue
	}
}

func readStringListEnv(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}

	rawParts := strings.Split(value, ",")
	result := make([]string, 0, len(rawParts))

	for _, part := range rawParts {
		cleanPart := strings.TrimSpace(part)
		if cleanPart != "" {
			result = append(result, cleanPart)
		}
	}

	return result
}
