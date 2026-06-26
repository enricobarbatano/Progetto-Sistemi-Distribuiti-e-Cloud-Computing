# Backup, snapshot e compaction test

Questo test valida il comportamento del Backup Service e raccoglie metriche su:

```text
- tempo di TriggerBackup;
- numero di snapshot scaricati;
- presenza dei file in /backup-data;
- dimensione dei file persistenti dei nodi prima/dopo backup e compaction.
```

## 1. Avvio cluster su EC2

Esempio con 5 nodi:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml up -d --build
curl http://localhost:8081/health
```

## 2. Misura storage prima del backup

Eseguire su EC2:

```bash
CLUSTER_SIZE=5 LABEL=before-backup ./scripts/measure_node_storage.sh
```

## 3. Benchmark backup dal PC locale

Dal PC locale:

```cmd
set KV_TARGET=<EC2_PUBLIC_IP>:8080
set BACKUP_TARGET=<EC2_PUBLIC_IP>:9090
set DATASET_SIZES=100,300,600
set CSV_OUT=reports/raw/backup_compaction_results.csv
set KEY_PREFIX=ec2-backup
set FORCE_SNAPSHOT=true
set COMPACT_AFTER_DOWNLOAD=true
go run .\cmd\backup-benchmark-client
```

## 4. Misura storage dopo backup/compaction

Eseguire su EC2:

```bash
CLUSTER_SIZE=5 LABEL=after-backup ./scripts/measure_node_storage.sh
```

## 5. Verifica file snapshot nel Backup Service

Eseguire su EC2:

```bash
docker exec -it sdcc-backup-service sh
ls -l /backup-data
cat /backup-data/*.json
exit
```

## 6. Grafici

Dal PC locale:

```cmd
python scripts\plot_backup_results.py
```

Output:

```text
reports/processed/backup_compaction_summary.csv
reports/figures/backup_duration_vs_dataset_size.png
reports/figures/downloaded_snapshots_vs_dataset_size.png
```
