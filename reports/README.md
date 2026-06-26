# Reports

Questa cartella contiene i risultati dei benchmark e i grafici usati nella relazione finale.

## Struttura

```text
reports/
  raw/        # CSV grezzi, una riga per operazione misurata
  processed/  # CSV aggregati con medie, percentili e success rate
  figures/    # grafici finali PNG
```

## Scalabilità

Il client `cmd/perf-client` produce un CSV raw e un CSV summary.

Esempio 3 nodi:

```bash
TARGET=localhost:8080 \
CLUSTER_SIZE=3 \
WARMUP_PUTS=20 \
WARMUP_GETS=20 \
PUTS=300 \
GETS=300 \
CONCURRENCY=1 \
KEY_PREFIX=scalability-3 \
CSV_OUT=reports/raw/scalability_3nodes.csv \
SUMMARY_OUT=reports/processed/scalability_3nodes_summary.csv \
go run ./cmd/perf-client
```

Su Windows CMD:

```cmd
set TARGET=localhost:8080
set CLUSTER_SIZE=3
set WARMUP_PUTS=20
set WARMUP_GETS=20
set PUTS=300
set GETS=300
set CONCURRENCY=1
set KEY_PREFIX=scalability-3
set CSV_OUT=reports/raw/scalability_3nodes.csv
set SUMMARY_OUT=reports/processed/scalability_3nodes_summary.csv
go run .\cmd\perf-client
```

## Campi CSV raw

```csv
timestamp,cluster_size,operation,index,success,latency_ms,error
```

Il CSV raw contiene solo le operazioni misurate. Le operazioni di warm-up non vengono salvate.

## Campi CSV summary

```csv
cluster_size,operation,count,success_rate,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,min_latency_ms,max_latency_ms
```

## Nota

I CSV raw possono diventare grandi. Per la relazione finale sono più importanti:

```text
reports/processed/*.csv
reports/figures/*.png
```
