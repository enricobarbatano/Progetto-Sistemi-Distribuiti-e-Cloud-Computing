# Failover test

Questo test misura il downtime percepito dal client durante il crash del leader.

## Configurazione consigliata

Avviare su EC2 il cluster da 5 nodi:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml up -d --build
curl http://localhost:8081/health
```

Dal PC locale, lanciare:

```cmd
set TARGET=<EC2_PUBLIC_IP>:8080
set CLUSTER_SIZE=5
set TRIALS=10
set CSV_OUT=reports/raw/failover_trials.csv
set KEY_PREFIX=ec2-failover
go run .\cmd\failover-client
```

Per ogni trial il client stampa il leader corrente e il comando da eseguire su EC2:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml stop node-X
```

Dopo lo stop, premere INVIO nel terminale del client locale. Il client misura:

```text
- tempo fino al nuovo leader;
- tempo fino alla prima Put riuscita;
- downtime percepito;
- numero di Put fallite;
- numero di Put riuscite durante la finestra di osservazione.
```

Dopo ogni trial, riavviare il nodo fermato:

```bash
docker compose -f deployments/docker/docker-compose-5nodes.yml start node-X
```

## Grafici

```cmd
python scripts\plot_failover.py
```

Output:

```text
reports/processed/failover_summary.csv
reports/figures/failover_downtime_histogram.png
reports/figures/failover_downtime_cdf.png
reports/figures/failover_failed_puts.png
```
