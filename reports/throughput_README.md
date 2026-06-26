# Throughput test

Questo test calcola il throughput usando i CSV raw già prodotti dal benchmark di scalabilità.

## Input

```text
reports/raw/scalability_3nodes.csv
reports/raw/scalability_5nodes.csv
reports/raw/scalability_7nodes.csv
```

Ogni CSV contiene una riga per operazione, con timestamp, tipo di operazione e latenza.

## Comando

```cmd
python scripts\plot_throughput.py
```

## Output

```text
reports/processed/throughput_summary.csv
reports/figures/throughput_vs_cluster_size.png
```

## Metrica

Il throughput viene calcolato come:

```text
throughput_ops_sec = operazioni riuscite / durata della finestra di test in secondi
```

La durata della finestra viene calcolata come differenza tra timestamp massimo e minimo per ogni coppia:

```text
cluster_size + operation
```
