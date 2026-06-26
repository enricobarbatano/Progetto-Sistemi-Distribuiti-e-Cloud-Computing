#!/usr/bin/env python3
"""
Calcola e plotta il throughput dai CSV raw di scalabilità.

Input richiesti:
  reports/raw/scalability_3nodes.csv
  reports/raw/scalability_5nodes.csv
  reports/raw/scalability_7nodes.csv

Output prodotti:
  reports/processed/throughput_summary.csv
  reports/figures/throughput_vs_cluster_size.png

Uso:
  python scripts/plot_throughput.py
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "reports" / "raw"
PROCESSED_DIR = ROOT / "reports" / "processed"
FIGURES_DIR = ROOT / "reports" / "figures"

INPUT_FILES = [
    RAW_DIR / "scalability_3nodes.csv",
    RAW_DIR / "scalability_5nodes.csv",
    RAW_DIR / "scalability_7nodes.csv",
]

OUTPUT_CSV = PROCESSED_DIR / "throughput_summary.csv"
OUTPUT_FIG = FIGURES_DIR / "throughput_vs_cluster_size.png"

REQUIRED_COLUMNS = {
    "timestamp",
    "cluster_size",
    "operation",
    "index",
    "success",
    "latency_ms",
    "error",
}


def load_raw_data() -> pd.DataFrame:
    missing = [str(path) for path in INPUT_FILES if not path.exists()]
    if missing:
        raise FileNotFoundError("File raw mancanti:\n" + "\n".join(missing))

    frames = []
    for path in INPUT_FILES:
        frame = pd.read_csv(path)
        missing_columns = REQUIRED_COLUMNS.difference(frame.columns)
        if missing_columns:
            raise ValueError(f"Nel file {path} mancano colonne: {sorted(missing_columns)}")
        frames.append(frame)

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="raise")
    df["cluster_size"] = pd.to_numeric(df["cluster_size"], errors="raise")
    df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="raise")
    df["operation"] = df["operation"].astype(str).str.lower()
    df["success"] = df["success"].astype(str).str.lower().isin(["true", "1", "yes"])
    return df


def build_throughput_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    grouped = df.groupby(["cluster_size", "operation"], sort=True)
    for (cluster_size, operation), group in grouped:
        group = group.sort_values("timestamp")
        successful = group[group["success"]]

        start = group["timestamp"].min()
        end = group["timestamp"].max()
        duration_seconds = (end - start).total_seconds()

        # Se le operazioni sono molto ravvicinate, evitiamo divisione per zero.
        if duration_seconds <= 0:
            duration_seconds = group["latency_ms"].sum() / 1000.0

        total_count = len(group)
        success_count = len(successful)
        success_rate = success_count / total_count if total_count else 0.0
        throughput_ops_sec = success_count / duration_seconds if duration_seconds > 0 else 0.0

        rows.append(
            {
                "cluster_size": int(cluster_size),
                "operation": operation,
                "count": total_count,
                "success_count": success_count,
                "success_rate": success_rate,
                "duration_seconds": duration_seconds,
                "throughput_ops_sec": throughput_ops_sec,
            }
        )

    summary = pd.DataFrame(rows)
    summary = summary.sort_values(["operation", "cluster_size"]).reset_index(drop=True)
    return summary


def plot_throughput(summary: pd.DataFrame) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(7.2, 4.4))

    for operation in ["put", "get"]:
        subset = summary[summary["operation"] == operation].sort_values("cluster_size")
        if subset.empty:
            continue

        ax.plot(
            subset["cluster_size"],
            subset["throughput_ops_sec"],
            marker="o",
            linewidth=2,
            label=operation.upper(),
        )

        for _, row in subset.iterrows():
            ax.annotate(
                f"{row['throughput_ops_sec']:.2f}",
                (row["cluster_size"], row["throughput_ops_sec"]),
                textcoords="offset points",
                xytext=(0, 7),
                ha="center",
                fontsize=8,
            )

    ax.set_xlabel("Numero di nodi del cluster")
    ax.set_ylabel("Throughput (operazioni/s)")
    ax.set_title("Throughput vs numero di nodi")
    ax.set_xticks(sorted(summary["cluster_size"].unique()))
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUTPUT_FIG, dpi=180)
    plt.close(fig)


def main() -> int:
    try:
        df = load_raw_data()
        summary = build_throughput_summary(df)

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        FIGURES_DIR.mkdir(parents=True, exist_ok=True)

        summary.to_csv(OUTPUT_CSV, index=False)
        plot_throughput(summary)

        print(f"CSV throughput creato: {OUTPUT_CSV}")
        print(f"Grafico throughput creato: {OUTPUT_FIG}")
        print(summary.to_string(index=False))
        return 0
    except Exception as exc:
        print(f"Errore: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
