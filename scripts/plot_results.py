#!/usr/bin/env python3
"""
Genera grafici e CSV aggregato dai benchmark di scalabilità.

Input richiesti:
  reports/processed/scalability_3nodes_summary.csv
  reports/processed/scalability_5nodes_summary.csv
  reports/processed/scalability_7nodes_summary.csv

Output prodotti:
  reports/processed/scalability_summary.csv
  reports/figures/latency_avg_vs_cluster_size.png
  reports/figures/latency_p95_vs_cluster_size.png
  reports/figures/latency_p99_vs_cluster_size.png
  reports/figures/success_rate_vs_cluster_size.png

Uso:
  python scripts/plot_results.py
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "reports" / "processed"
FIGURES_DIR = ROOT / "reports" / "figures"

INPUT_FILES = [
    PROCESSED_DIR / "scalability_3nodes_summary.csv",
    PROCESSED_DIR / "scalability_5nodes_summary.csv",
    PROCESSED_DIR / "scalability_7nodes_summary.csv",
]

COMBINED_OUT = PROCESSED_DIR / "scalability_summary.csv"

REQUIRED_COLUMNS = {
    "cluster_size",
    "operation",
    "count",
    "success_rate",
    "avg_latency_ms",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "min_latency_ms",
    "max_latency_ms",
}


def load_scalability_data() -> pd.DataFrame:
    missing_files = [str(path) for path in INPUT_FILES if not path.exists()]
    if missing_files:
        raise FileNotFoundError(
            "Mancano questi file summary:\n" + "\n".join(missing_files)
        )

    frames = []
    for path in INPUT_FILES:
        frame = pd.read_csv(path)
        missing_columns = REQUIRED_COLUMNS.difference(frame.columns)
        if missing_columns:
            raise ValueError(
                f"Nel file {path} mancano colonne: {sorted(missing_columns)}"
            )
        frames.append(frame)

    df = pd.concat(frames, ignore_index=True)

    # Normalizza i tipi numerici, così matplotlib non riceve stringhe.
    numeric_columns = [
        "cluster_size",
        "count",
        "success_rate",
        "avg_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="raise")

    df["operation"] = df["operation"].astype(str).str.lower()
    df = df.sort_values(["operation", "cluster_size"]).reset_index(drop=True)
    return df


def plot_latency_metric(
    df: pd.DataFrame,
    metric: str,
    ylabel: str,
    title: str,
    output_name: str,
) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(7.2, 4.4))

    for operation in ["put", "get"]:
        subset = df[df["operation"] == operation].sort_values("cluster_size")
        if subset.empty:
            continue
        ax.plot(
            subset["cluster_size"],
            subset[metric],
            marker="o",
            linewidth=2,
            label=operation.upper(),
        )

        # Etichette leggere sui punti, utili nella relazione.
        for _, row in subset.iterrows():
            ax.annotate(
                f"{row[metric]:.2f}",
                (row["cluster_size"], row[metric]),
                textcoords="offset points",
                xytext=(0, 7),
                ha="center",
                fontsize=8,
            )

    ax.set_xlabel("Numero di nodi del cluster")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(sorted(df["cluster_size"].unique()))
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / output_name, dpi=180)
    plt.close(fig)


def plot_success_rate(df: pd.DataFrame) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(7.2, 4.4))

    for operation in ["put", "get"]:
        subset = df[df["operation"] == operation].sort_values("cluster_size")
        if subset.empty:
            continue
        ax.plot(
            subset["cluster_size"],
            subset["success_rate"] * 100.0,
            marker="o",
            linewidth=2,
            label=operation.upper(),
        )

    ax.set_xlabel("Numero di nodi del cluster")
    ax.set_ylabel("Success rate (%)")
    ax.set_title("Success rate vs numero di nodi")
    ax.set_xticks(sorted(df["cluster_size"].unique()))
    ax.set_ylim(0, 105)
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "success_rate_vs_cluster_size.png", dpi=180)
    plt.close(fig)


def main() -> int:
    try:
        df = load_scalability_data()

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        FIGURES_DIR.mkdir(parents=True, exist_ok=True)

        df.to_csv(COMBINED_OUT, index=False)

        plot_latency_metric(
            df,
            metric="avg_latency_ms",
            ylabel="Latenza media (ms)",
            title="Latenza media vs numero di nodi",
            output_name="latency_avg_vs_cluster_size.png",
        )
        plot_latency_metric(
            df,
            metric="p95_latency_ms",
            ylabel="Latenza P95 (ms)",
            title="Latenza P95 vs numero di nodi",
            output_name="latency_p95_vs_cluster_size.png",
        )
        plot_latency_metric(
            df,
            metric="p99_latency_ms",
            ylabel="Latenza P99 (ms)",
            title="Latenza P99 vs numero di nodi",
            output_name="latency_p99_vs_cluster_size.png",
        )
        plot_success_rate(df)

        print(f"CSV aggregato creato: {COMBINED_OUT}")
        print(f"Grafici creati in: {FIGURES_DIR}")
        for path in sorted(FIGURES_DIR.glob("*.png")):
            print(f"- {path}")
        return 0
    except Exception as exc:
        print(f"Errore: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
