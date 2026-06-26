#!/usr/bin/env python3
"""
Genera grafici dai risultati del Backup Service.

Input:
  reports/raw/backup_compaction_results.csv

Output:
  reports/processed/backup_compaction_summary.csv
  reports/figures/backup_duration_vs_dataset_size.png
  reports/figures/downloaded_snapshots_vs_dataset_size.png
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "reports" / "raw" / "backup_compaction_results.csv"
PROCESSED = ROOT / "reports" / "processed"
FIGURES = ROOT / "reports" / "figures"


def main() -> int:
    try:
        if not RAW.exists():
            raise FileNotFoundError(f"File non trovato: {RAW}")

        df = pd.read_csv(RAW)
        for column in ["dataset_size", "put_successes", "put_failures", "put_duration_ms", "downloaded_snapshots", "backup_duration_ms"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df["backup_accepted"] = df["backup_accepted"].astype(str).str.lower().isin(["true", "1", "yes"])

        PROCESSED.mkdir(parents=True, exist_ok=True)
        FIGURES.mkdir(parents=True, exist_ok=True)

        summary = df.sort_values("dataset_size")
        summary.to_csv(PROCESSED / "backup_compaction_summary.csv", index=False)

        plot_backup_duration(summary)
        plot_downloaded_snapshots(summary)

        print(f"Summary creato: {PROCESSED / 'backup_compaction_summary.csv'}")
        print(f"Grafici creati in: {FIGURES}")
        return 0
    except Exception as exc:
        print(f"Errore: {exc}", file=sys.stderr)
        return 1


def plot_backup_duration(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(df["dataset_size"], df["backup_duration_ms"], marker="o", linewidth=2)
    for _, row in df.iterrows():
        ax.annotate(
            f"{row['backup_duration_ms']:.0f}",
            (row["dataset_size"], row["backup_duration_ms"]),
            textcoords="offset points",
            xytext=(0, 7),
            ha="center",
            fontsize=8,
        )
    ax.set_xlabel("Numero di chiavi inserite")
    ax.set_ylabel("Durata backup (ms)")
    ax.set_title("Durata TriggerBackup vs dimensione dataset")
    ax.grid(True, alpha=0.35)
    fig.tight_layout()
    fig.savefig(FIGURES / "backup_duration_vs_dataset_size.png", dpi=180)
    plt.close(fig)


def plot_downloaded_snapshots(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.bar(df["dataset_size"].astype(str), df["downloaded_snapshots"])
    ax.set_xlabel("Numero di chiavi inserite")
    ax.set_ylabel("Snapshot scaricati")
    ax.set_title("Snapshot scaricati vs dimensione dataset")
    ax.grid(True, axis="y", alpha=0.35)
    fig.tight_layout()
    fig.savefig(FIGURES / "downloaded_snapshots_vs_dataset_size.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    raise SystemExit(main())
