#!/usr/bin/env python3
"""
Genera grafici dai test di failover.

Input:
  reports/raw/failover_trials.csv

Output:
  reports/processed/failover_summary.csv
  reports/figures/failover_downtime_histogram.png
  reports/figures/failover_downtime_cdf.png
  reports/figures/failover_failed_puts.png
"""

from __future__ import annotations

from pathlib import Path
import sys

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "reports" / "raw" / "failover_trials.csv"
PROCESSED = ROOT / "reports" / "processed"
FIGURES = ROOT / "reports" / "figures"


def main() -> int:
    try:
        if not RAW.exists():
            raise FileNotFoundError(f"File non trovato: {RAW}")

        df = pd.read_csv(RAW)
        for column in ["downtime_ms", "new_leader_time_ms", "first_successful_put_ms", "failed_puts"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        valid = df[df["downtime_ms"] >= 0].copy()
        if valid.empty:
            raise ValueError("Nessun trial valido con downtime_ms >= 0")

        PROCESSED.mkdir(parents=True, exist_ok=True)
        FIGURES.mkdir(parents=True, exist_ok=True)

        summary = valid[["downtime_ms", "new_leader_time_ms", "first_successful_put_ms", "failed_puts"]].describe(
            percentiles=[0.5, 0.95, 0.99]
        )
        summary.to_csv(PROCESSED / "failover_summary.csv")

        plot_histogram(valid)
        plot_cdf(valid)
        plot_failed_puts(valid)

        print(f"Summary creato: {PROCESSED / 'failover_summary.csv'}")
        print(f"Grafici creati in: {FIGURES}")
        return 0
    except Exception as exc:
        print(f"Errore: {exc}", file=sys.stderr)
        return 1


def plot_histogram(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.hist(df["downtime_ms"], bins=min(10, max(3, len(df))), edgecolor="black")
    ax.set_xlabel("Downtime percepito dal client (ms)")
    ax.set_ylabel("Frequenza")
    ax.set_title("Distribuzione dei tempi di failover")
    ax.grid(True, alpha=0.35)
    fig.tight_layout()
    fig.savefig(FIGURES / "failover_downtime_histogram.png", dpi=180)
    plt.close(fig)


def plot_cdf(df: pd.DataFrame) -> None:
    values = sorted(df["downtime_ms"].tolist())
    y = [(i + 1) / len(values) for i in range(len(values))]

    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.plot(values, y, marker="o", linewidth=2)
    ax.set_xlabel("Downtime percepito dal client (ms)")
    ax.set_ylabel("CDF")
    ax.set_title("CDF dei tempi di failover")
    ax.grid(True, alpha=0.35)
    fig.tight_layout()
    fig.savefig(FIGURES / "failover_downtime_cdf.png", dpi=180)
    plt.close(fig)


def plot_failed_puts(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    ax.bar(df["trial"].astype(str), df["failed_puts"])
    ax.set_xlabel("Trial")
    ax.set_ylabel("Put fallite durante failover")
    ax.set_title("Put fallite per trial di failover")
    ax.grid(True, axis="y", alpha=0.35)
    fig.tight_layout()
    fig.savefig(FIGURES / "failover_failed_puts.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    raise SystemExit(main())
