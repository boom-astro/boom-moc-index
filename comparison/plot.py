"""
Plot the SkyPortal-baseline vs boom-moc-index scaling comparison.

Reads comparison/results/{skyportal,boom}_*.json and writes
    comparison/results/scaling.png

Usage:
    conda activate boom
    python comparison/plot.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

RESULTS = Path(__file__).parent / "results"
N_MOCS_VALUES = [1, 3, 10, 30, 100, 300]


def load(impl: str, n: int) -> dict:
    return json.loads((RESULTS / f"{impl}_{n}.json").read_text())


def main() -> None:
    skyportal = [load("skyportal", n) for n in N_MOCS_VALUES]
    boom = [load("boom", n) for n in N_MOCS_VALUES]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # ---- Panel 1: per-alert latency vs N_MOCs ----
    sp_p50 = [r["p50_seconds"] * 1e6 for r in skyportal]
    sp_p99 = [r["p99_seconds"] * 1e6 for r in skyportal]
    bm_p50 = [r["p50_seconds"] * 1e6 for r in boom]
    bm_p99 = [r["p99_seconds"] * 1e6 for r in boom]

    ax1.plot(N_MOCS_VALUES, sp_p50, marker="o", color="C0",
             label="SkyPortal-style: in-memory loop (mocpy) — p50")
    ax1.plot(N_MOCS_VALUES, sp_p99, marker="o", color="C0",
             linestyle="--", alpha=0.5, label="SkyPortal-style — p99")
    ax1.plot(N_MOCS_VALUES, bm_p50, marker="s", color="C2",
             label="boom-moc-index: Valkey set lookup — p50")
    ax1.plot(N_MOCS_VALUES, bm_p99, marker="s", color="C2",
             linestyle="--", alpha=0.5, label="boom-moc-index — p99")
    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlabel("Number of active MOCs")
    ax1.set_ylabel("Per-alert latency (μs)")
    ax1.set_title("Lookup latency vs active-MOC population")
    ax1.grid(True, which="both", alpha=0.3)
    ax1.legend(fontsize=9, loc="upper left")

    # ---- Panel 2: throughput vs N_MOCs ----
    sp_thr = [r["throughput_per_second"] for r in skyportal]
    bm_thr = [r["throughput_per_second"] for r in boom]
    ax2.plot(N_MOCS_VALUES, sp_thr, marker="o", color="C0", label="SkyPortal-style")
    ax2.plot(N_MOCS_VALUES, bm_thr, marker="s", color="C2", label="boom-moc-index")
    ax2.set_xscale("log")
    ax2.set_yscale("log")
    ax2.set_xlabel("Number of active MOCs")
    ax2.set_ylabel("Throughput (alerts/s, single-thread)")
    ax2.set_title("Sustained throughput vs active-MOC population")
    ax2.grid(True, which="both", alpha=0.3)
    ax2.legend(fontsize=10)

    fig.suptitle(
        "Skymap-overlap lookup: linear-scan baseline vs HEALPix-indexed Valkey",
        fontsize=12, y=1.00,
    )
    fig.tight_layout()

    out = RESULTS / "scaling.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    print(f"wrote {out}")

    # Also write a small markdown summary
    rows = []
    for r_sp, r_bm in zip(skyportal, boom):
        n = r_sp["n_mocs"]
        speedup = r_sp["p50_seconds"] / r_bm["p50_seconds"]
        rows.append(
            f"| {n:>3} | {r_sp['p50_seconds']*1e6:>7.1f} µs | "
            f"{r_bm['p50_seconds']*1e6:>7.1f} µs | "
            f"{r_sp['throughput_per_second']:>7.0f} | "
            f"{r_bm['throughput_per_second']:>7.0f} | "
            f"{speedup:>5.1f}× |"
        )
    summary = (
        "| N_MOCs | SkyPortal-style p50 | boom-moc-index p50 | "
        "SkyPortal thr (a/s) | boom thr (a/s) | speedup |\n"
        "|---:|---:|---:|---:|---:|---:|\n" + "\n".join(rows) + "\n"
    )
    (RESULTS / "scaling.md").write_text(summary)
    print(f"wrote {RESULTS / 'scaling.md'}")
    print()
    print(summary)


if __name__ == "__main__":
    main()
