"""
SkyPortal-style baseline: mimics the per-alert iteration approach in
`crossmatch-alert-to-skymaps/utils/skymap.py`.

Loads N multi-order skymaps from the ORIGIN observing-scenarios runs, builds
mocpy MOCs at a credible level, and for each random sky position calls
`moc.contains_lonlat(ra, dec)` on every MOC --- the same loop the SkyPortal
pipeline runs in production.

Outputs latency percentiles in JSON to stdout for direct comparison with the
Rust benchmark in this repo.

Usage:
    conda activate boom
    python comparison/skyportal_baseline.py --n-mocs 10 --n-queries 10000
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
from astropy import units as u
from mocpy import MOC


def load_mocs(skymap_dir: Path, n_mocs: int, credible_level: float) -> list[MOC]:
    """Build N MOCs from ORIGIN BAYESTAR multi-order skymaps."""
    mocs: list[MOC] = []
    for i in range(n_mocs):
        path = skymap_dir / f"{i}.fits"
        # mocpy can read multi-order skymaps directly via from_skymap_fits at a CL
        moc = MOC.from_multiordermap_fits_file(str(path), cumul_to=credible_level)
        mocs.append(moc)
    return mocs


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n-mocs", type=int, default=10)
    p.add_argument("--n-queries", type=int, default=10_000)
    p.add_argument("--credible-level", type=float, default=0.9)
    p.add_argument(
        "--skymap-dir",
        type=Path,
        default=Path("/Users/mcoughlin/Code/ORIGIN/observing-scenarios/runs/O4HL/bgp/allsky"),
    )
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    rng = np.random.default_rng(args.seed)

    import sys

    print(
        f"Loading {args.n_mocs} skymaps at CL={args.credible_level}...",
        flush=True,
        file=sys.stderr,
    )
    t0 = time.perf_counter()
    mocs = load_mocs(args.skymap_dir, args.n_mocs, args.credible_level)
    pop_seconds = time.perf_counter() - t0
    print(
        f"  loaded in {pop_seconds:.2f}s ({1000 * pop_seconds / args.n_mocs:.0f} ms/MOC)",
        flush=True,
        file=sys.stderr,
    )

    # Uniform random positions on the sphere
    ra = rng.uniform(0.0, 360.0, size=args.n_queries) * u.deg
    sin_dec = rng.uniform(-1.0, 1.0, size=args.n_queries)
    dec = np.degrees(np.arcsin(sin_dec)) * u.deg

    latencies = np.empty(args.n_queries, dtype=np.float64)
    n_with_hits = 0

    print(f"Running {args.n_queries} per-alert lookups...", flush=True, file=sys.stderr)
    t_total = time.perf_counter()
    for i in range(args.n_queries):
        t = time.perf_counter()
        # SkyPortal pipeline calls contains_lonlat on every active MOC
        any_hit = False
        for moc in mocs:
            if moc.contains_lonlat(ra[i : i + 1], dec[i : i + 1])[0]:
                any_hit = True
                # SkyPortal keeps iterating to collect all matches; mirror that
        latencies[i] = time.perf_counter() - t
        if any_hit:
            n_with_hits += 1
    total_seconds = time.perf_counter() - t_total

    latencies.sort()

    def pct(p: float) -> float:
        idx = min(int(round(p / 100.0 * (len(latencies) - 1))), len(latencies) - 1)
        return float(latencies[idx])

    result = {
        "implementation": "skyportal_baseline_python_mocpy",
        "n_mocs": args.n_mocs,
        "n_queries": args.n_queries,
        "credible_level": args.credible_level,
        "load_time_seconds": pop_seconds,
        "wall_seconds": total_seconds,
        "throughput_per_second": args.n_queries / total_seconds,
        "p50_seconds": pct(50),
        "p90_seconds": pct(90),
        "p95_seconds": pct(95),
        "p99_seconds": pct(99),
        "p99_9_seconds": pct(99.9),
        "max_seconds": float(latencies[-1]),
        "n_hits": n_with_hits,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
