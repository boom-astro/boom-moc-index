#!/usr/bin/env bash
# Apples-to-apples scaling comparison:
#   - SkyPortal-style: per-alert iteration over all in-process MOCs (mocpy)
#   - boom-moc-index: Valkey set lookup (candidates-only)
#
# Sweeps N_MOCS = 1, 3, 10, 30, 100, 300 and writes JSON results to
# comparison/results/ for the plotter.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p comparison/results

PYTHON=/Users/mcoughlin/miniforge3/envs/boom/bin/python
N_QUERIES=2000

for N in 1 3 10 30 100 300; do
    echo "=== N_MOCS=$N: SkyPortal baseline ==="
    "$PYTHON" comparison/skyportal_baseline.py \
        --n-mocs "$N" --n-queries "$N_QUERIES" \
        > "comparison/results/skyportal_${N}.json"
    tail -8 "comparison/results/skyportal_${N}.json"
    echo

    echo "=== N_MOCS=$N: boom-moc-index ==="
    cargo run --release --quiet --bin benchmark -- \
        --n-mocs "$N" --n-queries "$N_QUERIES" --candidates-only --json \
        > "comparison/results/boom_${N}.json"
    tail -8 "comparison/results/boom_${N}.json"
    echo
done

echo "Done. JSON in comparison/results/{skyportal,boom}_*.json"
