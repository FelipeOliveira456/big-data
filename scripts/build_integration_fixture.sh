#!/usr/bin/env bash
# Build tests/integration/fixtures/pokec_0p1pct.npz from raw SNAP (one-time / when raw changes).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

RAW="${GRAPH_RAW_PATH:-data/raw/soc-pokec-relationships.txt}"
OUT="tests/integration/fixtures/pokec_0p1pct"
FRACTION="${INTEGRATION_FRACTION:-0.1}"
SEED="${INTEGRATION_SEED:-42}"

if [[ ! -f "$RAW" ]]; then
  echo "Missing $RAW — run: bash scripts/download_dataset.sh" >&2
  exit 1
fi

python - <<PY
from pathlib import Path
from preprocessing.graph_artifact import save_graph_artifact
from preprocessing.load_graph import load_graph_from_snap

raw = Path("${RAW}")
out = Path("${OUT}")
fraction = float("${FRACTION}")
seed = int("${SEED}")

print(f"Building {out}.npz from {raw} ({fraction}% seed={seed}) ...")
loaded = load_graph_from_snap(raw, fraction_pct=fraction, seed=seed)
meta = {
    "dataset_slug": "pokec",
    "fraction_pct": fraction,
    "seed": seed,
    "node_count": loaded.node_count,
    "edge_count": loaded.edge_count,
    "source": str(raw),
    "load_time_s_build": loaded.load_time_s,
}
npz, meta_path = save_graph_artifact(loaded.graph, out, meta=meta)
print(f"Wrote {npz} ({npz.stat().st_size / 1024:.0f} KB)")
print(f"Wrote {meta_path}")
print(f"  nodes={loaded.node_count:,} edges={loaded.edge_count:,}")
PY
