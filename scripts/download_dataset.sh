#!/usr/bin/env bash
# Download SNAP soc-Orkut undirected graph into data/raw/
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="data/raw"
OUT="$RAW_DIR/soc-orkut-relationships.txt"
URL="https://snap.stanford.edu/data/soc-orkut-relationships.txt.gz"

mkdir -p "$RAW_DIR"
if [[ -f "$OUT" ]]; then
  echo "Already present: $OUT"
  exit 0
fi

echo "Downloading $URL ..."
curl -fsSL "$URL" -o "$RAW_DIR/soc-orkut-relationships.txt.gz"
gunzip -f "$RAW_DIR/soc-orkut-relationships.txt.gz"
echo "Wrote $OUT"
