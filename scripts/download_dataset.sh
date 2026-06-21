#!/usr/bin/env bash
# Download SNAP soc-Pokec directed graph into data/raw/
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="data/raw"
OUT="$RAW_DIR/soc-pokec-relationships.txt"
URL="https://snap.stanford.edu/data/soc-Pokec/soc-pokec-relationships.txt.gz"

mkdir -p "$RAW_DIR"
if [[ -f "$OUT" ]]; then
  echo "Already present: $OUT"
  exit 0
fi

echo "Downloading $URL (~250 MB compressed) ..."
curl -fsSL "$URL" -o "$RAW_DIR/soc-pokec-relationships.txt.gz"
gunzip -f "$RAW_DIR/soc-pokec-relationships.txt.gz"
echo "Wrote $OUT"
