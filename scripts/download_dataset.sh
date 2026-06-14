#!/usr/bin/env bash
# Download SNAP email-Enron edge list into data/raw/
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="data/raw"
OUT="$RAW_DIR/email-Enron.txt"
URL="https://snap.stanford.edu/data/email-Enron.txt.gz"

mkdir -p "$RAW_DIR"
if [[ -f "$OUT" ]]; then
  echo "Already present: $OUT"
  exit 0
fi

echo "Downloading $URL ..."
curl -fsSL "$URL" -o "$RAW_DIR/email-Enron.txt.gz"
gunzip -f "$RAW_DIR/email-Enron.txt.gz"
echo "Wrote $OUT"
