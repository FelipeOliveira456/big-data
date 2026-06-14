#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate

: "${GRAPH_RAW_PATH:=data/raw/email-Enron.txt}"

bash scripts/download_dataset.sh
python -m cli.main preprocess --input "$GRAPH_RAW_PATH" --fractions 100
python -m cli.main benchmark --fractions 100 --runs 3
python -m cli.main report
