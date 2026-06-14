#!/usr/bin/env bash
set -euo pipefail
cd /app

: "${CONTAINER_ROLE:=pipeline}"
: "${GRAPH_RAW_PATH:=data/raw/email-Enron.txt}"

run_pipeline() {
  if [[ ! -f "$GRAPH_RAW_PATH" ]]; then
    bash scripts/download_dataset.sh
  fi
  python -m cli.main preprocess --fractions 100
  python -m cli.main benchmark --fractions 100 --runs 3
  python -m cli.main report
}

case "$CONTAINER_ROLE" in
  pipeline)
    run_pipeline
    ;;
  ray-head)
    exec ray start --head --port=6379 --dashboard-host=0.0.0.0 --block
    ;;
  ray-worker)
    if [[ -z "${RAY_HEAD_ADDRESS:-}" ]]; then
      echo "RAY_HEAD_ADDRESS is required for ray-worker" >&2
      exit 1
    fi
    exec ray start --address="$RAY_HEAD_ADDRESS" --block
    ;;
  dask-scheduler)
    exec dask scheduler --host 0.0.0.0 --port 8786 --dashboard-address :8787
    ;;
  dask-worker)
    if [[ -z "${DASK_SCHEDULER_ADDRESS:-}" ]]; then
      echo "DASK_SCHEDULER_ADDRESS is required for dask-worker" >&2
      exit 1
    fi
    exec dask worker "tcp://${DASK_SCHEDULER_ADDRESS#tcp://}"
    ;;
  *)
    echo "Unknown CONTAINER_ROLE: $CONTAINER_ROLE" >&2
    exit 1
    ;;
esac
