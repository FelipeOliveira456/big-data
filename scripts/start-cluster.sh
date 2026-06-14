#!/usr/bin/env bash
# Print docker run commands for a 4-VM Oracle cluster (copy/paste per VM).
set -euo pipefail

: "${HEAD_IP:?Set HEAD_IP to the private IP of VM1 (head node)}"
: "${IMAGE:=distributed-louvain:latest}"

cat <<EOF
=== VM1 (head) — run on VM1 ===

docker run -d --name ray-head --restart unless-stopped \\
  -p 6379:6379 -p 10001:10001 -p 8265:8265 \\
  -e CONTAINER_ROLE=ray-head \\
  $IMAGE

docker run -d --name dask-scheduler --restart unless-stopped \\
  -p 8786:8786 -p 8787:8787 \\
  -e CONTAINER_ROLE=dask-scheduler \\
  $IMAGE

=== VM2–VM4 (workers) — run on each worker VM ===

docker run -d --name ray-worker --restart unless-stopped \\
  -e CONTAINER_ROLE=ray-worker \\
  -e RAY_HEAD_ADDRESS=${HEAD_IP}:6379 \\
  $IMAGE

docker run -d --name dask-worker --restart unless-stopped \\
  -e CONTAINER_ROLE=dask-worker \\
  -e DASK_SCHEDULER_ADDRESS=${HEAD_IP}:8786 \\
  $IMAGE

=== VM1 — pipeline (after workers register, ~30s) ===

docker run --rm \\
  -e CONTAINER_ROLE=pipeline \\
  -e RAY_HEAD_ADDRESS=${HEAD_IP} \\
  -e DASK_SCHEDULER_ADDRESS=${HEAD_IP}:8786 \\
  -v "\$(pwd)/data:/app/data" \\
  -v "\$(pwd)/reports:/app/reports" \\
  -v "\$(pwd)/config.yaml.example:/app/config.yaml:ro" \\
  $IMAGE

Open Oracle Security List for subnet CIDR on ports 6379, 10001, 8786, 8787.
EOF
