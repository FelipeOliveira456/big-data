"""Write shared Parquet graph artifacts and metadata sidecar."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_graph_parquet(
    edges: list[tuple[int, int, float]],
    output_path: Path,
    meta: dict,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    src = [s for s, _, _ in edges]
    dst = [d for _, d, _ in edges]
    weight = [w for _, _, w in edges]
    table = pa.table({"src": src, "dst": dst, "weight": weight})
    pq.write_table(table, output_path)

    meta_path = output_path.with_suffix(".meta.json")
    meta.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def read_edges_parquet(path: Path) -> list[tuple[int, int, float]]:
    table = pq.read_table(path)
    src = table.column("src").to_pylist()
    dst = table.column("dst").to_pylist()
    weight = table.column("weight").to_pylist()
    return list(zip(src, dst, weight, strict=True))
