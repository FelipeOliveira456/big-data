"""Load and normalize SNAP edge-list graphs."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from preprocessing.write_artifact import write_graph_parquet

BATCH_SIZE = 100_000
# Above this, skip in-memory NetworkX LCC on very large induced subgraphs
LCC_IN_MEMORY_MAX_EDGES = 8_000_000


def iter_normalized_edges(path: Path) -> Iterator[tuple[int, int, float]]:
    """Yield undirected edges (src, dst, weight) with src < dst."""
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            u, v = int(parts[0]), int(parts[1])
            if u == v:
                continue
            src, dst = (u, v) if u < v else (v, u)
            yield src, dst, 1.0


def collect_nodes(path: Path) -> list[int]:
    nodes: set[int] = set()
    for src, dst, _ in iter_normalized_edges(path):
        nodes.add(src)
        nodes.add(dst)
    return sorted(nodes)


def _db_path(path: Path) -> Path:
    return path.parent / f".{path.stem}_edges.sqlite"


def build_edge_db(path: Path, nodes: set[int]) -> tuple[sqlite3.Connection, int]:
    """Stream file into on-disk SQLite; return (connection, edge_count)."""
    db_path = _db_path(path)
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(
        "CREATE TABLE edges (src INTEGER NOT NULL, dst INTEGER NOT NULL, "
        "weight REAL NOT NULL, PRIMARY KEY (src, dst))"
    )

    batch: list[tuple[int, int, float]] = []
    for src, dst, w in iter_normalized_edges(path):
        if src in nodes and dst in nodes:
            batch.append((src, dst, w))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT OR IGNORE INTO edges (src, dst, weight) VALUES (?, ?, ?)",
                    batch,
                )
                conn.commit()
                batch.clear()

    if batch:
        conn.executemany(
            "INSERT OR IGNORE INTO edges (src, dst, weight) VALUES (?, ?, ?)",
            batch,
        )
        conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    return conn, int(count)


def close_edge_db(conn: sqlite3.Connection, raw_path: Path) -> None:
    conn.close()
    _db_path(raw_path).unlink(missing_ok=True)


def load_edges_from_db(conn: sqlite3.Connection) -> list[tuple[int, int, float]]:
    rows = conn.execute("SELECT src, dst, weight FROM edges").fetchall()
    return [(int(s), int(d), float(w)) for s, d, w in rows]


def export_db_to_parquet(
    conn: sqlite3.Connection,
    output_path: Path,
    meta: dict,
) -> dict:
    """Stream SQLite edges to Parquet without loading all rows in RAM."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cursor = conn.execute("SELECT src, dst, weight FROM edges")

    writer: pq.ParquetWriter | None = None
    edge_count = 0
    nodes_seen: set[int] = set()

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break
        src = [int(r[0]) for r in rows]
        dst = [int(r[1]) for r in rows]
        weight = [float(r[2]) for r in rows]
        for s, d in zip(src, dst, strict=True):
            nodes_seen.add(s)
            nodes_seen.add(d)
        edge_count += len(rows)
        table = pa.table({"src": src, "dst": dst, "weight": weight})
        if writer is None:
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)

    if writer is None:
        write_graph_parquet([], output_path, meta)
    else:
        writer.close()

    meta = {**meta, "node_count": len(nodes_seen), "edge_count": edge_count}
    meta.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    output_path.with_suffix(".meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    return meta


def load_normalized_edges(path: Path) -> list[tuple[int, int, float]]:
    """Load full graph (small graphs / tests only)."""
    nodes = set(collect_nodes(path))
    conn, _ = build_edge_db(path, nodes)
    edges = load_edges_from_db(conn)
    close_edge_db(conn, path)
    return edges
