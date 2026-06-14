"""End-to-end preprocessing: SNAP edge list → Parquet artifacts per fraction."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from preprocessing.artifacts import artifact_path
from preprocessing.load_snap import (
    LCC_IN_MEMORY_MAX_EDGES,
    build_edge_db,
    close_edge_db,
    export_db_to_parquet,
    iter_normalized_edges,
    load_edges_from_db,
)
from preprocessing.sample_lcc import (
    collect_lcc_node_ids,
    extract_lcc,
    induced_edges,
    sample_connected_node_ids,
)
from preprocessing.write_artifact import write_graph_parquet


def build_artifact(
    raw_path: Path,
    output_path: Path,
    fraction_pct: int,
    seed: int,
) -> dict:
    """Build one artifact (used by unit tests)."""
    lcc_nodes = collect_lcc_node_ids(raw_path)
    all_edges = list(iter_normalized_edges(raw_path))
    if fraction_pct >= 100:
        sampled = set(lcc_nodes)
    else:
        sampled = sample_connected_node_ids(all_edges, lcc_nodes, fraction_pct, seed)
    conn, edge_count = build_edge_db(raw_path, sampled)
    if edge_count > LCC_IN_MEMORY_MAX_EDGES:
        meta = export_db_to_parquet(
            conn,
            output_path,
            {
                "fraction_pct": fraction_pct,
                "seed": seed,
                "source": str(raw_path),
                "dropped_nodes_lcc": 0,
                "lcc_skipped": True,
            },
        )
        close_edge_db(conn, raw_path)
        return meta
    edges = load_edges_from_db(conn)
    close_edge_db(conn, raw_path)
    induced = induced_edges(edges, sampled)
    lcc_edges, dropped = extract_lcc(induced)
    nodes = {s for s, _, _ in lcc_edges} | {d for _, d, _ in lcc_edges}
    meta = {
        "fraction_pct": fraction_pct,
        "seed": seed,
        "node_count": len(nodes),
        "edge_count": len(lcc_edges),
        "dropped_nodes_lcc": dropped,
        "source": str(raw_path),
        "lcc_skipped": False,
    }
    write_graph_parquet(lcc_edges, output_path, meta)
    return meta


def run_preprocess(
    input_path: Path,
    output_dir: Path,
    seed: int,
    fractions: list[int],
    dataset_slug: str = "email-enron",
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Pass 1: collecting nodes from {input_path} ...", flush=True)
    lcc_nodes = collect_lcc_node_ids(input_path)
    print(f"  {len(lcc_nodes)} LCC nodes (full graph)", flush=True)
    all_edges = list(iter_normalized_edges(input_path))

    written: list[Path] = []
    for frac in fractions:
        out = artifact_path(output_dir, dataset_slug, frac)
        print(f"Fraction {frac}% → {out} ...", flush=True)
        if frac >= 100:
            sampled = set(lcc_nodes)
        else:
            sampled = sample_connected_node_ids(all_edges, lcc_nodes, frac, seed)
        conn, edge_count = build_edge_db(input_path, sampled)
        print(f"  SQLite: {edge_count} edges for sampled nodes", flush=True)

        base_meta = {
            "fraction_pct": frac,
            "seed": seed,
            "source": str(input_path),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        if edge_count > LCC_IN_MEMORY_MAX_EDGES:
            print(
                "  Large graph: streaming to Parquet (skip in-memory LCC)", flush=True
            )
            meta = export_db_to_parquet(
                conn,
                out,
                {**base_meta, "dropped_nodes_lcc": 0, "lcc_skipped": True},
            )
            close_edge_db(conn, input_path)
        else:
            edges = load_edges_from_db(conn)
            close_edge_db(conn, input_path)
            induced = induced_edges(edges, sampled)
            lcc_edges, dropped = extract_lcc(induced)
            nodes = {s for s, _, _ in lcc_edges} | {d for _, d, _ in lcc_edges}
            meta = {
                **base_meta,
                "node_count": len(nodes),
                "edge_count": len(lcc_edges),
                "dropped_nodes_lcc": dropped,
                "lcc_skipped": False,
            }
            write_graph_parquet(lcc_edges, out, meta)

        print(
            f"  done: {meta.get('node_count')} nodes, {meta.get('edge_count')} edges",
            flush=True,
        )
        written.append(out)

    return written
