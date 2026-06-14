"""Tests for SNAP preprocessing."""

from pathlib import Path

import pyarrow.parquet as pq

from preprocessing.load_snap import iter_normalized_edges
from preprocessing.pipeline import build_artifact
from preprocessing.sample_lcc import (
    extract_lcc,
    induced_edges,
    sample_connected_node_ids,
    sample_node_ids,
)
from tests.fixtures.toy_graphs import DIRECTED_SAMPLE_LINES


def test_iter_normalized_skips_self_loops(tmp_path: Path):
    p = tmp_path / "edges.txt"
    p.write_text("".join(DIRECTED_SAMPLE_LINES))
    edges = list(iter_normalized_edges(p))
    assert all(s < d for s, d, _ in edges)
    assert all(s != d for s, d, _ in edges)


def test_sample_and_lcc_reproducible():
    edges = [(0, 1, 1.0), (1, 2, 1.0), (0, 2, 1.0), (3, 4, 1.0)]
    nodes = sorted({s for s, _, _ in edges} | {d for _, d, _ in edges})
    a = sample_node_ids(nodes, 100, 42)
    b = sample_node_ids(nodes, 100, 42)
    assert a == b
    induced = induced_edges(edges, a)
    lcc, _ = extract_lcc(induced)
    assert len(lcc) >= 1


def test_sample_connected_preserves_fraction():
    edges = [(i, i + 1, 1.0) for i in range(99)]
    nodes = list(range(100))
    sampled = sample_connected_node_ids(edges, nodes, 10, 42)
    assert len(sampled) == 10
    induced = induced_edges(edges, sampled)
    lcc, _ = extract_lcc(induced)
    assert len({s for s, _, _ in lcc} | {d for _, d, _ in lcc}) == 10


def test_build_artifact_parquet(tmp_path: Path):
    raw = tmp_path / "raw.txt"
    raw.write_text("1 2\n2 3\n3 1\n4 5\n5 4\n")
    out = tmp_path / "email-enron_100pct.parquet"
    meta = build_artifact(raw, out, 100, 42)
    assert meta["node_count"] >= 1
    schema = pq.read_schema(out)
    assert set(schema.names) == {"src", "dst", "weight"}
