"""Smoke test with tiny Parquet fixture (not full Enron)."""

from pathlib import Path

from louvain_core.graph import Graph
from louvain_core.runner import run_louvain
from preprocessing.write_artifact import read_edges_parquet


def test_sequential_louvain_on_fixture(tiny_artifact: Path):
    g = Graph.from_edges(read_edges_parquet(tiny_artifact))
    res = run_louvain(g)
    assert max(res.level_q) > 0, "expected positive Q at some hierarchical level"
