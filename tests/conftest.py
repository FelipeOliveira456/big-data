"""Pytest configuration and shared fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.fixtures.toy_graphs import TWO_CLiques_EDGES

TINY_ARTIFACT_META = {
    "fraction_pct": 100,
    "seed": 42,
    "node_count": 6,
    "edge_count": len(TWO_CLiques_EDGES),
    "dataset_slug": "email-enron",
}


@pytest.fixture
def tiny_artifact(tmp_path: Path) -> Path:
    """Small Parquet graph (~6 nodes) for fast unit/integration smoke."""
    from preprocessing.write_artifact import write_graph_parquet

    out = tmp_path / "email-enron_100pct.parquet"
    write_graph_parquet(TWO_CLiques_EDGES, out, dict(TINY_ARTIFACT_META))
    return out
