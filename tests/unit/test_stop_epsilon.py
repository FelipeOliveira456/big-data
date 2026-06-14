"""Tests for hierarchical epsilon stopping."""

from louvain_core.graph import Graph
from louvain_core.runner import run_louvain, should_stop_levels
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES


def test_should_stop_levels():
    assert should_stop_levels(0.5, 0.5 + 1e-7, 1e-6)
    assert not should_stop_levels(0.5, 0.52, 1e-6)


def test_run_louvain_converges_with_epsilon():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    res = run_louvain(g, epsilon=1e-6)
    assert res.converged
    assert res.num_levels >= 1
    assert res.modularity > 0
    assert -1.0 <= res.modularity <= 1.0
