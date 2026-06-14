"""Tests for modularity computation."""

import pytest

from louvain_core.graph import Graph
from louvain_core.modularity import compute_modularity

SIMPLE_TRIANGLE = [(0, 1, 1.0), (1, 2, 1.0), (0, 2, 1.0)]


def test_modularity_each_node_own_community():
    g = Graph.from_edges(SIMPLE_TRIANGLE)
    partition = {n: n for n in g.nodes}
    assert compute_modularity(partition, g) == 0.0


def test_modularity_single_community():
    g = Graph.from_edges(SIMPLE_TRIANGLE)
    partition = {n: 0 for n in g.nodes}
    assert compute_modularity(partition, g) == pytest.approx(1 / 6)


def test_modularity_ignores_self_loop_in_adj():
    g = Graph.from_edges([(0, 1, 1.0)])
    g.adj[0][0] = 4.0
    g.degree[0] = g.degree.get(0, 0.0) + 4.0
    partition = {0: 0, 1: 0}
    assert compute_modularity(partition, g) == pytest.approx(0.027777777777777773)


def test_modularity_zero_edges():
    g = Graph()
    g.adj[1] = {}
    g.degree[1] = 0.0
    assert compute_modularity({1: 1}, g) == 0.0
