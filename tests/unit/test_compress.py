"""Tests for community compression."""

import pytest

from louvain_core.compress import compress_communities
from louvain_core.graph import Graph
from tests.fixtures.toy_graphs import TRIANGLE_EDGES, TWO_CLiques_EDGES


def test_compress_aggregates_weights():
    g = Graph.from_edges(TRIANGLE_EDGES)
    partition = {0: 0, 1: 0, 2: 0, 3: 1}
    g2, node_to_super = compress_communities(partition, g)
    assert len(g2.nodes) == 2
    assert g2.m == pytest.approx(g.m)
    assert len(node_to_super) == len(g.nodes)


def test_compress_two_cliques():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1}
    g2, _ = compress_communities(partition, g)
    assert len(g2.nodes) == 2
    assert g2.m == pytest.approx(7.0)


def test_compress_skips_self_loop_in_adj():
    g = Graph.from_edges([(0, 1, 1.0)])
    g.adj[0][0] = 3.0
    g.degree[0] += 3.0
    partition = {0: 0, 1: 0}
    g2, _ = compress_communities(partition, g)
    assert g2.m == pytest.approx(1.0)
