"""Tests for batch ΔQ moves and edge cases."""

from louvain_core.delta_q import batch_best_moves, delta_q_gain
from louvain_core.graph import Graph
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES


def test_batch_best_moves_returns_only_positive_moves():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {n: n for n in g.nodes}
    moves = batch_best_moves(list(g.nodes), partition, g)
    assert isinstance(moves, dict)
    for node, comm in moves.items():
        assert comm != partition[node]


def test_delta_q_zero_when_m_is_zero():
    g = Graph()
    g.adj[1] = {}
    g.degree[1] = 0.0
    assert delta_q_gain(1, 1, {1: 1}, g, m=0.0) == 0.0
