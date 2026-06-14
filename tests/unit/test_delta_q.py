"""Tests for ΔQ moves."""

import pytest

from louvain_core.delta_q import (
    batch_best_moves,
    best_move,
    delta_q_gain,
    neighbor_communities,
)
from louvain_core.graph import Graph
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES

NODE2_DQ = 0.08163265306122448


def test_best_move_positive_gain():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {n: n for n in g.nodes}
    comm, dq = best_move(2, partition, g)
    assert comm == 0
    assert dq == pytest.approx(NODE2_DQ)


def test_best_move_no_gain_on_isolated():
    g = Graph()
    g.adj[1] = {}
    g.degree[1] = 0.0
    comm, dq = best_move(1, {1: 1}, g)
    assert comm == 1
    assert dq == 0.0


def test_neighbor_communities_includes_neighbors():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {n: n for n in g.nodes}
    comms = neighbor_communities(2, partition, g)
    assert comms == {0, 1, 2, 3}


def test_neighbor_communities_isolated_node():
    g = Graph()
    g.adj[5] = {}
    g.degree[5] = 0.0
    assert neighbor_communities(5, {5: 5}, g) == {5}


def test_delta_q_gain_unknown_community():
    g = Graph.from_edges([(0, 1, 1.0)])
    partition = {0: 0, 1: 1}
    assert delta_q_gain(0, 99, partition, g) == 0.0


def test_delta_q_gain_positive_for_bridge_move():
    g = Graph.from_edges([(0, 1, 1.0)])
    partition = {0: 0, 1: 1}
    assert delta_q_gain(0, 1, partition, g) > 0


def test_batch_best_moves_exact_on_two_cliques():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {n: n for n in g.nodes}
    moves = batch_best_moves(list(g.nodes), partition, g)
    assert moves[2] == 0
    assert moves[0] not in moves or moves[0] != partition[0]


def test_batch_best_moves_empty_when_stable():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1}
    assert batch_best_moves(list(g.nodes), partition, g) == {}
