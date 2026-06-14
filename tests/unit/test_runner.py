"""Tests for sequential Louvain runner (mutation-sensitive)."""

import pytest

from louvain_core.graph import Graph
from louvain_core.runner import LouvainResult, _phase1, run_louvain, should_stop_levels
from tests.fixtures.toy_graphs import TRIANGLE_EDGES, TWO_CLiques_EDGES

Q_AFTER_PHASE1 = 0.08163265306122448
Q_FULL_LOUVAIN = 0.2653061224489796


def test_run_louvain_empty_graph():
    r = run_louvain(Graph())
    assert r == LouvainResult({}, 0.0, 0, 0, [], True)


def test_should_stop_levels_boundary():
    assert should_stop_levels(0.5, 0.5 + 1e-7, 1e-6)
    assert not should_stop_levels(0.5, 0.52, 1e-6)
    assert not should_stop_levels(0.0, 0.5, 1e-6)


def test_phase1_merges_two_cliques_partially():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    partition = {n: n for n in g.nodes}
    _phase1(g, partition)
    assert partition == {0: 2, 1: 1, 2: 2, 3: 5, 4: 4, 5: 5}


def test_run_louvain_full_two_cliques():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    r = run_louvain(g, epsilon=1e-6)
    assert r.partition == {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1}
    assert r.modularity == pytest.approx(Q_FULL_LOUVAIN)
    assert r.num_communities == 2
    assert r.num_levels == 3
    assert r.converged is True
    assert len(r.level_q) == 4
    assert r.level_q[0] == pytest.approx(0.0)
    assert r.level_q[1] == pytest.approx(Q_AFTER_PHASE1)


def test_run_louvain_stops_early_with_large_epsilon():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    r = run_louvain(g, epsilon=1.0)
    assert r.num_levels == 1
    assert r.modularity == pytest.approx(Q_AFTER_PHASE1)
    assert r.num_communities == 4
    assert len(r.level_q) == 2


def test_run_louvain_respects_max_levels():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    r = run_louvain(g, epsilon=1e-12, max_levels=2)
    assert r.num_levels == 2
    assert r.num_communities >= 2


def test_run_louvain_single_node():
    g = Graph.from_edges([])
    g.adj[7] = {}
    g.degree[7] = 0.0
    r = run_louvain(g, epsilon=1e-6)
    assert r.partition == {7: 7}
    assert r.num_communities == 1
    assert r.num_levels == 1


def test_phase1_triangle_converges():
    g = Graph.from_edges(TRIANGLE_EDGES)
    partition = {n: n for n in g.nodes}
    _phase1(g, partition, max_sweeps=10)
    assert len(set(partition.values())) < len(g.nodes)
