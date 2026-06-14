"""Tests for Graph helper methods."""

from louvain_core.graph import Graph, build_sigma_tot


def test_graph_edges_deduplicates_undirected():
    g = Graph.from_edges([(0, 1, 1.0), (1, 0, 1.0), (1, 2, 2.0)])
    edges = g.edges()
    assert len(edges) == 2
    assert all(u < v for u, v, _ in edges)


def test_graph_ignores_self_loops_in_from_edges():
    g = Graph.from_edges([(1, 1, 1.0), (1, 2, 1.0)])
    assert 1 in g.nodes and 2 in g.nodes
    assert len(g.edges()) == 1


def test_graph_m_total_weight():
    g = Graph.from_edges([(0, 1, 2.0), (1, 2, 4.0)])
    assert g.m == 6.0


def test_add_edge_accumulates_weight():
    g = Graph()
    g.add_edge(0, 1, 1.0)
    g.add_edge(0, 1, 2.0)
    assert g.adj[0][1] == 3.0
    assert g.degree[0] == 3.0
    assert g.m == 3.0


def test_build_sigma_tot_missing_degree_defaults_zero():
    sigma = build_sigma_tot({0: 0, 99: 1}, {0: 2.0})
    assert sigma[0] == 2.0
    assert sigma[1] == 0.0


def test_build_sigma_tot_sums_by_community():
    degree = {0: 2.0, 1: 3.0, 2: 5.0}
    partition = {0: 0, 1: 0, 2: 1}
    sigma = build_sigma_tot(partition, degree)
    assert sigma[0] == 5.0
    assert sigma[1] == 5.0
