"""Tests for LouvainHierarchy and modularity on the original graph."""

import pytest

from louvain_core.graph import Graph
from louvain_core.hierarchy import LouvainHierarchy
from louvain_core.runner import run_louvain
from tests.fixtures.toy_graphs import TWO_CLiques_EDGES

Q_AFTER_PHASE1 = 0.08163265306122448


def test_modularity_on_original_after_hierarchy():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    res = run_louvain(g, epsilon=1e-6)
    assert res.modularity > 0
    assert res.num_communities == 2
    assert len(res.partition) == len(g.nodes)


def test_hierarchy_tracks_original_nodes():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    hierarchy = LouvainHierarchy.from_graph(g)
    partition = {n: n for n in g.nodes}
    assert hierarchy.modularity_on_original(partition) == 0.0
    partition[0] = partition[1]
    assert hierarchy.modularity_on_original(partition) > 0


def test_hierarchy_compress_level_maps_back():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    hierarchy = LouvainHierarchy.from_graph(g)
    partition = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1}
    g2, new_part = hierarchy.compress_level(partition, g)
    assert len(g2.nodes) == 2
    assert new_part == {0: 0, 1: 1}
    orig = hierarchy.orig_partition(new_part)
    assert set(orig.keys()) == set(g.nodes)
    assert len(hierarchy.cur_to_orig[0]) == 3
    assert len(hierarchy.cur_to_orig[1]) == 3


def test_hierarchy_modularity_after_phase1_partition():
    g = Graph.from_edges(TWO_CLiques_EDGES)
    hierarchy = LouvainHierarchy.from_graph(g)
    partition = {0: 2, 1: 1, 2: 2, 3: 5, 4: 4, 5: 5}
    assert hierarchy.modularity_on_original(partition) == pytest.approx(Q_AFTER_PHASE1)
