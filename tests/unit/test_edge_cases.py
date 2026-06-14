"""Edge case tests."""

from louvain_core.graph import Graph
from louvain_core.modularity import compute_modularity
from louvain_core.runner import run_louvain


def test_empty_graph():
    g = Graph()
    res = run_louvain(g)
    assert res.num_communities == 0
    assert res.modularity == 0.0


def test_single_node():
    g = Graph()
    g.adj[1] = {}
    g.degree[1] = 0.0
    partition = {1: 1}
    assert compute_modularity(partition, g) == 0.0
