"""Modularity Q computation."""

from __future__ import annotations

from louvain_core.graph import Graph


def compute_modularity(partition: dict[int, int], graph: Graph) -> float:
    """
    Newman modularity for undirected weighted graph.

    Q = (1/2m) Σ_ij (A_ij - k_i k_j / 2m) δ(c_i, c_j)
    """
    m = graph.m
    if m == 0:
        return 0.0

    q = 0.0
    for u, nbrs in graph.adj.items():
        c_u = partition[u]
        k_u = graph.degree[u]
        for v, w in nbrs.items():
            if u >= v:
                continue
            c_v = partition[v]
            if c_u != c_v:
                continue
            k_v = graph.degree[v]
            q += w - (k_u * k_v) / (2.0 * m)
    return q / (2.0 * m)
