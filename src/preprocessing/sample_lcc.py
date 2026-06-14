"""Sample nodes, induced subgraph, and largest connected component."""

from __future__ import annotations

import random
from collections import defaultdict
from pathlib import Path

import networkx as nx

from preprocessing.load_snap import iter_normalized_edges


def sample_node_ids(all_nodes: list[int], fraction_pct: int, seed: int) -> set[int]:
    """Random sample of fraction_pct percent of node ids."""
    if fraction_pct >= 100:
        return set(all_nodes)
    rng = random.Random(seed)
    k = max(1, int(len(all_nodes) * fraction_pct / 100))
    return set(rng.sample(all_nodes, k))


def sample_connected_node_ids(
    edges: list[tuple[int, int, float]],
    lcc_nodes: list[int],
    fraction_pct: int,
    seed: int,
) -> set[int]:
    """
    BFS expansion from a random LCC seed until k nodes are collected.

    Unlike uniform random sampling + induced subgraph + LCC (which collapses to
    a handful of nodes), this yields a connected ~fraction_pct subgraph.
    """
    if fraction_pct >= 100:
        return set(lcc_nodes)

    rng = random.Random(seed)
    k = max(1, int(len(lcc_nodes) * fraction_pct / 100))
    lcc_set = set(lcc_nodes)
    adj: dict[int, list[int]] = defaultdict(list)
    for s, d, _ in edges:
        if s in lcc_set and d in lcc_set:
            adj[s].append(d)
            adj[d].append(s)

    start = rng.choice(lcc_nodes)
    visited: set[int] = {start}
    queue = [start]
    while len(visited) < k and queue:
        u = queue.pop(0)
        neighbors = adj.get(u, [])
        rng.shuffle(neighbors)
        for v in neighbors:
            if v not in visited:
                visited.add(v)
                queue.append(v)
                if len(visited) >= k:
                    break
    return visited


def collect_lcc_node_ids(raw_path: Path) -> list[int]:
    """
    Node ids in the largest connected component of the full SNAP graph.

    Fractions are defined as a sample of LCC nodes (not raw file nodes), so
    1% of a ~34k-node LCC yields ~340 nodes instead of a sparse induced stub.
    """
    edges = list(iter_normalized_edges(raw_path))
    lcc_edges, _ = extract_lcc(edges)
    nodes = {s for s, _, _ in lcc_edges} | {d for _, d, _ in lcc_edges}
    return sorted(nodes)


def induced_edges(
    edges: list[tuple[int, int, float]],
    nodes: set[int],
) -> list[tuple[int, int, float]]:
    return [(s, d, w) for s, d, w in edges if s in nodes and d in nodes]


def extract_lcc(
    edges: list[tuple[int, int, float]],
) -> tuple[list[tuple[int, int, float]], int]:
    """
    Return (edges in LCC, num_nodes_dropped).

    num_nodes_dropped counts nodes not in LCC (including isolates).
    """
    if not edges:
        return [], 0

    g = nx.Graph()
    for s, d, w in edges:
        g.add_edge(s, d, weight=w)

    if g.number_of_nodes() == 0:
        return [], 0

    components = list(nx.connected_components(g))
    lcc_nodes = max(components, key=len)
    dropped = g.number_of_nodes() - len(lcc_nodes)

    lcc_edges = [(s, d, w) for s, d, w in edges if s in lcc_nodes and d in lcc_nodes]
    return lcc_edges, dropped
