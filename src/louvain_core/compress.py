"""Phase 2: compress communities into super-nodes."""

from __future__ import annotations

from collections import defaultdict

from louvain_core.graph import Graph


def compress_communities(
    partition: dict[int, int], graph: Graph
) -> tuple[Graph, dict[int, int]]:
    """
    Collapse communities to super-nodes. Returns (compressed_graph, node_to_super).

    node_to_super maps original node id -> super-node id (dense community index).
    """
    comm_ids = sorted(set(partition.values()))
    comm_map = {c: i for i, c in enumerate(comm_ids)}

    agg: dict[tuple[int, int], float] = defaultdict(float)
    for u, nbrs in graph.adj.items():
        for v, w in nbrs.items():
            if u >= v:
                continue
            cu = comm_map[partition[u]]
            cv = comm_map[partition[v]]
            key = (min(cu, cv), max(cu, cv))
            agg[key] += w

    new_graph = Graph()
    for (a, b), w in agg.items():
        new_graph.add_edge(a, b, w)

    node_to_super = {n: comm_map[partition[n]] for n in partition}
    return new_graph, node_to_super
