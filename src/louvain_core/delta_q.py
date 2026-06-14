"""Modularity gain ΔQ for Louvain local moves (Blondel et al.)."""

from __future__ import annotations


from louvain_core.graph import Graph, build_sigma_tot


def delta_q_gain(
    node: int,
    target_community: int,
    partition: dict[int, int],
    graph: Graph,
    m: float | None = None,
) -> float:
    """
    ΔQ when moving node to target_community (already excluding its current community).

    ΔQ = k_i_in/m - sigma_tot * k_i / (2m²)
    """
    if m is None:
        m = graph.m
    if m == 0:
        return 0.0

    k_i = graph.degree.get(node, 0.0)
    sigma = build_sigma_tot(partition, graph.degree)
    sigma_tot = sigma.get(target_community, 0.0)

    # Remove node's degree from sigma_tot of target if node already there
    if partition.get(node) == target_community:
        sigma_tot -= k_i

    k_i_in = 0.0
    for nbr, w in graph.adj.get(node, {}).items():
        if partition.get(nbr) == target_community:
            k_i_in += w

    return (k_i_in / m) - (sigma_tot * k_i) / (2.0 * m * m)


def neighbor_communities(
    node: int, partition: dict[int, int], graph: Graph
) -> set[int]:
    comms = {partition[node]}
    for nbr in graph.adj.get(node, {}):
        comms.add(partition[nbr])
    return comms


def best_move(
    node: int,
    partition: dict[int, int],
    graph: Graph,
    m: float | None = None,
) -> tuple[int, float]:
    """Return (best_community, best_positive_delta) or (current, 0) if no gain."""
    current = partition[node]
    best_comm = current
    best_dq = 0.0
    for comm in neighbor_communities(node, partition, graph):
        if comm == current:
            continue
        dq = delta_q_gain(node, comm, partition, graph, m)
        if dq > best_dq:
            best_dq = dq
            best_comm = comm
    return best_comm, best_dq


def batch_best_moves(
    nodes: list[int],
    partition: dict[int, int],
    graph: Graph,
) -> dict[int, int]:
    """Compute best move per node; only include nodes that actually move."""
    moves: dict[int, int] = {}
    m = graph.m
    for node in nodes:
        new_comm, dq = best_move(node, partition, graph, m)
        if dq > 0 and new_comm != partition[node]:
            moves[node] = new_comm
    return moves
