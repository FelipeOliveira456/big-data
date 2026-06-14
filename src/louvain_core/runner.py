"""Sequential Louvain (reference implementation for tests and small graphs)."""

from __future__ import annotations

from dataclasses import dataclass, field

from louvain_core.delta_q import best_move
from louvain_core.graph import Graph
from louvain_core.hierarchy import LouvainHierarchy


@dataclass
class LouvainResult:
    partition: dict[int, int]
    modularity: float
    num_communities: int
    num_levels: int
    level_q: list[float] = field(default_factory=list)
    converged: bool = True


def _phase1(graph: Graph, partition: dict[int, int], max_sweeps: int = 500) -> None:
    """Local moving: full sweeps until no node moves in a complete pass."""
    for _ in range(max_sweeps):
        moved = False
        for node in graph.nodes:
            new_comm, dq = best_move(node, partition, graph)
            if dq > 0 and new_comm != partition[node]:
                partition[node] = new_comm
                moved = True
        if not moved:
            return


def run_louvain(
    graph: Graph,
    epsilon: float = 1e-6,
    max_levels: int = 50,
) -> LouvainResult:
    """Hierarchical Louvain; stop when gain of Q between levels < epsilon."""
    if not graph.nodes:
        return LouvainResult({}, 0.0, 0, 0, [], True)

    g = graph
    hierarchy = LouvainHierarchy.from_graph(g)
    partition = {n: n for n in g.nodes}
    level_q: list[float] = []
    prev_q = hierarchy.modularity_on_original(partition)
    level_q.append(prev_q)
    levels = 1

    while levels < max_levels:
        _phase1(g, partition)
        q = hierarchy.modularity_on_original(partition)
        level_q.append(q)

        if levels >= 1 and should_stop_levels(prev_q, q, epsilon):
            orig_part = hierarchy.orig_partition(partition)
            return LouvainResult(
                partition=orig_part,
                modularity=q,
                num_communities=len(set(orig_part.values())),
                num_levels=levels,
                level_q=level_q,
                converged=True,
            )

        prev_q = q
        g, partition = hierarchy.compress_level(partition, g)
        if len(g.nodes) <= 1:
            break
        levels += 1

    orig_part = hierarchy.orig_partition(partition)
    final_q = hierarchy.modularity_on_original(partition)
    return LouvainResult(
        partition=orig_part,
        modularity=final_q,
        num_communities=len(set(orig_part.values())),
        num_levels=levels,
        level_q=level_q,
        converged=True,
    )


def should_stop_levels(q_prev: float, q_curr: float, epsilon: float) -> bool:
    """True when modularidade gain between levels is below epsilon."""
    return (q_curr - q_prev) < epsilon
