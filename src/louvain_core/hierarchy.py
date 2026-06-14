"""Track hierarchical Louvain state for correct modularity on the original graph."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from louvain_core.compress import compress_communities
from louvain_core.graph import Graph
from louvain_core.modularity import compute_modularity


@dataclass
class LouvainHierarchy:
    """Maps current-level nodes back to original graph nodes for reporting Q."""

    graph_orig: Graph
    cur_to_orig: dict[int, frozenset[int]] = field(default_factory=dict)

    @classmethod
    def from_graph(cls, graph: Graph) -> LouvainHierarchy:
        return cls(
            graph_orig=graph,
            cur_to_orig={n: frozenset({n}) for n in graph.nodes},
        )

    def orig_partition(self, partition: dict[int, int]) -> dict[int, int]:
        """Project current-level communities onto original node ids."""
        orig_part: dict[int, int] = {}
        for cur_node, comm in partition.items():
            for orig in self.cur_to_orig[cur_node]:
                orig_part[orig] = comm
        return orig_part

    def modularity_on_original(self, partition: dict[int, int]) -> float:
        return compute_modularity(self.orig_partition(partition), self.graph_orig)

    def compress_level(
        self, partition: dict[int, int], graph: Graph
    ) -> tuple[Graph, dict[int, int]]:
        """Compress communities and update orig-node mapping for the next level."""
        new_graph, node_to_super = compress_communities(partition, graph)
        merged: dict[int, set[int]] = defaultdict(set)
        for cur_node, super_id in node_to_super.items():
            merged[super_id].update(self.cur_to_orig[cur_node])
        self.cur_to_orig = {k: frozenset(v) for k, v in merged.items()}
        new_partition = {n: n for n in new_graph.nodes}
        return new_graph, new_partition
