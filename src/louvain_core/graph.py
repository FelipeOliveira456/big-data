"""Graph utilities shared by Louvain implementations."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class Graph:
    """Undirected weighted graph as adjacency list."""

    adj: dict[int, dict[int, float]] = field(default_factory=dict)
    degree: dict[int, float] = field(default_factory=dict)

    @property
    def nodes(self) -> list[int]:
        return list(self.adj.keys())

    @property
    def m(self) -> float:
        """Total edge weight / 2."""
        return sum(self.degree.values()) / 2.0

    @classmethod
    def from_edges(cls, edges: list[tuple[int, int, float]]) -> Graph:
        g = cls()
        for src, dst, w in edges:
            if src == dst:
                continue
            g.add_edge(src, dst, w)
        return g

    def add_edge(self, u: int, v: int, w: float = 1.0) -> None:
        self.adj.setdefault(u, {})
        self.adj.setdefault(v, {})
        self.adj[u][v] = self.adj[u].get(v, 0.0) + w
        self.adj[v][u] = self.adj[v].get(u, 0.0) + w
        self.degree[u] = self.degree.get(u, 0.0) + w
        self.degree[v] = self.degree.get(v, 0.0) + w

    def edges(self) -> list[tuple[int, int, float]]:
        seen: set[tuple[int, int]] = set()
        out: list[tuple[int, int, float]] = []
        for u, nbrs in self.adj.items():
            for v, w in nbrs.items():
                if u < v:
                    key = (u, v)
                    if key not in seen:
                        seen.add(key)
                        out.append((u, v, w))
        return out


def build_sigma_tot(
    partition: dict[int, int], degree: dict[int, float]
) -> dict[int, float]:
    sigma: dict[int, float] = defaultdict(float)
    for node, comm in partition.items():
        sigma[comm] += degree.get(node, 0.0)
    return dict(sigma)
