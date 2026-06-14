"""Distributed Louvain using Ray (manual implementation)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import ray

from louvain_core.delta_q import best_move
from louvain_core.graph import Graph
from louvain_core.hierarchy import LouvainHierarchy
from louvain_core.runner import should_stop_levels
from preprocessing.write_artifact import read_edges_parquet


@dataclass
class RayLouvainResult:
    modularity: float
    num_communities: int
    num_levels: int
    init_time_s: float
    algorithm_time_s: float
    level_times_s: list[float] = field(default_factory=list)
    converged: bool = True


@ray.remote
def calcular_ganho_batch(
    batch_nos: list[int],
    adj: dict[int, dict[int, float]],
    degree: dict[int, float],
    partition: dict[int, int],
) -> dict[int, int]:
    g = Graph(adj=adj, degree=dict(degree))
    part = dict(partition)
    moves: dict[int, int] = {}
    for node in batch_nos:
        new_comm, dq = best_move(node, part, g)
        if dq > 0 and new_comm != part[node]:
            moves[node] = new_comm
    return moves


def _phase1_ray(g: Graph, partition: dict[int, int], batch_size: int) -> None:
    """Fase 1: parallel batches per sweep; synchronous apply; repeat until stable."""
    for _ in range(500):
        nodes = list(g.nodes)
        batches = [nodes[i : i + batch_size] for i in range(0, len(nodes), batch_size)]
        snapshot = dict(partition)
        refs = [
            calcular_ganho_batch.remote(b, g.adj, g.degree, snapshot) for b in batches
        ]
        moves: dict[int, int] = {}
        for part in ray.get(refs):
            moves.update(part)
        if not moves:
            return
        for node, comm in moves.items():
            partition[node] = comm


def _ray_client_address(ray_head_address: str) -> str:
    host = ray_head_address.split(":")[0]
    return f"ray://{host}:10001"


def run_louvain_ray(
    artifact_path: str | Path,
    epsilon: float = 1e-6,
    batch_size: int = 1000,
    num_cpus: int | None = None,
    ray_head_address: str | None = None,
) -> RayLouvainResult:
    edges = read_edges_parquet(Path(artifact_path))
    g0 = Graph.from_edges(edges)

    t0 = time.perf_counter()
    if not ray.is_initialized():
        if ray_head_address:
            ray.init(
                address=_ray_client_address(ray_head_address),
                ignore_reinit_error=True,
            )
        else:
            ray.init(num_cpus=num_cpus, ignore_reinit_error=True)
    init_time = time.perf_counter() - t0

    t1 = time.perf_counter()
    hierarchy = LouvainHierarchy.from_graph(g0)
    g = g0
    partition = {n: n for n in g.nodes}
    level_times: list[float] = []
    prev_q = hierarchy.modularity_on_original(partition)
    levels = 1
    final_q = prev_q
    final_communities = len(g.nodes)

    while levels < 100:
        lvl_start = time.perf_counter()
        _phase1_ray(g, partition, batch_size)
        q = hierarchy.modularity_on_original(partition)
        level_times.append(time.perf_counter() - lvl_start)
        orig_part = hierarchy.orig_partition(partition)
        final_q = q
        final_communities = len(set(orig_part.values()))

        if levels >= 1 and should_stop_levels(prev_q, q, epsilon):
            break
        prev_q = q
        g, partition = hierarchy.compress_level(partition, g)
        if len(g.nodes) <= 1:
            break
        levels += 1

    return RayLouvainResult(
        modularity=final_q,
        num_communities=final_communities,
        num_levels=levels,
        init_time_s=init_time,
        algorithm_time_s=time.perf_counter() - t1,
        level_times_s=level_times,
        converged=True,
    )
