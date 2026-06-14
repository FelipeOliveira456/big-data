"""Distributed Louvain using Dask (manual implementation)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from dask.distributed import Client, LocalCluster

from config import AppConfig, load_config
from louvain_core.delta_q import best_move
from louvain_core.graph import Graph
from louvain_core.hierarchy import LouvainHierarchy
from louvain_core.runner import should_stop_levels
from preprocessing.write_artifact import read_edges_parquet


@dataclass
class DaskLouvainResult:
    modularity: float
    num_communities: int
    num_levels: int
    init_time_s: float
    algorithm_time_s: float
    level_times_s: list[float] = field(default_factory=list)
    converged: bool = True


def _batch_best_moves(
    batch_nodes: list[int],
    adj: dict[int, dict[int, float]],
    degree: dict[int, float],
    partition: dict[int, int],
) -> dict[int, int]:
    g = Graph(adj=adj, degree=dict(degree))
    part = dict(partition)
    moves: dict[int, int] = {}
    for node in batch_nodes:
        new_comm, dq = best_move(node, part, g)
        if dq > 0 and new_comm != part[node]:
            moves[node] = new_comm
    return moves


def _phase1_dask(
    client: Client,
    g: Graph,
    partition: dict[int, int],
    batch_size: int,
    graph_refs: tuple | None = None,
) -> tuple | None:
    """Fase 1: parallel batches per sweep; synchronous apply; repeat until stable."""
    if graph_refs is None:
        graph_refs = client.scatter([g.adj, g.degree], broadcast=True)

    for _ in range(500):
        nodes = list(g.nodes)
        batches = [nodes[i : i + batch_size] for i in range(0, len(nodes), batch_size)]
        snapshot = dict(partition)
        moves: dict[int, int] = {}
        adj_ref, degree_ref = graph_refs
        futures = [
            client.submit(_batch_best_moves, batch, adj_ref, degree_ref, snapshot)
            for batch in batches
        ]
        for part in client.gather(futures):
            moves.update(part)
        if not moves:
            return graph_refs
        for node, comm in moves.items():
            partition[node] = comm
    return graph_refs


def _scheduler_url(address: str) -> str:
    if address.startswith("tcp://"):
        return address
    return f"tcp://{address}"


def run_louvain_dask(
    artifact_path: str | Path,
    epsilon: float = 1e-6,
    batch_size: int = 1000,
    n_workers: int | None = None,
    cfg: AppConfig | None = None,
) -> DaskLouvainResult:
    cfg = cfg or load_config()
    if n_workers is None:
        n_workers = cfg.dask_n_workers

    edges = read_edges_parquet(Path(artifact_path))
    g0 = Graph.from_edges(edges)

    t0 = time.perf_counter()
    cluster: LocalCluster | None = None
    if cfg.dask_scheduler_address:
        client = Client(_scheduler_url(cfg.dask_scheduler_address))
    else:
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            processes=True,
            silence_logs=True,
        )
        client = Client(cluster)
    init_time = time.perf_counter() - t0

    t1 = time.perf_counter()
    try:
        hierarchy = LouvainHierarchy.from_graph(g0)
        g = g0
        partition = {n: n for n in g.nodes}
        level_times: list[float] = []
        prev_q = hierarchy.modularity_on_original(partition)
        levels = 1
        final_q = prev_q
        final_communities = len(g.nodes)
        graph_refs = None

        while levels < 100:
            lvl_start = time.perf_counter()
            graph_refs = _phase1_dask(client, g, partition, batch_size, graph_refs)
            q = hierarchy.modularity_on_original(partition)
            level_times.append(time.perf_counter() - lvl_start)
            orig_part = hierarchy.orig_partition(partition)
            final_q = q
            final_communities = len(set(orig_part.values()))

            if levels >= 1 and should_stop_levels(prev_q, q, epsilon):
                break
            prev_q = q
            g, partition = hierarchy.compress_level(partition, g)
            graph_refs = None
            if len(g.nodes) <= 1:
                break
            levels += 1

        return DaskLouvainResult(
            modularity=final_q,
            num_communities=final_communities,
            num_levels=levels,
            init_time_s=init_time,
            algorithm_time_s=time.perf_counter() - t1,
            level_times_s=level_times,
            converged=True,
        )
    finally:
        client.close()
        if cluster is not None:
            cluster.close()
