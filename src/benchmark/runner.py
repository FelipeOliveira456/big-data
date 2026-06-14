"""Benchmark orchestration: 3 runs per approach × fraction."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from benchmark.metrics import track_memory_peaks, throughput_nodes_per_s
from config import AppConfig, load_config
from preprocessing.artifacts import artifact_path
from preprocessing.validate_artifact import validate_artifact
from preprocessing.write_artifact import read_edges_parquet
from ray_impl.louvain_ray import run_louvain_ray

CSV_HEADER = [
    "approach",
    "fraction_pct",
    "run_index",
    "init_time_s",
    "algorithm_time_s",
    "peak_memory_mb",
    "peak_driver_rss_mb",
    "peak_process_tree_rss_mb",
    "throughput_nodes_per_s",
    "modularity_q",
    "num_communities",
    "num_levels",
    "epsilon",
    "seed",
    "converged",
    "status",
    "error_message",
    "level_times_json",
]


@dataclass
class BenchmarkRow:
    approach: str
    fraction_pct: int
    run_index: int
    init_time_s: float
    algorithm_time_s: float
    peak_memory_mb: float
    peak_driver_rss_mb: float
    peak_process_tree_rss_mb: float
    throughput_nodes_per_s: float
    modularity_q: float
    num_communities: int
    num_levels: int
    epsilon: float
    seed: int
    converged: bool
    status: str
    error_message: str
    level_times_json: str


def _node_count(artifact: Path) -> int:
    edges = read_edges_parquet(artifact)
    nodes = set()
    for s, d, _ in edges:
        nodes.add(s)
        nodes.add(d)
    return len(nodes)


def _run_ray(artifact: Path, cfg: AppConfig) -> BenchmarkRow:
    with track_memory_peaks() as mem:
        res = run_louvain_ray(
            artifact,
            epsilon=cfg.epsilon,
            batch_size=cfg.ray_batch_size,
            num_cpus=cfg.ray_num_cpus,
            ray_head_address=cfg.ray_head_address,
        )
    nc = _node_count(artifact)
    return BenchmarkRow(
        approach="ray",
        fraction_pct=int(artifact.stem.split("_")[1].replace("pct", "")),
        run_index=0,
        init_time_s=res.init_time_s,
        algorithm_time_s=res.algorithm_time_s,
        peak_memory_mb=mem.tracemalloc_mb,
        peak_driver_rss_mb=mem.driver_rss_mb,
        peak_process_tree_rss_mb=mem.process_tree_rss_mb,
        throughput_nodes_per_s=throughput_nodes_per_s(nc, res.algorithm_time_s),
        modularity_q=res.modularity,
        num_communities=res.num_communities,
        num_levels=res.num_levels,
        epsilon=cfg.epsilon,
        seed=cfg.seed,
        converged=res.converged,
        status="success",
        error_message="",
        level_times_json=json.dumps(res.level_times_s),
    )


def _run_dask(artifact: Path, cfg: AppConfig) -> BenchmarkRow:
    from dask_impl.louvain_dask import run_louvain_dask

    with track_memory_peaks() as mem:
        res = run_louvain_dask(
            artifact,
            epsilon=cfg.epsilon,
            batch_size=cfg.ray_batch_size,
            n_workers=cfg.dask_n_workers,
            cfg=cfg,
        )
    nc = _node_count(artifact)
    return BenchmarkRow(
        approach="dask",
        fraction_pct=int(artifact.stem.split("_")[1].replace("pct", "")),
        run_index=0,
        init_time_s=res.init_time_s,
        algorithm_time_s=res.algorithm_time_s,
        peak_memory_mb=mem.tracemalloc_mb,
        peak_driver_rss_mb=mem.driver_rss_mb,
        peak_process_tree_rss_mb=mem.process_tree_rss_mb,
        throughput_nodes_per_s=throughput_nodes_per_s(nc, res.algorithm_time_s),
        modularity_q=res.modularity,
        num_communities=res.num_communities,
        num_levels=res.num_levels,
        epsilon=cfg.epsilon,
        seed=cfg.seed,
        converged=res.converged,
        status="success",
        error_message="",
        level_times_json=json.dumps(res.level_times_s),
    )


def run_benchmark_campaign(
    artifacts_dir: Path,
    output_csv: Path,
    runs: int = 3,
    fractions: list[int] | None = None,
    cfg: AppConfig | None = None,
) -> Path:
    cfg = cfg or load_config()
    fractions = fractions or [100]
    rows: list[BenchmarkRow] = []

    for frac in fractions:
        artifact = artifact_path(artifacts_dir, cfg.dataset_slug, frac)
        validate_artifact(artifact)
        for approach in ("ray", "dask"):
            for run_idx in range(1, runs + 1):
                try:
                    if approach == "ray":
                        row = _run_ray(artifact, cfg)
                    else:
                        row = _run_dask(artifact, cfg)
                    row.run_index = run_idx
                    rows.append(row)
                except Exception as exc:  # noqa: BLE001
                    rows.append(
                        BenchmarkRow(
                            approach=approach,
                            fraction_pct=frac,
                            run_index=run_idx,
                            init_time_s=0.0,
                            algorithm_time_s=0.0,
                            peak_memory_mb=0.0,
                            peak_driver_rss_mb=0.0,
                            peak_process_tree_rss_mb=0.0,
                            throughput_nodes_per_s=0.0,
                            modularity_q=0.0,
                            num_communities=0,
                            num_levels=0,
                            epsilon=cfg.epsilon,
                            seed=cfg.seed,
                            converged=False,
                            status="failed",
                            error_message=str(exc),
                            level_times_json="[]",
                        )
                    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))
    return output_csv
