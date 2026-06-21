"""Benchmark runner tests with mocked backends."""

from pathlib import Path
from unittest.mock import patch

import numpy as np

from benchmark.runner import BenchmarkRow, run_benchmark_campaign
from config import AppConfig
from graph.graph import Graph
from lpa_core.lpa import LpaResult
from preprocessing.load_graph import GraphLoadResult


def _loaded(graph: Graph, fraction_pct: float = 100.0) -> GraphLoadResult:
    return GraphLoadResult(
        graph=graph,
        load_time_s=0.1,
        node_count=graph.num_nodes,
        edge_count=int(graph.m),
        fraction_pct=fraction_pct,
    )


def test_run_benchmark_campaign_ray_only(tmp_path: Path, tiny_graph: Graph):
    raw = tmp_path / "edges.txt"
    raw.write_text("0 1\n1 2\n0 2\n", encoding="utf-8")

    def _row(approach: str) -> BenchmarkRow:
        return BenchmarkRow(
            approach=approach,
            fraction_pct=100,
            run_index=0,
            node_count=6,
            graph_load_time_s=0.1,
            init_time_s=0.1,
            algorithm_time_s=0.2,
            total_time_s=0.3,
            peak_memory_mb=1.0,
            peak_driver_rss_mb=10.0,
            peak_process_tree_rss_mb=100.0,
            throughput_nodes_per_s=15.0,
            num_communities=2,
            num_levels=3,
            max_iter=50.0,
            seed=42,
            converged=True,
            status="success",
            error_message="",
            level_times_json="[0.2]",
            partition_summary="",
            communities_json="",
            vm_peaks_json="{}",
            peak_cluster_rss_mb=0.0,
        )

    out_csv = tmp_path / "metrics.csv"
    cfg = AppConfig(
        graph_raw_path=raw,
        dataset_slug="pokec",
        reports_dir=tmp_path,
        seed=42,
        lpa_max_iter=50,
        lpa_chunk_divisor=12,
        ray_num_cpus=1,
        dask_n_workers=2,
        ray_head_address=None,
        dask_scheduler_address=None,
    )

    with (
        patch(
            "benchmark.runner.load_graph_from_snap",
            return_value=_loaded(tiny_graph),
        ),
        patch(
            "benchmark.runner._run_approach",
            side_effect=lambda approach, *a, **k: _row(approach),
        ),
    ):
        path = run_benchmark_campaign(
            raw,
            out_csv,
            runs=1,
            fractions=[100],
            cfg=cfg,
        )

    text = path.read_text(encoding="utf-8")
    assert "ray" in text
    assert "dask" in text
    assert "success" in text
    assert "graph_load_time_s" in text.splitlines()[0]


def test_run_benchmark_campaign_filtered_approaches(tmp_path: Path, tiny_graph: Graph):
    raw = tmp_path / "edges.txt"
    raw.write_text("0 1\n1 2\n0 2\n", encoding="utf-8")
    cfg = AppConfig(
        graph_raw_path=raw,
        dataset_slug="pokec",
        reports_dir=tmp_path,
        seed=42,
        lpa_max_iter=50,
        lpa_chunk_divisor=12,
        ray_num_cpus=1,
        dask_n_workers=2,
        ray_head_address=None,
        dask_scheduler_address=None,
    )

    fake = LpaResult(
        num_communities=2,
        num_levels=2,
        init_time_s=0.1,
        algorithm_time_s=0.2,
        level_times_s=[0.2],
        converged=True,
        node_count=6,
        labels={0: 0, 1: 0, 2: 1, 3: 1, 4: 1, 5: 1},
        partition_node_ids=np.array([0, 1, 2, 3, 4, 5], dtype=np.int64),
        partition_labels=np.array([0, 0, 1, 1, 1, 1], dtype=np.int64),
    )

    with (
        patch(
            "benchmark.runner.load_graph_from_snap",
            return_value=_loaded(tiny_graph),
        ),
        patch("benchmark.runner.run_lpa_ray", return_value=fake),
    ):
        path = run_benchmark_campaign(
            raw,
            tmp_path / "ray_only.csv",
            runs=1,
            fractions=[100],
            cfg=cfg,
            approaches=["ray"],
            run_stamp="20260101T120000",
        )
    text = path.read_text(encoding="utf-8")
    assert "ray" in text
    assert "dask" not in text.splitlines()[1:]
    part_dir = tmp_path / "partitions_20260101T120000"
    assert part_dir.is_dir()
    assert list(part_dir.glob("ray_*.communities.json"))
    assert list(part_dir.glob("ray_*.summary.json"))
