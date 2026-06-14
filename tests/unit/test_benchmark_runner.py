"""Benchmark runner tests with mocked backends."""

from pathlib import Path
from unittest.mock import patch

from benchmark.runner import BenchmarkRow, _node_count, run_benchmark_campaign
from config import AppConfig


def test_node_count(tiny_artifact: Path):
    assert _node_count(tiny_artifact) == 6


def test_run_benchmark_campaign_ray_only(tmp_path: Path, tiny_artifact: Path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    artifact = artifacts_dir / "email-enron_100pct.parquet"
    artifact.write_bytes(tiny_artifact.read_bytes())

    def _row(approach: str) -> BenchmarkRow:
        return BenchmarkRow(
            approach=approach,
            fraction_pct=100,
            run_index=0,
            init_time_s=0.1,
            algorithm_time_s=0.2,
            peak_memory_mb=1.0,
            peak_driver_rss_mb=10.0,
            peak_process_tree_rss_mb=100.0,
            throughput_nodes_per_s=15.0,
            modularity_q=0.4,
            num_communities=2,
            num_levels=1,
            epsilon=1e-6,
            seed=42,
            converged=True,
            status="success",
            error_message="",
            level_times_json="[0.2]",
        )

    out_csv = tmp_path / "metrics.csv"
    cfg = AppConfig(
        graph_raw_path=Path("data/raw/email-Enron.txt"),
        dataset_slug="email-enron",
        artifact_dir=artifacts_dir,
        reports_dir=tmp_path,
        seed=42,
        epsilon=1e-6,
        ray_num_cpus=1,
        ray_batch_size=10,
        dask_n_workers=2,
        ray_head_address=None,
        dask_scheduler_address=None,
    )

    with (
        patch("benchmark.runner._run_ray", lambda a, c: _row("ray")),
        patch("benchmark.runner._run_dask", lambda a, c: _row("dask")),
    ):
        path = run_benchmark_campaign(
            artifacts_dir,
            out_csv,
            runs=1,
            fractions=[100],
            cfg=cfg,
        )

    text = path.read_text(encoding="utf-8")
    assert "ray" in text
    assert "dask" in text
    assert "success" in text
