"""Ray Louvain on tiny Parquet fixture (local Ray)."""

from pathlib import Path

import pytest

from ray_impl.louvain_ray import run_louvain_ray

ray = pytest.importorskip("ray")


def test_run_louvain_ray_tiny_graph(tiny_artifact: Path):
    if ray.is_initialized():
        ray.shutdown()
    try:
        res = run_louvain_ray(
            tiny_artifact,
            epsilon=1e-6,
            batch_size=4,
            num_cpus=1,
        )
    finally:
        if ray.is_initialized():
            ray.shutdown()
    assert res.num_levels >= 1
    assert res.num_communities >= 1
    assert res.algorithm_time_s > 0
