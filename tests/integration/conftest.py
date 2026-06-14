"""Integration test fixtures (isolated from unit conftest)."""

from __future__ import annotations

from pathlib import Path

import pytest

from benchmark.paths import benchmark_run_stamp, write_run_stamp
from config import AppConfig
from tests.integration.enron import (
    DATASET_SLUG,
    ENRON_RAW_PATH,
    OUTPUT_DIR,
    SEED,
    EnronIntegrationWorkspace,
)


@pytest.fixture
def enron_raw_path() -> Path:
    if not ENRON_RAW_PATH.is_file():
        pytest.skip(f"Missing {ENRON_RAW_PATH} — run: bash scripts/download_dataset.sh")
    return ENRON_RAW_PATH


@pytest.fixture
def enron_integration_workspace(
    enron_raw_path: Path, request: pytest.FixtureRequest
) -> EnronIntegrationWorkspace:
    fraction_pct = request.param
    run_stamp = benchmark_run_stamp()
    root = OUTPUT_DIR
    artifacts_dir = root / "artifacts"
    benchmark_dir = root / "benchmark"
    for path in (artifacts_dir, benchmark_dir):
        path.mkdir(parents=True, exist_ok=True)
    write_run_stamp(benchmark_dir, run_stamp)

    cfg = AppConfig(
        graph_raw_path=enron_raw_path,
        dataset_slug=DATASET_SLUG,
        artifact_dir=artifacts_dir,
        reports_dir=benchmark_dir,
        seed=SEED,
        epsilon=1e-6,
        ray_num_cpus=2,
        ray_batch_size=500,
        dask_n_workers=2,
        ray_head_address=None,
        dask_scheduler_address=None,
    )
    return EnronIntegrationWorkspace(
        root=root,
        artifacts_dir=artifacts_dir,
        benchmark_dir=benchmark_dir,
        cfg=cfg,
        raw_path=enron_raw_path,
        fraction_pct=fraction_pct,
        run_stamp=run_stamp,
    )
