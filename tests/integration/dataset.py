"""Workspace and constants for soc-Pokec integration tests (0.1% fixture)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from benchmark.paths import comparison_md_path, metrics_csv_path, run_log_path
from config import REPO_ROOT

if TYPE_CHECKING:
    from config import AppConfig

FIXTURE_PATH = REPO_ROOT / "tests" / "integration" / "fixtures" / "pokec_0p1pct.npz"
OUTPUT_DIR = REPO_ROOT / "tests" / "integration" / "output"
SEED = 42
DATASET_SLUG = "pokec"
FRACTION_PCT = 0.1
# From pokec_0p1pct fixture (seed=42, BFS 0.1% on soc-Pokec LCC)
FIXTURE_NODE_COUNT = 1_632
FIXTURE_EDGE_COUNT = 3_910


@dataclass
class IntegrationWorkspace:
    """Integration workspace; writes under tests/integration/output/."""

    root: Path
    benchmark_dir: Path
    cfg: "AppConfig"
    graph_path: Path
    fraction_pct: float
    run_stamp: str

    @property
    def metrics_csv(self) -> Path:
        return metrics_csv_path(self.benchmark_dir, self.run_stamp)

    @property
    def comparison_md(self) -> Path:
        return comparison_md_path(self.benchmark_dir, self.run_stamp)

    @property
    def run_log(self) -> Path:
        return run_log_path(self.benchmark_dir, self.run_stamp)
