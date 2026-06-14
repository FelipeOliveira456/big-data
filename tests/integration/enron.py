"""Workspace and constants for email-Enron integration tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from benchmark.paths import comparison_md_path, metrics_csv_path
from config import REPO_ROOT

if TYPE_CHECKING:
    from config import AppConfig

ENRON_RAW_PATH = REPO_ROOT / "data" / "raw" / "email-Enron.txt"
OUTPUT_DIR = REPO_ROOT / "tests" / "integration" / "output"
LCC_NODE_COUNT = 33_696
SEED = 42
DATASET_SLUG = "email-enron"
INTEGRATION_FRACTIONS = (1, 5)


@dataclass
class EnronIntegrationWorkspace:
    """Integration workspace; writes under tests/integration/output/."""

    root: Path
    artifacts_dir: Path
    benchmark_dir: Path
    cfg: "AppConfig"
    raw_path: Path
    fraction_pct: int
    run_stamp: str

    @property
    def artifact_path(self) -> Path:
        return self.artifacts_dir / f"{DATASET_SLUG}_{self.fraction_pct}pct.parquet"

    @property
    def meta_path(self) -> Path:
        return self.artifact_path.with_suffix(".meta.json")

    @property
    def metrics_csv(self) -> Path:
        return metrics_csv_path(self.benchmark_dir, self.run_stamp)

    @property
    def comparison_md(self) -> Path:
        return comparison_md_path(self.benchmark_dir, self.run_stamp)


def load_meta(meta_path: Path) -> dict:
    return json.loads(meta_path.read_text(encoding="utf-8"))
