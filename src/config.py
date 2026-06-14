"""Load configuration from YAML and environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class AppConfig:
    graph_raw_path: Path
    dataset_slug: str
    artifact_dir: Path
    reports_dir: Path
    seed: int
    epsilon: float
    ray_num_cpus: int | None
    ray_batch_size: int
    dask_n_workers: int | None
    ray_head_address: str | None
    dask_scheduler_address: str | None


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _optional_int(value: Any) -> int | None:
    if value is None or value == "" or value == "null":
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None or value == "" or value == "null":
        return None
    return str(value)


def load_config(config_path: Path | None = None) -> AppConfig:
    path = config_path or REPO_ROOT / "config.yaml"
    if not path.is_file():
        path = REPO_ROOT / "config.yaml.example"
    raw = _load_yaml(path)

    def get(key: str, default: Any) -> Any:
        env_key = key.upper()
        if env_key in os.environ:
            return os.environ[env_key]
        alt = {
            "graph_raw_path": "GRAPH_RAW_PATH",
            "dataset_slug": "DATASET_SLUG",
            "artifact_dir": "ARTIFACT_DIR",
            "seed": "SEED",
            "epsilon": "EPSILON",
            "dask_n_workers": "DASK_N_WORKERS",
            "ray_head_address": "RAY_HEAD_ADDRESS",
            "dask_scheduler_address": "DASK_SCHEDULER_ADDRESS",
        }.get(key)
        if alt and alt in os.environ:
            return os.environ[alt]
        return raw.get(key, default)

    return AppConfig(
        graph_raw_path=Path(get("graph_raw_path", "data/raw/email-Enron.txt")),
        dataset_slug=str(get("dataset_slug", "email-enron")),
        artifact_dir=Path(get("artifact_dir", "data/artifacts")),
        reports_dir=Path(get("reports_dir", "reports")),
        seed=int(get("seed", 42)),
        epsilon=float(get("epsilon", 1e-6)),
        ray_num_cpus=int(os.environ["RAY_NUM_CPUS"])
        if os.environ.get("RAY_NUM_CPUS")
        else None,
        ray_batch_size=int(get("ray_batch_size", 1000)),
        dask_n_workers=_optional_int(get("dask_n_workers", None)),
        ray_head_address=_optional_str(get("ray_head_address", None)),
        dask_scheduler_address=_optional_str(get("dask_scheduler_address", None)),
    )
