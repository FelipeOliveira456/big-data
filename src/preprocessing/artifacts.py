"""Shared graph artifact naming."""

from __future__ import annotations

from pathlib import Path


def artifact_stem(dataset_slug: str, fraction_pct: int) -> str:
    return f"{dataset_slug}_{fraction_pct}pct"


def artifact_path(output_dir: Path, dataset_slug: str, fraction_pct: int) -> Path:
    return output_dir / f"{artifact_stem(dataset_slug, fraction_pct)}.parquet"
