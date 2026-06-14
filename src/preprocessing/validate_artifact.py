"""Validate graph Parquet artifacts against contract schema."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

REQUIRED_COLUMNS = {"src", "dst", "weight"}


def validate_artifact(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Artifact not found: {path}")

    schema = pq.read_schema(path)
    names = set(schema.names)
    missing = REQUIRED_COLUMNS - names
    if missing:
        raise ValueError(f"Schema mismatch: missing columns {missing}")

    table = pq.read_table(path, columns=["src", "dst"])
    src = table.column("src").to_pylist()
    dst = table.column("dst").to_pylist()
    for s, d in zip(src, dst, strict=True):
        if s >= d:
            raise ValueError(f"Invalid edge: src={s} must be < dst={d}")
        if s == d:
            raise ValueError(f"Self-loop: {s}")
