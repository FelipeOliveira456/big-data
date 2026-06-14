"""Tests for Parquet artifact validation."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from preprocessing.validate_artifact import validate_artifact
from preprocessing.write_artifact import write_graph_parquet


def test_validate_artifact_ok(tmp_path: Path):
    path = tmp_path / "ok.parquet"
    write_graph_parquet([(0, 1, 1.0), (1, 2, 1.0)], path, {"edge_count": 2})
    validate_artifact(path)


def test_validate_artifact_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        validate_artifact(tmp_path / "missing.parquet")


def test_validate_artifact_bad_schema(tmp_path: Path):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"src": [0], "dst": [1]}), path)
    with pytest.raises(ValueError, match="Schema mismatch"):
        validate_artifact(path)


def test_validate_artifact_src_not_less_than_dst(tmp_path: Path):
    path = tmp_path / "bad_edge.parquet"
    pq.write_table(
        pa.table({"src": [2], "dst": [1], "weight": [1.0]}),
        path,
    )
    with pytest.raises(ValueError, match="src=2 must be < dst=1"):
        validate_artifact(path)
