"""Tests for configuration loading."""

from pathlib import Path

from config import load_config


def test_load_config_defaults():
    cfg = load_config(Path("/nonexistent/config.yaml"))
    assert cfg.seed == 42
    assert cfg.epsilon == 1e-6
    assert cfg.ray_batch_size == 1000
    assert cfg.artifact_dir == Path("data/artifacts")
    assert cfg.dataset_slug == "email-enron"
    assert cfg.dask_n_workers is None
    assert cfg.ray_head_address is None
    assert cfg.dask_scheduler_address is None


def test_load_config_cluster_env(tmp_path: Path, monkeypatch):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("seed: 42\n", encoding="utf-8")
    monkeypatch.setenv("RAY_HEAD_ADDRESS", "10.0.0.5")
    monkeypatch.setenv("DASK_SCHEDULER_ADDRESS", "10.0.0.5:8786")
    monkeypatch.setenv("DASK_N_WORKERS", "")
    cfg = load_config(yaml_path)
    assert cfg.ray_head_address == "10.0.0.5"
    assert cfg.dask_scheduler_address == "10.0.0.5:8786"
    assert cfg.dask_n_workers is None


def test_load_config_from_yaml(tmp_path: Path):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        "seed: 7\nepsilon: 0.001\nartifact_dir: custom/artifacts\n",
        encoding="utf-8",
    )
    cfg = load_config(yaml_path)
    assert cfg.seed == 7
    assert cfg.epsilon == 0.001
    assert cfg.artifact_dir == Path("custom/artifacts")


def test_load_config_env_override(tmp_path: Path, monkeypatch):
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("seed: 7\n", encoding="utf-8")
    monkeypatch.setenv("SEED", "99")
    cfg = load_config(yaml_path)
    assert cfg.seed == 99
