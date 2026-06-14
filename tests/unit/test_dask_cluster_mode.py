"""Tests for Dask cluster vs LocalCluster init."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from config import AppConfig
from dask_impl.louvain_dask import run_louvain_dask


def _cfg(**kwargs) -> AppConfig:
    defaults = dict(
        graph_raw_path=Path("data/raw/email-Enron.txt"),
        dataset_slug="email-enron",
        artifact_dir=Path("data/artifacts"),
        reports_dir=Path("reports"),
        seed=42,
        epsilon=1e-6,
        ray_num_cpus=None,
        ray_batch_size=500,
        dask_n_workers=None,
        ray_head_address=None,
        dask_scheduler_address=None,
    )
    defaults.update(kwargs)
    return AppConfig(**defaults)


def test_run_louvain_dask_local_cluster(tiny_artifact: Path):
    mock_client = MagicMock()
    mock_cluster = MagicMock()
    with patch(
        "dask_impl.louvain_dask.LocalCluster", return_value=mock_cluster
    ) as mock_lc, patch(
        "dask_impl.louvain_dask.Client", return_value=mock_client
    ), patch("dask_impl.louvain_dask._phase1_dask"), patch(
        "dask_impl.louvain_dask.LouvainHierarchy"
    ) as mock_hierarchy:
        mock_hierarchy.from_graph.return_value.modularity_on_original.return_value = 0.1
        mock_hierarchy.from_graph.return_value.orig_partition.side_effect = (
            lambda p: p
        )
        mock_hierarchy.from_graph.return_value.compress_level.side_effect = (
            lambda p, g: (g, p)
        )
        run_louvain_dask(tiny_artifact, cfg=_cfg())
        mock_lc.assert_called_once()
        assert mock_lc.call_args.kwargs["n_workers"] is None
        mock_client.close.assert_called_once()
        mock_cluster.close.assert_called_once()


def test_run_louvain_dask_remote_scheduler(tiny_artifact: Path):
    mock_client = MagicMock()
    with patch("dask_impl.louvain_dask.LocalCluster") as mock_lc, patch(
        "dask_impl.louvain_dask.Client", return_value=mock_client
    ) as mock_client_cls, patch("dask_impl.louvain_dask._phase1_dask"), patch(
        "dask_impl.louvain_dask.LouvainHierarchy"
    ) as mock_hierarchy:
        mock_hierarchy.from_graph.return_value.modularity_on_original.return_value = 0.1
        mock_hierarchy.from_graph.return_value.orig_partition.side_effect = (
            lambda p: p
        )
        mock_hierarchy.from_graph.return_value.compress_level.side_effect = (
            lambda p, g: (g, p)
        )
        run_louvain_dask(
            tiny_artifact,
            cfg=_cfg(dask_scheduler_address="10.0.0.5:8786"),
        )
        mock_lc.assert_not_called()
        mock_client_cls.assert_called_once_with("tcp://10.0.0.5:8786")
