"""Tests for Ray cluster vs local init."""

from pathlib import Path
from unittest.mock import patch

from ray_impl.louvain_ray import run_louvain_ray


def test_run_louvain_ray_local_init(tiny_artifact: Path):
    with patch("ray_impl.louvain_ray.ray.is_initialized", return_value=False), patch(
        "ray_impl.louvain_ray.ray.init"
    ) as mock_init, patch("ray_impl.louvain_ray._phase1_ray"), patch(
        "ray_impl.louvain_ray.LouvainHierarchy"
    ) as mock_hierarchy:
        mock_hierarchy.from_graph.return_value.modularity_on_original.return_value = 0.1
        mock_hierarchy.from_graph.return_value.orig_partition.side_effect = (
            lambda p: p
        )
        mock_hierarchy.from_graph.return_value.compress_level.side_effect = (
            lambda p, g: (g, p)
        )
        run_louvain_ray(tiny_artifact, num_cpus=2)
        mock_init.assert_called_once_with(num_cpus=2, ignore_reinit_error=True)


def test_run_louvain_ray_cluster_init(tiny_artifact: Path):
    with patch("ray_impl.louvain_ray.ray.is_initialized", return_value=False), patch(
        "ray_impl.louvain_ray.ray.init"
    ) as mock_init, patch("ray_impl.louvain_ray._phase1_ray"), patch(
        "ray_impl.louvain_ray.LouvainHierarchy"
    ) as mock_hierarchy:
        mock_hierarchy.from_graph.return_value.modularity_on_original.return_value = 0.1
        mock_hierarchy.from_graph.return_value.orig_partition.side_effect = (
            lambda p: p
        )
        mock_hierarchy.from_graph.return_value.compress_level.side_effect = (
            lambda p, g: (g, p)
        )
        run_louvain_ray(tiny_artifact, ray_head_address="10.0.0.5")
        mock_init.assert_called_once_with(
            address="ray://10.0.0.5:10001",
            ignore_reinit_error=True,
        )
