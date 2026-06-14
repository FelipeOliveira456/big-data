"""Full email-Enron integration (BFS subgraph + LCC, Ray + Dask).

Pipeline: preprocess → validate → benchmark → report.
Outputs persist under tests/integration/output/ with timestamped CSV/MD.
"""

from __future__ import annotations

import csv

import pytest

from benchmark.report import generate_report
from benchmark.runner import run_benchmark_campaign
from preprocessing.load_snap import iter_normalized_edges
from preprocessing.pipeline import run_preprocess
from preprocessing.sample_lcc import (
    collect_lcc_node_ids,
    sample_connected_node_ids,
)
from preprocessing.validate_artifact import validate_artifact
from preprocessing.write_artifact import read_edges_parquet
from tests.integration.enron import (
    INTEGRATION_FRACTIONS,
    LCC_NODE_COUNT,
    SEED,
    EnronIntegrationWorkspace,
    load_meta,
)


def _assert_bfs_subgraph(ws: EnronIntegrationWorkspace) -> None:
    """Artifact nodes must come from BFS sampling on the full LCC."""
    edges = read_edges_parquet(ws.artifact_path)
    nodes_in_graph = {s for s, _, _ in edges} | {d for _, d, _ in edges}
    lcc_nodes = collect_lcc_node_ids(ws.raw_path)
    all_edges = list(iter_normalized_edges(ws.raw_path))
    sampled = sample_connected_node_ids(
        all_edges, lcc_nodes, ws.fraction_pct, SEED
    )

    assert nodes_in_graph <= sampled
    assert all(s in nodes_in_graph and d in nodes_in_graph for s, d, _ in edges)
    assert len(nodes_in_graph) >= len(sampled) * 0.9


def _assert_memory_metrics(rows: list[dict[str, str]], fraction_pct: int) -> None:
    """RSS must exceed tracemalloc; distributed runs use hundreds of MB."""
    for row in rows:
        heap_mb = float(row["peak_memory_mb"])
        driver_rss = float(row["peak_driver_rss_mb"])
        tree_rss = float(row["peak_process_tree_rss_mb"])
        assert tree_rss >= driver_rss >= heap_mb
        assert tree_rss > 100.0, (
            f"{row['approach']} {fraction_pct}%: expected RSS > 100 MB, got {tree_rss:.0f}"
        )


@pytest.mark.integration
@pytest.mark.parametrize("enron_integration_workspace", INTEGRATION_FRACTIONS, indirect=True)
def test_enron_fraction_full_pipeline(
    enron_integration_workspace: EnronIntegrationWorkspace,
):
    ws = enron_integration_workspace
    fraction = ws.fraction_pct

    written = run_preprocess(
        ws.raw_path,
        ws.artifacts_dir,
        seed=SEED,
        fractions=[fraction],
        dataset_slug=ws.cfg.dataset_slug,
    )
    assert written == [ws.artifact_path]
    assert ws.artifact_path.parent == ws.artifacts_dir
    assert ws.meta_path.is_file()

    validate_artifact(ws.artifact_path)
    meta = load_meta(ws.meta_path)
    assert meta["fraction_pct"] == fraction
    assert meta["seed"] == SEED
    expected_nodes = max(1, int(LCC_NODE_COUNT * fraction / 100))
    assert meta["node_count"] >= expected_nodes * 0.9
    assert meta["edge_count"] >= 50
    _assert_bfs_subgraph(ws)

    run_benchmark_campaign(
        ws.artifacts_dir,
        ws.metrics_csv,
        runs=1,
        fractions=[fraction],
        cfg=ws.cfg,
    )
    assert ws.metrics_csv.is_file()
    assert ws.run_stamp in ws.metrics_csv.name
    with ws.metrics_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    approaches = {r["approach"] for r in rows}
    assert approaches == {"ray", "dask"}
    assert all(r["status"] == "success" for r in rows)
    assert all(r["fraction_pct"] == str(fraction) for r in rows)
    assert all(float(r["modularity_q"]) > 0 for r in rows)
    assert all(int(r["num_communities"]) >= 1 for r in rows)
    _assert_memory_metrics(rows, fraction)

    generate_report(ws.metrics_csv, ws.comparison_md)
    assert ws.comparison_md.is_file()
    assert ws.run_stamp in ws.comparison_md.name
    body = ws.comparison_md.read_text(encoding="utf-8")
    assert "ray" in body.lower()
    assert "dask" in body.lower()

    assert ws.root in ws.artifact_path.parents
    assert ws.root in ws.metrics_csv.parents
    assert ws.root in ws.comparison_md.parents
