#!/usr/bin/env python3
"""Generate performance and clustering figures from benchmark artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

STAMP = "20260622T005654"
REPO = Path(__file__).resolve().parent.parent


def load_communities(path: Path) -> list[int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [c["size"] for c in payload["clusters"]]


def load_metrics(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _style_axes(ax: plt.Axes) -> None:
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def plot_cluster_rank_size(sizes: list[int], run_index: int, out: Path) -> None:
    ranked = sorted(sizes, reverse=True)
    ranks = np.arange(1, len(ranked) + 1)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.loglog(ranks, ranked, color="#2563eb", linewidth=1.6, marker=".", markersize=3)
    ax.set_xlabel("Rank da comunidade")
    ax.set_ylabel("Tamanho (nós, escala log)")
    ax.set_title(
        f"soc-Orkut — distribuição rank-size (run {run_index})\n"
        f"{len(ranked)} comunidades, 3,07M nós"
    )
    _style_axes(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_cluster_treemap_top(sizes: list[int], run_index: int, out: Path, top_k: int = 25) -> None:
    ranked = sorted(sizes, reverse=True)[:top_k]
    labels = [f"C{i+1}" for i in range(len(ranked))]
    colors = plt.cm.tab20(np.linspace(0, 1, len(ranked)))

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(range(len(ranked)), ranked, color=colors)
    ax.set_yticks(range(len(ranked)))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Nós na comunidade")
    ax.set_title(
        f"Top {top_k} comunidades — run {run_index}\n"
        f"(visualização agregada; grafo completo tem 3M+ nós)"
    )
    for bar, value in zip(bars, ranked, strict=True):
        pct = 100 * value / sum(sizes)
        ax.text(bar.get_width() + max(ranked) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{value:,} ({pct:.1f}%)", va="center", fontsize=8)
    _style_axes(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_cluster_cumulative(sizes: list[int], run_index: int, out: Path) -> None:
    ranked = sorted(sizes, reverse=True)
    cum_pct = 100 * np.cumsum(ranked) / sum(ranked)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(range(1, len(ranked) + 1), cum_pct, color="#059669", linewidth=2)
    ax.axhline(90, color="#94a3b8", linestyle=":", linewidth=1)
    ax.axhline(99, color="#94a3b8", linestyle=":", linewidth=1)
    idx_90 = int(np.searchsorted(cum_pct, 90))
    idx_99 = int(np.searchsorted(cum_pct, 99))
    ax.annotate(f"90% em {idx_90 + 1} comunidades", xy=(idx_90 + 1, 90),
                xytext=(idx_90 + 40, 82), arrowprops=dict(arrowstyle="->", color="#64748b"),
                fontsize=9)
    ax.annotate(f"99% em {idx_99 + 1} comunidades", xy=(idx_99 + 1, 99),
                xytext=(idx_99 + 40, 91), arrowprops=dict(arrowstyle="->", color="#64748b"),
                fontsize=9)
    ax.set_xlabel("Número de comunidades (maiores primeiro)")
    ax.set_ylabel("% dos nós acumulados")
    ax.set_title(f"Concentração da partição LPA — run {run_index}")
    ax.set_xlim(1, len(ranked))
    ax.set_ylim(0, 101)
    _style_axes(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_performance(metrics: list[dict[str, str]], out: Path) -> None:
    ray = [r for r in metrics if r["approach"] == "ray" and r["status"] == "success"]
    dask = [r for r in metrics if r["approach"] == "dask" and r["status"] == "success"]
    runs = sorted({int(r["run_index"]) for r in ray + dask})

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    metrics_spec = [
        ("algorithm_time_s", "Tempo do algoritmo (s)", "#2563eb", "#dc2626"),
        ("throughput_nodes_per_s", "Throughput (nós/s)", "#2563eb", "#dc2626"),
        ("peak_process_tree_rss_mb", "RSS total pico (MB)", "#2563eb", "#dc2626"),
    ]
    width = 0.35
    for ax, (field, ylabel, c_ray, c_dask) in zip(axes, metrics_spec, strict=True):
        x = np.arange(len(runs))
        ray_vals = [
            float(next(r for r in ray if int(r["run_index"]) == run)[field])
            if any(int(r["run_index"]) == run for r in ray) else 0
            for run in runs
        ]
        dask_vals = [
            float(next(r for r in dask if int(r["run_index"]) == run)[field])
            if any(int(r["run_index"]) == run for r in dask) else 0
            for run in runs
        ]
        ax.bar(x - width / 2, ray_vals, width, label="Ray", color=c_ray)
        ax.bar(x + width / 2, dask_vals, width, label="Dask", color=c_dask)
        ax.set_xticks(x)
        ax.set_xticklabels([f"Run {i}" for i in runs])
        ax.set_ylabel(ylabel)
        _style_axes(ax)
    axes[0].legend(loc="upper left")
    fig.suptitle("Ray vs Dask — soc-Orkut 100% (campanha 20260622T005654)", y=1.02)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_iteration_times(metrics: list[dict[str, str]], out: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    for approach, color in (("ray", "#2563eb"), ("dask", "#dc2626")):
        for row in metrics:
            if row["approach"] != approach or row["status"] != "success":
                continue
            times = json.loads(row["level_times_json"])
            label = f"{approach} run {row['run_index']} (seed {row['seed']})"
            ax.plot(range(1, len(times) + 1), times, color=color, alpha=0.55, linewidth=1,
                    label=label)
    ax.set_xlabel("Iteração LPA")
    ax.set_ylabel("Tempo por iteração (s)")
    ax.set_title("Custo por iteração — Ray estável ~6,5 s; Dask ~12–16 s nas primeiras dezenas")
    ax.legend(fontsize=8, ncol=2)
    _style_axes(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_convergence_proxy(log_path: Path, out: Path) -> None:
    import re

    pattern = re.compile(
        r"\[(ray|dask)\]\[iter=(\d+)\] changed=(\d+)/(\d+) .* elapsed=([\d.]+)s"
    )
    series: dict[str, tuple[list[int], list[float]]] = {}
    for line in log_path.read_text(encoding="utf-8").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        backend, iteration, changed, _total, elapsed = match.groups()
        key = backend
        if key not in series:
            series[key] = ([], [])
        series[key][0].append(int(iteration))
        series[key][1].append(int(changed))

    if not series:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = {"ray": "#2563eb", "dask": "#dc2626"}
    for backend, (iters, changed) in series.items():
        ax.plot(iters, changed, label=f"{backend} (1ª run bem-sucedida no log)", color=colors[backend])
    ax.set_yscale("log")
    ax.set_xlabel("Iteração")
    ax.set_ylabel("Nós que mudaram de rótulo (log)")
    ax.set_title("Convergência parcial — LPA não estabilizou em 100 iterações")
    ax.legend()
    _style_axes(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stamp", default=STAMP)
    parser.add_argument("--results-dir", type=Path, default=REPO / "results")
    args = parser.parse_args()

    reports = args.results_dir / "reports"
    partitions = reports / f"partitions_{args.stamp}"
    figures = args.results_dir / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    metrics_path = reports / f"metrics_raw_{args.stamp}.csv"
    log_path = reports / f"benchmark_run_{args.stamp}.log"
    metrics = load_metrics(metrics_path)

    for run in (1, 2, 3):
        comm_path = partitions / f"ray_orkut_100p0pct_run{run}.communities.json"
        sizes = load_communities(comm_path)
        plot_cluster_rank_size(sizes, run, figures / f"clusterization_run{run}_rank_size.png")
        plot_cluster_treemap_top(sizes, run, figures / f"clusterization_run{run}_top25.png")
        plot_cluster_cumulative(sizes, run, figures / f"clusterization_run{run}_cumulative.png")

    plot_performance(metrics, figures / "performance_comparison.png")
    plot_iteration_times(metrics, figures / "iteration_times.png")
    if log_path.is_file():
        plot_convergence_proxy(log_path, figures / "convergence_changed_nodes.png")

    print(f"Figures written to {figures}")


if __name__ == "__main__":
    main()
