#!/usr/bin/env python3
"""Generate performance figures from benchmark artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

MAIN_STAMP = "20260622T005654"
DASK_RUN1_STAMP = "20260622T030138"
REPO = Path(__file__).resolve().parent.parent


def load_metrics(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def merge_metrics_for_plot(
    main_csv: Path,
    dask_run1_csv: Path | None,
) -> tuple[list[dict[str, str]], dict[int, str]]:
    """Ray + Dask runs 2–3 from main campaign; Dask run 1 from isolated success."""
    metrics = load_metrics(main_csv)
    notes: dict[int, str] = {}

    if dask_run1_csv and dask_run1_csv.is_file():
        isolated = [
            r
            for r in load_metrics(dask_run1_csv)
            if r["approach"] == "dask" and r["status"] == "success"
        ]
        if isolated:
            metrics = [
                r
                for r in metrics
                if not (r["approach"] == "dask" and int(r["run_index"]) == 1)
            ] + isolated
            notes[1] = (
                "Dask run 1: campanha isolada "
                f"({dask_run1_csv.stem.removeprefix('metrics_raw_')})"
            )

    failed_dask = [
        r
        for r in load_metrics(main_csv)
        if r["approach"] == "dask"
        and int(r["run_index"]) == 1
        and r["status"] != "success"
    ]
    if failed_dask:
        notes.setdefault(
            1,
            notes.get(1, "")
            + " · 2 tentativas falharam por OOM na campanha mista (005654, 024351)",
        )

    return metrics, notes


def _style_axes(ax: plt.Axes) -> None:
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def plot_performance(
    metrics: list[dict[str, str]],
    out: Path,
    notes: dict[int, str],
) -> None:
    ray = [r for r in metrics if r["approach"] == "ray" and r["status"] == "success"]
    dask = [r for r in metrics if r["approach"] == "dask" and r["status"] == "success"]
    runs = [1, 2, 3]

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    metrics_spec = [
        ("algorithm_time_s", "Tempo do algoritmo (s)"),
        ("throughput_nodes_per_s", "Throughput (nós/s)"),
        ("peak_process_tree_rss_mb", "RSS total pico (MB)"),
    ]
    width = 0.35
    for ax, (field, ylabel) in zip(axes, metrics_spec, strict=True):
        x = np.arange(len(runs))
        ray_vals = [
            float(next(r for r in ray if int(r["run_index"]) == run)[field])
            for run in runs
        ]
        dask_vals = [
            float(next(r for r in dask if int(r["run_index"]) == run)[field])
            for run in runs
        ]
        ray_bars = ax.bar(
            x - width / 2, ray_vals, width, label="Ray", color="#2563eb"
        )
        dask_bars = ax.bar(
            x + width / 2,
            dask_vals,
            width,
            label="Dask",
            color="#dc2626",
        )
        if 1 in notes:
            dask_bars[0].set_hatch("///")
            dask_bars[0].set_edgecolor("#991b1b")
        ax.set_xticks(x)
        ax.set_xticklabels([f"Run {i}" for i in runs])
        ax.set_ylabel(ylabel)
        _style_axes(ax)

    axes[0].legend(loc="upper left")
    title = "Ray vs Dask — soc-Orkut 100%"
    if notes.get(1):
        title += f"\n{notes[1]}"
    fig.suptitle(title, y=1.05, fontsize=11)
    fig.tight_layout()
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_iteration_times(metrics: list[dict[str, str]], out: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    for approach, color in (("ray", "#2563eb"), ("dask", "#dc2626")):
        for row in sorted(
            [r for r in metrics if r["approach"] == approach and r["status"] == "success"],
            key=lambda r: int(r["run_index"]),
        ):
            times = json.loads(row["level_times_json"])
            run = int(row["run_index"])
            extra = " (isolada)" if approach == "dask" and run == 1 else ""
            label = f"{approach} run {run}{extra} (seed {row['seed']})"
            linestyle = "--" if approach == "dask" and run == 1 else "-"
            ax.plot(
                range(1, len(times) + 1),
                times,
                color=color,
                alpha=0.7,
                linewidth=1,
                linestyle=linestyle,
                label=label,
            )
    ax.set_xlabel("Iteração LPA")
    ax.set_ylabel("Tempo por iteração (s)")
    ax.set_title("Custo por iteração — Ray ~6,5 s; Dask ~12–16 s")
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
    series: dict[str, tuple[list[int], list[int]]] = {}
    for line in log_path.read_text(encoding="utf-8").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        backend, iteration, changed, _total, _elapsed = match.groups()
        if backend not in series:
            series[backend] = ([], [])
        series[backend][0].append(int(iteration))
        series[backend][1].append(int(changed))

    if not series:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = {"ray": "#2563eb", "dask": "#dc2626"}
    for backend, (iters, changed) in series.items():
        ax.plot(
            iters,
            changed,
            label=f"{backend} (1ª run no log da campanha mista)",
            color=colors[backend],
        )
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
    parser.add_argument("--stamp", default=MAIN_STAMP)
    parser.add_argument("--dask-run1-stamp", default=DASK_RUN1_STAMP)
    parser.add_argument("--results-dir", type=Path, default=REPO / "results")
    args = parser.parse_args()

    reports = args.results_dir / "reports"
    figures = args.results_dir / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    main_csv = reports / f"metrics_raw_{args.stamp}.csv"
    dask_run1_csv = reports / f"metrics_raw_{args.dask_run1_stamp}.csv"
    log_path = reports / f"benchmark_run_{args.stamp}.log"

    metrics, notes = merge_metrics_for_plot(main_csv, dask_run1_csv)
    plot_performance(metrics, figures / "performance_comparison.png", notes)
    plot_iteration_times(metrics, figures / "iteration_times.png")
    if log_path.is_file():
        plot_convergence_proxy(log_path, figures / "convergence_changed_nodes.png")

    print(f"Figures written to {figures}")


if __name__ == "__main__":
    main()
