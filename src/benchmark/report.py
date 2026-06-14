"""Generate Markdown comparison report from metrics CSV."""

from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path

from benchmark.report_sections import RACE_CONDITION_RAY, REFERENCES

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def _loc_count(glob_paths: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for pattern in glob_paths:
        for path in REPO_ROOT.glob(pattern):
            if path.suffix == ".py":
                counts[str(path.relative_to(REPO_ROOT))] = sum(
                    1
                    for line in path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                )
    return counts


def generate_report(input_csv: Path, output_md: Path) -> Path:
    rows: list[dict[str, str]] = []
    with input_csv.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    success = [r for r in rows if r.get("status") == "success"]
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in success:
        groups[(r["approach"], r["fraction_pct"])].append(r)

    lines = [
        "# Relatório comparativo: Ray vs Dask",
        "",
        "## Resumo",
        "",
        "Comparação de Louvain distribuído no dataset email-Enron (artefatos Parquet compartilhados).",
        "",
        "## Desempenho (média ± desvio)",
        "",
        "Memória RSS = driver + workers (filhos do processo). "
        "`peak_memory_mb` = heap Python (tracemalloc, só driver).",
        "",
        "| Abordagem | Fração % | Tempo algo (s) | RSS total (MB) | RSS driver (MB) | Throughput (nós/s) | Q |",
        "|-----------|----------|----------------|----------------|-----------------|---------------------|---|",
    ]

    for (approach, frac), items in sorted(groups.items()):
        times = [float(x["algorithm_time_s"]) for x in items]
        tree_mem = [
            float(x.get("peak_process_tree_rss_mb") or x.get("peak_memory_mb", 0))
            for x in items
        ]
        driver_mem = [
            float(x.get("peak_driver_rss_mb") or x.get("peak_memory_mb", 0))
            for x in items
        ]
        thr = [float(x["throughput_nodes_per_s"]) for x in items]
        qs = [float(x["modularity_q"]) for x in items]
        lines.append(
            f"| {approach} | {frac} | "
            f"{statistics.mean(times):.2f} ± {statistics.pstdev(times) if len(times) > 1 else 0:.2f} | "
            f"{statistics.mean(tree_mem):.0f} ± {statistics.pstdev(tree_mem) if len(tree_mem) > 1 else 0:.0f} | "
            f"{statistics.mean(driver_mem):.0f} ± {statistics.pstdev(driver_mem) if len(driver_mem) > 1 else 0:.0f} | "
            f"{statistics.mean(thr):.0f} ± {statistics.pstdev(thr) if len(thr) > 1 else 0:.0f} | "
            f"{statistics.mean(qs):.4f} ± {statistics.pstdev(qs) if len(qs) > 1 else 0:.4f} |"
        )

    lines.extend(["", "## Qualidade", ""])
    for (approach, frac), items in sorted(groups.items()):
        qs = [float(x["modularity_q"]) for x in items]
        comms = [int(x["num_communities"]) for x in items]
        lines.append(
            f"- **{approach} {frac}%**: Q médio={statistics.mean(qs):.4f}, "
            f"comunidades≈{statistics.mean(comms):.0f}"
        )

    ray_loc = sum(_loc_count(["src/ray_impl/**/*.py"]).values())
    dask_loc = sum(_loc_count(["src/dask_impl/**/*.py"]).values())
    lines.extend(
        [
            "",
            "## Engenharia",
            "",
            f"- Linhas (aprox.) Ray: {ray_loc}",
            f"- Linhas (aprox.) Dask: {dask_loc}",
            "- Infra Ray: Python + pip",
            "- Infra Dask: Python + dask[distributed]",
            "",
            RACE_CONDITION_RAY,
            "",
            REFERENCES,
        ]
    )

    failed = [r for r in rows if r.get("status") != "success"]
    if failed:
        lines.extend(["", "## Execuções falhadas", ""])
        for r in failed:
            lines.append(
                f"- {r['approach']} {r['fraction_pct']}% run {r['run_index']}: {r.get('error_message', '')}"
            )

    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_md
