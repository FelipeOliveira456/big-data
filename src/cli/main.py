"""CLI entrypoint for distributed Louvain pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from benchmark.paths import (
    benchmark_run_stamp,
    metrics_csv_path,
    resolve_report_paths,
    write_run_stamp,
)
from benchmark.report import generate_report
from config import load_config
from preprocessing.pipeline import run_preprocess
from preprocessing.validate_artifact import validate_artifact
from ray_impl.louvain_ray import run_louvain_ray


def _parse_fractions(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def cmd_preprocess(args: argparse.Namespace) -> int:
    cfg = load_config()
    input_path = Path(args.input or cfg.graph_raw_path)
    output_dir = Path(args.output_dir or cfg.artifact_dir)
    fractions = _parse_fractions(args.fractions)
    if not input_path.is_file():
        print(f"ERROR: input file not found: {input_path}", file=sys.stderr)
        return 1
    paths = run_preprocess(
        input_path,
        output_dir,
        args.seed or cfg.seed,
        fractions,
        dataset_slug=cfg.dataset_slug,
    )
    for p in paths:
        validate_artifact(p)
        print(f"Wrote {p}", flush=True)
    return 0


def cmd_louvain_ray(args: argparse.Namespace) -> int:
    cfg = load_config()
    validate_artifact(Path(args.artifact))
    res = run_louvain_ray(
        args.artifact,
        epsilon=args.epsilon or cfg.epsilon,
        batch_size=args.batch_size or cfg.ray_batch_size,
        num_cpus=args.num_cpus or cfg.ray_num_cpus,
    )
    print(json.dumps(res.__dict__, indent=2))
    return 0


def cmd_louvain_dask(args: argparse.Namespace) -> int:
    from dask_impl.louvain_dask import run_louvain_dask

    cfg = load_config()
    validate_artifact(Path(args.artifact))
    res = run_louvain_dask(
        args.artifact,
        epsilon=args.epsilon or cfg.epsilon,
        batch_size=args.batch_size or cfg.ray_batch_size,
        n_workers=args.n_workers or cfg.dask_n_workers,
        cfg=cfg,
    )
    print(json.dumps(res.__dict__, indent=2))
    return 0


def cmd_benchmark(args: argparse.Namespace) -> int:
    from benchmark.runner import run_benchmark_campaign

    cfg = load_config()
    reports_dir = Path(cfg.reports_dir)
    stamp = benchmark_run_stamp()
    out = (
        Path(args.output_csv)
        if args.output_csv
        else metrics_csv_path(reports_dir, stamp)
    )
    if not args.output_csv:
        write_run_stamp(reports_dir, stamp)
    run_benchmark_campaign(
        Path(args.artifacts_dir or cfg.artifact_dir),
        out,
        runs=args.runs,
        fractions=_parse_fractions(args.fractions),
        cfg=cfg,
    )
    print(f"Wrote {out}")
    print(f"Run stamp: {stamp}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    cfg = load_config()
    reports_dir = Path(cfg.reports_dir)
    input_csv = Path(args.input_csv) if args.input_csv else None
    output_md = Path(args.output_md) if args.output_md else None
    csv_path, md_path = resolve_report_paths(reports_dir, input_csv, output_md)
    md = generate_report(csv_path, md_path)
    print(f"Wrote {md}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed Louvain on SNAP graphs")
    sub = parser.add_subparsers(dest="command", required=True)

    p_pre = sub.add_parser("preprocess", help="SNAP TXT → Parquet artifacts")
    p_pre.add_argument("--input", type=str, default=None)
    p_pre.add_argument("--output-dir", type=str, default=None)
    p_pre.add_argument("--seed", type=int, default=None)
    p_pre.add_argument("--fractions", type=str, default="100")
    p_pre.set_defaults(func=cmd_preprocess)

    p_ray = sub.add_parser("louvain-ray", help="Run Ray Louvain")
    p_ray.add_argument("--artifact", type=str, required=True)
    p_ray.add_argument("--epsilon", type=float, default=None)
    p_ray.add_argument("--batch-size", type=int, default=None)
    p_ray.add_argument("--num-cpus", type=int, default=None)
    p_ray.set_defaults(func=cmd_louvain_ray)

    p_dask = sub.add_parser("louvain-dask", help="Run Dask distributed Louvain")
    p_dask.add_argument("--artifact", type=str, required=True)
    p_dask.add_argument("--epsilon", type=float, default=None)
    p_dask.add_argument("--batch-size", type=int, default=None)
    p_dask.add_argument("--n-workers", type=int, default=None)
    p_dask.set_defaults(func=cmd_louvain_dask)

    p_bench = sub.add_parser("benchmark", help="Full benchmark campaign")
    p_bench.add_argument("--artifacts-dir", type=str, default=None)
    p_bench.add_argument("--output-csv", type=str, default=None)
    p_bench.add_argument("--runs", type=int, default=3)
    p_bench.add_argument("--fractions", type=str, default="100")
    p_bench.set_defaults(func=cmd_benchmark)

    p_rep = sub.add_parser("report", help="Markdown report from CSV")
    p_rep.add_argument("--input-csv", type=str, default=None)
    p_rep.add_argument("--output-md", type=str, default=None)
    p_rep.set_defaults(func=cmd_report)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
