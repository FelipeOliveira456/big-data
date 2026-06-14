"""CLI smoke tests with mocked heavy backends."""

from argparse import Namespace
from pathlib import Path
import cli.main as cli_main
from cli.main import (
    cmd_benchmark,
    cmd_louvain_ray,
    cmd_preprocess,
    cmd_report,
    main,
)
from preprocessing.write_artifact import write_graph_parquet
from ray_impl.louvain_ray import RayLouvainResult


def test_main_preprocess_missing_input(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    code = main(["preprocess", "--input", str(tmp_path / "nope.txt")])
    assert code == 1


def test_cmd_preprocess_success(tmp_path: Path, monkeypatch):
    raw = tmp_path / "raw.txt"
    raw.write_text("1 2\n2 3\n3 1\n", encoding="utf-8")
    out_dir = tmp_path / "artifacts"
    monkeypatch.setattr(
        cli_main,
        "run_preprocess",
        lambda inp, out, seed, fracs, dataset_slug: [
            out / "email-enron_100pct.parquet"
        ],
    )
    monkeypatch.setattr(cli_main, "validate_artifact", lambda p: None)
    args = Namespace(
        input=str(raw),
        output_dir=str(out_dir),
        seed=42,
        fractions="100",
    )
    assert cmd_preprocess(args) == 0


def test_cmd_louvain_ray_json_output(tmp_path: Path, capsys, monkeypatch):
    artifact = tmp_path / "g.parquet"
    write_graph_parquet([(0, 1, 1.0)], artifact, {})
    fake = RayLouvainResult(
        modularity=0.5,
        num_communities=1,
        num_levels=1,
        init_time_s=0.01,
        algorithm_time_s=0.1,
    )
    monkeypatch.setattr(cli_main, "validate_artifact", lambda p: None)
    monkeypatch.setattr(cli_main, "run_louvain_ray", lambda *a, **k: fake)
    args = Namespace(
        artifact=str(artifact),
        epsilon=None,
        batch_size=None,
        num_cpus=None,
    )
    assert cmd_louvain_ray(args) == 0
    assert '"modularity": 0.5' in capsys.readouterr().out


def test_cmd_report_and_benchmark(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "m.csv"
    csv_path.write_text("approach,fraction_pct\n", encoding="utf-8")
    md_path = tmp_path / "r.md"
    monkeypatch.setattr(
        cli_main,
        "generate_report",
        lambda inp, out: out.write_text("# ok\n") or out,
    )
    assert cmd_report(Namespace(input_csv=str(csv_path), output_md=str(md_path))) == 0

    out_csv = tmp_path / "bench.csv"
    monkeypatch.setattr(
        "benchmark.runner.run_benchmark_campaign",
        lambda *a, **k: out_csv,
    )
    assert (
        cmd_benchmark(
            Namespace(
                artifacts_dir=str(tmp_path),
                output_csv=str(out_csv),
                runs=1,
                fractions="100",
            )
        )
        == 0
    )
