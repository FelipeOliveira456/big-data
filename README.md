# Distributed Label Propagation (soc-Orkut)

Trabalho de Big Data: detecção de comunidades com **Label Propagation** distribuído — **Ray** vs **Dask**.

**Dataset:** [soc-Orkut](https://snap.stanford.edu/data/soc-Orkut.html) (~3,07M nós, ~117M arestas **não direcionadas**; CSR simetrizado na carga). Integração usa fixture **0,1%** (~1,6k nós).

## Documentação

**[docs/DOCUMENTACAO_PROJETO.md](docs/DOCUMENTACAO_PROJETO.md)** — arquitetura, LPA, Ray vs Dask, Docker e CLI.

---

## Pré-requisitos

| Modo | O que precisa |
|------|----------------|
| **Desenvolvimento / QA** | Python **3.11+**, venv, `pip install -e ".[dev]"` |
| **Produção (1 VM)** | Docker + **16+ GB RAM** recomendado para Orkut 100% |
| **Testes E2E** | venv + fixture `orkut_0p1pct.npz` |

---

## Setup (venv)

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp config.yaml.example config.yaml   # opcional
bash scripts/build_integration_fixture.sh   # gera orkut_0p1pct.npz se ausente
```

Workers e chunks LPA: **auto = número de CPUs** (`LPA_WORKERS=4` para fixar).

---

## QA

```bash
make qa          # ruff, pylint, bandit, pytest+coverage, mutmut
make qa-check    # só lint
```

---

## Testes

```bash
pytest tests/unit/ -v
pytest tests/integration/test_lpa_orkut.py -m integration -v -s
```

Fixture: `bash scripts/build_integration_fixture.sh` (usa raw Orkut ou grafo sintético).

---

## CLI (host)

```bash
bash scripts/download_dataset.sh
python -m cli.main benchmark --input data/raw/soc-orkut-relationships.txt --fractions 100 --runs 3
python -m cli.main report
```

Pipeline completo: `bash scripts/run_all.sh`

---

## Docker (1 VM)

Grafo carrega **só nesta máquina**. Ray e Dask usam workers locais (1 por CPU).

```bash
bash scripts/run-docker.sh
# ou:
docker compose up --build
```

Volumes: `./data`, `./reports`. Env no `docker-compose.yml`:

| Variável | Default |
|----------|---------|
| `BENCHMARK_FRACTIONS` | `100` |
| `BENCHMARK_RUNS` | `3` |
| `BENCHMARK_BACKEND` | `both` (Ray → Dask → report) |
| `LPA_MAX_ITER` | `100` |
| `LPA_WORKERS` | auto (CPUs do container) |

Build ARM (Oracle Ampere): `docker build -t distributed-lpa:latest .`

---

## Saídas

| Artefato | Onde |
|----------|------|
| `metrics_raw_<stamp>.csv` | `reports/` |
| `partitions_<stamp>/*.communities.json` | `reports/` |
| `comparison_<stamp>.md` | `reports/` |

---

## Estrutura

```text
src/lpa_core/   src/graph/   src/ray_impl/   src/dask_impl/
src/preprocessing/   src/benchmark/   src/cli/
scripts/   docker-compose.yml   config.yaml.example
```
