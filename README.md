# Distributed Louvain (email-Enron)

Trabalho de Big Data: detecção de comunidades com Louvain distribuído — **Ray** (manual) vs **Dask** (manual).

**Dataset:** [email-Enron](https://snap.stanford.edu/data/email-Enron.html) (~37k nós, ~184k arestas) — cabe em VM free tier (24 GB).

## Documentação

**[docs/DOCUMENTACAO_PROJETO.md](docs/DOCUMENTACAO_PROJETO.md)** — explicação completa do código, arquitetura, Louvain, Ray vs Dask, Docker, cluster e uso local.

---

## Pré-requisitos

| Modo | O que precisa |
|------|----------------|
| **Desenvolvimento / CLI / QA** | Python **3.11+**, venv, deps em `pyproject.toml` |
| **Docker (single host)** | Docker ≥ 24, Compose plugin — **não** precisa de venv no host |
| **Cluster Oracle (4 VMs)** | Docker em cada VM + Security List com portas 6379, 10001, 8786, 8787 |
| **Testes E2E** | venv + dataset Enron (download automático nos scripts) |

---

## Setup (venv) — obrigatório para QA e CLI local

Dependências de runtime e dev ficam no **`pyproject.toml`** (`[project]` e `[project.optional-dependencies] dev`). Não há `requirements.txt` separado.

O **`make qa`** e os comandos `python -m cli.main` rodam **no host**, dentro do venv. O Docker **não** usa o venv do host.

```bash
# 1. Criar e ativar venv
python3.11 -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# 2. Instalar projeto + ferramentas de dev (ruff, pytest, mutmut, etc.)
pip install --upgrade pip
pip install -e ".[dev]"

# ou: make install

# 3. Config opcional
cp config.yaml.example config.yaml   # ajuste paths se necessário
```

---

## QA (lint, testes, mutação)

Roda **no host com venv ativo**. Gera `reports/qa_<timestamp>.md` (gitignored).

```bash
source .venv/bin/activate

make qa-check    # rápido: ruff + pylint + bandit (sem pytest/mutmut)
make qa          # completo: ruff, pylint, bandit, pytest+coverage, mutmut
                 # exit code sempre 0 — relatório informativo, não é gate de CI
```

O script `scripts/run_qa.sh` ativa `.venv` automaticamente se existir, mas **as ferramentas precisam estar instaladas no venv** (`pip install -e ".[dev]"`).

---

## Testes

```bash
source .venv/bin/activate
pytest tests/unit/ -v              # unitários (~70 testes, segundos)
pytest tests/integration/ -v -m integration   # E2E Ray+Dask (lento, minutos)
```

---

## Dataset + CLI (host com venv)

```bash
source .venv/bin/activate

bash scripts/download_dataset.sh
python -m cli.main preprocess --fractions 100
python -m cli.main louvain-ray --artifact data/artifacts/email-enron_100pct.parquet
python -m cli.main louvain-dask --artifact data/artifacts/email-enron_100pct.parquet
python -m cli.main benchmark --fractions 100 --runs 3
python -m cli.main report
```

Pipeline completo no host: `bash scripts/run_all.sh` (requer venv em `.venv`).

### Saídas do pipeline

| Comando | Arquivo gerado |
|---------|----------------|
| `benchmark` | `reports/metrics_raw_YYYYMMDDTHHMMSS.csv` |
| `report` | `reports/comparison_YYYYMMDDTHHMMSS.md` |
| `make qa` | `reports/qa_YYYYMMDDTHHMMSS.md` |

Arquivos em `reports/` são gitignored. Testes E2E gravam em `tests/integration/output/benchmark/` sem tocar em `reports/`.

---

## Docker (single host)

**Não usa venv do host.** A imagem instala deps via `pip install .` no build (lê `pyproject.toml`).

```bash
docker build -t distributed-louvain:latest .
docker compose --profile single up
```

Volumes montados:

- `./data` → dataset bruto + Parquet
- `./reports` → CSV/MD timestampados
- `./config.yaml.example` → `/app/config.yaml` (read-only)

O contêiner baixa o Enron automaticamente se `data/raw/email-Enron.txt` não existir.

---

## Cluster (4 VMs Oracle ARM)

```bash
export HEAD_IP=<IP_PRIVADO_VM1>
bash scripts/start-cluster.sh   # imprime docker run por VM — copie em cada máquina
```

Abra no **Security List** da Oracle (subnet privada): **6379**, **10001**, **8786**, **8787**.

Build multi-arch (Oracle ARM + amd64 local):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t distributed-louvain:latest .
```

---

## Makefile — alvos principais

| Alvo | Onde roda | Descrição |
|------|-----------|-----------|
| `install` | venv | `pip install -e ".[dev]"` |
| `test` | venv | `pytest tests/unit/` |
| `qa` | venv | suite QA completa → `reports/qa_*.md` |
| `qa-check` | venv | lint rápido (ruff, pylint, bandit) |
| `preprocess` / `benchmark` / `report` | venv | atalhos para CLI |

---

## Estrutura do projeto

```text
src/louvain_core/     # ΔQ, modularidade, compressão
src/preprocessing/    # SNAP → Parquet (LCC)
src/ray_impl/         # Louvain Ray
src/dask_impl/        # Louvain Dask
src/benchmark/        # métricas + relatório
src/cli/              # entrypoint
tests/unit/
tests/integration/
data/artifacts/       # grafos Parquet (gitignored)
reports/              # saídas locais (gitignored)
scripts/              # download, docker, cluster, QA
pyproject.toml        # dependências runtime + dev
```
