# Detecção Distribuída de Comunidades com Louvain

## Documentação do Projeto — Código, Arquitetura e Uso

**Repositório:** [FelipeOliveira456/big-data](https://github.com/FelipeOliveira456/big-data)  
**Pacote Python:** `distributed-louvain`  
**Dataset:** email-Enron (SNAP)  
**Abordagens:** Ray (manual) vs Dask (manual)

---

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Objetivos](#2-objetivos)
3. [O algoritmo de Louvain](#3-o-algoritmo-de-louvain)
4. [Dataset email-Enron](#4-dataset-email-enron)
5. [Arquitetura do software](#5-arquitetura-do-software)
6. [Estrutura de diretórios](#6-estrutura-de-diretórios)
7. [Módulos e responsabilidades](#7-módulos-e-responsabilidades)
8. [Pré-processamento](#8-pré-processamento)
9. [Implementação Ray](#9-implementação-ray)
10. [Implementação Dask](#10-implementação-dask)
11. [Benchmark e relatórios](#11-benchmark-e-relatórios)
12. [Interface de linha de comando](#12-interface-de-linha-de-comando)
13. [Docker e cluster](#13-docker-e-cluster)
14. [Testes e QA](#14-testes-e-qa)
15. [Requisitos de hardware](#15-requisitos-de-hardware)
16. [Decisões de engenharia](#16-decisões-de-engenharia)
17. [Referências](#17-referências)

---

## 1. Visão geral

Este projeto implementa e compara **duas variantes distribuídas** do algoritmo de **Louvain** para detecção de comunidades:

| Abordagem | Tecnologia | Descrição |
|-----------|------------|-----------|
| **Ray** | Python + Ray Core | Louvain manual com `@ray.remote` e batches de nós |
| **Dask** | Python + Dask Distributed | Louvain manual com `Client.submit` e batches de nós |

Ambas leem o **mesmo artefato Parquet** gerado por um único pipeline de pré-processamento, garantindo comparação justa de modularidade e número de comunidades.

A lógica central (ΔQ, modularidade, compressão hierárquica) fica em `louvain_core/` — Ray e Dask são apenas camadas de paralelização da Fase 1.

---

## 2. Objetivos

1. Detectar comunidades na rede **email-Enron** (~37k nós, ~184k arestas).
2. Comparar **desempenho** (tempo, memória RSS, throughput) e **qualidade** (modularidade Q, número de comunidades).
3. Executar em **VM free tier** (Oracle ARM, 4 OCPU / 24 GB) ou cluster de 4 VMs.
4. Fornecer **testes unitários**, integração E2E e pipeline **Docker**.

---

## 3. O algoritmo de Louvain

Referência: Blondel et al. (2008).

### Fase 1 — Otimização local

- Cada nó começa na própria comunidade.
- Para cada nó *i*, calcula-se o ganho **ΔQ** ao movê-lo para cada comunidade vizinha *C*:

**ΔQ = (k_i_in / m) − (σ_tot × k_i) / (2m²)**

onde *k_i_in* = peso das arestas de *i* para *C*, *σ_tot* = soma dos graus em *C*, *k_i* = grau de *i*, *m* = soma total dos pesos / 2.

- O nó move-se para a comunidade com maior ΔQ > 0. Repete até estabilizar (até 500 sweeps por nível).

### Fase 2 — Compressão

- Cada comunidade vira um **super-nó**; arestas entre comunidades agregam pesos.
- Reinicia Fase 1 no grafo comprimido.

### Critério de parada

Parar quando o ganho de modularidade **entre níveis hierárquicos** for menor que **ε** (padrão **1e-6**), implementado em `louvain_core/runner.py` (`should_stop_levels`).

### Modularidade Q

**Q = (1/2m) × Σ_ij ( A_ij − (k_i × k_j)/(2m) ) × δ(c_i, c_j)**

---

## 4. Dataset email-Enron

| Atributo | Valor |
|----------|-------|
| Fonte | [SNAP — email-Enron](https://snap.stanford.edu/data/email-Enron.html) |
| Arquivo | `data/raw/email-Enron.txt` |
| Nós (LCC, ~100%) | ~36.662 |
| Arestas (LCC) | ~183.831 |
| Tamanho raw | ~5 MB |

### Pré-processamento aplicado

1. Ignorar cabeçalho e linhas inválidas.
2. Grafo **não direcionado**: `(u,v) = (v,u)`, manter `src < dst`.
3. Remover self-loops.
4. Peso uniforme **1.0**.
5. Subconjuntos experimentais: amostra **1%, 5%, 100%** dos nós (seed **42**).
6. Subgrafo **induzido** + maior componente conexa (**LCC**).
7. Exportar **Parquet** (`src`, `dst`, `weight`) + `.meta.json`.

### Artefatos (exemplos medidos)

| Fração | Nós | Arestas | Ficheiro |
|--------|-----|---------|----------|
| 1% | 336 | 4.353 | `email-enron_1pct.parquet` |
| 5% | 1.684 | 28.197 | `email-enron_5pct.parquet` |
| 100% | ~36.662 | ~183.831 | `email-enron_100pct.parquet` |

Download:

```bash
bash scripts/download_dataset.sh
```

---

## 5. Arquitetura do software

```text
email-Enron.txt (SNAP)
       |
       v
preprocessing/  -->  Parquet (1%, 5%, 100%)
       |
   +---+---+---+
   |       |   |
   v       v   v
ray_impl  louvain_core  dask_impl
 (Ray)    (fórmulas)     (Dask)
   |       |   |
   +---+---+---+
       v
benchmark/  -->  metrics_raw_<timestamp>.csv
              -->  comparison_<timestamp>.md
```

**Princípios:**

- Ray e Dask **não importam um ao outro**.
- Fórmulas Louvain centralizadas em `louvain_core/`.
- Um módulo por responsabilidade; config via `pyproject.toml` + `config.yaml`.

---

## 6. Estrutura de diretórios

```text
big-data/
├── src/
│   ├── louvain_core/       # ΔQ, Q, compressão, hierarquia
│   ├── preprocessing/      # SNAP → Parquet
│   ├── ray_impl/           # Louvain distribuído Ray
│   ├── dask_impl/          # Louvain distribuído Dask
│   ├── benchmark/          # Métricas, CSV, relatório MD
│   ├── cli/                # Entrypoint CLI
│   └── config.py           # Config YAML + env vars
├── tests/
│   ├── unit/               # pytest (~70 testes)
│   └── integration/        # E2E Ray+Dask (parametrizado 1%, 5%)
├── data/
│   ├── raw/                # email-Enron.txt (gitignored)
│   └── artifacts/          # Parquet por fração (gitignored)
├── reports/                # Saídas locais timestampadas (gitignored)
├── scripts/                # download, docker, cluster, QA
├── docs/                   # Esta documentação
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml          # dependências runtime + dev
├── config.yaml.example
└── README.md
```

---

## 7. Módulos e responsabilidades

### `louvain_core/`

| Ficheiro | Função |
|----------|--------|
| `graph.py` | Grafo como adjacência; propriedade `m` |
| `delta_q.py` | ΔQ e melhor movimento por nó |
| `modularity.py` | Modularidade Q |
| `compress.py` | Fase 2: super-nós |
| `hierarchy.py` | Níveis hierárquicos; Q no grafo original |
| `runner.py` | Louvain sequencial + `should_stop_levels` |

### `preprocessing/`

| Ficheiro | Função |
|----------|--------|
| `load_snap.py` | Leitura streaming; SQLite temporário |
| `sample_lcc.py` | Amostragem conectada, subgrafo induzido, LCC |
| `write_artifact.py` | Parquet + `.meta.json` |
| `validate_artifact.py` | Validação de schema |
| `pipeline.py` | Orquestração `preprocess` |

### `ray_impl/louvain_ray.py`

- `@ray.remote calcular_ganho_batch`: avalia movimentos para um batch.
- **Snapshot síncrono:** todos os workers usam a mesma partição; movimentos aplicados ao fim da rodada.
- Modo **local:** `ray.init(num_cpus=...)` — `null` = auto.
- Modo **cluster:** `ray.init(address="ray://<head>:10001")` quando `ray_head_address` definido.

### `dask_impl/louvain_dask.py`

- `client.submit(_batch_best_moves, ...)` por batch.
- Modo **local:** `LocalCluster(n_workers=None)` — auto-detecta CPUs.
- Modo **cluster:** `Client("tcp://<scheduler>:8786")` quando `dask_scheduler_address` definido.
- `scatter` de adj/degree reutilizado entre sweeps do mesmo nível.

### `benchmark/`

| Ficheiro | Função |
|----------|--------|
| `runner.py` | Campanha N runs × 2 abordagens × frações → CSV |
| `metrics.py` | tracemalloc + **psutil RSS** (driver + árvore de processos) |
| `paths.py` | Timestamps `YYYYMMDDTHHMMSS` nos nomes de ficheiro |
| `report.py` | Markdown comparativo (média ± desvio) |
| `report_sections.py` | Texto sobre race conditions + referências |

### `cli/main.py`

Subcomandos: `preprocess`, `louvain-ray`, `louvain-dask`, `benchmark`, `report`.

---

## 8. Pré-processamento

### Fluxo

1. Coletar nós da LCC global do ficheiro SNAP.
2. Por fração (1/5/100):
   - Amostrar nós conectados com `random.Random(seed)`.
   - Filtrar arestas (ambos endpoints no conjunto).
   - Extrair LCC do subgrafo induzido.
   - Escrever Parquet + metadados JSON.

### Comando

```bash
python -m cli.main preprocess --fractions 100
# ou frações múltiplas:
python -m cli.main preprocess --fractions 1,5,100
```

### Schema Parquet

| Coluna | Tipo | Regra |
|--------|------|-------|
| src | int64 | menor endpoint |
| dst | int64 | maior endpoint |
| weight | float64 | 1.0 |

---

## 9. Implementação Ray

### Paralelização (Fase 1)

1. Dividir nós em batches (`ray_batch_size`, padrão **1000**).
2. Lançar `calcular_ganho_batch.remote(...)` para cada batch.
3. `ray.get(refs)` em lote.
4. Aplicar movimentos ao fim da rodada.
5. Repetir até estabilizar.

### Paralelismo efectivo

Número de batches = `ceil(n_nós / batch_size)`. Com **336 nós** e batch **500** → **1 batch** → quase sem paralelismo. Com **~37k nós** e batch **1000** → **~37 batches** → paralelismo real.

### Race conditions

Workers partilham o **mesmo snapshot** da partição. Conflitos no mesmo nó são esperados; o último movimento aplicado na rodada prevalece.

### Comando

```bash
python -m cli.main louvain-ray \
  --artifact data/artifacts/email-enron_100pct.parquet \
  --batch-size 1000 \
  --num-cpus 4
```

Dashboard (modo local): http://localhost:8265

---

## 10. Implementação Dask

Mesma lógica de batches que Ray, via `Client.submit`.

### LocalCluster vs cluster remoto

| Config | Comportamento |
|--------|---------------|
| `dask_scheduler_address: null` | Cria `LocalCluster` na máquina |
| `dask_n_workers: null` | Auto (≈ CPUs disponíveis) |
| `dask_scheduler_address: 10.0.0.5:8786` | Conecta ao scheduler remoto |

### Comando

```bash
python -m cli.main louvain-dask \
  --artifact data/artifacts/email-enron_100pct.parquet \
  --batch-size 1000 \
  --n-workers 4
```

Dashboard (LocalCluster): http://localhost:8787

---

## 11. Benchmark e relatórios

### CSV (`reports/metrics_raw_YYYYMMDDTHHMMSS.csv`)

Colunas principais:

| Coluna | Descrição |
|--------|-----------|
| `approach` | `ray` ou `dask` |
| `fraction_pct` | 1, 5, 100, … |
| `init_time_s` | Tempo de `ray.init` / criação do cluster |
| `algorithm_time_s` | Tempo do algoritmo |
| `peak_driver_rss_mb` | RSS do processo driver |
| `peak_process_tree_rss_mb` | RSS driver + workers filhos |
| `modularity_q` | Modularidade final |
| `num_communities` | Comunidades no grafo original |
| `level_times_json` | Tempos por nível hierárquico |

### Markdown (`reports/comparison_YYYYMMDDTHHMMSS.md`)

Tabelas média ± desvio; secções Desempenho, Qualidade, Engenharia.

```bash
python -m cli.main benchmark --fractions 100 --runs 3
python -m cli.main report
```

O `report` emparelha automaticamente o CSV mais recente (via `latest_run.txt` ou timestamp).

---

## 12. Interface de linha de comando

| Comando | Descrição |
|---------|-----------|
| `preprocess` | TXT SNAP → Parquet por fração |
| `louvain-ray` | Louvain Ray num artefato |
| `louvain-dask` | Louvain Dask num artefato |
| `benchmark` | Campanha completa (N runs × 2 abordagens) |
| `report` | Gera Markdown a partir do CSV |

### Configuração (`config.yaml` / env vars)

| Chave / env | Padrão | Descrição |
|-------------|--------|-----------|
| `graph_raw_path` / `GRAPH_RAW_PATH` | `data/raw/email-Enron.txt` | Ficheiro SNAP |
| `dataset_slug` | `email-enron` | Prefixo dos artefatos |
| `seed` / `SEED` | `42` | Semente |
| `epsilon` / `EPSILON` | `1e-6` | Parada entre níveis |
| `ray_num_cpus` / `RAY_NUM_CPUS` | auto | CPUs Ray (local) |
| `ray_batch_size` | `1000` | Nós por batch |
| `dask_n_workers` / `DASK_N_WORKERS` | auto | Workers Dask (local) |
| `ray_head_address` / `RAY_HEAD_ADDRESS` | — | Head Ray remoto |
| `dask_scheduler_address` / `DASK_SCHEDULER_ADDRESS` | — | Scheduler Dask remoto |

Pipeline completo no host:

```bash
bash scripts/run_all.sh   # requer venv em .venv
```

---

## 13. Docker e cluster

### Single host

```bash
docker build -t distributed-louvain:latest .
docker compose --profile single up
```

O entrypoint (`scripts/docker-entrypoint.sh`) executa:

1. Download do dataset (se necessário)
2. `preprocess --fractions 100`
3. `benchmark --fractions 100 --runs 3`
4. `report`

Volumes: `./data`, `./reports`, `config.yaml.example` → `/app/config.yaml`.

**Defaults Docker:** `ray_num_cpus` e `dask_n_workers` em **auto** (null); `batch_size` **1000**.

### Cluster 4 VMs Oracle ARM

```bash
export HEAD_IP=<IP_PRIVADO_VM1>
bash scripts/start-cluster.sh
```

Imprime comandos `docker run` por VM (ray-head, dask-scheduler, workers, pipeline).

Portas na Security List: **6379**, **10001**, **8786**, **8787**.

---

## 14. Testes e QA

### Setup

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Dependências definidas em `pyproject.toml` (`[project]` + `[optional-dependencies] dev`).

### Testes

```bash
pytest tests/unit/ -v                           # ~70 testes, segundos
pytest tests/integration/ -v -m integration     # E2E 1% e 5%, minutos
```

E2E grava em `tests/integration/output/benchmark/` (não toca em `reports/`).

Testes de cluster (`test_ray_cluster_mode.py`, `test_dask_cluster_mode.py`) usam **mocks** — não requerem VMs.

### QA

```bash
make qa-check    # ruff + pylint + bandit
make qa          # + pytest+coverage + mutmut → reports/qa_<timestamp>.md
```

`make qa` tem **exit 0 sempre** — relatório informativo, não gate de CI.

---

## 15. Requisitos de hardware

### Dataset 100% Enron

| VM | Viável? | Notas |
|----|---------|-------|
| **4 OCPU / 24 GB** (Oracle free tier) | **Sim** | ~4–12 GB RSS estimado; benchmark completo leva **horas** |
| **1 OCPU / 6 GB** | Apertado | RAM e tempo limitados |
| **~4 GB RAM** (dev local) | Não recomendado | 1% ok; 5%+ benchmark pode não terminar |

### Estimativa a partir de medições (1%, 2 workers, batch 500)

| | Ray | Dask |
|---|-----|------|
| Tempo algo | ~101 s | ~153 s |
| RSS total | ~770 MB | ~448 MB |
| Nós | 336 | 336 |

Extrapolação para 100%: tempo **muito pior que linear** (mais níveis Louvain); ordem de **2–6 h por run** × 3 runs × 2 abordagens no single host.

---

## 16. Decisões de engenharia

| Decisão | Motivo |
|---------|--------|
| email-Enron em vez de Pokec | Cabe em VM free tier; comparação Ray vs Dask sem JVM/Spark |
| Dask manual (não GraphFrames) | Mesma lógica que Ray; comparação justa de runtime Python |
| Parquet compartilhado | Mesmo input para Ray e Dask |
| `louvain_core` compartilhado | Evitar duplicar fórmulas |
| Snapshot síncrono por rodada | Simplicidade; trade-off documentado (race conditions) |
| RSS via psutil | Medir workers filhos, não só heap Python |
| Relatórios timestampados | Evitar sobrescrever runs anteriores |
| `dask_n_workers: null` | Paridade com Ray auto-detect |
| Docker single + cluster script | Demo local vs Oracle ARM sem Swarm |

---

## 17. Referências

1. Blondel, V. D. et al. (2008). Fast unfolding of communities in large networks. *J. Stat. Mech.* P10008.
2. Leskovec, J., Krevl, A. (2014). SNAP Datasets. https://snap.stanford.edu/data
3. Moritz, P. et al. (2018). Ray: A Distributed Framework for Emerging AI Applications. OSDI.
4. Dask documentation: https://docs.dask.org/
5. Documentação Ray: https://docs.ray.io/

---

*Documentação alinhada ao código em `main` — Junho 2026.*
