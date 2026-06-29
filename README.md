<div align="center">

# Distributed Label Propagation

**Detecção de comunidades em grafos massivos — Ray vs Dask**

Python 3.11 · Numba · Ray Core · Dask Distributed · Docker

[Documentação completa](docs/DOCUMENTACAO_PROJETO.md) · [Relatório PDF](reports/REPORT_COMPLETO.pdf) · [SNAP soc-Orkut](https://snap.stanford.edu/data/com-Orkut.html)

</div>

---

## O problema

Redes sociais como o **Orkut** são grafos com **milhões de nós** e **centenas de milhões de ligações**. Objetivo: encontrar **comunidades** — conjuntos de nós densamente conectados entre si e pouco conectados ao resto.

| Desafio | Escala Orkut (100%) |
|---------|---------------------|
| Nós | ~3,07 milhões |
| Arestas (SNAP, undirected) | ~117 milhões |
| Arcos no CSR (simetrizado) | ~234 milhões |

Algoritmos sequenciais tornam-se lentos; daí paralelizar o LPA com **Ray** ou **Dask** na mesma VM, medindo tempo, memória e qualidade da partição.

---

## Label Propagation (LPA)

1. Cada nó começa com um **rótulo único** (derivado do `node_id`, permutação por `seed`).
2. A cada **iteração**, cada nó adopta o rótulo **mais frequente entre os vizinhos** (empate → menor rótulo).
3. Repete até convergir ou atingir `max_iter` (default **100**).

Actualização **síncrona** (snapshot por iteração) para comparar Ray e Dask de forma justa. Qualidade: modularidade **Q**.

<p align="center">
  <img src="docs/assets/lpa-propagation-iterations.png" alt="Label Propagation" width="720"/>
</p>

---

## O que este projeto faz

| | **Ray** | **Dask** |
|---|---------|----------|
| Modelo | `ray.put` + `@ray.remote` | `client.scatter` + `client.submit` |
| Workers | 1 processo / CPU | `LocalCluster`, 1 thread/worker |
| Núcleo | `lpa_core` + **Numba** | idem |

<p align="center">
  <img src="docs/assets/pipeline.svg" alt="Pipeline" width="800"/>
</p>

Benchmark numa **única VM** (Docker): CSV, logs, partições JSON, relatório Markdown/PDF.

---

## Resultados

**Relatório consolidado:** [`reports/REPORT_COMPLETO.pdf`](reports/REPORT_COMPLETO.pdf) (100% + 1% + 10%, gráficos incluídos).

### Orkut 100% (VM Docker, 6 workers, 3 runs)

| Métrica | Ray | Dask | Ray / Dask |
|---------|-----|------|------------|
| Tempo algo (média) | **649 s** | 1298 s | **~2,0×** |
| Throughput | **4704 n/s** | 2368 n/s | **~2,0×** |
| Comunidades | 590 | 590 | idênticas |

<p align="center">
  <img src="results/figures/performance_comparison.png" alt="Ray vs Dask 100%" width="720"/>
</p>

### Escalabilidade 1% e 10% (6 workers, 3 runs)

| Fração | Ray (algo) | Dask (algo) | Ray / Dask |
|--------|------------|-------------|------------|
| 1% | **3,1 s** | 10,7 s | **~3,5×** |
| 10% | **27,9 s** | 57,5 s | **~2,1×** |

<p align="center">
  <img src="reports/figures_local_1p10p/performance_comparison_10pct.png" alt="Ray vs Dask 10%" width="720"/>
</p>

---

## Início rápido

### Smoke test — fixture 0,1% (~1 min)

Grafo pequeno **incluído no repositório**; não precisa baixar o Orkut.

```bash
git clone <repo-url> big-data && cd big-data
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m cli.main benchmark \
  --input tests/integration/fixtures/orkut_0p1pct.npz \
  --fractions 0.1 --runs 1

python -m cli.main report
```

| Fixture | Nós | Arcos |
|---------|-----|-------|
| `tests/integration/fixtures/orkut_0p1pct.npz` | 1.632 | ~9.774 |

### Benchmark completo — Docker Compose

Pipeline automatizado: download do Orkut (se faltar) → Ray → Dask → relatório. O container **termina sozinho** (`Exited (0)` = sucesso).

**Requisitos:** VM **6+ vCPUs**, **16+ GB RAM**, **~20 GB disco**.

```bash
git clone <repo-url> big-data && cd big-data

mkdir -p data/raw reports
sudo chown -R 1000:1000 data reports

docker compose up --build -d
docker compose logs -f
```

**Grid default:** 3 frações (1%, 10%, 100%) × **6 workers** × Ray+Dask × **3 runs** = 18 execuções LPA + relatório.

| Variável | Default | Efeito |
|----------|---------|--------|
| `BENCHMARK_FRACTIONS` | `1,10,100` | Frações do Orkut |
| `LPA_WORKERS` | `6` | Workers fixos |
| `BENCHMARK_RUNS` | `3` | Repetições (tempo/RAM) |
| `BENCHMARK_BACKEND` | `both` | `ray`, `dask` ou `both` |
| `LPA_MAX_ITER` | `100` | Teto de iterações |

**Só um backend ou grid reduzido:**

```bash
docker compose run --rm \
  -e BENCHMARK_FRACTIONS=1 \
  -e BENCHMARK_RUNS=1 \
  -e BENCHMARK_BACKEND=both \
  lpa
```

**Saídas:** `reports/metrics_raw_<stamp>.csv`, `reports/comparison_<stamp>.md`, `reports/partitions_<stamp>/`.

Frações &lt; 100% geram artefacto BFS em `data/artifacts/` (reutilizado; tempo BFS **não** entra em `algorithm_time_s`).

---

## Dataset

| | |
|---|---|
| Fonte | [SNAP com-Orkut](https://snap.stanford.edu/data/com-Orkut.html) |
| Ficheiro | `data/raw/soc-orkut-relationships.txt` |
| Download | `bash scripts/download_dataset.sh` (automático no Docker) |

---

## Testes

```bash
pytest tests/unit/ -v
pytest tests/integration/test_lpa_orkut.py -m integration -v -s
```

---

## Estrutura

```text
big-data/
├── src/lpa_core/     graph/     ray_impl/     dask_impl/
├── src/benchmark/    src/cli/   src/preprocessing/
├── tests/integration/fixtures/orkut_0p1pct.npz
├── docker-compose.yml
├── reports/REPORT_COMPLETO.pdf
└── results/figures/          # gráficos campanha 100%
```

---

## Referências

1. Raghavan et al. (2007). *Phys. Rev. E* 76, 036106.
2. Blondel et al. (2008). *J. Stat. Mech.* P10008.
3. [Stanford SNAP — soc-Orkut](https://snap.stanford.edu/data/com-Orkut.html)
