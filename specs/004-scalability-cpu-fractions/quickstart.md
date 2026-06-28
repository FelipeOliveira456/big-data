# Quickstart: Scalability Benchmark Grid

## Pré-requisitos

- Dataset Orkut em `data/raw/soc-orkut-relationships.txt` (ou download automático via Docker)
- 6 vCPUs disponíveis, 16 GB RAM

---

## Opção A — Docker Compose (recomendado na VM)

```bash
# Grid completo: 3 frações × 3 workers = 9 combinações × Ray + Dask = 18 runs
BENCHMARK_FRACTIONS=1,10,100 BENCHMARK_WORKERS=2,4,6 docker compose up --build -d
docker compose logs -f
```

Relatório em `reports/comparison_<stamp>.md` quando `Exited (0)`.

---

## Opção B — CLI direto (fora do Docker)

```bash
cd /home/felipe/Documents/BIG-DATA/big-data
PYTHONPATH=src python -m cli.main benchmark \
  --input data/raw/soc-orkut-relationships.txt \
  --fractions 1,10,100 \
  --workers 2,4,6 \
  --runs 3

PYTHONPATH=src python -m cli.main report
```

---

## Opção C — Subset rápido para validar (fixture ~1,6k nós)

```bash
PYTHONPATH=src python -m cli.main benchmark \
  --input tests/integration/fixtures/orkut_0p1pct.npz \
  --fractions 0.1 \
  --workers 2,4 \
  --runs 1

PYTHONPATH=src python -m cli.main report
```

Deve completar em < 2 minutos.

---

## CSV de saída

Localização: `reports/metrics_raw_<stamp>.csv`

Colunas relevantes para escalabilidade:

| Coluna | O quê |
|--------|--------|
| `fraction_pct` | Fração do grafo usada |
| `workers_requested` | Workers configurados (2, 4 ou 6) |
| `workers_actual` | Workers que o backend reportou |
| `algorithm_time_s` | Tempo do LPA (sem carga do grafo) |
| `peak_process_tree_rss_mb` | RAM total (driver + workers) |
| `throughput_nodes_per_s` | Nós processados por segundo |

---

## Nota sobre seeds

O LPA distribuído síncrono produz partições **idênticas** independentemente do seed (ver `research.md`). As 3 runs com seeds diferentes medem **variabilidade temporal**, não variabilidade algorítmica. Isto é documentado automaticamente no relatório gerado.
