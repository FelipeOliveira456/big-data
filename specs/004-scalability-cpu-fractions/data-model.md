# Data Model: Scalability Benchmark Grid + Seed Investigation

## Entidades existentes (modificadas)

### BenchmarkRow (dataclass — `src/benchmark/runner.py`)

Adicionar 2 campos no final:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `workers_requested` | `int` | Valor passado via `--workers` (ou derivado de CPU count) |
| `workers_actual` | `int` | Valor que o backend efetivamente usou (`LpaResult.num_workers`) |

Todos os outros campos permanecem inalterados.

### CSV_HEADER (lista — `src/benchmark/runner.py`)

Adicionar ao final:
```
"workers_requested",
"workers_actual",
```

Backward compat: CSVs antigos (sem essas colunas) continuam a funcionar no relatório via `row.get("workers_requested", "?")`.

---

## Entidades existentes (sem modificação)

### AppConfig (dataclass — `src/config.py`)

Sem mudança estrutural. O campo `lpa_chunk_divisor` já serve como workers count. O runner usa `replace(cfg, lpa_chunk_divisor=w)` por run para isolar cada workers value — sem modificar o objeto global.

### LpaResult (dataclass — `src/lpa_core/lpa.py`)

Sem modificação. O campo `num_workers` já existe e é usado como `workers_actual`.

---

## Fluxo de dados — varredura de escalabilidade

```
CLI args: --fractions 1,10,100 --workers 2,4,6
                │
                ▼
run_benchmark_campaign(
    fractions=[1.0, 10.0, 100.0],
    workers_list=[2, 4, 6],     ← novo parâmetro
    ...
)
                │
    for frac in fractions:
      load_graph (1x por fração)
      for w in workers_list:
        cfg_w = replace(cfg, lpa_chunk_divisor=w)
        for approach in approaches:
          for run in range(runs):
            _run_approach(..., cfg_w, workers_requested=w)
                │
                ▼
    BenchmarkRow(
        fraction_pct=frac,
        workers_requested=w,
        workers_actual=res.num_workers,
        ...
    )
                │
                ▼
metrics_raw_<stamp>.csv  (18+ linhas para grid 3×3×2)
```

---

## Relatório — nova seção escalabilidade

Agrupamento no `generate_report`:

```
(approach, fraction_pct, workers_requested) → lista de rows
```

Tabela por backend:

```
| Fração % | Workers | Throughput (nós/s) | Tempo algo (s) | RSS total (MB) |
|----------|---------|-------------------|----------------|----------------|
| 1        | 2       | ...               | ...            | ...            |
| 1        | 4       | ...               | ...            | ...            |
| 1        | 6       | ...               | ...            | ...            |
| 10       | 2       | ...               | ...            | ...            |
...
```
