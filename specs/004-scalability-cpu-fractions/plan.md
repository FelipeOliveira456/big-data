# Implementation Plan: Scalability Benchmark Grid + Seed Investigation

**Branch**: `004-scalability-cpu-fractions` | **Date**: 2026-06-28 | **Spec**: [spec.md](spec.md)

## Summary

Adicionar dimensão `workers` ao benchmark (produto cartesiano frações × workers) com um único comando; documentar (não corrigir — é comportamento esperado) por que o LPA distribuído síncrono produz partições idênticas independentemente do seed; estender o relatório com tabela de escalabilidade.

Ver [research.md](research.md) para decisões de design.

## Technical Context

**Language/Version**: Python 3.11+  
**Primary Dependencies**: Ray 2.x, Dask[distributed], NumPy, psutil, pytest  
**Storage**: CSV (métricas), JSON (partições), Markdown (relatório)  
**Testing**: pytest (unitários em `tests/unit/`, integração em `tests/integration/`)  
**Target Platform**: Linux VM, 6 vCPUs, 16 GB RAM; Docker container  
**Project Type**: CLI tool + benchmark library  
**Performance Goals**: 18 runs (3 frações × 3 workers × 2 backends) em < 24h na VM  
**Constraints**: backward compat com CSV sem `workers_requested`; zero breaking changes nos testes existentes  
**Scale/Scope**: ~3M nós (Orkut 100%), ~9.7k nós (fixture 0.1%)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [x] **Legibilidade**: parâmetro `workers_list` segue o padrão `fractions` já existente; `replace(cfg, lpa_chunk_divisor=w)` é idiomático
- [x] **Superfície mínima**: 2 colunas CSV novas, 1 arg CLI novo, extensão do loop existente — nada de abstrações novas
- [x] **Modularidade**: mudanças isoladas em `runner.py`, `cli/main.py`, `report.py`, `entrypoint.sh` — sem acoplamento novo
- [x] **Stack**: Python 3.11, pytest, estrutura `src/` + `tests/` existente — sem desvios
- [x] Sem violações a justificar

## Project Structure

### Documentation (this feature)

```text
specs/004-scalability-cpu-fractions/
├── plan.md              ← este ficheiro
├── spec.md
├── research.md          ← decisões de design
├── data-model.md        ← entidades e CSV
├── quickstart.md        ← como executar
├── contracts/
│   └── cli-benchmark.md ← contrato do argumento --workers
├── checklists/
│   └── requirements.md
└── tasks.md             ← gerado por /speckit-tasks
```

### Source Code — ficheiros modificados

```text
src/
├── cli/
│   └── main.py                  # + --workers arg + _parse_workers()
├── benchmark/
│   ├── runner.py                # + workers_list param + loop + 2 colunas CSV
│   └── report.py                # + seção escalabilidade (fração × workers)
└── lpa_core/
    └── distributed.py           # + docstring explicando determinismo do seed

scripts/
└── docker-entrypoint.sh         # + BENCHMARK_WORKERS passthrough

docker-compose.yml               # + BENCHMARK_WORKERS env var (vazio = default)

tests/
├── unit/
│   ├── test_benchmark_runner.py # + testes do workers loop
│   └── test_cli.py              # + teste do --workers arg
└── integration/
    └── test_lpa_orkut.py        # + parametrize workers=[2, 4] no fixture
```

## Complexity Tracking

> Sem violações — nenhuma linha necessária.

---

## Phase 0: Research

Resolvido em [research.md](research.md). Decisões principais:

1. **Seed determinístico** — LPA síncrono/batch: todos os nós leem do snapshot → resultado independente do seed → documentar, não "corrigir".
2. **Workers loop** — `replace(cfg, lpa_chunk_divisor=w)` por run; loop `fractions → workers → approaches → runs`.
3. **CSV backward compat** — colunas no final; `row.get("workers_requested", "?")` no relatório.
4. **Docker** — `BENCHMARK_WORKERS` env var no entrypoint e compose.

---

## Phase 1: Design & Contracts

### 1.1 `src/cli/main.py`

Adicionar função auxiliar e argumento:

```python
def _parse_workers(value: str) -> list[int]:
    """Parse '2,4,6' → [2, 4, 6]; '' → []."""
    return [int(x.strip()) for x in value.split(",") if x.strip()]
```

Argumento no subparser `benchmark`:

```python
p_bench.add_argument("--workers", type=str, default="",
    help="Comma-separated worker counts, e.g. '2,4,6' (default: os.cpu_count())")
```

Passagem ao runner:

```python
run_benchmark_campaign(
    ...,
    workers_list=_parse_workers(args.workers) or None,
)
```

### 1.2 `src/benchmark/runner.py`

**BenchmarkRow** — 2 novos campos (no final):

```python
workers_requested: int = 0
workers_actual: int = 0
```

**CSV_HEADER** — 2 novos itens no final:

```python
"workers_requested",
"workers_actual",
```

**`run_benchmark_campaign`** — novo parâmetro e loop:

```python
def run_benchmark_campaign(
    ...,
    workers_list: list[int] | None = None,   # NOVO
) -> Path:
    ...
    effective_workers = workers_list or [cfg.lpa_chunk_divisor]

    for frac in fractions:
        loaded = load_graph(...)
        for w in effective_workers:           # NOVO loop externo
            cfg_w = replace(cfg, lpa_chunk_divisor=w)
            for approach in selected:
                for run_idx in range(1, runs + 1):
                    row = _run_approach(..., cfg_w, workers_requested=w)
```

**`_run_approach`** — novo parâmetro:

```python
def _run_approach(..., workers_requested: int) -> BenchmarkRow:
    ...
    row = _row_from_result(..., workers_requested=workers_requested)
```

**`_row_from_result`** — preenche campos novos:

```python
workers_requested=workers_requested,
workers_actual=res.num_workers,
```

### 1.3 `src/benchmark/report.py`

Agrupamento estendido:

```python
groups[(approach, fraction_pct, workers_requested)] → list[rows]
```

Nova seção no relatório gerado:

```markdown
## Escalabilidade: Throughput por Fração × Workers

### Ray
| Fração % | 2 workers | 4 workers | 6 workers |
|----------|-----------|-----------|-----------|
| 1        | X nós/s   | Y nós/s   | Z nós/s   |
...

### Dask
...

## Escalabilidade: Memória por Fração × Workers
...
```

Se CSV não tem coluna `workers_requested` (compatibilidade): seção escalabilidade é omitida; tabela existente funciona normalmente.

### 1.4 `src/lpa_core/distributed.py`

Adicionar ao docstring de `run_lpa_distributed`:

```
Note — seed determinism:
    This implementation is *synchronous/batch*: all chunks read from the same
    frozen ``snapshot`` before any label is updated.  The vote kernel breaks
    ties by minimum label (deterministic).  Therefore ``seed`` only affects
    chunk boundaries, not the algorithm outcome; runs with different seeds
    produce identical partitions.  This is expected behaviour.  Seed variation
    measures temporal variance, not algorithmic variance.
```

### 1.5 `scripts/docker-entrypoint.sh`

```bash
: "${BENCHMARK_WORKERS:=}"
...
workers_flag=()
[[ -n "$BENCHMARK_WORKERS" ]] && workers_flag=(--workers "$BENCHMARK_WORKERS")
...
python -m cli.main benchmark \
  ...
  "${workers_flag[@]}"
```

### 1.6 `docker-compose.yml`

```yaml
environment:
  BENCHMARK_WORKERS: ""   # ex: "2,4,6"; vazio = usa os.cpu_count()
```

### 1.7 Testes

**Unitários novos** (`tests/unit/test_benchmark_runner.py`):
- `test_workers_list_generates_cartesian_product`: mock do runner, verifica que 3 frações × 3 workers = 9 chamadas a `_run_approach`.
- `test_workers_actual_in_row`: verifica que `workers_actual = res.num_workers`.
- `test_no_workers_list_uses_cfg_divisor`: backward compat.

**Unitários novos** (`tests/unit/test_cli.py`):
- `test_parse_workers_valid`: `"2,4,6"` → `[2, 4, 6]`.
- `test_parse_workers_empty`: `""` → `[]`.

**Integração** (`tests/integration/test_lpa_orkut.py`):
- Parametrizar `workers=[2, 4]` com fixture orkut_0p1pct.npz; verificar que CSV tem `workers_requested` correto.

**Unitário para determinismo** (`tests/unit/test_lpa_seed_determinism.py`):
- Corre LPA distribuído (mock Ray/Dask com single-process) com seeds 42 e 99 no mesmo grafo pequeno; verifica partições idênticas; documenta como comportamento esperado.

---

## Constitution Check (pós-design)

- [x] **Legibilidade**: `workers_list` espelha `fractions`; sem nova indireção
- [x] **Superfície mínima**: diff mínimo — 2 colunas, 1 loop, 1 arg CLI, 1 env var
- [x] **Modularidade**: `runner.py` recebe lista e itera; sem dependência nova
- [x] Sem violações

---

## Artifacts gerados nesta fase

| Artefacto | Estado |
|-----------|--------|
| `specs/004-scalability-cpu-fractions/research.md` | ✅ |
| `specs/004-scalability-cpu-fractions/data-model.md` | ✅ |
| `specs/004-scalability-cpu-fractions/quickstart.md` | ✅ |
| `specs/004-scalability-cpu-fractions/contracts/cli-benchmark.md` | ✅ |
| `specs/004-scalability-cpu-fractions/plan.md` | ✅ (este ficheiro) |
| `specs/004-scalability-cpu-fractions/tasks.md` | ⏳ — gerado por `/speckit-tasks` |
