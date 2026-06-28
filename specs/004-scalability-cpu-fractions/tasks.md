# Tasks: Scalability Benchmark Grid + Seed Investigation

**Input**: Design documents em `specs/004-scalability-cpu-fractions/`  
**Branch**: `004-scalability-cpu-fractions`

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Pode correr em paralelo (ficheiros diferentes, sem dependências em aberto)
- **[Story]**: Qual user story a task pertence (US1, US2, US3)
- Todos os caminhos são relativos à raiz do repositório

---

## Phase 1: Setup

**Purpose**: Nenhum scaffolding novo — projeto existente. Verificar branch correto.

- [x] T001 Confirmar que o branch activo é `004-scalability-cpu-fractions` (`git branch --show-current`)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Adicionar as duas colunas `workers_requested` / `workers_actual` ao dataclass e ao CSV header em `src/benchmark/runner.py`. Bloqueia US1 e US3 (ambas lêem/escrevem estas colunas).

⚠️ **CRITICAL**: US1, US3 dependem desta fase.

- [x] T002 Adicionar campos `workers_requested: int = 0` e `workers_actual: int = 0` ao dataclass `BenchmarkRow` (no final) em `src/benchmark/runner.py`
- [x] T003 Adicionar `"workers_requested"` e `"workers_actual"` ao final de `CSV_HEADER` em `src/benchmark/runner.py`
- [x] T004 Adicionar parâmetro `workers_requested: int` a `_run_approach` e `_row_from_result` em `src/benchmark/runner.py`; preencher `workers_requested=workers_requested` e `workers_actual=res.num_workers` no `BenchmarkRow` retornado

**Checkpoint**: `BenchmarkRow` tem os novos campos; CSV header actualizado; `_row_from_result` popula os dois valores.

---

## Phase 3: User Story 1 — Varredura de escalabilidade com um único comando (P1) 🎯 MVP

**Goal**: Um único comando `python -m cli.main benchmark --fractions 1,10,100 --workers 2,4,6` produz todas as 18 runs (3×3×2 backends) sem intervenção manual.

**Independent Test**: Executar com a fixture `tests/integration/fixtures/orkut_0p1pct.npz --fractions 0.1 --workers 2,4 --runs 1` e verificar CSV com 4 linhas (2 workers × 2 backends), `status=success`, colunas `workers_requested` com valores corretos.

### Implementação US1

- [x] T005 [US1] Adicionar função `_parse_workers(value: str) -> list[int]` ao módulo `src/cli/main.py` (ex: `"2,4,6"` → `[2, 4, 6]`; `""` → `[]`)
- [x] T006 [US1] Adicionar argumento `--workers` (type=str, default="") ao subparser `benchmark` em `src/cli/main.py`; passar `_parse_workers(args.workers) or None` como `workers_list` a `run_benchmark_campaign`
- [x] T007 [US1] Adicionar parâmetro `workers_list: list[int] | None = None` a `run_benchmark_campaign` em `src/benchmark/runner.py`; calcular `effective_workers = workers_list or [cfg.lpa_chunk_divisor]`; envolver o loop de approaches e runs com `for w in effective_workers: cfg_w = replace(cfg, lpa_chunk_divisor=w)` e passar `cfg_w` e `workers_requested=w` a `_run_approach` (depende de T004)
- [x] T008 [P] [US1] Adicionar variável `BENCHMARK_WORKERS` ao `scripts/docker-entrypoint.sh` (`: "${BENCHMARK_WORKERS:=}"`); construir flag `--workers "$BENCHMARK_WORKERS"` e passá-la ao `python -m cli.main benchmark` se não vazia
- [x] T009 [P] [US1] Adicionar `BENCHMARK_WORKERS: ""` à secção `environment` do serviço `lpa` em `docker-compose.yml`

### Testes US1

- [x] T010 [P] [US1] Adicionar `test_parse_workers_valid` e `test_parse_workers_empty` em `tests/unit/test_cli.py`
- [x] T011 [US1] Adicionar `test_workers_list_cartesian_product` em `tests/unit/test_benchmark_runner.py`: mock `_run_approach`; chamar `run_benchmark_campaign` com `fractions=[1.0, 10.0]` e `workers_list=[2, 4]`; verificar que `_run_approach` é chamado 4× (2 frações × 2 workers) por approach
- [x] T012 [US1] Adicionar `test_workers_actual_in_csv_row` em `tests/unit/test_benchmark_runner.py`: verificar que `BenchmarkRow.workers_requested == w` e `workers_actual == res.num_workers`
- [x] T013 [US1] Adicionar caso de integração `test_lpa_workers_grid` em `tests/integration/test_lpa_orkut.py`: usar fixture `orkut_0p1pct.npz`, `workers=[2, 4]`, `runs=1`; verificar que CSV tem 4 linhas (2 workers × 2 backends) com `workers_requested` correcto e `status=success`

**Checkpoint**: `python -m cli.main benchmark --input <fixture> --fractions 0.1 --workers 2,4 --runs 1` produz CSV com 4 linhas; `pytest tests/unit/ -k workers` passa.

---

## Phase 4: User Story 2 — Investigação: partições idênticas entre seeds (P2)

**Goal**: Documentar (no código e relatório) que o LPA síncrono/batch produz partições determinísticas independentemente do seed — comportamento esperado, não bug.

**Independent Test**: `pytest tests/unit/test_lpa_seed_determinism.py -v` passa e a asserção confirma que seeds 42 e 99 produzem partições idênticas num grafo de teste pequeno.

### Implementação US2

- [x] T014 [US2] Adicionar ao docstring de `run_lpa_distributed` em `src/lpa_core/distributed.py` uma nota "Note — seed determinism" explicando que o LPA síncrono lê do snapshot congelado, o desempate é determinístico (min label), e portanto seeds distintos produzem partições idênticas — comportamento esperado (ver research.md)

### Testes US2

- [x] T015 [P] [US2] Criar `tests/unit/test_lpa_seed_determinism.py` com `test_distributed_lpa_identical_across_seeds`: construir grafo pequeno (≥10 nós); correr `run_lpa_distributed` com mock `run_one_iteration` que usa `lpa_iteration_chunk` directamente; comparar partições de seeds 42 e 99; `assert np.array_equal(labels_42, labels_99)` com mensagem explicativa

**Checkpoint**: `pytest tests/unit/test_lpa_seed_determinism.py -v` passa; docstring actualizado.

---

## Phase 5: User Story 3 — Relatório comparativo de escalabilidade (P3)

**Goal**: O relatório gerado (`comparison_<stamp>.md`) inclui tabelas throughput e memória organizadas por fração × workers para cada backend.

**Independent Test**: Dado um CSV com colunas `workers_requested` e 12 linhas (3 frações × 2 workers × 2 backends × 1 run), chamar `generate_report` e verificar que o Markdown resultante contém a secção "Escalabilidade" com tabelas para Ray e Dask.

### Implementação US3

- [x] T016 [US3] Estender agrupamento em `generate_report` em `src/benchmark/report.py`: se coluna `workers_requested` existir no CSV, agrupar por `(approach, fraction_pct, workers_requested)`; caso contrário, manter comportamento actual (backward compat para CSVs antigos)
- [x] T017 [US3] Adicionar secção "Escalabilidade: Throughput por Fração × Workers" ao relatório gerado em `src/benchmark/report.py`: tabela com frações como linhas, workers como colunas, throughput médio em cada célula; uma tabela por backend (Ray e Dask); omitir secção se CSV não tem `workers_requested`
- [x] T018 [US3] Adicionar secção "Escalabilidade: Memória por Fração × Workers" ao relatório em `src/benchmark/report.py` com o mesmo layout mas `peak_process_tree_rss_mb` (em GB) em cada célula

### Testes US3

- [x] T019 [P] [US3] Adicionar `test_report_scalability_section` em `tests/unit/test_report.py`: construir CSV em memória com `workers_requested`; chamar `generate_report`; verificar que output Markdown contém "Escalabilidade" e células com throughput

**Checkpoint**: `python -m cli.main report --input-csv <csv_com_workers>` inclui tabelas de escalabilidade; `pytest tests/unit/test_report.py -k scalability` passa.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [x] T020 [P] Actualizar secção "Docker" do `README.md` com `BENCHMARK_WORKERS` e exemplo `"2,4,6"`; actualizar tabela de variáveis de ambiente
- [x] T021 [P] Actualizar secção "Configuração" do `README.md` com `--workers` e nota sobre seed determinismo
- [x] T022 Adicionar comentário `# ex: BENCHMARK_WORKERS=2,4,6` ao `docker-compose.yml` junto à nova variável
- [x] T023 Executar validação do quickstart: `PYTHONPATH=src python -m cli.main benchmark --input tests/integration/fixtures/orkut_0p1pct.npz --fractions 0.1 --workers 2,4 --runs 1` seguido de `python -m cli.main report`; confirmar CSV com 4 linhas e relatório com secção de escalabilidade
- [x] T024 Correr suite completa `pytest tests/unit/ -v` e `pytest tests/integration/ -m integration -v`; confirmar zero regressões

---

## Extra (user request): BFS artifact pre-build

- [x] T025 Criar `src/preprocessing/fraction_artifacts.py` com `ensure_fraction_artifact` e `load_fraction_for_benchmark` — BFS fora de `algorithm_time_s`
- [x] T026 Integrar `load_fraction_for_benchmark` em `src/benchmark/runner.py` para frações &lt; 100%
- [x] T027 Testes em `tests/unit/test_fraction_artifacts.py`
- [x] T028 Code review em `review_20260628_195600.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1**: Sem dependências — pode começar imediatamente
- **Phase 2**: Depende de Phase 1 — **bloqueia US1 e US3**
- **Phase 3 (US1)**: Depende da Phase 2
- **Phase 4 (US2)**: Independente — pode começar após Phase 1 (não depende da Phase 2)
- **Phase 5 (US3)**: Depende da Phase 2 (precisa das colunas `workers_requested`)
- **Phase 6**: Depende de US1, US2, US3 concluídas

### User Story Dependencies

- **US2**: Totalmente independente — pode correr em paralelo com Phase 2/US1
- **US1**: Depende de Phase 2 (T002–T004)
- **US3**: Depende de Phase 2 (T002–T004) e recomenda US1 completo para ter CSV real

### Within Each User Story

- Implementação antes dos testes de integração
- `_parse_workers` (T005) antes de `--workers` arg (T006)
- Arg CLI (T006) antes de `workers_list` no runner (T007)
- `workers_list` no runner (T007) antes do teste de integração (T013)

### Parallel Opportunities

- **T008 e T009** (Docker): completamente independentes entre si e da lógica Python
- **T010** (teste parse_workers): pode correr logo após T005
- **T014 e T015** (US2): independentes de US1 e US3
- **T016, T017, T018** (US3): sequenciais entre si mas independentes de US2

---

## Parallel Execution Examples

```bash
# Phase 2 — pode fazer T002→T003→T004 em sequência (mesmo ficheiro)

# US1 + US2 em paralelo (ficheiros diferentes):
# Agente A: T005 → T006 → T007 → T008/T009 → T010/T011/T012 → T013
# Agente B: T014 → T015

# Dentro de US1:
# T008 (entrypoint.sh) e T009 (docker-compose.yml) em paralelo
# T010 (test_cli.py) e T011/T012 (test_benchmark_runner.py) em paralelo
```

---

## Implementation Strategy

### MVP (US1 apenas)

1. Phase 1: T001
2. Phase 2: T002 → T003 → T004
3. US1: T005 → T006 → T007 → T008/T009 (paralelo) → T010/T011/T012 (paralelo) → T013
4. **VALIDAR**: `pytest tests/unit/ -k workers` + quickstart fixture
5. Entrega intermédia: benchmark com `--workers` funcional

### Entrega incremental completa

1. MVP (US1) → valida grid de escalabilidade
2. US2 (T014/T015) → documenta seed + teste
3. US3 (T016 → T017 → T018 → T019) → relatório com tabelas
4. Phase 6 (polish) → documentação + validação final

---

## Notes

- [P] = ficheiros diferentes, sem dependências em aberto naquele momento
- US2 pode ser feita em qualquer ordem relativamente a US1/US3
- Não adicionar colunas ao meio do `CSV_HEADER` — apenas no final (backward compat)
- `replace(cfg, lpa_chunk_divisor=w)` é o mecanismo correcto; não modificar `AppConfig` globalmente
- O teste T013 usa a fixture existente `orkut_0p1pct.npz` — não cria novo fixture
