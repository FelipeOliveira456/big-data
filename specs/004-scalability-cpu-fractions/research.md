# Research: Scalability Benchmark Grid + Seed Investigation

## Decision 1: Por que as partições são idênticas entre seeds?

**Decision**: Comportamento **esperado** (por design), não é bug.

**Rationale**:

O LPA distribuído implementado é **síncrono/batch**: todos os chunks leem do **mesmo snapshot** antes de qualquer update (`np.copyto(snapshot, labels)` antes do dispatch). Portanto:

- Dentro de uma iteração, o label de nó A atualizado por chunk-0 **não** é visível para nó B processado por chunk-1 — ambos leem o `snapshot` congelado.
- O desempate no `_vote_kernel` é determinístico: `cnt > best_count OR (cnt == best_count AND lbl < best_label)` → **sempre o menor label vence** em empate.
- O seed só afeta `shuffled_node_chunks`: muda quais nós ficam em qual chunk, mas como todos os chunks leem do mesmo snapshot com o mesmo desempate determinístico, o **resultado por iteração é idêntico independentemente do seed**.
- `init_labels()` = `node_ids.astype(int64)` — completamente determinístico.

**Isto difere do LPA assíncrono** (implementação sequencial `run_lpa_sequential`), onde o nó A vê os labels já atualizados dos vizinhos processados antes dele na mesma iteração → o seed muda a ordem e pode mudar o resultado.

**Consequência para o benchmark**: os 3 runs com seeds 42, 43, 44 produzem partições idênticas. Isso é válido estatisticamente para medir **tempo e memória** (o grafo e o algoritmo são os mesmos), mas não serve como "3 amostras independentes do algoritmo estocástico". O relatório deve documentar isso.

**Alternativas consideradas**:
- Adicionar aleatoriedade ao `init_labels` (seed-dependente): mudaria o algoritmo e invalidaria comparação com literatura. Rejeitado.
- Usar LPA assíncrono para ter variação: fora do escopo da feature; muda o algoritmo.
- Documentar como-está: **escolhido** — correto e honesto.

---

## Decision 2: Como adicionar a dimensão `workers` ao benchmark

**Decision**: Adicionar `--workers` como lista CSV ao CLI; looping `fractions → workers → approaches → runs` no runner; `replace(cfg, lpa_chunk_divisor=w)` para isolar cada workers value.

**Rationale**:

- O grafo amostrado é determinístico (depende de `cfg.seed`, não do seed LPA), então carregar uma vez por fração ainda funciona.
- `replace(cfg, lpa_chunk_divisor=w)` é o mecanismo já existente para substituir configuração por run sem alterar o objeto global.
- `lpa_chunk_divisor` já controla diretamente `num_cpus` no Ray e `n_workers` no Dask via `effective_ray_cpus` / `effective_dask_workers`.
- Adicionar `workers_requested` e `workers_actual` ao CSV (usando `res.num_workers` existente em `LpaResult`) é não-breaking: backward compat mantida com colunas novas no final do header.

**Alternativas consideradas**:
- Loop workers como loop mais externo (fora de fractions): recarregaria o grafo N vezes. Rejeitado.
- Novo campo no AppConfig para lista de workers: adiciona estado global desnecessário. Rejeitado.
- Subcomando CLI separado `benchmark-scalability`: duplicação de código desnecessária. Rejeitado.

---

## Decision 3: Como o relatório apresenta a dimensão workers

**Decision**: Adicionar seção "Escalabilidade" ao relatório existente com tabela fração × workers por backend (throughput e memória); agrupamento `(approach, fraction_pct, workers_requested)`.

**Rationale**:

- O relatório atual já agrupa por `(approach, fraction_pct)` — basta incluir `workers_requested` no agrupamento e na exibição.
- Mantém compatibilidade com CSVs que não têm a coluna `workers_requested` (gera seção escalabilidade só se a coluna existe).

**Alternativas consideradas**:
- Script separado de análise de escalabilidade: duplicação de pipeline. Rejeitado.
- Gráficos matplotlib por linha de comando: fora do escopo mínimo; o professor pediu tabelas comparativas. Dejado como melhoria futura.

---

## Decision 4: Como passar workers via Docker Compose

**Decision**: `BENCHMARK_WORKERS` env var no `docker-compose.yml` e no `docker-entrypoint.sh`; passado como `--workers "$BENCHMARK_WORKERS"` ao CLI se não vazio.

**Rationale**: Padrão já usado para `BENCHMARK_FRACTIONS`, `BENCHMARK_RUNS`, etc. Mínimo impacto no entrypoint existente.

---

## Decision 5: Documentar seed na saída

**Decision**: Adicionar nota na seção "Métricas" do relatório gerado explicando que o LPA distribuído síncrono produz partições determinísticas (seeds não afetam o resultado, só o tempo).

**Rationale**: Transparência científica. O professor precisa saber que os 3 runs medem variabilidade temporal, não variabilidade algorítmica.
