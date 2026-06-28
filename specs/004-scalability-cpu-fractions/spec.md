# Feature Specification: Scalability Benchmark Grid + Seed Investigation

**Feature Branch**: `004-scalability-cpu-fractions`  
**Created**: 2026-06-28  
**Status**: Draft  

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Varredura de escalabilidade com um único comando (Priority: P1)

O investigador, numa VM com 6 vCPUs, quer executar **um único comando** e obter automaticamente os resultados de **todas as combinações** fração × CPU: 1%, 10%, 100% × {2, 4, 6 vCPUs}, tanto para Ray quanto para Dask — ao todo, 9 combinações de parâmetros e 18 runs de algoritmo (9 × 2 backends).

**Por que P1**: É o objetivo central do trabalho de escalabilidade. Sem isto, o benchmark não serve para demonstrar como o sistema escala.

**Independent Test**: Executar um único comando `python -m cli.main benchmark --fractions 1,10,100 --workers 2,4,6 --runs 3` (ou via Docker Compose com variável de ambiente) e verificar que o CSV de saída contém linhas para todas as 18 combinações (9 fração × CPU, 2 backends), com `status=success`.

**Acceptance Scenarios**:

1. **Given** a VM com 6 vCPUs e o dataset Orkut disponível, **When** o comando de benchmark é executado com `fractions=1,10,100` e `workers=2,4,6`, **Then** o sistema produz um CSV com 18 linhas de sucesso (ou indica falha por OOM nas linhas correspondentes), sem intervenção manual.
2. **Given** o benchmark completo, **When** o relatório é gerado, **Then** o relatório inclui tabelas/gráficos comparando throughput e memória para cada combinação fração × vCPU para cada backend.
3. **Given** uma combinação com 2 vCPUs e 100% do Orkut, **When** o benchmark é executado, **Then** o sistema não ultrapassa os recursos da VM (OOM é registado como `status=failed` no CSV, não causa crash global).

---

### User Story 2 — Investigação: partições idênticas entre seeds (Priority: P2)

O investigador suspeita que os resultados do LPA são idênticos independentemente da seed usada nas campanhas atuais. Quer confirmar se isto é comportamento esperado do algoritmo no grafo Orkut, ou se há um bug que impede a seed de ter efeito real.

**Por que P2**: Compromete a validade estatística do benchmark se os 3 runs não forem de facto independentes. Deve ser resolvido antes ou junto com os novos testes.

**Independent Test**: Executar LPA Ray e LPA Dask com seeds 42, 43, 44 no mesmo grafo e comparar as partições resultantes — se todas forem byte-a-byte idênticas, o bug existe; se divergirem (mesmo que parcialmente), é comportamento esperado.

**Acceptance Scenarios**:

1. **Given** o código atual com 3 seeds distintas, **When** o benchmark corre 3 runs, **Then** existe uma análise documentada explicando se as partições idênticas são esperadas (grafo determinístico) ou um bug.
2. **Given** a análise confirmar bug, **When** a correção for aplicada, **Then** runs com seeds distintas produzem partições com número de comunidades diferente e/ou atribuição de nós diferente em pelo menos 5% dos nós.
3. **Given** a análise confirmar comportamento esperado (convergência determinística), **When** o relatório for gerado, **Then** inclui uma nota explicando que o LPA neste grafo converge para a mesma solução independentemente da ordem de processamento.

---

### User Story 3 — Relatório comparativo de escalabilidade (Priority: P3)

O investigador quer um relatório estruturado que mostre claramente como o desempenho (throughput, tempo total, memória) escala ao variar fração do grafo e número de vCPUs, comparando Ray vs Dask em cada ponto.

**Por que P3**: Produto final entregável ao professor; sem o relatório, os dados brutos do CSV não têm valor comunicativo.

**Independent Test**: Dado um CSV com as 18 linhas preenchidas, gerar o relatório e verificar que contém: (a) tabela fração × vCPU com throughput; (b) tabela fração × vCPU com pico de memória; (c) comparação Ray vs Dask para cada combinação.

**Acceptance Scenarios**:

1. **Given** o CSV completo, **When** o relatório é gerado, **Then** inclui uma dimensão `workers` como eixo adicional (além de `approach` e `fraction`).
2. **Given** o relatório, **When** o professor consulta a escalabilidade, **Then** consegue ver se dobrar vCPUs reduz o tempo em proporção linear, sublinear ou superlinear, para cada fração.

---

### Edge Cases

- O que acontece se 2 vCPUs não chegarem para completar 100% do Orkut com Dask (OOM)? → Linha marcada como `status=failed`; restantes combinações continuam.
- O que acontece se a fração 1% resultar em grafo desligado ou com apenas 1 comunidade? → Resultado registado; relatório assinala o caso.
- O que acontece se o número de workers pedido (ex. 6) exceder os vCPUs disponíveis no contentor? → Sistema usa no máximo os vCPUs disponíveis e regista o valor real utilizado.
- O que acontece se a seed investigation mostrar que o bug está na inicialização de labels e não no chunk shuffling? → A correção deve ser cirúrgica e não quebrar outros testes.

## Requirements *(mandatory)*

### Constitution Alignment

Requisitos e escopo MUST respeitar `.specify/memory/constitution.md`: legibilidade, código mínimo necessário e limites modulares. Requisitos que impliquem abstrações extras, acoplamento forte ou código sem valor MUST ser revisados ou justificados no plano da feature.

### Functional Requirements

- **FR-001**: O sistema MUST aceitar uma lista de frações (ex. `1,10,100`) como parâmetro do comando de benchmark, executando **todas** em sequência numa única invocação.
- **FR-002**: O sistema MUST aceitar uma lista de contagens de workers/vCPUs (ex. `2,4,6`) como parâmetro do comando de benchmark.
- **FR-003**: O sistema MUST executar o produto cartesiano fração × workers, produzindo uma linha de resultado no CSV para cada combinação e cada backend.
- **FR-004**: O CSV de saída MUST incluir uma coluna `workers_requested` (valor configurado pelo utilizador) e `workers_actual` (valor efetivamente usado pelo cluster) para cada linha.
- **FR-005**: O sistema MUST investigar e documentar — no código e/ou relatório — por que as partições LPA aparecem idênticas entre seeds, concluindo se é comportamento esperado ou bug.
- **FR-006**: Se for confirmado bug, o sistema MUST corrigi-lo de modo que seeds diferentes produzam partições distintas de forma reprodutível.
- **FR-007**: O relatório MUST exibir tabelas de throughput e de memória organizadas por fração × workers, uma por backend.
- **FR-008**: O sistema MUST permitir execução de toda a varredura via Docker Compose com variáveis de ambiente (`BENCHMARK_FRACTIONS=1,10,100` e `BENCHMARK_WORKERS=2,4,6`), sem alterações no código.
- **FR-009**: Uma falha (ex. OOM) numa combinação MUST ser registada como `status=failed` no CSV sem interromper as combinações restantes.
- **FR-010**: O comando de benchmark MUST continuar suportando o uso atual (`--fractions 100`, workers derivados dos CPUs) sem alteração de comportamento para utilizadores existentes.

### Key Entities

- **BenchmarkRun**: resultado de uma execução LPA para uma combinação específica (approach, fraction, workers, run_index, seed); atributos: `workers_requested`, `workers_actual`, todos os campos atuais do CSV.
- **ScalabilityGrid**: conjunto de combinações (fração × workers) geradas pelo produto cartesiano dos parâmetros fornecidos.
- **PartitionFingerprint**: resumo compacto de uma partição (ex. histograma de tamanhos de comunidade, número de comunidades) usado para comparar resultados entre seeds.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Um único comando produz resultados para todas as 9 combinações (3 frações × 3 contagens de CPU) para ambos os backends, sem intervenção manual, em menos de 24 horas numa VM de 6 vCPUs / 16 GB RAM.
- **SC-002**: O CSV de saída contém no mínimo 18 linhas (9 combinações × 2 backends), com `status=success` ou `status=failed` para cada uma — nenhuma combinação fica omissa.
- **SC-003**: A investigação de seeds produz uma conclusão documentada (nota no relatório ou comentário no código) verificável por terceiros que leiam o repositório.
- **SC-004**: Se corrigido o bug de seed, runs com seeds distintas divergem em partição de pelo menos 1% dos nós (para o grafo Orkut 1% e 10%).
- **SC-005**: O relatório gerado inclui, para cada backend, uma tabela com fração como linhas e workers como colunas, mostrando throughput (nós/s) e pico de memória (GB).
- **SC-006**: O comportamento do benchmark existente (100%, workers=CPUs) permanece inalterado — os testes unitários e de integração atuais continuam a passar.

## Assumptions

- A VM de execução tem exatamente 6 vCPUs visíveis ao OS dentro do contentor Docker; configurações de 2 e 4 workers limitam artificialmente o paralelismo sem alterar os recursos do host.
- O dataset Orkut completo está disponível em `data/raw/soc-orkut-relationships.txt` antes da execução (ou é descarregado automaticamente pelo entrypoint existente).
- "Frações iguais independentemente da seed" refere-se às partições de comunidade, não ao grafo amostrado — assume-se que o grafo amostrado é igual entre runs (comportamento atual, gerado com `cfg.seed`, não com o seed LPA por run).
- Runs com 2 vCPUs e 100% Orkut podem falhar por OOM no Dask (comportamento já observado) — este cenário é aceitável como `status=failed`.
- O relatório existente (`cli.main report`) será estendido, não substituído.
- Testes unitários e de integração existentes MUST continuar a passar sem modificação.
