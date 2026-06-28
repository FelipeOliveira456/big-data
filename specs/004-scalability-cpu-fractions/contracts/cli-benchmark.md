# Contrato: CLI benchmark — argumento `--workers`

## Comando

```
python -m cli.main benchmark \
  --input <path> \
  --fractions <csv-float>   # ex: "1,10,100"
  --workers  <csv-int>      # NOVO — ex: "2,4,6" (default: "" → usa os.cpu_count())
  --runs <int>
  [--ray-only | --dask-only]
  [--run-stamp <str>]
  [--append]
```

## Novo argumento `--workers`

| Aspecto | Detalhe |
|---------|---------|
| Tipo | `str` (comma-separated ints) |
| Default | `""` → usa `os.cpu_count()` como único valor |
| Exemplo | `"2,4,6"` → gera 3 workers configurations |
| Env var equivalente | `BENCHMARK_WORKERS` |

## Produto cartesiano gerado

Com `--fractions 1,10,100 --workers 2,4,6 --runs 1 (both backends)`:

| # | approach | fraction | workers |
|---|----------|----------|---------|
| 1 | ray | 1% | 2 |
| 2 | dask | 1% | 2 |
| 3 | ray | 1% | 4 |
| 4 | dask | 1% | 4 |
| 5 | ray | 1% | 6 |
| 6 | dask | 1% | 6 |
| 7 | ray | 10% | 2 |
| … | … | … | … |
| 18 | dask | 100% | 6 |

## CSV de saída — colunas adicionadas

| Coluna | Tipo | Exemplo |
|--------|------|---------|
| `workers_requested` | int | `4` |
| `workers_actual` | int | `4` |

Estas colunas aparecem no final do header. CSVs sem estas colunas continuam válidos para o relatório (backfilled com `"?"`).

## Variável de ambiente Docker

```yaml
# docker-compose.yml
environment:
  BENCHMARK_WORKERS: "2,4,6"   # nova variável; default vazio → cpu_count()
```

```bash
# docker-entrypoint.sh
: "${BENCHMARK_WORKERS:=}"
# passado como --workers "$BENCHMARK_WORKERS" se não vazio
```
