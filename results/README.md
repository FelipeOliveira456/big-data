# Resultados — benchmark soc-Orkut (LPA distribuído)

Artefatos extraídos de `reports.zip` (campanha principal `20260622T005654`).

## Estrutura

```text
results/
├── REPORT.md                 # relatório completo
├── figures/                  # gráficos de desempenho
└── reports/                  # CSV, logs e JSON (originais)
```

## Regenerar figuras

```bash
source .venv/bin/activate
pip install matplotlib   # se necessário
python scripts/generate_results_report.py
```

O script inclui **Dask run 1** da campanha isolada `030138` (barra hachurada no gráfico).
