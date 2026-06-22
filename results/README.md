# Resultados — benchmark soc-Orkut (LPA distribuído)

Artefatos extraídos de `reports.zip` (campanha principal `20260622T005654`).

## Estrutura

```text
results/
├── README.md                 # este ficheiro
├── REPORT.md                 # relatório completo (desempenho + clusterização)
├── figures/                  # gráficos gerados
└── reports/                  # CSV, logs e JSON de partições (originais)
    ├── metrics_raw_20260622T005654.csv
    ├── benchmark_run_20260622T005654.log
    └── partitions_20260622T005654/
```

## Regenerar figuras

```bash
source .venv/bin/activate
pip install matplotlib   # se necessário
python scripts/generate_results_report.py
```
