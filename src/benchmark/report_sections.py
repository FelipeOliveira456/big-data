"""Reusable Markdown sections for benchmark reports."""

RACE_CONDITION_RAY = """
## Ray: race conditions no Louvain distribuído

Na Fase 1, workers Ray avaliam movimentos com base no **mesmo snapshot** da
partição de comunidades. Dois workers podem propor comunidades diferentes para o
mesmo nó; a aplicação **síncrona ao fim da rodada** (último movimento vence para
conflitos no mesmo nó) é comportamento esperado em Louvain paralelo assíncrono.

O algoritmo continua a convergir na prática porque cada rodada completa
reavalia todos os nós sobre o estado atualizado. Esta limitação está documentada
conforme FR-012.
""".strip()

REFERENCES = """
## Referências

1. Blondel et al. (2008). Fast unfolding of communities in large networks. J. Stat. Mech. P10008.
2. Leskovec & Krevl (2014). SNAP Datasets. https://snap.stanford.edu/data
3. Moritz et al. (2018). Ray: A Distributed Framework for Emerging AI Applications. OSDI 2018.
4. Rocklin (2015). Dask: Parallel Computation with Blocked algorithms and Task Scheduling. SciPy 2015.
5. Ray Core: https://docs.ray.io/en/latest/ray-core/walkthrough.html
6. Dask Distributed: https://distributed.dask.org/
""".strip()
