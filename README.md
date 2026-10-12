# Spark Fundamentals — material do curso

Notebooks Databricks e dataset dos labs do curso **Spark Fundamentals** (OPB School).

## Conteúdo

| Pasta | O que tem |
|---|---|
| `notebooks/` | Os 5 labs em formato de notebook Databricks (`.py`) |
| `dados/` | `bitcoin_historico.csv`: preço diário do BTC/USD, 18/07/2010 a 02/08/2020, 3.669 linhas |

## Como usar

1. No workspace Databricks: **Workspace → Import → URL** e cole o link do notebook, ou use **Git folder** apontando para este repositório.
2. Rode no compute descrito no início de cada notebook: Databricks Runtime **17.3 LTS**, compute clássico all-purpose, access mode **Dedicated**, pelo menos **1 worker**, Unity Catalog, **Photon desligado**.
3. Rode os labs em ordem. O Lab 4 lê a tabela que o Lab 3 grava.

O setup de cada notebook baixa o CSV deste repositório direto para um Volume do Unity Catalog.

## Dataset

Export histórico do investing.com, copiado de [MainakRepositor/Datasets](https://github.com/MainakRepositor/Datasets) para que os labs não dependam de um repositório de terceiros.
