# Spark Fundamentals — material do curso

Notebooks Databricks dos labs do curso **Spark Fundamentals** (DL Treinamentos).

## Conteúdo

| Pasta | O que tem |
|---|---|
| `notebooks/` | Os 5 labs em formato de notebook Databricks (`.py`) |

## Como usar

1. No workspace Databricks, crie uma **Git folder** apontando para este repositório. Se preferir, use **Workspace → Import → URL** com o link de cada notebook.
2. Rode no compute descrito no início de cada notebook: Databricks Runtime **17.3 LTS**, compute clássico all-purpose, access mode **Dedicated**, pelo menos **1 worker**, Unity Catalog, **Photon desligado**. O cluster precisa de saída para a internet.
3. Rode os labs em ordem. Os Labs 2 e 3 leem o arquivo que o Lab 1 extrai; o Lab 4 lê a tabela que o Lab 3 grava; o Lab 6 usa os dois.

## Dataset

Histórico diário do **BTC/USD** na corretora Bitstamp: abertura, máxima, mínima, fechamento e volume de cada dia. Nenhum arquivo de dados fica neste repositório. O setup do Lab 1 chama a [API pública da Bitstamp](https://www.bitstamp.net/api/) (sem chave, sem cadastro) e grava a resposta num Volume do Unity Catalog, em `/Volumes/<catálogo>/spark_fundamentals/dados/btcusd/`.

A extração é incremental:

| Quando | O que ela traz | Arquivo gravado |
|---|---|---|
| Lab 1, primeira execução | O histórico, de 18/08/2011 a 30/09/2026 (5.523 dias) | `btcusd_2011-08-18_2026-09-30.json` |
| Lab 1, execuções seguintes | Nada: a pasta já tem esses dias | nenhum |
| Lab 6, a cada execução | Do dia seguinte ao último já gravado até ontem (D-1, em UTC). Se a pasta já está em dia, nada | `btcusd_<primeiro dia>_<último dia>.json` |

Os Labs 1 a 4 usam só o arquivo do histórico, que é um período fechado. Por isso as contagens que você vê batem com as do vídeo, em qualquer dia que você rodar. O Lab 6 faz a carga diária e acrescenta os dias novos na tabela Delta.

O dia corrente nunca entra: ele ainda não fechou, e o preço mudaria a cada chamada.

Cada arquivo é JSON Lines, um dia por linha. Todos os campos chegam como texto, do jeito que a API manda:

```json
{"timestamp": "1313625600", "open": "10.90", "high": "10.90", "low": "10.90", "close": "10.90", "volume": "0.48990826"}
```

`timestamp` é o dia em segundos desde 01/01/1970 (meia-noite UTC). `volume` é a quantidade de bitcoins negociados no dia.
