# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 — ETL: limpando e enriquecendo o histórico do BTC/USD
# MAGIC
# MAGIC **Fecha o Módulo 3.** Chega de exemplo de brinquedo — este lab é o pipeline de ETL básico
# MAGIC completo, do zero, na ordem que as aulas 3.1 e 3.2 defenderam: **projetar cedo, tratar nulos
# MAGIC (inclusive um nulo disfarçado de texto), converter tipos, derivar coluna, categorizar, enriquecer**.
# MAGIC A ordem não é enfeite: cada passo prepara o próximo, e o checkpoint no fim pede que você justifique
# MAGIC cada um. É também aqui que o texto que os Labs 1 e 2 deixaram intocado finalmente vira número e
# MAGIC data.
# MAGIC
# MAGIC **Ambiente:** o mesmo dos Labs 1 e 2 — Databricks Runtime 17.3 LTS, compute clássico, access mode
# MAGIC Dedicated, Unity Catalog.
# MAGIC
# MAGIC **Requer o Lab 1 já rodado** — este lab lê o arquivo que o setup do Lab 1 extraiu da API.
# MAGIC
# MAGIC **Objetivo:** ao final, ter rodado o pipeline inteiro e saber justificar, passo a passo, **por que**
# MAGIC aquela ordem — não só executar os comandos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração e setup
# MAGIC
# MAGIC Mesma configuração dos labs anteriores. O arquivo do histórico já está no Volume.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)
print(f"Catálogo: {CATALOGO} | Schema: {SCHEMA} | Volume: {VOLUME}")

# O timestamp da API marca meia-noite em UTC. A sessão no mesmo relógio garante a data certa.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

import os

PASTA_BTC = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/btcusd"
CAMINHO_HISTORICO = f"{PASTA_BTC}/btcusd_2011-08-18_2026-09-30.json"
TABELA_LIMPA = f"{CATALOGO}.{SCHEMA}.bitcoin_limpo"

assert os.path.exists(CAMINHO_HISTORICO), "Arquivo do histórico não encontrado — rode o setup do Lab 1."

# COMMAND ----------

from pyspark.sql.functions import col

# Tudo STRING de propósito — é o que o arquivo traz. A conversão de verdade é o Exercício 4.
schema = "timestamp STRING, open STRING, high STRING, low STRING, close STRING, volume STRING"

btc_raw_df = spark.read.format("json").schema(schema).load(CAMINHO_HISTORICO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — projete cedo (*filter early, filter often*)
# MAGIC
# MAGIC O dataset bruto tem 6 colunas. O pipeline só precisa de 4: `timestamp`, `close` (o preço de
# MAGIC fechamento do dia), `open` e `volume`. Corte agora, antes de gastar processamento limpando coluna
# MAGIC que nem vai sobreviver — e renomeie com `col().alias()`, como na aula 3.1 (`alias()` é método do
# MAGIC objeto Column).

# COMMAND ----------

btc_cols_df = btc_raw_df.select(
    col("timestamp").alias("data_raw"),
    col("close").alias("preco_raw"),
    col("open").alias("abertura_raw"),
    col("volume").alias("volume_raw"),
)

contagem_inicial = btc_cols_df.count()
print(f"Linhas antes de qualquer limpeza: {contagem_inicial}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 2 — diagnostique nulos (e caia na pegadinha de propósito)
# MAGIC
# MAGIC Mesma pergunta, duas sintaxes — reforça o Lab 2: se as duas formas divergirem, uma delas está
# MAGIC errada.

# COMMAND ----------

btc_cols_df.createOrReplaceTempView("btc_cols_view")

spark.sql("""
    SELECT count_if(volume_raw IS NULL) AS nulos_volume
    FROM btc_cols_view
""").show()

# COMMAND ----------

from pyspark.sql.functions import count, when

btc_cols_df.select(
    count(when(col("volume_raw").isNull(), 1)).alias("nulos_volume")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Os dois números batem — e os dois dão **zero**. Parece que `volume_raw` não tem nenhum problema.
# MAGIC Mas confira isto:

# COMMAND ----------

btc_cols_df.filter(col("volume_raw") == "0.00000000").count()

# COMMAND ----------

# MAGIC %md
# MAGIC **33.** O arquivo não tem campo vazio nenhum — nos dias em que ninguém negociou, a API gravou o
# MAGIC texto `"0.00000000"` no volume e repetiu o último preço conhecido em `open`, `high`, `low` e
# MAGIC `close`. Não é um preço daquele dia: é a ausência de preço, preenchida. Pro Spark, `"0.00000000"` é
# MAGIC um texto válido como qualquer outro — `isNull()` não pega isso. É a pegadinha que o Lab 1 antecipou
# MAGIC (Exercício 5): nem toda ausência de dado chega como `null`.
# MAGIC
# MAGIC ## Exercício 3 — trate os nulos (de verdade, desta vez)
# MAGIC
# MAGIC Primeiro transforme o `"0.00000000"` em `null` de verdade. Só depois o `na.drop` consegue enxergar
# MAGIC essas linhas.

# COMMAND ----------

btc_normalizado_df = btc_cols_df.withColumn(
    "volume_raw", when(col("volume_raw") == "0.00000000", None).otherwise(col("volume_raw"))
)

# sem negociação não há preço do dia — a linha não serve pro pipeline
btc_sem_nulos_df = btc_normalizado_df.na.drop(subset=["volume_raw"])

print(f"Linhas depois do na.drop(volume_raw): {btc_sem_nulos_df.count()} (era {contagem_inicial})")

# COMMAND ----------

# MAGIC %md
# MAGIC Das 33 linhas removidas, 30 são de 2011, quando a corretora tinha acabado de abrir e passava dias
# MAGIC sem nenhum negócio. As outras 3 são seguidas: 6 a 8/jan/2015, quando ela ficou fora do ar. É pouco
# MAGIC (33 em 5.523, ~0,6%), mas é real — não é erro de leitura.
# MAGIC
# MAGIC ## Exercício 4 — converta os tipos (com `try_cast`)
# MAGIC
# MAGIC **Spark 3 × Spark 4.** No Spark 3, `.cast("date")` num texto inválido devolvia `NULL` sem avisar.
# MAGIC No Spark 4 o **ANSI mode vem ligado**: o mesmo valor inválido **derruba o job**. Veja com os
# MAGIC próprios olhos — `data_raw` é o dia em segundos desde 1970 (`"1313625600"`), e a tentação é mandar
# MAGIC um `cast` direto pra data:

# COMMAND ----------

try:
    btc_sem_nulos_df.select(col("data_raw").cast("date")).collect()
except Exception as erro:
    print(type(erro).__name__)
    print(str(erro).splitlines()[0])
else:
    print("Não deu erro — confira se o ANSI mode está ligado: spark.conf.get('spark.sql.ansi.enabled')")

# COMMAND ----------

# MAGIC %md
# MAGIC O erro (`CAST_INVALID_INPUT`) diz qual valor quebrou e ainda sugere a saída: `try_cast`. Com ele,
# MAGIC o que não converter vira `NULL` e a gente decide o que fazer. Mas repare: aqui o `try_cast("date")`
# MAGIC devolveria `NULL` nas 5.490 linhas. Ele **tolera** o valor que não converte; não adivinha o formato.
# MAGIC
# MAGIC O caminho certo pro `timestamp` Unix tem dois passos: texto → número inteiro (`try_cast("bigint")`)
# MAGIC e segundos → data e hora (`timestamp_seconds`). Daí pra `date` é um `cast` comum. Preço e volume
# MAGIC são mais simples: o texto já está no formato de número, `try_cast("double")` resolve.
# MAGIC
# MAGIC A data sai certa porque a configuração lá em cima pôs a sessão em UTC, o mesmo relógio da API.
# MAGIC Com a sessão no fuso de Brasília, meia-noite UTC seria 21h do dia anterior, e toda data sairia
# MAGIC um dia antes.

# COMMAND ----------

from pyspark.sql.functions import timestamp_seconds, year

btc_convertido_df = (
    btc_sem_nulos_df.withColumn("preco_usd", col("preco_raw").try_cast("double"))
    .withColumn("abertura_usd", col("abertura_raw").try_cast("double"))
    .withColumn("volume", col("volume_raw").try_cast("double"))
    .withColumn("data", timestamp_seconds(col("data_raw").try_cast("bigint")).cast("date"))
    .withColumn("ano", year(col("data")))
    .filter(col("data").isNotNull() & col("preco_usd").isNotNull())  # descarta o que não converteu
)

print(f"Linhas depois da conversão: {btc_convertido_df.count()}")
btc_convertido_df.select("data", "ano", "preco_usd", "abertura_usd", "volume").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC O filtro no fim não derrubou nenhuma linha a mais: todo preço converteu e todo `timestamp` virou
# MAGIC data. Ainda assim o padrão fica — `try_cast` + filtro é o que protege o pipeline no dia em que um
# MAGIC lote vier num formato diferente.
# MAGIC
# MAGIC `volume` é a quantidade de **bitcoins** negociados no dia, não o valor em dólar.
# MAGIC
# MAGIC ## Exercício 5 — derive uma coluna: `fechou_em_alta`
# MAGIC
# MAGIC `true` quando o dia fechou acima da abertura, `false` caso contrário.

# COMMAND ----------

btc_derivado_df = btc_convertido_df.withColumn(
    "fechou_em_alta", when(col("preco_usd") > col("abertura_usd"), True).otherwise(False)
)
btc_derivado_df.groupBy("fechou_em_alta").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 6 — categorize por faixa de preço
# MAGIC
# MAGIC "baixo" (< US$ 1.000), "médio" (US$ 1.000–10.000), "alto" (> US$ 10.000). É o `when().otherwise()`
# MAGIC da aula 3.2 — o `CASE WHEN` da DataFrame API. A primeira condição que bater é a que vale.

# COMMAND ----------

btc_categorizado_df = btc_derivado_df.withColumn(
    "faixa_preco",
    when(col("preco_usd") < 1000, "baixo")
    .when(col("preco_usd") <= 10000, "médio")
    .otherwise("alto"),
)
btc_categorizado_df.groupBy("faixa_preco").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 7 — `preco_zscore`, com e sem UDF
# MAGIC
# MAGIC A aula 3.2 foi clara: Z-score é subtração e divisão — não precisa de UDF. O Pandas UDF abaixo é
# MAGIC exercício pra praticar a ferramenta. Faça das duas formas e confirme que batem.

# COMMAND ----------

from pyspark.sql.functions import mean, stddev, pandas_udf

# Um Pandas UDF recebe LOTES (até 10.000 linhas por padrão, nunca mais que uma partição), não o
# DataFrame inteiro. Calcular média/desvio DENTRO da função normalizaria cada lote pela própria
# média. Aqui o dataset (5.490 linhas, 1 partição) cabe num lote só, então o erro nem apareceria —
# mas apareceria com mais dado ou mais partições. O certo: calcular as estatísticas globais ANTES,
# com uma agregação normal, e passar por closure.
stats = btc_categorizado_df.select(
    mean("preco_usd").alias("media"), stddev("preco_usd").alias("desvio")
).first()
media_global, desvio_global = stats["media"], stats["desvio"]


@pandas_udf("double")
def z_score(valor_series):
    return (valor_series - media_global) / desvio_global


btc_udf_df = btc_categorizado_df.withColumn("preco_zscore_udf", z_score(col("preco_usd")))

# COMMAND ----------

# A mesma conta só com expressão de coluna — sem UDF nenhuma
btc_final_df = btc_udf_df.withColumn(
    "preco_zscore", (col("preco_usd") - media_global) / desvio_global
)

btc_final_df.select("data", "preco_usd", "preco_zscore_udf", "preco_zscore").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC As duas colunas batem. A `_udf` era só pra praticar: a versão que vai pra tabela final é a
# MAGIC built-in, que o Catalyst enxerga e que não paga serialização entre JVM e Python.
# MAGIC
# MAGIC ## Exercício 8 — consolide numa única cadeia
# MAGIC
# MAGIC Igual ao fechamento da demo oficial: uma célula só, do `spark.read` até `saveAsTable`, sem
# MAGIC DataFrames intermediários soltos pela sessão. Só transformações e **uma** action no fim.
# MAGIC
# MAGIC Pra caber numa cadeia só, o Z-score aqui usa window function (`mean(...).over(janela)`), que calcula
# MAGIC a média global sem precisar de um `first()` no meio do caminho. Uma janela sem coluna de partição
# MAGIC (`Window.partitionBy()` vazio) joga todas as linhas numa partição só — tudo bem pra 5.490 linhas,
# MAGIC problema em dado grande.

# COMMAND ----------

from pyspark.sql import Window

janela_global = Window.partitionBy()

bitcoin_limpo_df = (
    spark.read.format("json").schema(schema).load(CAMINHO_HISTORICO)
    .select(
        col("timestamp").alias("data_raw"), col("close").alias("preco_raw"),
        col("open").alias("abertura_raw"), col("volume").alias("volume_raw"),
    )
    .withColumn("volume_raw", when(col("volume_raw") == "0.00000000", None).otherwise(col("volume_raw")))
    .na.drop(subset=["volume_raw"])
    .withColumn("preco_usd", col("preco_raw").try_cast("double"))
    .withColumn("abertura_usd", col("abertura_raw").try_cast("double"))
    .withColumn("volume", col("volume_raw").try_cast("double"))
    .withColumn("data", timestamp_seconds(col("data_raw").try_cast("bigint")).cast("date"))
    .withColumn("ano", year(col("data")))
    .filter(col("data").isNotNull() & col("preco_usd").isNotNull())
    .withColumn("fechou_em_alta", when(col("preco_usd") > col("abertura_usd"), True).otherwise(False))
    .withColumn(
        "faixa_preco",
        when(col("preco_usd") < 1000, "baixo").when(col("preco_usd") <= 10000, "médio").otherwise("alto"),
    )
    .withColumn(
        "preco_zscore",
        (col("preco_usd") - mean("preco_usd").over(janela_global)) / stddev("preco_usd").over(janela_global),
    )
    .select("data", "ano", "preco_usd", "abertura_usd", "volume", "fechou_em_alta", "faixa_preco", "preco_zscore")
)

bitcoin_limpo_df.write.mode("overwrite").saveAsTable(TABELA_LIMPA)

# COMMAND ----------

display(spark.table(TABELA_LIMPA).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC Para cada passo do pipeline, explique **por que** aquela ordem — não decore, justifique:
# MAGIC - por que projetar (Exercício 1) vem antes de limpar nulo?
# MAGIC - por que `isNull()` sozinho não bastou no Exercício 2 — e o que isso ensina sobre confiar cego em
# MAGIC   um único método de diagnóstico?
# MAGIC - por que limpar nulo (Exercício 3) vem antes de converter (Exercício 4)?
# MAGIC - por que categorizar (Exercício 6) só faz sentido depois da conversão (Exercício 4)?
# MAGIC
# MAGIC Se travar em alguma, volte pra aula 3.1 antes de seguir.
# MAGIC
# MAGIC ## Fechamento
# MAGIC
# MAGIC A tabela `bitcoin_limpo` que você acabou de gravar tem nulo (inclusive o disfarçado de texto)
# MAGIC tratado, tipo certo, coluna derivada, categoria e enriquecimento — Módulo 3 fechado. No **Lab 4**
# MAGIC essa mesma tabela vai mostrar, na Spark UI, exatamente onde o shuffle acontece.
