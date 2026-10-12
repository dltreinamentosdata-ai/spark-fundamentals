# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6 — Delta Lake na prática: carga diária, time travel, MERGE e RESTORE
# MAGIC
# MAGIC **Fecha o Módulo 6 (Bônus).** `bitcoin_limpo` já é uma tabela Delta — o Lab 3 gravou ela com
# MAGIC `saveAsTable`, que no Databricks usa Delta por padrão, sem ninguém ter pedido. Este lab explora o
# MAGIC que isso já dava de graça: histórico de versões, time travel, upsert com `MERGE` e a volta no tempo
# MAGIC com `RESTORE`. E é aqui que o dataset deixa de ser um período fechado: você vai rodar a **carga
# MAGIC diária**, que traz da API os dias depois de 30/09/2026 e acrescenta na tabela.
# MAGIC
# MAGIC **Nota de escopo:** igual à aula 6.1, este lab é só o modelo mental — não é manutenção de tabela.
# MAGIC `OPTIMIZE`, `VACUUM` e liquid clustering ficam fora deste curso.
# MAGIC
# MAGIC **Ambiente:** o mesmo dos outros labs (Databricks Runtime 17.3 LTS, Unity Catalog). Este lab não
# MAGIC depende da Spark UI nem da RDD API. O cluster precisa de saída pra internet: a carga diária chama
# MAGIC a API.
# MAGIC
# MAGIC **Requer os Labs 1 e 3 já rodados** — este lab acrescenta arquivos na pasta que o Lab 1 criou e
# MAGIC lê e modifica a tabela `bitcoin_limpo` que o Lab 3 gravou.
# MAGIC No último exercício você desfaz todas as mudanças. Se parar antes dele, rode o Lab 3 de novo
# MAGIC antes de repetir este lab — assim a tabela volta ao estado limpo.
# MAGIC
# MAGIC **Objetivo:** consultar o histórico de versões de uma tabela Delta, acrescentar um lote novo com
# MAGIC uma carga incremental, ler uma versão anterior por time travel, fazer um upsert com `MERGE` e
# MAGIC restaurar a tabela.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)

TABELA_LIMPA = f"{CATALOGO}.{SCHEMA}.bitcoin_limpo"

# O timestamp da API marca meia-noite em UTC. A sessão no mesmo relógio garante a data certa.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — confirme que é Delta e veja o histórico

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {TABELA_LIMPA}").select("format", "numFiles").show()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Formato `delta` — e em nenhum lugar do Lab 3 você escreveu essa palavra.
# MAGIC
# MAGIC No histórico, cada linha é um commit no `_delta_log`. Se você rodou o Lab 3 mais de uma vez, vai
# MAGIC ver mais de uma linha: cada execução gravou uma versão nova, não uma sobrescrita silenciosa. Leia a
# MAGIC coluna `operation` — ela diz o que criou cada versão.
# MAGIC
# MAGIC Guarde o número da versão mais recente: é pra ela que vamos voltar no fim.

# COMMAND ----------

versao_inicial = spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").selectExpr("max(version)").first()[0]
linhas_iniciais = spark.table(TABELA_LIMPA).count()
print(f"Versão inicial: {versao_inicial} | linhas: {linhas_iniciais}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 2 — a carga diária
# MAGIC
# MAGIC A tabela parou em 30/09/2026. O processo de todo dia tem três passos: **extrair** os dias novos,
# MAGIC **limpar** com a mesma cadeia do Lab 3 e **acrescentar** na tabela.
# MAGIC
# MAGIC Primeiro a extração. É a função do setup do Lab 1, sem mudar uma linha — só que agora chamada sem
# MAGIC data limite. Ela olha o último dia que já está na pasta e traz dali até **ontem (D-1)**.

# COMMAND ----------

import datetime
import json
import os
import urllib.request

PASTA_BTC = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/btcusd"
URL_API = (
    "https://www.bitstamp.net/api/v2/ohlc/btcusd/"
    "?step=86400&limit=1000&exclude_current_candle=true&start={inicio}"
)
PRIMEIRO_DIA = datetime.date(2011, 8, 18)  # primeiro dia que a Bitstamp tem
UTC = datetime.timezone.utc


def para_epoch(dia):
    return int(datetime.datetime(dia.year, dia.month, dia.day, tzinfo=UTC).timestamp())


def extrair_btc(ate=None):
    """Baixa os dias que ainda faltam na pasta: do último já gravado até `ate` (padrão: ontem)."""
    os.makedirs(PASTA_BTC, exist_ok=True)
    ontem = datetime.datetime.now(UTC).date() - datetime.timedelta(days=1)
    ate = min(ate or ontem, ontem)

    # Marca d'água: o nome de cada arquivo termina no último dia que ele contém.
    ultimos_dias = [
        datetime.date.fromisoformat(nome[-15:-5])
        for nome in os.listdir(PASTA_BTC)
        if nome.endswith(".json")
    ]
    de = max(ultimos_dias) + datetime.timedelta(days=1) if ultimos_dias else PRIMEIRO_DIA
    if de > ate:
        print(f"Nada novo: a pasta já tem os dias até {max(ultimos_dias)}.")
        return None

    dias = []
    inicio = para_epoch(de)
    while inicio <= para_epoch(ate):  # a API devolve no máximo 1.000 dias por chamada
        with urllib.request.urlopen(URL_API.format(inicio=inicio), timeout=60) as resposta:
            dias += json.load(resposta)["data"]["ohlc"]
        inicio += 1000 * 86400
    dias = [d for d in dias if para_epoch(de) <= int(d["timestamp"]) <= para_epoch(ate)]

    esperados = (ate - de).days + 1  # o histórico tem uma linha por dia do calendário
    if len(dias) != esperados:
        raise RuntimeError(f"A API devolveu {len(dias)} dias, eram {esperados}. Nada foi gravado; rode de novo.")

    caminho = f"{PASTA_BTC}/btcusd_{de}_{ate}.json"
    with open(caminho, "w") as arquivo:
        for dia in dias:  # um objeto JSON por linha (JSON Lines)
            arquivo.write(json.dumps(dia) + "\n")
    print(f"{len(dias)} dias gravados ({de} a {ate}) em {caminho}")
    return caminho

# COMMAND ----------

extrair_btc()
display(dbutils.fs.ls(PASTA_BTC))

# COMMAND ----------

# MAGIC %md
# MAGIC A pasta ganhou um arquivo só com os dias novos; o do histórico não foi tocado. Rode a célula de
# MAGIC novo: ela avisa que não há nada novo e não chama a API. Amanhã, traria um dia.
# MAGIC
# MAGIC Agora a limpeza e a gravação. A pasta guarda tudo o que já foi extraído; quem diz **até onde já foi
# MAGIC carregado** é a própria tabela, pelo maior valor de `data`. Só passa o que vem depois dele.
# MAGIC
# MAGIC O Z-score dos dias novos usa a média e o desvio que a tabela já tem, pra ficar na mesma régua das
# MAGIC linhas antigas.

# COMMAND ----------

from pyspark.sql.functions import col, mean, stddev, timestamp_seconds, when, year

df = spark.table(TABELA_LIMPA)

ultimo_dia = df.selectExpr("max(data)").first()[0]
stats = df.select(mean("preco_usd").alias("media"), stddev("preco_usd").alias("desvio")).first()

schema = "timestamp STRING, open STRING, high STRING, low STRING, close STRING, volume STRING"

novos_df = (
    spark.read.format("json").schema(schema).load(PASTA_BTC)  # a pasta inteira: histórico + lotes diários
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
    .filter(col("data") > ultimo_dia)  # só o que a tabela ainda não tem
    .withColumn("fechou_em_alta", when(col("preco_usd") > col("abertura_usd"), True).otherwise(False))
    .withColumn(
        "faixa_preco",
        when(col("preco_usd") < 1000, "baixo").when(col("preco_usd") <= 10000, "médio").otherwise("alto"),
    )
    .withColumn("preco_zscore", (col("preco_usd") - stats["media"]) / stats["desvio"])
    .select("data", "ano", "preco_usd", "abertura_usd", "volume", "fechou_em_alta", "faixa_preco", "preco_zscore")
)

print(f"Tabela ia até {ultimo_dia}. Dias novos: {novos_df.count()}")
novos_df.write.mode("append").saveAsTable(TABELA_LIMPA)

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Apareceu **uma linha nova** no topo, com `version` maior que a inicial. Os arquivos Parquet que já
# MAGIC existiam não foram tocados — o `_delta_log` só ganhou um commit novo, listando o arquivo que este
# MAGIC append acrescentou. É assim que uma tabela Delta cresce um lote por dia.
# MAGIC
# MAGIC ## Exercício 3 — time travel

# COMMAND ----------

df_versao_antiga = spark.read.option("versionAsOf", versao_inicial).table(TABELA_LIMPA)
df_atual = spark.table(TABELA_LIMPA)

print(f"Linhas na versão {versao_inicial} (antes da carga diária): {df_versao_antiga.count()}")
print(f"Linhas na versão atual (depois da carga diária): {df_atual.count()}")

# COMMAND ----------

# A mesma consulta em SQL
spark.sql(f"SELECT count(*) AS linhas FROM {TABELA_LIMPA} VERSION AS OF {versao_inicial}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC A diferença é exatamente o número de dias que a carga trouxe. A versão anterior continua lá, intacta;
# MAGIC ela só não é mais a que a leitura pega por padrão (sem `versionAsOf`, a leitura sempre usa a versão
# MAGIC mais recente).
# MAGIC
# MAGIC ## Exercício 4 — `MERGE`: upsert de verdade
# MAGIC
# MAGIC Vamos simular uma correção de preço numa data que já existe (a primeira do dataset) e, no mesmo
# MAGIC comando, a chegada de uma data que ainda não existe.

# COMMAND ----------

data_existente = df.selectExpr("min(data)").first()[0]
data_nova = df.selectExpr("max(data)").first()[0] + datetime.timedelta(days=1)   # o dia seguinte ao último da tabela

spark.sql(f"""
    SELECT data, preco_usd FROM {TABELA_LIMPA}
    WHERE data IN ('{data_existente}', '{data_nova}')
""").show()

# COMMAND ----------

from delta.tables import DeltaTable

novidades_df = spark.createDataFrame(
    [(data_existente, 9999.99), (data_nova, 11000.0)], "data DATE, preco_usd DOUBLE"
)

tabela_delta = DeltaTable.forName(spark, TABELA_LIMPA)
_ = (
    tabela_delta.alias("atual")
    .merge(novidades_df.alias("novo"), "atual.data = novo.data")
    .whenMatchedUpdate(set={"preco_usd": "novo.preco_usd"})
    .whenNotMatchedInsert(values={"data": "novo.data", "preco_usd": "novo.preco_usd"})
    .execute()
)

# COMMAND ----------

spark.sql(f"""
    SELECT data, preco_usd FROM {TABELA_LIMPA}
    WHERE data IN ('{data_existente}', '{data_nova}')
    ORDER BY data
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Uma linha foi **atualizada** (a data que já existia agora tem `preco_usd` 9999.99) e uma foi
# MAGIC **inserida** (a data nova) — no mesmo comando. `whenMatchedUpdate` age nas linhas que casaram;
# MAGIC `whenNotMatchedInsert`, nas que não casaram. É isso que dá ao `MERGE` o nome de upsert.
# MAGIC
# MAGIC ## Exercício 5 — ligando operação a versão

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Aponte qual `version` corresponde a cada exercício: a gravação do Lab 3, o `WRITE` da carga
# MAGIC diária (Exercício 2) e o `MERGE` (Exercício 4). É o mesmo log que a aula 6.1 desenhou no board, agora na
# MAGIC tela. Repare que o `MERGE` aparece como **uma versão só**, mesmo tendo atualizado uma linha e
# MAGIC inserido outra: é um commit atômico, não duas operações separadas.
# MAGIC
# MAGIC ## Exercício 6 — `RESTORE`: desfazendo tudo
# MAGIC
# MAGIC Time travel **lê** o passado. Pra **voltar** a tabela inteira pra uma versão anterior, o comando é
# MAGIC `RESTORE`.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {TABELA_LIMPA} TO VERSION AS OF {versao_inicial}")

print(f"Linhas depois do RESTORE: {spark.table(TABELA_LIMPA).count()} (eram {linhas_iniciais} no início)")
spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela voltou a ter o número de linhas do início — e o histórico **não encolheu**: ganhou mais
# MAGIC uma linha, `RESTORE`. O log nunca é reescrito; voltar no tempo é, ele mesmo, um commit novo. As
# MAGIC versões da carga diária e do `MERGE` continuam lá, consultáveis por time travel.
# MAGIC
# MAGIC Os arquivos que a extração baixou continuam na pasta. Se você rodar este lab de novo, a carga
# MAGIC diária reaproveita esses arquivos e só chama a API pelos dias que ainda faltam.
# MAGIC
# MAGIC ## Checkpoint
# MAGIC
# MAGIC No `DESCRIBE HISTORY` da tabela: aponte qual versão corresponde a qual operação, explique por que
# MAGIC o `MERGE` vira um commit só, e por que o `RESTORE` aumentou o histórico em vez de diminuir.
# MAGIC
# MAGIC ## Fechamento — o bônus, numa frase
# MAGIC
# MAGIC Delta não é mágica: é Parquet mais um log, que você acabou de ler várias vezes nesta sessão — e
# MAGIC que cresce um commit por vez, a cada lote que chega.
# MAGIC Manter e otimizar essa tabela (`OPTIMIZE`, `VACUUM`, liquid clustering) fica fora deste curso —
# MAGIC aqui você fechou o modelo mental.
