# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6 — Delta Lake na prática: histórico, time travel, MERGE e RESTORE
# MAGIC
# MAGIC **Fecha o Módulo 6 (Bônus).** `bitcoin_limpo` já é uma tabela Delta — o Lab 3 gravou ela com
# MAGIC `saveAsTable`, que no Databricks usa Delta por padrão, sem ninguém ter pedido. Este lab explora o
# MAGIC que isso já dava de graça: histórico de versões, time travel, upsert com `MERGE` e a volta no tempo
# MAGIC com `RESTORE`.
# MAGIC
# MAGIC **Nota de escopo:** igual à aula 6.1, este lab é só o modelo mental — não é manutenção de tabela.
# MAGIC `OPTIMIZE`, `VACUUM` e liquid clustering ficam fora deste curso.
# MAGIC
# MAGIC **Ambiente:** o mesmo dos outros labs (Databricks Runtime 17.3 LTS, Unity Catalog). Este lab não
# MAGIC depende da Spark UI nem da RDD API.
# MAGIC
# MAGIC **Requer o Lab 3 já rodado** — este lab lê e modifica a tabela `bitcoin_limpo` que o Lab 3 gravou.
# MAGIC No último exercício você desfaz todas as mudanças. Se parar antes dele, rode o Lab 3 de novo
# MAGIC antes de repetir este lab — assim a tabela volta ao estado limpo.
# MAGIC
# MAGIC **Objetivo:** consultar o histórico de versões de uma tabela Delta, ler uma versão anterior por
# MAGIC time travel, fazer um upsert com `MERGE` e restaurar a tabela.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)

TABELA_LIMPA = f"{CATALOGO}.{SCHEMA}.bitcoin_limpo"

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
# MAGIC ## Exercício 2 — gere uma versão nova de propósito
# MAGIC
# MAGIC Vamos acrescentar uma linha: a de maior preço, repetida.

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table(TABELA_LIMPA)

linha_maior_preco = df.orderBy(col("preco_usd").desc()).limit(1)
linha_maior_preco.write.mode("append").saveAsTable(TABELA_LIMPA)

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {TABELA_LIMPA}").select("version", "timestamp", "operation").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Apareceu **uma linha nova** no topo, com `version` maior que a inicial. Os arquivos Parquet que já
# MAGIC existiam não foram tocados — o `_delta_log` só ganhou um commit novo, listando o arquivo que este
# MAGIC append acrescentou.
# MAGIC
# MAGIC ## Exercício 3 — time travel

# COMMAND ----------

df_versao_antiga = spark.read.option("versionAsOf", versao_inicial).table(TABELA_LIMPA)
df_atual = spark.table(TABELA_LIMPA)

print(f"Linhas na versão {versao_inicial} (antes do Exercício 2): {df_versao_antiga.count()}")
print(f"Linhas na versão atual (depois do Exercício 2): {df_atual.count()}")

# COMMAND ----------

# A mesma consulta em SQL
spark.sql(f"SELECT count(*) AS linhas FROM {TABELA_LIMPA} VERSION AS OF {versao_inicial}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC A diferença é exatamente 1 — a linha que você acrescentou. A versão anterior continua lá, intacta;
# MAGIC ela só não é mais a que a leitura pega por padrão (sem `versionAsOf`, a leitura sempre usa a versão
# MAGIC mais recente).
# MAGIC
# MAGIC ## Exercício 4 — `MERGE`: upsert de verdade
# MAGIC
# MAGIC Vamos simular uma correção de preço numa data que já existe (a primeira do dataset) e, no mesmo
# MAGIC comando, a chegada de uma data que ainda não existe.

# COMMAND ----------

import datetime

data_existente = df.selectExpr("min(data)").first()[0]
data_nova = datetime.date(2020, 8, 3)   # o dia seguinte ao último do dataset

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
# MAGIC Aponte qual `version` corresponde a cada exercício: a gravação do Lab 3, o `WRITE` do append
# MAGIC (Exercício 2) e o `MERGE` (Exercício 4). É o mesmo log que a aula 6.1 desenhou no board, agora na
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
# MAGIC versões do append e do `MERGE` continuam lá, consultáveis por time travel.
# MAGIC
# MAGIC ## Checkpoint
# MAGIC
# MAGIC No `DESCRIBE HISTORY` da tabela: aponte qual versão corresponde a qual operação, explique por que
# MAGIC o `MERGE` vira um commit só, e por que o `RESTORE` aumentou o histórico em vez de diminuir.
# MAGIC
# MAGIC ## Fechamento — o bônus, numa frase
# MAGIC
# MAGIC Delta não é mágica: é Parquet mais um log, que você acabou de ler várias vezes nesta sessão.
# MAGIC Manter e otimizar essa tabela (`OPTIMIZE`, `VACUUM`, liquid clustering) fica fora deste curso —
# MAGIC aqui você fechou o modelo mental.
