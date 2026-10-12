# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 — DataFrame vs. SQL: provando que dá exatamente no mesmo
# MAGIC
# MAGIC **Fecha o Módulo 2.** A aula 2.2 afirmou: DataFrame API e Spark SQL são duas portas de entrada
# MAGIC pro mesmo Catalyst Optimizer — o plano físico não muda. Neste lab você não vai confiar na palavra
# MAGIC do professor: vai **provar isso na tela**, com o mesmo dataset do Bitcoin Historical Data.
# MAGIC
# MAGIC **Ambiente:** o mesmo do Lab 1 — Databricks Runtime 17.3 LTS, compute clássico, access mode
# MAGIC Dedicated, Unity Catalog.
# MAGIC
# MAGIC **Objetivo:** definir schema explícito (Python e DDL), escrever em Parquet e em tabela, e provar —
# MAGIC com `explain()` e `sameSemantics()`, não de olho — que DataFrame API e SQL geram o mesmo plano.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração e setup
# MAGIC
# MAGIC Mesma configuração do Lab 1. Pode rodar de novo sem medo: schema e volume só são criados se não
# MAGIC existirem, e o CSV é baixado por cima do anterior.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)
print(f"Catálogo: {CATALOGO} | Schema: {SCHEMA} | Volume: {VOLUME}")

# COMMAND ----------

import urllib.request

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")

URL_CSV = "https://raw.githubusercontent.com/dltreinamentosdata-ai/spark-fundamentals/main/dados/bitcoin_historico.csv"
CAMINHO_CSV = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/bitcoin_historico.csv"
CAMINHO_PARQUET = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/bitcoin_historico.parquet"

urllib.request.urlretrieve(URL_CSV, CAMINHO_CSV)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — schema por inferência, e os Jobs escondidos
# MAGIC
# MAGIC Antes de rodar, abra a Spark UI. Rode a célula abaixo e volte pra lá.

# COMMAND ----------

df_inferido = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(CAMINHO_CSV)
)
df_inferido.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Confira a Spark UI: apareceram **Jobs novos**, e você ainda não rodou nenhuma action. Ler é
# MAGIC normalmente lazy; inferência de schema é a exceção — pra adivinhar os tipos, o Spark precisa abrir
# MAGIC o arquivo e varrer o dado. É uma passada extra, paga antes de qualquer trabalho de verdade.
# MAGIC
# MAGIC E o resultado nem saiu bom. Como você viu no Lab 1, a vírgula de milhar em
# MAGIC `Price`/`Open`/`High`/`Low` e o sufixo `K`/`M`/`%` em `Vol.`/`Change %` fazem a inferência desistir:
# MAGIC tudo virou `string`. Em outros arquivos o erro é mais traiçoeiro: telefone, CEP e número de conta
# MAGIC viram número e perdem o zero à esquerda, sem aviso.
# MAGIC
# MAGIC Custo escondido e tipo não confiável. Schema explícito resolve os dois.
# MAGIC
# MAGIC ## Exercício 2 — schema explícito, versão Python (`StructType`)
# MAGIC
# MAGIC Só `Date` ganha tipo de verdade aqui (`DateType`, via `dateFormat` na leitura) — as outras 6
# MAGIC colunas ficam `StringType` de propósito: limpar vírgula de milhar e sufixo `K`/`M`/`%` é conteúdo
# MAGIC do Lab 3, não deste.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType

schema_python = StructType([
    StructField("Date", DateType(), True),
    StructField("Price", StringType(), True),
    StructField("Open", StringType(), True),
    StructField("High", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Vol.", StringType(), True),
    StructField("Change %", StringType(), True),
])

df_schema_python = (
    spark.read.format("csv")
    .option("header", "true")
    .option("dateFormat", "MMM dd, yyyy")
    .schema(schema_python)
    .load(CAMINHO_CSV)
)
df_schema_python.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Volte na Spark UI. Confirme: **nenhum Job novo** apareceu com esta leitura — schema explícito é só
# MAGIC metadado, não toca o dado.
# MAGIC
# MAGIC ## Exercício 3 — o mesmo schema, versão DDL
# MAGIC
# MAGIC Mesma ideia, outra sintaxe — a que se parece com SQL puro, e a que você mais vai ver no Databricks.

# COMMAND ----------

schema_ddl = (
    "Date DATE, Price STRING, Open STRING, High STRING, Low STRING, "
    "`Vol.` STRING, `Change %` STRING"
)

df_schema_ddl = (
    spark.read.format("csv")
    .option("header", "true")
    .option("dateFormat", "MMM dd, yyyy")
    .schema(schema_ddl)
    .load(CAMINHO_CSV)
)
df_schema_ddl.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Compare os dois `printSchema()` acima — são idênticos. Duas sintaxes, mesmo resultado: é o primeiro
# MAGIC sinal de que "forma de escrever" e "o que o Spark realmente faz" são coisas separadas.
# MAGIC
# MAGIC ## Exercício 4 — escrevendo em Parquet e em tabela
# MAGIC
# MAGIC Schema validado, hora de persistir. Dois caminhos: arquivo num Volume e tabela no Unity Catalog.
# MAGIC O `mode("overwrite")` está aí de propósito: sem ele o padrão é `errorifexists`, e rodar a célula
# MAGIC pela segunda vez daria erro.

# COMMAND ----------

df_schema_ddl.write.mode("overwrite").parquet(CAMINHO_PARQUET)
display(dbutils.fs.ls(CAMINHO_PARQUET))

# COMMAND ----------

# MAGIC %md
# MAGIC Repare: o Parquet não é um arquivo, é um **diretório**. Cada partição do DataFrame é gravada por
# MAGIC uma task, em paralelo, e vira um arquivo `part-`. Aqui o DataFrame tem 1 partição, então é 1
# MAGIC arquivo de dado (mais os arquivos de controle). E, diferente do CSV, o Parquet carrega o schema
# MAGIC junto — pra ler de volta não precisa inferir nem declarar nada.
# MAGIC
# MAGIC Agora a tabela. Arquivo solto funciona, mas o padrão de produção no Databricks é tabela no Unity
# MAGIC Catalog.

# COMMAND ----------

# Tabela Delta (o formato padrão do Databricks) não aceita espaço em nome de coluna: gravar
# "Change %" direto falha com DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES. Ponto não é proibido
# (`Vol.` sozinho passaria), mas renomeamos os dois juntos por clareza.
TABELA_HISTORICO = f"{CATALOGO}.{SCHEMA}.bitcoin_historico"

df_para_tabela = df_schema_ddl.withColumnRenamed("Vol.", "Vol").withColumnRenamed("Change %", "Change_pct")
df_para_tabela.write.mode("overwrite").saveAsTable(TABELA_HISTORICO)

# COMMAND ----------

# MAGIC %md
# MAGIC Abra o **Catalog Explorer** e confirme que a tabela `bitcoin_historico` existe dentro do schema
# MAGIC `spark_fundamentals`. É uma tabela gerenciada, em formato Delta — você não pediu Delta em lugar
# MAGIC nenhum; é o padrão. (O módulo bônus abre essa caixa.)
# MAGIC
# MAGIC ## Exercício 5 — a prova de equivalência
# MAGIC
# MAGIC A pergunta: "quantos dias de registro por ano, do maior pro menor?" — respondida de duas formas.

# COMMAND ----------

from pyspark.sql.functions import col, count, year

df_schema_ddl.createOrReplaceTempView("bitcoin_view")

sql_result = spark.sql("""
    SELECT year(Date) AS ano, count(*) AS total
    FROM bitcoin_view
    GROUP BY year(Date)
    ORDER BY total DESC
""")

df_result = (
    df_schema_ddl.groupBy(year(col("Date")).alias("ano"))
    .agg(count("*").alias("total"))
    .orderBy(col("total").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuidado clássico:** `sql_result.explain() == df_result.explain()` **sempre dá `True`** — o
# MAGIC método `.explain()` só *imprime* o plano na tela e devolve `None`. `None == None` é sempre
# MAGIC verdadeiro, mesmo com planos completamente diferentes. Isso não prova nada.
# MAGIC
# MAGIC Duas formas que provam de verdade:

# COMMAND ----------

# Forma 1 — visual: os dois planos físicos impressos, um embaixo do outro
sql_result.explain()

# COMMAND ----------

df_result.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Leia os dois de baixo pra cima: mesmos operadores, na mesma ordem. Só mudam os números depois do
# MAGIC `#` — identificadores internos que o Spark gera a cada consulta. É por causa deles que comparar o
# MAGIC texto dos planos com `==` não funciona.

# COMMAND ----------

# Forma 2 — em código: sameSemantics compara os planos lógicos e ignora esses identificadores
sql_result.sameSemantics(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC `True`. Não são planos "parecidos": pro Spark, é a mesma consulta.
# MAGIC
# MAGIC ## Desafio — fim de semana vs. dia de semana
# MAGIC
# MAGIC Repita a prova com outra dimensão de agrupamento: quantos dias de registro caem em fim de semana
# MAGIC (`dayofweek` 1 = domingo ou 7 = sábado) vs. dia de semana — só 2 grupos, contraste fácil de ler.

# COMMAND ----------

from pyspark.sql.functions import dayofweek, when

sql_desafio = spark.sql("""
    SELECT
        CASE WHEN dayofweek(Date) IN (1, 7) THEN 'fim_de_semana' ELSE 'dia_util' END AS tipo_dia,
        count(*) AS total
    FROM bitcoin_view
    GROUP BY CASE WHEN dayofweek(Date) IN (1, 7) THEN 'fim_de_semana' ELSE 'dia_util' END
""")

# Agrupando DIRETO na expressão, como o SQL faz
df_desafio = (
    df_schema_ddl.groupBy(
        when(dayofweek(col("Date")).isin(1, 7), "fim_de_semana").otherwise("dia_util").alias("tipo_dia")
    )
    .agg(count("*").alias("total"))
)

sql_desafio.sameSemantics(df_desafio)

# COMMAND ----------

# MAGIC %md
# MAGIC `True` de novo. Agora a pegadinha: e se o lado DataFrame criar a coluna com `withColumn()` antes
# MAGIC de agrupar? O resultado é o mesmo, mas olhe o que o `sameSemantics` responde.

# COMMAND ----------

df_desafio_withcolumn = (
    df_schema_ddl.withColumn(
        "tipo_dia", when(dayofweek(col("Date")).isin(1, 7), "fim_de_semana").otherwise("dia_util")
    )
    .groupBy("tipo_dia")
    .agg(count("*").alias("total"))
)

print("Mesmo resultado?", sorted(sql_desafio.collect()) == sorted(df_desafio_withcolumn.collect()))
print("sameSemantics?  ", sql_desafio.sameSemantics(df_desafio_withcolumn))

# COMMAND ----------

# MAGIC %md
# MAGIC Mesmo resultado, `sameSemantics` = `False`. Isso quer dizer que o Spark vai executar de um jeito
# MAGIC diferente? Olhe os dois planos físicos:

# COMMAND ----------

sql_desafio.explain()

# COMMAND ----------

df_desafio_withcolumn.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Os mesmos operadores, na mesma ordem. O plano físico é o mesmo. (A saída vem mais comprida que no
# MAGIC Exercício 5 porque estas consultas já rodaram: o AQE mostra o plano inicial e o final.)
# MAGIC
# MAGIC O que aconteceu: o `sameSemantics` compara o plano **lógico**, antes de o otimizador terminar o
# MAGIC trabalho. O `withColumn()` deixa um passo a mais nesse plano; o Catalyst elimina esse passo, e o que
# MAGIC vai pra execução é idêntico. Lição: `sameSemantics` `True` prova que é a mesma consulta; `False`
# MAGIC **não** prova que a execução é diferente — só que o caminho escrito foi outro. Quem dá a palavra
# MAGIC final é o plano físico.
# MAGIC
# MAGIC Curiosidade que o resultado confirma: Bitcoin negocia 24/7 — o dataset não tem um único dia
# MAGIC faltando no calendário inteiro (18/07/2010–02/08/2020), diferente de um dataset de bolsa de
# MAGIC ações, que pularia sábado e domingo.
# MAGIC
# MAGIC ## Checkpoint
# MAGIC
# MAGIC Mostre, na tela, os dois planos físicos do Exercício 5 e aponte: não são "parecidos" — são
# MAGIC **idênticos**. Depois explique, com as suas palavras, por que o `sameSemantics` do desafio deu
# MAGIC `False` mesmo com resultado e plano físico iguais.
# MAGIC
# MAGIC ## Fechamento
# MAGIC
# MAGIC Schema explícito (Python e DDL, mesmo resultado), gravação em Parquet e em tabela, e a prova real de
# MAGIC que SQL e DataFrame API terminam no mesmo plano — Módulo 2 fechado. No **Lab 3** você aplica o
# MAGIC mesmo dataset num pipeline de ETL completo, do zero — inclusive limpando, finalmente, a vírgula de
# MAGIC milhar e os sufixos que este lab deixou de propósito sem tocar.
