# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 — DataFrame vs. SQL: provando que dá exatamente no mesmo
# MAGIC
# MAGIC **Fecha o Módulo 2.** A aula 2.2 afirmou: DataFrame API e Spark SQL são duas portas de entrada
# MAGIC pro mesmo Catalyst Optimizer — o plano físico não muda. Neste lab você não vai confiar na palavra
# MAGIC do professor: vai **provar isso na tela**, com o mesmo histórico do BTC/USD do Lab 1.
# MAGIC
# MAGIC **Ambiente:** o mesmo do Lab 1 — Databricks Runtime 17.3 LTS, compute clássico, access mode
# MAGIC Dedicated, Unity Catalog.
# MAGIC
# MAGIC **Requer o Lab 1 já rodado** — este lab lê o arquivo que o setup do Lab 1 extraiu da API.
# MAGIC
# MAGIC **Objetivo:** definir schema explícito (Python e DDL), escrever em Parquet e em tabela, e provar —
# MAGIC com `explain()` e `sameSemantics()`, não de olho — que DataFrame API e SQL geram o mesmo plano.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração e setup
# MAGIC
# MAGIC Mesma configuração do Lab 1. O arquivo do histórico já está no Volume; aqui só conferimos.

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
CAMINHO_PARQUET = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/bitcoin_historico.parquet"

assert os.path.exists(CAMINHO_HISTORICO), "Arquivo do histórico não encontrado — rode o setup do Lab 1."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — schema por inferência, e os Jobs escondidos
# MAGIC
# MAGIC Antes de rodar, abra a Spark UI. Rode a célula abaixo e volte pra lá.

# COMMAND ----------

df_inferido = spark.read.format("json").load(CAMINHO_HISTORICO)
df_inferido.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Confira a Spark UI: apareceu **um Job novo**, e você ainda não rodou nenhuma action. Ler é
# MAGIC normalmente lazy; inferência de schema é a exceção — pra descobrir colunas e tipos, o Spark precisa
# MAGIC abrir o arquivo e varrer o dado. É uma passada extra, paga antes de qualquer trabalho de verdade.
# MAGIC
# MAGIC E o resultado nem saiu bom. Como você viu no Lab 1, a API manda os números entre aspas, então tudo
# MAGIC virou `string`, e as colunas vieram em ordem alfabética. Em outros arquivos o erro é mais
# MAGIC traiçoeiro: telefone, CEP e número de conta viram número e perdem o zero à esquerda, sem aviso.
# MAGIC
# MAGIC Custo escondido e resultado que você não controla. Schema explícito resolve os dois.
# MAGIC
# MAGIC ## Exercício 2 — schema explícito, versão Python (`StructType`)
# MAGIC
# MAGIC As 6 colunas ficam `StringType` de propósito: é o que o arquivo realmente traz. Converter texto em
# MAGIC número e em data é conteúdo do Lab 3, não deste. O que você ganha aqui é a ordem das colunas que
# MAGIC você escolheu e uma leitura sem passada extra.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema_python = StructType([
    StructField("timestamp", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
])

df_schema_python = spark.read.format("json").schema(schema_python).load(CAMINHO_HISTORICO)
df_schema_python.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Volte na Spark UI. Confirme: **nenhum Job novo** apareceu com esta leitura — schema explícito é só
# MAGIC metadado, não toca o dado.
# MAGIC
# MAGIC Um aviso antes de seguir: dá vontade de declarar logo `close` como `DoubleType`. Não faça isso
# MAGIC aqui. No arquivo o número está entre aspas, e o leitor de JSON não converte texto em `double`: em
# MAGIC vez de erro, a coluna volta **`NULL` em todas as linhas**, sem aviso. Depois de trocar um tipo no
# MAGIC schema, olhe o dado, não só o `printSchema()`.
# MAGIC
# MAGIC ## Exercício 3 — o mesmo schema, versão DDL
# MAGIC
# MAGIC Mesma ideia, outra sintaxe — a que se parece com SQL puro, e a que você mais vai ver no Databricks.

# COMMAND ----------

schema_ddl = "timestamp STRING, open STRING, high STRING, low STRING, close STRING, volume STRING"

df_schema_ddl = spark.read.format("json").schema(schema_ddl).load(CAMINHO_HISTORICO)
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
# MAGIC arquivo de dado (mais os arquivos de controle). E, diferente do JSON, o Parquet carrega o schema
# MAGIC junto — pra ler de volta não precisa inferir nem declarar nada.
# MAGIC
# MAGIC Agora a tabela. Arquivo solto funciona, mas o padrão de produção no Databricks é tabela no Unity
# MAGIC Catalog.

# COMMAND ----------

TABELA_HISTORICO = f"{CATALOGO}.{SCHEMA}.bitcoin_historico"

df_schema_ddl.write.mode("overwrite").saveAsTable(TABELA_HISTORICO)

# COMMAND ----------

# MAGIC %md
# MAGIC Abra o **Catalog Explorer** e confirme que a tabela `bitcoin_historico` existe dentro do schema
# MAGIC `spark_fundamentals`. É uma tabela gerenciada, em formato Delta — você não pediu Delta em lugar
# MAGIC nenhum; é o padrão. (O módulo bônus abre essa caixa.)
# MAGIC
# MAGIC ## Exercício 5 — a prova de equivalência
# MAGIC
# MAGIC A pergunta: "quantos dias de registro por ano, do maior pro menor?" — respondida de duas formas.
# MAGIC
# MAGIC Pra tirar o ano de um `timestamp` Unix em texto são dois passos: `cast` pra número inteiro e
# MAGIC `timestamp_seconds`, que transforma segundos em data e hora. O Lab 3 destrincha essa conversão;
# MAGIC aqui ela só precisa ser **a mesma** dos dois lados. (No SQL abaixo, `timestamp` é o nome da
# MAGIC coluna, não o tipo.)

# COMMAND ----------

from pyspark.sql.functions import col, count, timestamp_seconds, year

df_schema_ddl.createOrReplaceTempView("bitcoin_view")

sql_result = spark.sql("""
    SELECT year(timestamp_seconds(CAST(timestamp AS BIGINT))) AS ano, count(*) AS total
    FROM bitcoin_view
    GROUP BY year(timestamp_seconds(CAST(timestamp AS BIGINT)))
    ORDER BY total DESC
""")

df_result = (
    df_schema_ddl.groupBy(year(timestamp_seconds(col("timestamp").cast("bigint"))).alias("ano"))
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
        CASE WHEN dayofweek(timestamp_seconds(CAST(timestamp AS BIGINT))) IN (1, 7)
             THEN 'fim_de_semana' ELSE 'dia_util' END AS tipo_dia,
        count(*) AS total
    FROM bitcoin_view
    GROUP BY CASE WHEN dayofweek(timestamp_seconds(CAST(timestamp AS BIGINT))) IN (1, 7)
                  THEN 'fim_de_semana' ELSE 'dia_util' END
""")

dia_da_semana = dayofweek(timestamp_seconds(col("timestamp").cast("bigint")))

# Agrupando DIRETO na expressão, como o SQL faz
df_desafio = (
    df_schema_ddl.groupBy(
        when(dia_da_semana.isin(1, 7), "fim_de_semana").otherwise("dia_util").alias("tipo_dia")
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
        "tipo_dia", when(dia_da_semana.isin(1, 7), "fim_de_semana").otherwise("dia_util")
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
# MAGIC Curiosidade que o resultado confirma: o mercado de Bitcoin não fecha — o dataset tem uma linha
# MAGIC pra cada dia do calendário (18/08/2011–30/09/2026), sábado e domingo incluídos, diferente de um
# MAGIC dataset de bolsa de ações.
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
# MAGIC mesmo dataset num pipeline de ETL completo, do zero — inclusive convertendo, finalmente, o texto
# MAGIC em número e em data, que este lab deixou de propósito sem tocar.
