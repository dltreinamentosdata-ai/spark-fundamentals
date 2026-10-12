# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 — Explorando a arquitetura do Spark com a Spark UI
# MAGIC
# MAGIC **Fecha o Módulo 1.** Até aqui você só ouviu falar de Driver, Executor, DAG, Job → Stage → Task.
# MAGIC Chegou a hora de ver isso na tela, ao vivo, rodando sobre um dataset real: o **Bitcoin Historical
# MAGIC Data** — preço diário do BTC/USD, 18/07/2010 a 02/08/2020, 3.669 registros. É um export histórico
# MAGIC fechado — **estático de propósito**, sem streaming, sem API ao vivo.
# MAGIC
# MAGIC A aula 1.4 respondeu **por que 8 arquivos viraram 8 tasks**: eram 8 partições, e cada partição vira
# MAGIC uma task. Aqui você vê o caso mais simples da mesma regra, com os próprios olhos: **1 arquivo
# MAGIC pequeno → 1 partição → 1 task**. Quando um arquivo vira várias partições (e vários arquivos viram
# MAGIC uma só) é assunto da aula 4.2; o Lab 4 volta nisso.
# MAGIC
# MAGIC **Ambiente:** workspace com Unity Catalog e um compute clássico all-purpose criado assim:
# MAGIC - **Databricks Runtime:** 17.3 LTS (Spark 4.0);
# MAGIC - **Access mode:** **Dedicated**. O padrão da tela é *Auto*, que cai em Standard — troque em
# MAGIC   *Advanced*. Em Standard a célula do `df.rdd` (Exercício 3) falha;
# MAGIC - **Workers:** pelo menos 1. Em *single node* o Driver faz tudo sozinho e não há Worker pra
# MAGIC   observar no Exercício 4.
# MAGIC
# MAGIC Serverless não serve pra este lab: ele abstrai os internals, e a ideia aqui é justamente olhar a
# MAGIC Spark UI.
# MAGIC
# MAGIC **Objetivo:** sair sabendo apontar, na tela real do Databricks, onde vive o Driver, onde vivem os
# MAGIC Executors, e como um `count()` vira Job → Stage → Task na Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração — onde este lab grava
# MAGIC
# MAGIC Tudo fica no Unity Catalog: o CSV num **Volume** e, nos próximos labs, as tabelas no mesmo schema.
# MAGIC `CATALOGO` pega o catálogo padrão do seu workspace; troque pelo nome de outro catálogo se quiser
# MAGIC (se o nome tiver hífen, escreva entre crases). Você precisa de permissão pra criar schema nele.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)
print(f"Catálogo: {CATALOGO} | Schema: {SCHEMA} | Volume: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — baixando o dataset
# MAGIC
# MAGIC Isto não é a lição — é só a preparação. Baixamos o CSV estático (export histórico do
# MAGIC investing.com, hospedado no GitHub) direto pra um Volume, que é onde o Databricks recomenda guardar
# MAGIC arquivo que não é tabela. Pode rodar de novo quantas vezes quiser.

# COMMAND ----------

import urllib.request

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")

URL_CSV = "https://raw.githubusercontent.com/dltreinamentosdata-ai/spark-fundamentals/main/dados/bitcoin_historico.csv"
CAMINHO_CSV = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/bitcoin_historico.csv"

urllib.request.urlretrieve(URL_CSV, CAMINHO_CSV)
display(dbutils.fs.ls(f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — a SparkSession já existe
# MAGIC
# MAGIC Primeira coisa a notar: você **nunca** precisou escrever `SparkSession.builder`. Rode a célula
# MAGIC abaixo sozinha — sem importar nada, sem construir nada — e
# MAGIC olhe o que já está lá.

# COMMAND ----------

spark

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC É a mesma sessão que o notebook inteiro vai usar — o Databricks criou o ponto de entrada pra você,
# MAGIC exatamente como a aula 1.3 descreveu. A versão confirma: Spark 4.
# MAGIC
# MAGIC ## Exercício 2 — lendo o CSV com inferência de schema
# MAGIC
# MAGIC A SparkSession sozinha não faz nada — precisa de dado. Carregue o CSV.

# COMMAND ----------

btc_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(CAMINHO_CSV)
)
btc_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Repare: **nenhuma** coluna virou tipo numérico. `Price`/`Open`/`High`/`Low` têm separador de milhar
# MAGIC por vírgula (`"11,105.8"`) e `Vol.`/`Change %` têm sufixo `K`/`M`/`%` — o `inferSchema` não consegue
# MAGIC ler nada disso como número, então tudo fica `string`. Guarde essa observação — é o gancho do Lab 2.
# MAGIC
# MAGIC Outra coisa pra guardar: essa leitura **já disparou Jobs** pequenos (o Spark precisou abrir o arquivo
# MAGIC pra ler o cabeçalho e pra inferir os tipos). O Lab 2 volta nisso. O Job que interessa agora é o
# MAGIC próximo.
# MAGIC
# MAGIC ## Exercício 3 — a action que vira Job → Stage → Task
# MAGIC
# MAGIC Primeiro, quantas partições esse DataFrame tem?

# COMMAND ----------

btc_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **1.** Arquivo pequeno, uma partição só. (`df.rdd` usa a RDD API — é por ela que este lab pede
# MAGIC access mode Dedicated; em compute Standard e em Serverless ela não é suportada.)
# MAGIC
# MAGIC Agora a action. Antes de rodar, deixe a **Spark UI** aberta em outra aba: **Compute → seu cluster →
# MAGIC Spark UI**. Depois de rodar, o link **View** embaixo da célula leva direto pro Job.

# COMMAND ----------

btc_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Agora vá para a Spark UI, aba Jobs.** Localize o que esse `count()` criou:
# MAGIC - o trabalho se divide em **2 Stages**: o primeiro conta as linhas de cada partição (1 task por
# MAGIC   partição — aqui, 1 partição, então 1 task); o segundo soma esses parciais numa contagem final.
# MAGIC   Mesmo um `count()` simples precisa de 2 etapas, porque o resultado depende de reunir o trabalho
# MAGIC   de todas as partições;
# MAGIC - é comum esse único `count()` aparecer como **2 Jobs** na lista. É o AQE (Adaptive Query
# MAGIC   Execution, ligado por padrão): ele roda o primeiro Stage, mede o resultado e só então planeja o
# MAGIC   segundo. No Job seguinte o primeiro Stage aparece como *skipped* — já estava pronto, não rodou de
# MAGIC   novo;
# MAGIC - dentro de cada Stage, as **Tasks**.
# MAGIC
# MAGIC ## Exercício 4 — Spark UI × Master UI
# MAGIC
# MAGIC O Spark tem duas interfaces: a *Application UI* (uma por sessão — é a aba **Spark UI** que você
# MAGIC acabou de usar) e a *Master UI* (uma por cluster — Workers, recursos, aplicações rodando). Na página
# MAGIC do seu cluster, abra a aba da Master UI e veja a lista de Workers. Depois volte pra Spark UI e abra a
# MAGIC aba **Executors**: confira quantos Executors existem, com quantos cores e quanta memória cada um.
# MAGIC Lembra da aula 1.3? Tipicamente é um Executor por Worker.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 5 — transformação sem action: nada acontece na UI
# MAGIC
# MAGIC Rode a célula abaixo e volte pra Spark UI. **Nenhum Job novo** deve aparecer — é lazy evaluation:
# MAGIC o Spark só planeja, não executa, até uma action ser chamada.
# MAGIC
# MAGIC Esse filtro não é decorativo: `"-"` é o valor que o export usa quando não tem volume registrado
# MAGIC naquele dia (6 linhas no dataset inteiro) — a pegadinha que o Lab 3 vai explorar a fundo (nem toda
# MAGIC ausência de dado chega como `null`).
# MAGIC
# MAGIC Repare no nome da coluna entre crases: `` col("`Vol.`") ``, não `col("Vol.")`. O ponto no nome
# MAGIC faz o `col()` tentar ler como caminho de campo aninhado e quebra com `Syntax error in the attribute
# MAGIC name`. Nome de coluna com caractere especial exige crase.

# COMMAND ----------

from pyspark.sql.functions import col

btc_sem_action = btc_df.filter(col("`Vol.`") != "-")
print(type(btc_sem_action))  # ainda é só um plano, nenhum Job disparado

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 6 — o que está em cache (aba Storage)
# MAGIC
# MAGIC A aula 1.3 falou do cache do Executor: a região onde ficam as partições que você manda guardar.
# MAGIC Aqui é só pra **ver** onde isso aparece — quando vale a pena usar cache é assunto do Advanced.
# MAGIC `cache()` sozinho é lazy; o dado só vai pra memória quando uma action roda.

# COMMAND ----------

btc_df.cache()
btc_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Abra a aba **Storage** da Spark UI: o DataFrame aparece lá, com as partições em cache e o tamanho
# MAGIC ocupado em memória. (Na aba Jobs, repare que o primeiro `count()` depois do `cache()` gera um Job
# MAGIC a mais: é o Spark preenchendo o cache.) Depois de olhar, libere:

# COMMAND ----------

_ = btc_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desafio — `display()` vs. `count()`
# MAGIC
# MAGIC Rode as duas células abaixo e compare, na Spark UI, o que cada uma gerou.

# COMMAND ----------

display(btc_df)

# COMMAND ----------

btc_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC `display()` também dispara uma action por dentro (precisa materializar linhas pra desenhar a
# MAGIC tabela) — por isso aparece na Spark UI como Job, mesmo sem você ter chamado `count()` ou
# MAGIC `collect()`. E repare na diferença: `display()` só busca linhas, não agrega nada — não precisa do
# MAGIC segundo Stage que o `count()` precisa.
# MAGIC
# MAGIC ## Checkpoint
# MAGIC
# MAGIC Escolha um Job na Spark UI e complete a frase, em voz alta: **"esse trabalho tem N Stages
# MAGIC porque..."** — ou "teve uma operação que exigiu reunir dado de todas as partições" (2+ Stages), ou
# MAGIC "cada partição foi processada isolada" (1 Stage só). Se não souber justificar, volte pra aula
# MAGIC 1.3/1.4 antes de seguir.
# MAGIC
# MAGIC ## Fechamento
# MAGIC
# MAGIC Você acabou de ver, com os próprios olhos: a SparkSession pronta, um `count()` virando Job → Stage
# MAGIC → Task, lazy evaluation na prática, o cache na aba Storage, e onde ficam a Spark UI e a Master UI
# MAGIC no Databricks. Módulo 1 fechado — no **Lab 2** você vai provar que DataFrame API e SQL não são
# MAGIC "parecidos": são o mesmo plano físico.
