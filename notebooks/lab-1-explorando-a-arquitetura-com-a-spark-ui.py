# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 — Explorando a arquitetura do Spark com a Spark UI
# MAGIC
# MAGIC **Fecha o Módulo 1.** Até aqui você só ouviu falar de Driver, Executor, DAG, Job → Stage → Task.
# MAGIC Chegou a hora de ver isso na tela, ao vivo, rodando sobre um dataset real: o **histórico diário do
# MAGIC BTC/USD**, extraído da API pública da Bitstamp — 18/08/2011 a 30/09/2026, 5.523 registros. Os Labs
# MAGIC 1 a 4 trabalham sobre esse período **fechado**, pra que o seu número bata com o do vídeo. A carga
# MAGIC dos dias seguintes, um lote por dia, fica pro Lab 6.
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
# MAGIC   observar no Exercício 4;
# MAGIC - **Photon:** desligado. Com ele ligado, os planos e a contagem de Jobs saem diferentes dos do vídeo;
# MAGIC - **Rede:** o cluster precisa de saída pra internet — o setup chama uma API pública.
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
# MAGIC Tudo fica no Unity Catalog: os arquivos num **Volume** e, nos próximos labs, as tabelas no mesmo schema.
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
# MAGIC ## Setup — extraindo o dataset da API
# MAGIC
# MAGIC Isto não é a lição — é só a preparação. A função abaixo chama a API pública da Bitstamp (sem
# MAGIC chave, sem cadastro) e grava os dias num Volume, um por linha, com os campos do jeito que a API
# MAGIC manda. Volume é onde o Databricks recomenda guardar arquivo que não é tabela.
# MAGIC
# MAGIC Ela é **incremental**: olha o que já está na pasta e baixa só os dias que faltam. Na primeira vez
# MAGIC traz o histórico inteiro; nas seguintes, só o que veio depois. Nunca traz o dia de hoje — ele
# MAGIC ainda não fechou, e o preço mudaria a cada chamada. O limite é sempre **ontem (D-1)**, contado em
# MAGIC UTC, que é o relógio da API.

# COMMAND ----------

import datetime
import json
import os
import urllib.request

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")

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

# MAGIC %md
# MAGIC Agora a carga histórica, com o limite travado em 30/09/2026. Rode a célula duas vezes: na segunda
# MAGIC ela avisa que não há nada novo e não chama a API.

# COMMAND ----------

CORTE = datetime.date(2026, 9, 30)
CAMINHO_HISTORICO = f"{PASTA_BTC}/btcusd_{PRIMEIRO_DIA}_{CORTE}.json"

extrair_btc(ate=CORTE)
display(dbutils.fs.ls(PASTA_BTC))

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
# MAGIC ## Exercício 2 — lendo o JSON com inferência de schema
# MAGIC
# MAGIC A SparkSession sozinha não faz nada — precisa de dado. Carregue o arquivo do histórico. Em JSON o
# MAGIC Spark infere o schema sozinho, sem você pedir.

# COMMAND ----------

btc_df = spark.read.format("json").load(CAMINHO_HISTORICO)
btc_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Repare: **nenhuma** coluna virou tipo numérico. A API manda preço e volume entre aspas
# MAGIC (`"close": "10.90"`), e o `timestamp` também — é o dia em segundos desde 1970 (Unix), escrito como
# MAGIC texto. Pro JSON, o que está entre aspas é texto, então a inferência devolve tudo `string`. E as
# MAGIC colunas saíram em ordem alfabética, não na ordem do arquivo. Guarde essas duas observações — são o
# MAGIC gancho do Lab 2.
# MAGIC
# MAGIC Outra coisa pra guardar: essa leitura **já disparou um Job** pequeno (o Spark precisou varrer o
# MAGIC arquivo pra descobrir as colunas e os tipos). O Lab 2 volta nisso. O Job que interessa agora é o
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
# MAGIC Esse filtro não é decorativo: `"0.00000000"` é o que a API manda nos dias em que não houve
# MAGIC nenhuma negociação (33 linhas no dataset inteiro). Neste dataset, volume zero quer dizer que
# MAGIC aquele dia não teve preço — a pegadinha que o Lab 3 vai explorar a fundo (nem toda ausência de
# MAGIC dado chega como `null`).

# COMMAND ----------

from pyspark.sql.functions import col

btc_sem_action = btc_df.filter(col("volume") != "0.00000000")
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
