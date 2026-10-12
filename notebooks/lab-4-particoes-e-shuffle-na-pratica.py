# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 — Partições e shuffle na prática: vendo o Map/Shuffle/Reduce acontecer
# MAGIC
# MAGIC **Fecha o Módulo 4** — o último lab antes do fechamento (a aula 5.1 é recap, sem lab; o módulo
# MAGIC de Delta Lake tem o Lab 6). Até aqui, "shuffle" era um
# MAGIC desenho no board. Hoje você vai apontar, na Spark UI, o Stage exato onde ele acontece — primeiro na
# MAGIC tabela do curso, depois no exemplo de vendas da aula 4.4, rodado de verdade.
# MAGIC
# MAGIC **Nota de escopo:** este lab é **reconhecimento**, não tuning. O objetivo é apontar onde o
# MAGIC shuffle acontece e por quê — não otimizar tempo de execução. Medir e comparar performance de
# MAGIC verdade é conteúdo do curso Advanced.
# MAGIC
# MAGIC **Ambiente:** o mesmo do Lab 1 — Databricks Runtime 17.3 LTS, compute clássico com pelo menos 1
# MAGIC worker, access mode **Dedicated** (o lab usa `df.rdd`, que não é suportado em compute Standard nem
# MAGIC em Serverless), Unity Catalog.
# MAGIC
# MAGIC **Requer o Lab 3 já rodado** — este lab lê a tabela `bitcoin_limpo` que o Lab 3 gravou.
# MAGIC
# MAGIC **Objetivo:** contar as partições de um DataFrame, mudar esse número de forma consciente, e
# MAGIC apontar na Spark UI **onde** um `groupBy` dispara shuffle.

# COMMAND ----------

CATALOGO = spark.catalog.currentCatalog()
SCHEMA = "spark_fundamentals"
VOLUME = "dados"

assert CATALOGO not in ("hive_metastore", "spark_catalog"), (
    "CATALOGO precisa ser um catálogo do Unity Catalog — troque o valor acima pelo nome do seu."
)

TABELA_LIMPA = f"{CATALOGO}.{SCHEMA}.bitcoin_limpo"
df = spark.table(TABELA_LIMPA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 1 — baseline
# MAGIC
# MAGIC Quantas partições tem o DataFrame, e quantos cores o cluster oferece?

# COMMAND ----------

print("Partições do DataFrame:", df.rdd.getNumPartitions())
print("Cores disponíveis no cluster:", spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC Anote os dois números. A tabela é pequena (5.490 linhas, um arquivo só), então é **1 partição** —
# MAGIC e 1 partição = 1 task: por mais cores que o cluster tenha, só um trabalha nessa leitura. É o
# MAGIC cenário "menos partições que cores" da aula 4.2, ao vivo.
# MAGIC
# MAGIC ## Exercício 2 — Map puro, sem shuffle
# MAGIC
# MAGIC Antes de rodar, abra a Spark UI. Dica pra este lab inteiro: pra achar o Job certo, use o link que
# MAGIC aparece embaixo da célula depois que ela roda — a lista geral pode ter outros Jobs pequenos, de
# MAGIC leitura dos metadados da tabela.

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("preco_usd") > 1000).select("data", "faixa_preco").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Confira o Job na Spark UI: **1 Stage só**. `filter` + `select` são narrow transformations — cada
# MAGIC partição é processada isolada, sem precisar do resultado de nenhuma outra. Repare que usamos
# MAGIC `.collect()`, não `.count()`: `count()` soma um total global, e isso por si só já exige um segundo
# MAGIC Stage (você viu isso no Lab 1); `.collect()` não agrega nada, só traz as linhas.
# MAGIC
# MAGIC ## Exercício 3 — dispare o shuffle
# MAGIC
# MAGIC O Exercício 2 mostrou o caso fácil (narrow). Agora peça algo que exige juntar dado de todas as
# MAGIC partições numa mesma chave — é aí que o board vira tela.

# COMMAND ----------

df.groupBy("faixa_preco").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Volte pra Spark UI e localize o que esse `groupBy` gerou. Agora são **2 Stages**, e o segundo
# MAGIC depende do primeiro (com AQE, costumam aparecer como 2 Jobs, o segundo com o primeiro Stage
# MAGIC *skipped* — igual ao Lab 1). Abra o Stage final e procure a métrica **Shuffle Read**: é o dado que
# MAGIC foi redistribuído pra agrupar por `faixa_preco`. No Stage anterior, a métrica irmã é **Shuffle
# MAGIC Write**. (Só reconhecimento — ler essas métricas pra diagnosticar é conteúdo do Advanced.)
# MAGIC
# MAGIC Repare também em quantas tasks o Stage final tem. O padrão de `spark.sql.shuffle.partitions` é
# MAGIC 200, mas você não vai ver 200 tasks: o AQE olhou o tamanho real do dado e juntou as partições
# MAGIC pequenas.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercício 4 — repartition consciente

# COMMAND ----------

df_repart = df.repartition(8)
df_repart.groupBy("faixa_preco").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Na Spark UI esse trabalho tem **um Stage a mais** que o do Exercício 3 (com AQE, costuma aparecer
# MAGIC como 3 Jobs, cada um reaproveitando os Stages anteriores como *skipped*). O Stage de leitura
# MAGIC continua com 1 task (é ditado pela entrada, não pelo `repartition`), mas o Stage **logo depois do
# MAGIC `repartition(8)`** tem **8 tasks** — é aí que a relação partição ↔ task aparece. O Stage final,
# MAGIC pós-`groupBy`, volta a ter poucas tasks: de novo o AQE juntando partições pequenas. O objetivo é
# MAGIC ver partição ↔ task, não concluir se ficou "mais rápido".
# MAGIC
# MAGIC Confira também pelo código:

# COMMAND ----------

print("Partições depois do repartition(8):", df_repart.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC **8.** (Um detalhe: com AQE, pedir o `.rdd` de um DataFrame reparticionado já executa o shuffle do
# MAGIC `repartition` — por isso essa célula, sozinha, também gerou um Job.)
# MAGIC
# MAGIC ## Exercício 5 — `coalesce` vs. `repartition`
# MAGIC
# MAGIC Partindo das 8 partições do exercício anterior: `coalesce(2)` **funde** partições que já existem;
# MAGIC `repartition(2)` **reembaralha** tudo. Os dois chegam em 2 partições — por caminhos diferentes.

# COMMAND ----------

print("coalesce(2):   ", df_repart.coalesce(2).rdd.getNumPartitions(), "partições")
print("repartition(2):", df_repart.repartition(2).rdd.getNumPartitions(), "partições")
print("coalesce(16):  ", df_repart.coalesce(16).rdd.getNumPartitions(), "partições  <- coalesce nunca aumenta")

# COMMAND ----------

df_repart.coalesce(2).explain()

# COMMAND ----------

df_repart.repartition(2).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Compare os dois planos, lendo de baixo pra cima. Cada linha `Exchange` é um shuffle.
# MAGIC - No `coalesce(2)`, o único `Exchange` é o do `repartition(8)` que já estava lá. O `Coalesce 2`
# MAGIC   aparece em cima dele como um passo **sem troca**: só funde partições, não redistribui dado.
# MAGIC - No `repartition(2)`, o plano tem um `Exchange RoundRobinPartitioning(2)`: redistribuição completa.
# MAGIC   E repare que o `Exchange` de 8 **sumiu** — dois `repartition` seguidos, o Catalyst fica só com o
# MAGIC   último. É o otimizador da aula 2.2 cortando trabalho inútil.
# MAGIC
# MAGIC Resumo: `coalesce` nunca cria um shuffle novo (e por isso nunca aumenta o número de partições);
# MAGIC `repartition` sempre cria.
# MAGIC
# MAGIC ## Exercício 6 — a régua de sizing

# COMMAND ----------

cores = spark.sparkContext.defaultParallelism
particoes_alvo = cores * 3  # régua de 2 a 3 tasks por core (aula 4.2)

df_dimensionado = df.repartition(particoes_alvo)
print(f"Cores: {cores} | alvo (3x): {particoes_alvo} | partições agora: {df_dimensionado.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC A conta fecha: 3 × cores. Agora a pergunta honesta: **valeu a pena** reparticionar 5.490 linhas?
# MAGIC Não. O `repartition` é um shuffle, e num dataset desse tamanho o shuffle custa mais do que o
# MAGIC paralelismo devolve. A régua de 2 a 3 tasks por core é pra dado que ocupa o cluster — aqui o
# MAGIC objetivo foi só ver o número de partições obedecer à conta.
# MAGIC
# MAGIC ## Exercício 7 — o exemplo de vendas da aula 4.4, rodando
# MAGIC
# MAGIC A aula 4.4 desenhou o exemplo da lição oficial: 4 arquivos de vendas, `select` + `filter` (Map),
# MAGIC `groupBy` (Shuffle), `agg` (Reduce). Vamos criar esses 4 arquivos num Volume e rodar.

# COMMAND ----------

import os

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")
CAMINHO_VENDAS = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/vendas"
CAMINHO_SAIDA = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}/vendas_por_id"

arquivos = {
    "file1.csv": "A,100,US\nC,200,US\nB,30,EU",
    "file2.csv": "C,60,EU\nD,150,EU\nB,75,US",
    "file3.csv": "A,120,US\nA,90,EU\nD,50,EU",
    "file4.csv": "C,80,US\nC,140,EU\nB,110,US",
}
os.makedirs(CAMINHO_VENDAS, exist_ok=True)
for nome, linhas in arquivos.items():
    with open(f"{CAMINHO_VENDAS}/{nome}", "w") as f:
        f.write("id,amt,region\n" + linhas + "\n")

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# READ — 4 arquivos
input_df = spark.read.csv(CAMINHO_VENDAS, header=True, schema="id STRING, amt INT, region STRING")
print("Partições de entrada:", input_df.rdd.getNumPartitions())

# MAP — narrow, sem mover dado
filtered_df = input_df.select("id", "amt").filter("amt > 50")

# SHUFFLE — groupBy define a chave
grouped_df = filtered_df.groupBy("id")

# REDUCE — agrega cada grupo
aggregated_df = grouped_df.agg(_sum("amt").alias("total"))

# WRITE — a action: só aqui tudo executa
aggregated_df.write.mode("overwrite").csv(CAMINHO_SAIDA, header=True)

display(spark.read.csv(CAMINHO_SAIDA, header=True).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC O resultado é o do board: **A 310, B 185, C 480, D 150**. Agora compare a execução real com o
# MAGIC desenho:
# MAGIC - **Partições de entrada:** o desenho mostra 4 arquivos → 4 partições. Você viu o número real na
# MAGIC   saída acima — ele depende de quantos cores o cluster tem, porque o Spark agrupa arquivos pequenos
# MAGIC   (aula 4.2).
# MAGIC - **Arquivos de saída:** o desenho mostra 2. Liste a pasta de saída: com o AQE juntando as
# MAGIC   partições pequenas depois do shuffle, o normal aqui é sair **1** arquivo `part-`.
# MAGIC
# MAGIC O desenho ensina o padrão; os números exatos quem decide é o tamanho do dado e a configuração.
# MAGIC Na Spark UI, localize o Stage do Map (leitura + filtro) e o Stage do Reduce (pós-shuffle).

# COMMAND ----------

display(dbutils.fs.ls(CAMINHO_SAIDA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC Ao vivo, na Spark UI: aponte o Stage exato onde o shuffle do Exercício 3 acontece e explique por
# MAGIC que ele existe. Depois, pro `groupBy("faixa_preco").count()`, diga qual parte é Map, qual é Shuffle
# MAGIC e qual é Reduce. Não se espera conclusão de performance — só reconhecimento do padrão.
# MAGIC
# MAGIC ## Fechamento — o curso inteiro, numa cadeia
# MAGIC
# MAGIC Partições contadas e redistribuídas conscientemente, shuffle localizado ao vivo na Spark UI,
# MAGIC `coalesce` vs. `repartition` diferenciados, e o padrão Map/Shuffle/Reduce rodado com o exemplo da
# MAGIC aula — Módulo 4 fechado. Você agora fechou o modelo mental do curso inteiro: arquitetura (M1) →
# MAGIC API (M2) → ETL (M3) → sistemas distribuídos (M4). A aula 5.1 amarra os quatro; se quiser aplicar
# MAGIC esse modelo mental com acompanhamento antes dos próximos cursos saírem, a mentoria 1:1 está aberta
# MAGIC agora.
