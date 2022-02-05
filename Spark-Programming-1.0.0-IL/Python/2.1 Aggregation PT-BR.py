# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Agregação
# MAGIC 
# MAGIC 1. Agrupando dados
# MAGIC 1. Métodos de dados agrupados
# MAGIC 1. Construção de funções de agregação
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframe#pyspark.sql.DataFrame" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>): `groupBy`
# MAGIC - Grouped Data (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=groupeddata#pyspark.sql.GroupedData" target="_blank" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/RelationalGroupedDataset.html" target="_blank">Scala</a>): `agg`, `avg`, `count`, `max`, `sum`
# MAGIC - Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=functions#module-pyspark.sql.functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>): `approx_count_distinct`, `avg`, `sum`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Novamente, o dataset utilizado será o BedBricks.

# COMMAND ----------

df = spark.read.parquet(eventsPath)
display(df)

# COMMAND ----------

# MAGIC %md ### Agrupando dados
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Agrupando dados
# MAGIC 
# MAGIC Utilizando o comando `groupBy`, será possível criar um objeto de dados agrupados.
# MAGIC 
# MAGIC Esse objeto é nomeado como `RelationalGroupedDataset` em Scala e `GroupedData` em Python

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Métodos de agregação
# MAGIC Vários métodos de agregração são disponíveis para serem utilizados <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.html" target="_blank">Objetos de dados agrupados</a>.
# MAGIC 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | agg | Cálcula valores agregados permitindo outras  funções para uma coluna agrupada |
# MAGIC | avg | Calcula a média para uma coluna agrupada |
# MAGIC | count | Conta o número de linhas para uma coluna agrupada |
# MAGIC | max | Cálcula o maior valor para uma coluna agrupada |
# MAGIC | mean | Calcula a média para uma coluna agrupada |
# MAGIC | min | Calcula o menor valor para uma coluna agrupada |
# MAGIC | pivot | Realiza operações de pivot (transformar colunas em linhas) para uma coluna agrupada |
# MAGIC | sum | Calcula a soma para uma coluna agrupada |

# COMMAND ----------

eventCountsDF = df.groupBy("event_name").count()
display(eventCountsDF)

# COMMAND ----------

avgStatePurchasesDF = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avgStatePurchasesDF)

# COMMAND ----------

cityPurchaseQuantitiesDF = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity")
display(cityPurchaseQuantitiesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Construindo funções agregadas
# MAGIC 
# MAGIC Ao utilizar o método de agrupamento `agg` podemos realizar funções customizadas de maneira agregada.
# MAGIC 
# MAGIC Isso permite aplicar outras transformações no resultado da coluna bem como o `alias`

# COMMAND ----------

# MAGIC %md ### Funções agregadas
# MAGIC 
# MAGIC Aqui estão alguns exemplos de funções disponíveis para agregação. 
# MAGIC 
# MAGIC | Métodos | Descrição |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Retorna o número aproximado de itens distintos em um grupo |
# MAGIC | avg | Retorna a média de valores dentro de um grupo |
# MAGIC | collect_list | Retorna uma lista de objetos com valores duplicados |
# MAGIC | corr | Retorna a correção de coeficiente de Person para duas colunas |
# MAGIC | max | Calcula o valor máximo para cada valor de uma coluna agrupada |
# MAGIC | mean | Calcula a média para cada valor de uma coluna agrupada |
# MAGIC | stddev_samp | Retorna o desvio padrão da amostra em um grupo |
# MAGIC | sumDistinct | Retorna a soma de valores distintos em uma expressão matemática |
# MAGIC | var_pop | Retorna a variação populacional dos valores em um grupo de dados |
# MAGIC 
# MAGIC Utilizando o método de dados agrupado <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">`agg`</a> para aplicar funções agregadas
# MAGIC 
# MAGIC Isso permite aplicar outras transformações no resultado das colunas, como por exemplo a função <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">`alias`</a>.

# COMMAND ----------

from pyspark.sql.functions import sum

statePurchasesDF = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(statePurchasesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Aplicando múltiplas funções de agregações

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

stateAggregatesDF = df.groupBy("geo.state").agg(
  avg("ecommerce.total_item_quantity").alias("avg_quantity"),
  approx_count_distinct("user_id").alias("distinct_users"))

display(stateAggregatesDF)

# COMMAND ----------

# MAGIC %md ### Funções matemáticas
# MAGIC Aqui estão alguns exemplos de funções para operações matemáticas.
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | ceil | Retorna o inteiro mais próximo ou igual a um valor informado. |
# MAGIC | cos | Calcula o cosseno de um valor informado.  |
# MAGIC | log | Calcula o logaritmo de um valor informado.  |
# MAGIC | round | Retorna o valor da coluna arredonda para 0 casas decimais com o modo de arredonamento HALF_UP. |
# MAGIC | sqrt | Calcula a raiz quadrada de um valor informado |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt, ceil

display(
    spark.range(10)  # Cria um dataframe com uma coluna de identificação
    .withColumn("sqrt", sqrt("id"))
    .withColumn("cos", cos("id"))
    .withColumn("ceil", ceil("sqrt"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Receita por Trafégo Lab
# MAGIC Retorne as 3 fontes de trafégo que geram a maior receita total. 
# MAGIC 1. Agregar a receita por fonte de tráfego
# MAGIC 2. Buscar o top 3 tráfego pelo total de receita
# MAGIC 3. Limpar as colunas de receita com duas casas decimais
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">DataFrame</a>: groupBy, sort, limit
# MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html" target="_blank">Column</a>: alias, desc, cast, operators
# MAGIC - <a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Built-in Functions</a>: avg, sum

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Executar o código abaixo começando pelo DataFrame **`df`**.

# COMMAND ----------

from pyspark.sql.functions import col

# purchase events logged on the BedBricks website
df = (spark.read.parquet(eventsPath)
  .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
  .filter(col("revenue").isNotNull())
  .drop("event_name"))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Agregando a receita pelo tráfego 
# MAGIC - Group by **`traffic_source`**
# MAGIC - Retornar a soma de **`revenue`** como **`total_rev`**
# MAGIC - Retornar a média de **`revenue`** como **`avg_rev`**

# COMMAND ----------

trafficDF = df.groupBy("traffic_source").agg(
  sum("revenue").alias("total_rev"),
  avg("revenue").alias("avg_rev"))


display(trafficDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validando o agrupamento

# COMMAND ----------

from pyspark.sql.functions import round
expected1 = [(12704560.0, 1083.175), (78800000.3, 983.2915), (24797837.0, 1076.6221), (47218429.0, 1086.8303), (16177893.0, 1083.4378), (8044326.0, 1087.218)]
testDF = trafficDF.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected1 == result1)

print("Validação concluída. Todos os valores estão dentro do esperado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Obtendo o top três fontes de tráfego por total de receita
# MAGIC - Ordenando o **`total_rev`** em ordem descendente
# MAGIC - Utilizando limit para retornar as três primeiras linhas

# COMMAND ----------

topTrafficDF = (trafficDF.sort(col("total_rev").desc()).limit(3)
)
display(topTrafficDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validando o top 3

# COMMAND ----------

expected2 = [(78800000.3, 983.2915), (47218429.0, 1086.8303), (24797837.0, 1076.6221)]
testDF = topTrafficDF.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected2 == result2)

("Validação de dados concluída com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Limitando as colunas de receita com duas casas decimais
# MAGIC - Modifique as colunas **`avg_rev`** e **`total_rev`** para conter apenas duas casas decimais
# MAGIC   - Utilize a função **`withColumn()`**  com os mesmos nomes para substituir essas colunas 
# MAGIC   - Para limitar as colunas em duas casas decimais, multiplique cada coluna por 100, converta para long utilizando cast e então divida por 100

# COMMAND ----------

# TODO
finalDF = (topTrafficDF
           .withColumn("avg_rev", (col("avg_rev") * 100).cast("long") / 100)
           .withColumn("total_rev", (col("total_rev") * 100).cast("long") / 100

  )                            
)

display(finalDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validando os cálculos

# COMMAND ----------

expected3 = [(78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result3 = [(row.total_rev, row.avg_rev) for row in finalDF.collect()]

assert(expected3 == result3)

print("Cálculos validados com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Bônus: Escreva a sentença utilizando uma função matemática pronta
# MAGIC Encontre uma função capaz de arredonar um número para uma quantidade de casas decimais especificada

# COMMAND ----------

bonusDF = (topTrafficDF
           .withColumn("avg_rev", round(col("avg_rev"), 2))
           .withColumn("total_rev", round(col("total_rev"), 2))
)

display(bonusDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validando o arredondamento

# COMMAND ----------

expected4 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result4 = [(row.total_rev, row.avg_rev) for row in bonusDF.collect()]

assert(expected4 == result4)

print("Valores arredondados corretamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Faça todos os passos abordados anteriormente em apenas um bloco de código

# COMMAND ----------

chainDF = (df.groupBy("traffic_source").agg(
          sum("revenue").alias("total_rev"),
          avg("revenue").alias("avg_rev")

  )
         .sort(col("total_rev").desc()).limit(3)
         .withColumn("avg_rev", round(col("avg_rev"), 2))
         .withColumn("total_rev", round(col("total_rev"), 2))
)

display(chainDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validação final

# COMMAND ----------

expected5 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result5 = [(row.total_rev, row.avg_rev) for row in chainDF.collect()]

assert(expected5 == result5)

print("Todos os valores foram submetidos a validação e estão corretos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando o ambiente de estudos

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------


