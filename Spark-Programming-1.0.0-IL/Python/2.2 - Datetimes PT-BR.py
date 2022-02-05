# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções Datetime
# MAGIC 
# MAGIC ##### Objetivos
# MAGIC 1. Converter dados para timestamp
# MAGIC 2. Formatação de valores em datetime
# MAGIC 3. Extração de valores de timestamp
# MAGIC 4. Conversão para data
# MAGIC 5. Manipulação de dados em datetime
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column" target="_blank">Column</a>: `cast`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `date_format`, `to_date`, `date_add`, `year`, `month`, `dayofweek`, `minute`, `second`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md Será utilizado um subconjunto de dados do conjunto Eventos BredBricks para realizar as práticas a seguir.

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.parquet(eventsPath).select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

# MAGIC %md ### Funções para manipular dados em datetime
# MAGIC Aqui estão algumas funções para manipulação de dados em formato date e time em Spark. 
# MAGIC OBS: Para não prejudicar a tradução com base no meu entendimento, irei deixar o texto original em inglês, fornecido no curso. 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | add_months | Returns the date that is numMonths after startDate |
# MAGIC | current_timestamp | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | date_format | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | dayofweek | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC | from_unixtime | Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format |
# MAGIC | minute | Extracts the minutes as an integer from a given date/timestamp/string. |
# MAGIC | unix_timestamp | Converts time string with given pattern to Unix timestamp (in seconds) |

# COMMAND ----------

# MAGIC %md ### Conversão para timestamp

# COMMAND ----------

# MAGIC %md #### `cast()`
# MAGIC Converte uma coluna para diferentes tipos de dados, especificando se será utilizada uma representação em string ou data. 

# COMMAND ----------

timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestampDF)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestampDF)

# COMMAND ----------

# MAGIC %md ### Padrões de Datetime para formatação e análise 
# MAGIC Existem diversos cenários para utilização de datetime em spark: 
# MAGIC 
# MAGIC - CSV/JSON utilizando padrão de string para análise e formatação de conteúdo em datetime. 
# MAGIC - Funções datetime utilizadas para conversão de dados do tipo string para date ou timestamp, ex: `unix_timestamp`, `date_format`, `from_unixtime`, `to_date`, `to_timestamp`, etc.
# MAGIC 
# MAGIC Spark utiliza <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">alguns símbolos para formatação de dados</a>. Segue alguns exemplos abaixo.
# MAGIC 
# MAGIC | Símbolo | Significado     | Formato         | Exemplo                |
# MAGIC | ------ | ---------------  | ------------    | ---------------------- |
# MAGIC | G      | era              | texto           | AD; Anno Domini        |
# MAGIC | y      | ano              | ano             | 2020; 20               |
# MAGIC | D      | dia do ano       | númerico(3)     | 189                    |
# MAGIC | M/L    | mês do ano       | mês             | 7; 07; Jul; July       |
# MAGIC | d      | dia do mês       | númerico(3)     | 28                     |
# MAGIC | Q/q    | trimestre do ano | númerico/texto  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | dia da semana    | texto           | Tue; Tuesday           |

# COMMAND ----------

# MAGIC %md ### Formatos de data

# COMMAND ----------

# MAGIC %md #### `date_format()`
# MAGIC Converte uma data/timestamp/string para um formato especificado.

# COMMAND ----------

from pyspark.sql.functions import date_format

formattedDF = (timestampDF
               .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
               .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
              )
display(formattedDF)

# COMMAND ----------

# MAGIC %md ### Extraindo informações de um timestamp

# COMMAND ----------

# MAGIC %md #### `year`
# MAGIC Extrai o ano em formato integer de uma coluna data/timestamp/string.
# MAGIC 
# MAGIC ##### Métodos similares: `month`, `dayofweek`, `minute`, `second`, etc.

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetimeDF = (timestampDF
              .withColumn("year", year(col("timestamp")))
              .withColumn("month", month(col("timestamp")))
              .withColumn("dayofweek", dayofweek(col("timestamp")))
              .withColumn("minute", minute(col("timestamp")))
              .withColumn("second", second(col("timestamp")))
             )
display(datetimeDF)

# COMMAND ----------

# MAGIC %md ### Conversão para Data

# COMMAND ----------

# MAGIC %md #### `to_date`
# MAGIC Converte uma coluna do tipo data aplicando regras especificadas na conversão.

# COMMAND ----------

from pyspark.sql.functions import to_date

dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
display(dateDF)

# COMMAND ----------

# MAGIC %md ### Manipulando datetimes

# COMMAND ----------

# MAGIC %md #### `date_add`
# MAGIC Realiza uma soma na data informada através de um número n de dias informado.

# COMMAND ----------

from pyspark.sql.functions import date_add

plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus2DF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercícios
# MAGIC Realize a ploatagem dos usuários ativos e média de usuários ativos por dia da semana.
# MAGIC 1. Extraia os eventos em timestamp e date
# MAGIC 2. Obtenha os usuários ativos por dia
# MAGIC 3. Obtenha a média de usuários ativos por dia da semana
# MAGIC 4. Ordene a coluna dia da semana

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Execute a célula abaixo para criar o DataFrame inicial com os IDs e Timestamps dos eventos. 

# COMMAND ----------

df = (spark
      .read
      .parquet(eventsPath)
      .select("user_id", col("event_timestamp").alias("ts"))
     )

display(df)

# COMMAND ----------

# MAGIC %md ### 1. Extraia os eventos em timestamp e date
# MAGIC - Converta o **`ts`** de microssegundos para segundos realizando uma divisão por 1 milhão e sem seguida utilize um cast para conversão para timestamp. 
# MAGIC - Adicione uma coluna **`date`** convertendo a coluna **`ts`** para date utilizando `to_date`

# COMMAND ----------

datetimeDF = (df
              .withColumn("ts", (col("ts") / 1e6).cast(TimestampType()))
              .withColumn("date", to_date(col("ts")))
)
display(datetimeDF)

# COMMAND ----------

# MAGIC %md **Verificando se existem inconsistências**

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

expected1a = StructType([StructField("user_id", StringType(), True),
                         StructField("ts", TimestampType(), True),
                         StructField("date", DateType(), True)])

result1a = datetimeDF.schema

assert expected1a == result1a, "datetimeDF não possui o schema esperado."
print("O schema está de acordo com o esperado.")

# COMMAND ----------

import datetime

expected1b = datetime.date(2020, 6, 19)
result1b = datetimeDF.sort("date").first().date

assert expected1b == result1b, "datetimeDF não possui os valores esperados."
print("O dataframe está de acordo com os valores esperados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Obtenha os usuários ativos por dia
# MAGIC - Realize um group by da coluna date
# MAGIC - Utilize a função `approx_count_distinct` para realizar uma agregação da coluna **`user_id`** e por fim aplique um `alias` para alterar o nome da coluna para `"active_users"`
# MAGIC  
# MAGIC - Ordene a coluna date
# MAGIC - Plote um gráfico de linha

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

activeUsersDF = (datetimeDF
                 .groupBy("date")
                 .agg(approx_count_distinct("user_id").alias("active_users"))
                 .sort("date")
)
display(activeUsersDF)

# COMMAND ----------

# MAGIC %md **Verificando os dados**

# COMMAND ----------

from pyspark.sql.types import LongType

expected2a = StructType([StructField("date", DateType(), True),
                         StructField("active_users", LongType(), False)])

result2a = activeUsersDF.schema

assert expected2a == result2a, "activeUsersDF does not have the expected schema"

# COMMAND ----------

expected2b = [(datetime.date(2020, 6, 19), 251573), (datetime.date(2020, 6, 20), 357215), (datetime.date(2020, 6, 21), 305055), (datetime.date(2020, 6, 22), 239094), (datetime.date(2020, 6, 23), 243117)]

result2b = [(row.date, row.active_users) for row in activeUsersDF.take(5)]

assert expected2b == result2b, "activeUsersDF não possui os valores esperados."
print("O dataframe possui os valores esperados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Obtenha a média de usuários ativos por dia da semana 
# MAGIC - Adicione a coluna **`day`** extraindo o dia da semana da coluna**`date`** utilizando a função de conversão `date_format` 
# MAGIC - Realize um Group By na coluna **`day`
# MAGIC - Aplique uma média na coluna **`active_users`** e salve-a com o alias"avg_users"

# COMMAND ----------

# TODO
activeDowDF = (activeUsersDF
               .withColumn("day", date_format("date", "E"))
               .groupBy("day")
               .agg(avg("active_users").alias("avg_users"))
)
display(activeDowDF)


# COMMAND ----------

# MAGIC %md **Verificando os dados**

# COMMAND ----------

from pyspark.sql.types import DoubleType

expected3a = StructType([StructField("day", StringType(), True),
                         StructField("avg_users", DoubleType(), True)])

result3a = activeDowDF.schema

assert expected3a == result3a, "activeDowDF não possui o schema esperado."
print("O schema do dataframe activeDowDF está correto.")

# COMMAND ----------

expected3b = [("Fri", 247180.66666666666), ("Mon", 238195.5), ("Sat", 278482.0), ("Sun", 282905.5), ("Thu", 264620.0), ("Tue", 260942.5), ("Wed", 227214.0)]

result3b = [(row.day, row.avg_users) for row in activeDowDF.sort("day").collect()]

assert expected3b == result3b, "activeDowDF não possui os valores esperados para os dias da semana"
print("Todos os dias da semana estão com os valores corretos.")

# COMMAND ----------

# MAGIC %md ### Limpando o ambiente de estudos

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
