# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Funções definidas pelo usuário (User-defined functions - UDF)
# MAGIC 
# MAGIC ##### Objetivos
# MAGIC 1. Definir uma função
# MAGIC 1. Criar e aplicar uma UDF
# MAGIC 1. Registrar uma UDF para utilizar em SQL
# MAGIC 1. Criar e registrar uma UDF com Python decorator syntax
# MAGIC 1. Criar e aplicar pandas em uma UDF
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html?#pyspark.sql.functions.udf" target="_blank">UDF Registration (`spark.udf`)</a>: `register`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `udf`
# MAGIC - <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python.html#use-udf-with-dataframes" target="_blank">Python UDF Decorator</a>: `@udf`
# MAGIC - <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html#pandas-user-defined-functions" target="_blank">Pandas UDF Decorator</a>: `@pandas_udf`

# COMMAND ----------

# MAGIC %md ### User-Defined Function (UDF)
# MAGIC Uma UDF
# MAGIC 
# MAGIC - Não pode ser otimizada peloCatalyst Optimizer
# MAGIC - A função é serializada e enviada aos executores
# MAGIC - Os dados de linhas são desserualizados do formato binário nativo do spark. Para retornar para a UDF os resultados são serializados novamente no formato nativo do spark. 
# MAGIC - Para UDFs em python, pode ocorrer uma sobrecarga adicional de comunicação entre o executador e um interpretador python em execução em cada nó do worker. 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md Para este exemplo será o utilizado o dataset de vendas/sales

# COMMAND ----------

salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# MAGIC %md ### Defindo uma função
# MAGIC 
# MAGIC Será definida uma função para obter a primeira letra do campo `email`.

# COMMAND ----------

def firstLetterFunction(email):
    return email[0]

firstLetterFunction("annagray@kaufman.com")

# COMMAND ----------

# MAGIC %md ### Criando e aplicando a UDF
# MAGIC Registrando a função como UDF. Isso serializa a fuunção e a envia para os executadores serem capazes de transformar os registros do Dataframe.

# COMMAND ----------

firstLetterUDF = udf(firstLetterFunction)

# COMMAND ----------

# MAGIC %md Aplicando a UDF na coluna `email`.

# COMMAND ----------

from pyspark.sql.functions import col

display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %md ### Registrando a UDF para utilização em SQL 
# MAGIC Para registrar a utilização da UDF, é necessário usar o comando `spark.udf.register` para que a mesma seja disponibilizada para uso em SQL. 

# COMMAND ----------

salesDF.createOrReplaceTempView("sales")

firstLetterUDF = spark.udf.register("sql_udf", firstLetterFunction)

# COMMAND ----------

# Também é possível aplicar a UDF do python
display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agora, pode-se aplicar a UDF do SQL
# MAGIC SELECT sql_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# MAGIC %md ### Usando o Decorator Syntax (Somente em Python)
# MAGIC 
# MAGIC Como alternativa, podemos definir e registrar uma UDF utilizando a <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">sintaxe do Python decorator</a>. O parâmetro decorador do `@udf` é o tipo de dado que a função retorna.
# MAGIC 
# MAGIC Desse modo, não é mais possível chamar a função de maneira local, ou seja: `firstLetterUDF("annagray@kaufman.com")` não irá funcionar.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Este exemplo utiliza dicas de <a href="https://docs.python.org/3/library/typing.html" target="_blank">Python</a> que foram introduzidas na versão 3.5. As dicas não são necessárias para este exemplo, mas podem ser utilizadas como documentação para auxiliar os desenvolvedores a utilizarem a função de maneira correta. Elas são utilizadas em exemplos para enfatizar que a UDF processa um registro por vez, recebendo um único argumento `str` e retornando apenas um valor `str`.

# COMMAND ----------

# A entrada e saída é uma string
@udf("string")
def firstLetterUDF(email: str) -> str:
    return email[0]

# COMMAND ----------

# MAGIC %md E agora será utilizado o UDF decorator.

# COMMAND ----------

from pyspark.sql.functions import col

salesDF = spark.read.parquet("/mnt/training/ecommerce/sales/sales.parquet")
display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %md ### Pandas/UDFs Vetorizados
# MAGIC 
# MAGIC A partir do Spark 2.3, existem UDFs em Pandas disponíveis em Python para melhorar a eficiência das UDFs. Os UDFs em Pandas utilizam Apache Arrow para acelerar a computação.
# MAGIC 
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Post no blog</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentação</a>
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC 
# MAGIC As UDFs são executadas usando:  
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>, um formato de dados colunar em memória que é usado no Spark para transferências dados com eficiência entre os processos de Python eJVM com custo de desserialização próximo de 0. 
# MAGIC * Pandas dentro da função para trabalhar com instâncias e APIs em Pandas
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> A partir do Spark 3.0, é sempre necessário definir a UDF em Pandas usando as dicas de tipo Python. 

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# Observa uma string na entrada e saída
@pandas_udf("string")
def vectorizedUDF(email: pd.Series) -> pd.Series:
    return email.str[0]

# De modo alternativo
# def vectorizedUDF(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorizedUDF = pandas_udf(vectorizedUDF, "string")

# COMMAND ----------

display(salesDF.select(vectorizedUDF(col("email"))))

# COMMAND ----------

# MAGIC %md Também é possível registrar uma UDF Pandas para um namespace em SQL.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorizedUDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Utilizando a UDF pandas no SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# MAGIC %md
# MAGIC # Laboratório de Organização Diária
# MAGIC 
# MAGIC ##### Tarefas
# MAGIC 1. Crie um UDF para exibir o dia da semana
# MAGIC 1. Aplique um UDF para exibir e ordenar os dados pelo dia da semana 
# MAGIC 1. Plote um gráfico de barras com os usuários ativos por dia da semana

# COMMAND ----------

# MAGIC %md Inicie o dataframe com o número médio de usuários ativos por dia da semana.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .parquet(eventsPath)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Defina a UDF para exibir o dia da semana
# MAGIC 
# MAGIC Utilize a função **`labelDayOfWeek`** disponibilizada abaixo para criar o UDF **`labelDowUDF`**

# COMMAND ----------

def labelDayOfWeek(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
           "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

# COMMAND ----------

labelDowUDF = udf(labelDayOfWeek)

# COMMAND ----------

# MAGIC %md ### 2. Aplique o UDF para exibir e ordenar os dias da semana 
# MAGIC - Atualize a coluna **`day`** aplicando o UDF para substituir os dados nessa coluna
# MAGIC - Ordene os dados pela coluna **`day`**
# MAGIC - Plote um gráfico de barras

# COMMAND ----------

finalDF = (df
           .withColumn("day", labelDowUDF(col("day")))
           .select("day", "avg_users")
           .sort("day")
          )


#df.select(labelDowUDF(col("day")), "avg_users").sort("day")



display(finalDF)

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
