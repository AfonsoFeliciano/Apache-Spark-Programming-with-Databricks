# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções adicionais
# MAGIC 
# MAGIC ##### Objetivos
# MAGIC 1. Aplique funções para gerar dados para novas colunas
# MAGIC 1. Aplique funções de NA para lidar com valores nulos
# MAGIC 1. Realize join entre dataframes
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a>: `fill`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Funções</a>:
# MAGIC   - Aggregate: `collect_set`
# MAGIC   - Collection: `explode`
# MAGIC   - Non-aggregate and miscellaneous: `col`, `lit`

# COMMAND ----------

# MAGIC %md ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a> possui métodos para lidar com valores nulos. Obtenha uma instância do DataFrameNaFunctions acessando o atributo `na` em um DataFrame.
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | drop | Retorna um novo dataframe omitindo linhas com qualquer, todos ou um número especificado de valores nulos considerando um subconjunto opcional de colunas  |
# MAGIC | fill | Substitui valores nulos por um valor especificado na função para um subconjunto opcional de colunas  |
# MAGIC | replace | Retorna um novo dataframe substituindo um valor por outro considerando um subconjunto opcional de colunas |

# COMMAND ----------

# MAGIC %md ### Funções não agregadas entre outras
# MAGIC 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | col / column | Retorna uma coluna com base no nome de uma coluna fornecida |
# MAGIC | lit | Cria uma coluna de valor literal, isto é pode-se atribuir um mesmo valor para todas as linhas dessa coluna  |
# MAGIC | isnull | Retorna true se a coluna é nula  |
# MAGIC | rand | Gera uma coluna com valores aleatórios indepentes e distrubuídos de maneira idêntica e uniforme |

# COMMAND ----------

# MAGIC %md ### Join com DataFrames
# MAGIC O método de dataframe <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">`join`</a> realiza a união de dois dataframes com base em uma expressão de junção, similar ao SQL. Existem vários tipos tipos de join conforme exemplos: 
# MAGIC 
# MAGIC ```
# MAGIC # Inner join com base em valores iguais de uma coluna compartilhada denonimada 'name'
# MAGIC df1.join(df2, 'name')
# MAGIC 
# MAGIC # Inner join com base em mais de uma coluna compartilhada como por exemplo 'name' e 'age'
# MAGIC df1.join(df2, ['name', 'age'])
# MAGIC 
# MAGIC # Full outer join com base em valores iguais de uma coluna compartilhada denonimada 'name'
# MAGIC df1.join(df2, 'name', 'outer')
# MAGIC 
# MAGIC # Left outer join com base em uma expressão de coluna expliícita
# MAGIC df1.join(df2, df1['customer_name'] == df2['account_name'], 'left_outer')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Laborátório de carrinhos de compras abandonados
# MAGIC 
# MAGIC Obtenha itens do carrinho de compras que foram abandonados por email sem compras
# MAGIC 1. Obtenha os e-mails das transações
# MAGIC 2. Realize um join dos e-mails com os ids
# MAGIC 3. Obtenha o histórico de carrinho de compras para cada usuário 
# MAGIC 4. Faça um join do histórico de carrinho de compras com os emails 
# MAGIC 5. Filtre os e-mails com itens abandonados no carrinho de compras
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `join`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `collect_set`, `explode`, `lit`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a>: `fill`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Executa a célula abaixo para criar o ambiente que irá receber os dataframes **`salesDF`**, **`usersDF`**, and **`eventsDF`**.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# Transações de vendas
salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# IDs e emails de usuários
usersDF = spark.read.parquet(usersPath)
display(usersDF)

# COMMAND ----------

# Eventos do website
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md ### 1-A: Obtenha os emails convertidos para cada transação de usuário
# MAGIC - Selecione a coluna **`email`** no dataframe **`salesDF`** e remova os emails duplicados
# MAGIC - Adicione uma nova coluna nomeada como **`converted`** com o valor **`True`** para cada linha
# MAGIC 
# MAGIC Salve o resultado no dataframe **`convertedUsersDF`**.

# COMMAND ----------

from pyspark.sql.functions import *
convertedUsersDF = (salesDF
                    .select(
                      ("email"), 
                      lit(True).alias("converted")
                    ).dropDuplicates()                 
)
display(convertedUsersDF)

# COMMAND ----------

# MAGIC %md #### 1-B: Verificando o código
# MAGIC 
# MAGIC A célula abaixo irá verificar se o conteúdo do dataframe está de acordo com o esperado.

# COMMAND ----------

expectedColumns = ["email", "converted"]

expectedCount = 210370

assert convertedUsersDF.columns == expectedColumns, "convertedUsersDF não possui as colunas esperadas."

assert convertedUsersDF.count() == expectedCount, "convertedUsersDF não possui a mesma quantidade de linhas esperadas"

assert convertedUsersDF.select(col("converted")).first()[0] == True, "A coluna converted não está correta"

print("O código foi validado com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-A: Realize join dos emails com os ids de usuário 
# MAGIC - Execute um outer join dos dataframes **`convertedUsersDF`** e **`usersDF`** utilizando o campo **`email`** 
# MAGIC - Filtre os usuários onde a coluna **`email`** não é nula
# MAGIC - Preencha os valores da coluna **`converted`** com o valor **`False`**
# MAGIC 
# MAGIC Salve o resultado no dataframe **`conversionsDF`**.

# COMMAND ----------


conversionsDF = (usersDF
                 .join(convertedUsersDF, 'email', 'left_outer')
                 .filter("email is not null")
                 .na.fill({'converted': False})
                 
)
display(conversionsDF)

# COMMAND ----------

# MAGIC %md #### 2-B: Verificando os dados

# COMMAND ----------

expectedColumns = ["email", "user_id", "user_first_touch_timestamp", "converted"]

expectedCount = 782749

expectedFalseCount = 572379

assert conversionsDF.columns == expectedColumns, "As colunas não estão corretas"

assert conversionsDF.filter(col("email").isNull()).count() == 0, "A coluna email possui valores nulos"

assert conversionsDF.count() == expectedCount, "O número de linhas está incorreto"

assert conversionsDF.filter(col("converted") == False).count() == expectedFalseCount, "Existe um número incorreto de linhas com o valor falso na coluna converted"

print("O dataframe não possui erros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-A: Obtenha o histórico de carrinho para cada usuário 
# MAGIC - Realize um explode do campo **`items`** no dataframe **`eventsDF`** com os novos resultados substituindo os **`items`** atuais
# MAGIC - Aplique um Group By para a coluna **`user_id`**
# MAGIC   - Colete todo o conjunto de dados de **`items.item_id`** para cada usuário e aplique um alias nomeando a coluna para "cart"
# MAGIC 
# MAGIC Salve o resultado como **`cartsDF`**.

# COMMAND ----------

cartsDF = (eventsDF
           .withColumn("items", explode("items"))
           .groupby("user_id")
           .agg(collect_set("items.item_id").alias("cart"))
           
)
display(cartsDF)

# COMMAND ----------

# MAGIC %md #### 3-B: Verificando o dataframe

# COMMAND ----------

expectedColumns = ["user_id", "cart"]

expectedCount = 488403

assert cartsDF.columns == expectedColumns, "Colunas incorretas"

assert cartsDF.count() == expectedCount, "Número de linhas incorretas"

assert cartsDF.select(col("user_id")).drop_duplicates().count() == expectedCount, "A coluna user_ids possui valores duplicados"

print("Dataframe validado com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-A: Join dos históricos com os emails
# MAGIC - Execute um left join dos dataframes **`conversionsDF`** e **`cartsDF`** utilizando o campo **`user_id`**
# MAGIC 
# MAGIC Salve o resultado no dataframe **`emailCartsDF`**.

# COMMAND ----------

emailCartsDF = conversionsDF.join(cartsDF, 'user_id', 'left_outer')
display(emailCartsDF)

# COMMAND ----------

# MAGIC %md #### 4-B: Verificando o dataframe

# COMMAND ----------

expectedColumns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expectedCount = 782749

expectedCartNullCount = 397799

assert emailCartsDF.columns == expectedColumns, "As colunas não estão conforme o esperado"

assert emailCartsDF.count() == expectedCount, "A contagem de valores não corresponde com o esperado"

assert emailCartsDF.filter(col("cart").isNull()).count() == expectedCartNullCount, "A contagem dos carrinhos nulos está incorreta"

print("O dataframe está correto")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-A: Filtre os emails com itens abandonados no carrinho 
# MAGIC - Filtre o dataframe **`emailCartsDF`** para usuários onde a coluna **`converted`** é False
# MAGIC - Filtre os usuários com carrinhos não nulos
# MAGIC 
# MAGIC Salve os resultados em **`abandonedItemsDF`**.

# COMMAND ----------

abandonedCartsDF = (emailCartsDF
                    .filter("converted == False")
                    .filter("cart is not null")
)
display(abandonedCartsDF)

# COMMAND ----------

# MAGIC %md #### 5-B: Verificando o dataframe

# COMMAND ----------

expectedColumns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expectedCount = 204272

assert abandonedCartsDF.columns == expectedColumns, "As colunas não correspondem com o esperado"

assert abandonedCartsDF.count() == expectedCount, "A contagem de valores não corresponde com o esperado"

print("Não foram identificados erros no dataframe")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6-A: Atividade bônus
# MAGIC Realize a plotagem do número de itens abandonados no carrinho por produto

# COMMAND ----------

#Para realizar a atividade, primeiro é necessário aplicar um explode na coluna cart
#Após essa aplicação, podemos agrupar tudo pela colunas items e por fim realizar uma contagem dos valores
abandonedItemsDF = (abandonedCartsDF
                    .withColumn("items", explode("cart"))
                    .groupby("items")
                    .count()
)
display(abandonedItemsDF)

# COMMAND ----------

# MAGIC %md #### 6-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

abandonedItemsDF.count()

# COMMAND ----------

expectedColumns = ["items", "count"]

expectedCount = 12

assert abandonedItemsDF.count() == expectedCount, "A contagem de valores não corresponde com o esperado"

assert abandonedItemsDF.columns == expectedColumns, "As colunas presentes no dataframe não correspondem com o esperado"

print("Atividade bônus concluída com sucesso.")

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
