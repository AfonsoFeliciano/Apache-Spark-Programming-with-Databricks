# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Tipos de dados complexos
# MAGIC 
# MAGIC Neste notebook são exploradas funções para trabalhar com strings e coletas de dados em arrays
# MAGIC ##### Objetivos
# MAGIC 1. Aplicar funções para coletar e processar dados de arrays
# MAGIC 1. Realizar a união de dataframes
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `unionByName`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Funções</a>:
# MAGIC   - Aggregate: `collect_set`
# MAGIC   - Collection: `array_contains`, `element_at`, `explode`
# MAGIC   - String: `split`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md Nesta demonstração o dataset utilizado será o `sales`. 

# COMMAND ----------

df = spark.read.parquet(salesPath)
display(df)

# COMMAND ----------

# MAGIC %md ### Funções para manipulação de strings
# MAGIC 
# MAGIC Abaixo, podem ser observadas algumas das funções disponívies para manipulação de strings. 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | regexp_replace | Substitui todas as strings especificadas de que estejam de acordo com a regra definida na operação regex |
# MAGIC | regexp_extract | Extrai todas as strings especificadas de acordo com a regra definida na operação regex  |
# MAGIC | ltrim | Remove todos os espaços a esquerda da string |
# MAGIC | lower | Converte as strings de uma coluna para lower/minúscula  |
# MAGIC | split | Divide uma string em através de um ou vários caracteres definidos como delimitadores |

# COMMAND ----------

# MAGIC %md ### Funções para Collections
# MAGIC 
# MAGIC Segue abaixo algumas funções disponíveis para trabalhar com arrays. 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | array_contains | Retorna null se o array é null, true se o array contém valores e false de qualquer maneira. |
# MAGIC | element_at | Retorna o index de um array. Os index são enumerados iniciando sempre por **1**. |
# MAGIC | explode | Cria um nova linha para cada elemento dentro um array ou coluna map. |
# MAGIC | collect_set | Retorna um conjunto de objetos com elementos duplicados que foram eliminados. |

# COMMAND ----------

# MAGIC %md ### Funções de agregação
# MAGIC 
# MAGIC Abaixo se encontram alguns exemplos de funções de agregação disponíveis para criar e agrupar arrays. 
# MAGIC 
# MAGIC | Método | Descrição |
# MAGIC | --- | --- |
# MAGIC | collect_list | Retorna um array que consiste todos os valores dentro de um grupo. |
# MAGIC | collect_set | Retorna um array que consiste todos os valores distintos dentro de um grupo. |

# COMMAND ----------

# MAGIC %md
# MAGIC # Compras de usuários
# MAGIC 
# MAGIC Liste todos as opções referentes a tamanhos e qualidades de compras realizadas por cada comprador. 
# MAGIC 1. Extraia os detalhes de cada item de cada compra 
# MAGIC 2. Extraia as opções de tamanho e qualidade de cada compra de colchão 
# MAGIC 3. Extraia as opções de tamanho e qualidade de cada compra de almofada 
# MAGIC 4. Combine os dados de colchões e almofadas 
# MAGIC 5. Exiba todos as opções de tamanho e qualidade para cada usuário que realizou uma compra 

# COMMAND ----------

# MAGIC %md ### 1. Extraindo os detalhes de compras 
# MAGIC 
# MAGIC - Realize um explode na coluna **`items`** utilizando o dataframe **`df`** com os resultados substituindo o campo **`items`**
# MAGIC - Seleciona os campos **`email`** e **`item.item_name`** 
# MAGIC - Aplica um Split nas palavras para o campo **`item_name`** e aplique um alias para a coluna para ser nomeada como "details"
# MAGIC 
# MAGIC Atribua o resultado do dataframe para o dataframe nomeado como  **`detailsDF`**.

# COMMAND ----------

display(df)

# COMMAND ----------

#Visualizando o step 1, substituindo a coluna items por ela mesmo com o retorno do explode
step1Df = df.withColumn("items", explode("items"))
display(step1Df)

# COMMAND ----------

#Visualizando o step 2, com o passo do step 1 e seleção do email e items.item_name
step2Df = (df
            .withColumn("items", explode("items"))
            .select("email", "items.item_name")
          )   
display(step2Df)

# COMMAND ----------

#Visualizando o resultado final utilizando a função split
from pyspark.sql.functions import *

detailsDF = (df
             .withColumn("items", explode("items"))
             .select("email", "items.item_name")
             .withColumn("details", split(col("item_name"), " "))
            )
display(detailsDF)

# COMMAND ----------

# MAGIC %md Agora torna-se possível visualizar a coluna  **`details`** que contém um array com a qualidade, tamanho e tipo de objeto.

# COMMAND ----------

# MAGIC %md ### 2. Extraindo o tamanho e qualidade dos colchões que foram comprados 
# MAGIC 
# MAGIC - Filtre o dataframe**`detailsDF`** para os registro no qual a coluna **`details`** contém o valor  "Mattress" (colchões)
# MAGIC - Adicione uma coluna **`size`** extraindo o elemento da segunda posição 
# MAGIC - Adicione uma coluna **`quality`** extraindo o elemento da primeira posição 
# MAGIC 
# MAGIC Save o resultado no dataframe **`mattressDF`**.

# COMMAND ----------

mattressDF = (detailsDF
              .filter(array_contains(col("details"), "Mattress"))
              .withColumn("size", element_at(col("details"), 2))
              .withColumn("quality", element_at(col("details"), 1))
             )
display(mattressDF)

# COMMAND ----------

# MAGIC %md Agora, o mesmo será realizado para as compras de almofadas/pillow.

# COMMAND ----------

# MAGIC %md ### 3. Extraia as opções de tamanho e qualidade das compras de almofadas
# MAGIC - Filtre o dataframe **`detailsDF`** onde a coluna **`details`** contém registros iguais a "Pillow"
# MAGIC - Adicione uma coluna **`size`** extraindo o elemento da primeira posição 
# MAGIC - Adicione uma coluna **`quality`** extraindo o elemento da segunda posição 
# MAGIC 
# MAGIC Observe que as posições de **`size`** e **`quality`** estão trocadas no array para colchões e almofadas
# MAGIC Salve o resultado em um dataframe nomeado como **`pillowDF`**.

# COMMAND ----------

pillowDF = (detailsDF
            .filter(array_contains(col("details"), "Pillow"))
            .withColumn("size", element_at(col("details"), 1))
            .withColumn("quality", element_at(col("details"), 2))
           )
display(pillowDF)

# COMMAND ----------

# MAGIC %md ### 4. Combine os dados de colchões e almofadas 
# MAGIC 
# MAGIC - Realize um union entre os dataframes **`mattressDF`** e **`pillowDF`** 
# MAGIC - Remova a coluna **`details`**
# MAGIC 
# MAGIC Salve os resultados no dataframe **`unionDF`**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> O dataframe <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">`union`</a> possui o método union similar ao SQL no qual as colunas e schema precisam ser idênticos para todos os dataframes que serão combinados. Já o método <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">`unionByName`</a> realiza a união pelo nome das colunas.

# COMMAND ----------

unionDF = mattressDF.unionByName(pillowDF).drop("details")
display(unionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Liste todas as opções de tamanho e qualidade para cada usuário
# MAGIC 
# MAGIC - Agrupe as linhas do dataframe **`unionDF`** pela coluna **`email`**
# MAGIC   - Utilize o comando collect_set para todos os itens da coluna **`size`** para cada usuário e aplique um alias para nomear a coluna como "size options"
# MAGIC   - Utilize o comando collect_set para todos os itens da coluna **`quality`** para cada usuário e aplique um alias para nomear a coluna como "quality options"
# MAGIC 
# MAGIC Salve o resultado no dataframe **`optionsDF`**.

# COMMAND ----------

optionsDF = (unionDF
             .groupBy("email")
             .agg(collect_set("size").alias("size options"),
                  collect_set("quality").alias("quality options"))
            )
display(optionsDF)

# COMMAND ----------

# MAGIC %md ### Limpando o ambiente de estudos.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
