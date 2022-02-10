# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Otimização de Query
# MAGIC 
# MAGIC Serão explorados os planos e otimizações de query para alguns exemplos incluindo otimização lógica e seus exemplos com e sem a propriedade de filtro predicate pushdown
# MAGIC 
# MAGIC ##### Objetivos
# MAGIC 1. Otimizações lógicas
# MAGIC 1. Utilizando predicate pushdown
# MAGIC 1. Sem utilização de predicate pushdown
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `explain`

# COMMAND ----------

# MAGIC %md
# MAGIC ![query optimization](https://files.training.databricks.com/images/aspwd/query_optimization_catalyst.png)

# COMMAND ----------

# MAGIC %md ![query optimization aqe](https://files.training.databricks.com/images/aspwd/query_optimization_aqe.png)

# COMMAND ----------

# MAGIC %md A célula abaixo irá buscar um conjunto de dados e armazená-lo na varável `df`. Após isso os dados serão exibidos. 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

df = spark.read.parquet(eventsPath)
display(df)

# COMMAND ----------

# MAGIC %md ### Otimização lógica
# MAGIC 
# MAGIC O comando `explain(..)` exibe o plano de consulta, sendo possível formatá-lo para obter um modo mais detalhado. É possível comparar o plano lógico e físico obtervando como o Catalyst lida com múltiplas transformações que possuem `filter`.

# COMMAND ----------

from pyspark.sql.functions import col

limitEventsDF = (df
                 .filter(col("event_name") != "reviews")
                 .filter(col("event_name") != "checkout")
                 .filter(col("event_name") != "register")
                 .filter(col("event_name") != "email_coupon")
                 .filter(col("event_name") != "cc_info")
                 .filter(col("event_name") != "delivery")
                 .filter(col("event_name") != "shipping_info")
                 .filter(col("event_name") != "press")
                )

limitEventsDF.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC Do mesmo modo acima, a query poderia ser escrita utilizando apenas um `filter` com operadores lógicos. Vamos comparar a query anterior com a query a seguir.

# COMMAND ----------

betterDF = (df
            .filter((col("event_name").isNotNull()) &
                    (col("event_name") != "reviews") &
                    (col("event_name") != "checkout") &
                    (col("event_name") != "register") &
                    (col("event_name") != "email_coupon") &
                    (col("event_name") != "cc_info") &
                    (col("event_name") != "delivery") &
                    (col("event_name") != "shipping_info") &
                    (col("event_name") != "press"))
           )

betterDF.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC A seguir, será visualizado como o Catalyst lida com essa query. 

# COMMAND ----------

stupidDF = (df
            .filter(col("event_name") != "finalize")
            .filter(col("event_name") != "finalize")
            .filter(col("event_name") != "finalize")
            .filter(col("event_name") != "finalize")
            .filter(col("event_name") != "finalize")
           )

stupidDF.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cache
# MAGIC 
# MAGIC Por padrão os dados de um DataFrame estão presentes em apenas um Cluster Spark enquanto estão sendo processados durante uma consulta. Após a finalização do processamento os dados não são persistidos de maneira automática visto que o Spark é um motor de processamento e não um sistema de armazenamento. Porém é possível realizar uma requisição no spark para que o Dataframe persista no cluster através da chamada do método `cache`.
# MAGIC 
# MAGIC Além disso, se um DataFrame está armazenada em cache, recomenda-se sempre eliminá-lo de maneira explicita quando não for mais necessário. Para isso basta utilizar o método `unpersist`.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_best_32.png" alt="Best Practice"> Armazenar um dataframe em cache pode ser apropriado quando o mesmo será utilizado diversas vezes em tarefas como: 
# MAGIC - Análise Exploratória de Dados
# MAGIC - Treinamento de modelos de machine learning
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Além desses casos de uso, é recomendado **não** armazenar dataframes em cache pois isso irá degradar a performance da aplicação. 
# MAGIC - O armazenamento de dados em cache consome recursos do cluster que poderiam ser utilizados em execução de outras tarefas
# MAGIC - O armazenamento em cache pode impedir que o Spark aplique otimizações de consultas, como poderá ser visualizado a seguir

# COMMAND ----------

# MAGIC %md ### Predicate Pushdown
# MAGIC 
# MAGIC Abaixo podemos observar um exemplo de leitura de uma fonte em JDBC onde o Catalyst determina que o  *predicate pushdown* pode ocorrer. 

# COMMAND ----------

# MAGIC %scala
# MAGIC // Carregando o driver
# MAGIC Class.forName("org.postgresql.Driver")

# COMMAND ----------

jdbcURL = "jdbc:postgresql://54.213.33.240/training"

# Credenciais e direitos de w/read-only (escrita/leitura)
connProperties = {
    "user" : "training",
    "password" : "training"
}

ppDF = (spark
        .read
        .jdbc(
            url=jdbcURL,                  # URL do JDC
            table="training.people_1m",   # nome da tabela
            column="id",                  # nome da coluna utilizada para particionamento 
            lowerBound=1,                 # valor mínimo da coluna para decisão se haverá ou não particionamento 
            upperBound=1000000,           # valor máximo da coluna para decisão se haverá ou não particionamento 
            numPartitions=8,              # número de particições/conexões 
            properties=connProperties     # propriedades da conexão 
        )
        .filter(col("gender") == "M")   # Filtrando dados pelo genêro
       )

ppDF.explain()

# COMMAND ----------

# MAGIC %md Podemos observar a falta de um **filtro** e a presença de **PushedFilters** no scan. A operação de filtro é enviado ao banco de dados e apenas os registros correspondentes são enviados ao Spark. Isso pode reduzir a quantidade de dados que o spark precisa ingerir. 

# COMMAND ----------

# MAGIC %md ### Sem indicação de Predicate Pushdown
# MAGIC 
# MAGIC Em comparação, armazenando os dados em cache antes de filtrá-los elimina a possibilidade de encontrar um predicate push down.

# COMMAND ----------

cachedDF = (spark
            .read
            .jdbc(
                url=jdbcURL,
                table="training.people_1m",
                column="id",
                lowerBound=1,
                upperBound=1000000,
                numPartitions=8,
                properties=connProperties
            )
           )

cachedDF.cache()
filteredDF = cachedDF.filter(col("gender") == "M")

filteredDF.explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC Além do **Scan** visto no exemplo anterior, também foi visualizado um **InMemoryTableScan** seguido de um **Filter** no plano de execução. 
# MAGIC 
# MAGIC Isso significa que o Spark leu todos os dados do banco de dados e armazenou em cache, em seguido, escaneou os dados em cache para encontrar os registros que estavam de acordo com a condição. 

# COMMAND ----------

# MAGIC %md
# MAGIC Por fim, vamos remover os dados do armazenamento em cache. 

# COMMAND ----------

cachedDF.unpersist()

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
