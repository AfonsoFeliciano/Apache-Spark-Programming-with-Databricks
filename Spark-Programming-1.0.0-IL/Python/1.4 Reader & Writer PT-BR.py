# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Leitura e Escrita
# MAGIC 1. Leitura de arquivos CSV
# MAGIC 1. Leitura de arquivos JSON
# MAGIC 1. Escrita de DataFrames em arquivos
# MAGIC 1. Escrita de DataFrames em tabelas
# MAGIC 
# MAGIC ##### Métodos
# MAGIC - DataFrameReader (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframereader#pyspark.sql.DataFrameReader" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html" target="_blank">Scala</a>): `csv`, `json`, `option`, `schema`
# MAGIC - DataFrameWriter (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframereader#pyspark.sql.DataFrameWriter" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriter.html" target="_blank">Scala</a>): `mode`, `option`, `parquet`, `format`, `saveAsTable`
# MAGIC - StructType (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=structtype#pyspark.sql.types.StructType" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/StructType.html" target="_blank" target="_blank">Scala</a>): `toDDL`
# MAGIC 
# MAGIC ##### Spark Types
# MAGIC - Types (<a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=types#module-pyspark.sql.types" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/index.html" target="_blank">Scala</a>): `ArrayType`, `DoubleType`, `IntegerType`, `LongType`, `StringType`, `StructType`, `StructField`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lendo um arquivo CSV
# MAGIC Realizando a leitura de um arquivo CSV utilizando DataFrameReader's `csv` com os seguintes métodos:
# MAGIC 
# MAGIC Delimitador é um tab, usar a primeira linha como cabeçalho, infer schema igual a True

# COMMAND ----------

usersCsvPath = "/mnt/training/ecommerce/users/users-500k.csv"

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .option("inferSchema", True)
  .csv(usersCsvPath))

usersDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC É possível definir o schema manualmente através do `StructType` especificando os nomes das colunas e os tipos de dados

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
  StructField("user_id", StringType(), True),  
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("email", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Lendo um CSV utilizando um schema definido ao invés de inferir o schema de modo automático

# COMMAND ----------

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(userDefinedSchema)
  .csv(usersCsvPath))

# COMMAND ----------

# MAGIC %md
# MAGIC De modo alternativo, é possível definir o schema através de um DDL formatado em string

# COMMAND ----------

DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(DDLSchema)
  .csv(usersCsvPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lendo um arquivo JSON
# MAGIC 
# MAGIC Lendo um arquivo JSON inferindo o seu schema de maneira automática

# COMMAND ----------

eventsJsonPath = "/mnt/training/ecommerce/events/events-500k.json"

eventsDF = (spark.read
  .option("inferSchema", True)
  .json(eventsJsonPath))

eventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Realizando uma leitura de maneira mais rápida definindo o schema através do comando `StructType` informando os nomes das colunas e os tipos de dados

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
  StructField("device", StringType(), True),  
  StructField("ecommerce", StructType([
    StructField("purchaseRevenue", DoubleType(), True),
    StructField("total_item_quantity", LongType(), True),
    StructField("unique_items", LongType(), True)
  ]), True),
  StructField("event_name", StringType(), True),
  StructField("event_previous_timestamp", LongType(), True),
  StructField("event_timestamp", LongType(), True),
  StructField("geo", StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
  ]), True),
  StructField("items", ArrayType(
    StructType([
      StructField("coupon", StringType(), True),
      StructField("item_id", StringType(), True),
      StructField("item_name", StringType(), True),
      StructField("item_revenue_in_usd", DoubleType(), True),
      StructField("price_in_usd", DoubleType(), True),
      StructField("quantity", LongType(), True)
    ])
  ), True),
  StructField("traffic_source", StringType(), True),
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("user_id", StringType(), True)
])

eventsDF = (spark.read
  .schema(userDefinedSchema)
  .json(eventsJsonPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Também é possível utilizar o método `toDDL` em scala para possuir um DDL em string formatado. 

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

# COMMAND ----------

DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

eventsDF = (spark.read
  .schema(DDLSchema)
  .json(eventsJsonPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Salvando o dataframe em arquivos
# MAGIC 
# MAGIC O dataframe `usersDF` será salvo em arquivo parquet com as seguintes configurações:
# MAGIC 
# MAGIC Modo de compressão Snappy, e overwrite modo para sempre sobrescrever os dados

# COMMAND ----------

usersOutputPath = workingDir + "/users.parquet"

(usersDF.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(usersOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvando o dataframe `eventsDF` em formato <a href="https://delta.io/" target="_blank">Delta</a> com as seguintes configurações: 
# MAGIC 
# MAGIC Formato Delta e modo overwrite para sobrescrever os dados

# COMMAND ----------

eventsOutputPath = workingDir + "/delta/events"

(eventsDF.write
  .format("delta")
  .mode("overwrite")
  .save(eventsOutputPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Salvando DataFrames em tabelas
# MAGIC 
# MAGIC Salvando o dataframe `eventsDF` em uma tabela utilizando o método `saveAsTable`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Esse método cria uma tabela global, diferente da view temporária que é criada de maneira local através do método `createOrReplaceTempView`. Durante os cursos databricks são abordadas as diferenças entre objetos temporários, globais, etc.

# COMMAND ----------

eventsDF.write.mode("overwrite").saveAsTable("events_p")

# COMMAND ----------

# MAGIC %md
# MAGIC Essa tabela é salva dentro de um database criado através do arquivo de setup. Iremos visualizar o nome do database através do print abaixo.

# COMMAND ----------

print(databaseName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lab de Ingestão de Dados
# MAGIC 
# MAGIC Ler o arquivo CSV com dados de produtos
# MAGIC 1. Realizar a leitura utilizando o infer schema
# MAGIC 2. Realizar a leitura com m schema definido
# MAGIC 3. Realizar a leitura com um DDL formatado em string
# MAGIC 4. Escrever o dataframe em delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Realizar a leitura com o schema inferido
# MAGIC - Visualizar a primeira linha do arquivo CSV utilizando DBUtils com o método `fs.head` especificando o caminho do arquivo na variável `singleProductCsvFilePath`
# MAGIC - Criar o dataframe `productsDF` através da leitura do arquivo csv cujo caminho foi armazenado na variável `productsCsvPath`
# MAGIC - Configurar as opções de infer schema e primeira linha utilizada como cabeçalho

# COMMAND ----------

singleProductCsvFilePath = "/mnt/training/ecommerce/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"

print(singleProductCsvFilePath)

# COMMAND ----------

productsCsvPath = "/mnt/training/ecommerce/products/products.csv"
productsDF = (spark.read
              .option("sep", ",")
              .option("header", True)
              .option("inferSchema", True)
              .csv(productsCsvPath)
)

productsDF.printSchema()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Realizando uma checagem

# COMMAND ----------

assert(productsDF.count() == 12)

print("Nenhum erro encontrado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Realizando a leitura com o schema definido
# MAGIC Definindo um schema utilizando `StructType` com o nome das colunas e os tipos de dados

# COMMAND ----------

userDefinedSchema = StructType([
  StructField("item_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("price", DoubleType(), True)
  
])

productsDF2 = (spark.read
               .option("sep", ",")
               .option("header", True)
               .schema(userDefinedSchema)
               .csv(productsCsvPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Checando a importação com os StructType

# COMMAND ----------

assert(userDefinedSchema.fieldNames() == ["item_id", "name", "price"])

print("Não houve erros na importação")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = productsDF2.first()

assert(expected1 == result1)

print("Não houve erros ao validar a coluna price")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Realizando a leitura com o DDL formatado em string

# COMMAND ----------

# TODO
DDLSchema = "item_id string, name string, price double"

productsDF3 = (spark.read
               .option("sep", ",")
               .option("header", True)
               .schema(DDLSchema)
               .csv(productsCsvPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Validando o DDL

# COMMAND ----------

assert(productsDF3.count() == 12)

print("DDL validado com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Escrevendo em Delta
# MAGIC Salvando o dataframe `productsDF` no diretório armazenado na variável `productsOutputPath`

# COMMAND ----------

productsOutputPath = workingDir + "/delta/products"
(productsDF.write
  .format("delta")
  .mode("overwrite")
  .save(productsOutputPath)
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Verificando o dataframe salvo em delta

# COMMAND ----------

assert(len(dbutils.fs.ls(productsOutputPath)) == 5)

print("Dataframe salvo corretamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpando o ambiente de estudos

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
