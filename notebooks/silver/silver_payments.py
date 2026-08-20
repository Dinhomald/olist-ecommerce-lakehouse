# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/payments/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("payment_type", StringType(), nullable=True),
    StructField("payment_value", DoubleType(), nullable=True),
    StructField("payment_sequential", LongType(), nullable=True),
    StructField("payment_installments", LongType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("payments_raw"))
    .select(from_json(to_json(col("payments_raw")), order_schema).alias("p"))
    .select(col("p.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

df_silver = df_exploded

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.payments'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")