# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/geolocation/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

order_schema = StructType([
    StructField("geolocation_city", StringType(), nullable=False),
    StructField("geolocation_lat", DoubleType(), nullable=True),
    StructField("geolocation_lng", DoubleType(), nullable=True),
    StructField("geolocation_state", StringType(), nullable=True),
    StructField("geolocation_zip_code_prefix", StringType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("geolocation_raw"))
    .select(from_json(to_json(col("geolocation_raw")), order_schema).alias("g"))
    .select(col("g.*"))
)

df_exploded.printSchema()
display(df_exploded.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.geolocation'

df_silver = df_exploded

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")