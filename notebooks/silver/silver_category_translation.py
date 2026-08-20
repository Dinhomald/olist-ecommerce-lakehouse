# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/category_translation/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType

order_schema = StructType([
    StructField("product_category_name", StringType(), nullable=False),
    StructField("product_category_name_english", StringType(), nullable=False),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("category_translation_raw"))
    .select(from_json(to_json(col("category_translation_raw")), order_schema).alias("ct"))
    .select(col("ct.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

df_silver = df_exploded

silver_table = 'olist_lakehouse.silver.category_translation'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")