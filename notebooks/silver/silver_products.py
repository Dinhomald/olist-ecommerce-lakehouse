# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/products/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

order_schema = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_category_name", StringType(), nullable=False),
    StructField("product_name_lenght", DoubleType(), nullable=True),
    StructField("product_height_cm", DoubleType(), nullable=True),
    StructField("product_length_cm", DoubleType(), nullable=True),
    StructField("product_width_cm", DoubleType(), nullable=True),
    StructField("product_weight_g", DoubleType(), nullable=True),
    StructField("product_description_lenght", DoubleType(), nullable=True),
    StructField("product_photos_qty", DoubleType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("products_raw"))
    .select(from_json(to_json(col("products_raw")), order_schema).alias("o"))
    .select(col("o.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

df_silver = df_exploded

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.products'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")