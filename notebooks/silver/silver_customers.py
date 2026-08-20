# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/customers/"

df_bronze = spark.read.json(BRONZE_PATH)

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType

order_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_unique_id", StringType(), nullable=False),
    StructField("customer_city", StringType(), nullable=False),
    StructField("customer_state", StringType(), nullable=False),
    StructField("customer_zip_code_prefix", StringType(), nullable=False),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("customers_raw"))
    .select(from_json(to_json(col("customers_raw")), order_schema).alias("c"))
    .select(col("c.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

df_silver = df_exploded

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.customers'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)