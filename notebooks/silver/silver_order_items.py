# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/order_items/"

df_bronze = spark.read.json(BRONZE_PATH)

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_item_id", LongType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("seller_id", StringType(), nullable=False),
    StructField("price", DoubleType(), nullable=False),
    StructField("freight_value", DoubleType(), nullable=False),
    StructField("shipping_limit_date", StringType(), nullable=False),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("order_items_raw"))
    .select(from_json(to_json(col("order_items_raw")), order_schema).alias("oi"))
    .select(col("oi.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_silver = (
    df_exploded
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
)

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.order_items'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")


# COMMAND ----------

