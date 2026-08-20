# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/reviews/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType, LongType

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("review_id", StringType(), nullable=True),
    StructField("review_creation_date", StringType(), nullable=True),
    StructField("review_comment_title", StringType(), nullable=False),
    StructField("review_comment_message", StringType(), nullable=True),
    StructField("review_score", StringType(), nullable=True),
    StructField("review_answer_timestamp", LongType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("reviews_raw"))
    .select(from_json(to_json(col("reviews_raw")), order_schema).alias("r"))
    .select(col("r.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_silver = (
    df_exploded
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
)

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

silver_table = 'olist_lakehouse.silver.reviews'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela {silver_table} escrita com sucesso.")