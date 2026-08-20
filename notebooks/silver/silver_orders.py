# Databricks notebook source
BRONZE_PATH = "abfss://bronze@adlsolistdatalake.dfs.core.windows.net/orders/"

df_bronze = spark.read.json(BRONZE_PATH)

print(f"Linhas lidas (envelopes de página): {df_bronze.count()}")
df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode
from pyspark.sql.types import StructType, StructField, StringType

order_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", StringType(), nullable=True),
    StructField("order_approved_at", StringType(), nullable=True),
    StructField("order_delivered_carrier_date", StringType(), nullable=True),
    StructField("order_delivered_customer_date", StringType(), nullable=True),
    StructField("order_estimated_delivery_date", StringType(), nullable=True),
])

# COMMAND ----------

from pyspark.sql.functions import from_json, to_json

df_exploded = (
    df_bronze
    .select(explode(col("data")).alias("order_raw"))
    .select(from_json(to_json(col("order_raw")), order_schema).alias("o"))
    .select(col("o.*"))
)

print(f"Linhas após explode: {df_exploded.count()}")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_silver = (
    df_exploded
    .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
)

df_silver.printSchema()
display(df_silver.limit(10))

# COMMAND ----------

total_registros = df_silver.count()
distintos_order_id = df_silver.select("order_id").distinct().count()

print(f"Total de registros: {total_registros}")
print(f"order_id distintos: {distintos_order_id}")
print(f"Esperado (API total_records): 99441")

assert total_registros == 99441, "Contagem de registros não bate com total_records da API"
assert distintos_order_id == total_registros, "Existem order_id duplicados"

# COMMAND ----------

SILVER_TABLE = 'olist_lakehouse.silver.orders'

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Tabela {SILVER_TABLE} escrita com sucesso.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_registros, COUNT(DISTINCT order_id) AS order_id_distintos
# MAGIC FROM olist_lakehouse.silver.orders;