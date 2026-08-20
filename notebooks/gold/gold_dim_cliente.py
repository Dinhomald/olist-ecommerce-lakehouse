# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

df_customers_silver = spark.table("olist_lakehouse.silver.customers")
df_orders_silver = spark.table("olist_lakehouse.silver.orders")

# Junta customers com orders para saber a data de cada pedido do cliente,
# depois pega a linha mais recente por customer_unique_id.
df_customers_com_data = (
    df_customers_silver.alias("c")
    .join(
        df_orders_silver.select("customer_id", "order_purchase_timestamp").alias("o"),
        on="customer_id",
        how="inner"
    )
)

window_spec = Window.partitionBy("customer_unique_id").orderBy(col("order_purchase_timestamp").desc())

df_dim_cliente = (
    df_customers_com_data
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .select(
        col("customer_unique_id"),
        col("customer_city").alias("cidade"),
        col("customer_state").alias("estado"),
        col("customer_zip_code_prefix").alias("zip_code_prefix"),
    )
)

print(f"Total de clientes únicos: {df_dim_cliente.count()}")
df_dim_cliente.printSchema()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_dim_cliente_final = df_dim_cliente.withColumn(
    "sk_cliente", monotonically_increasing_id()
).select(
    "sk_cliente", "customer_unique_id", "cidade", "estado", "zip_code_prefix"
)

display(df_dim_cliente_final.limit(5))

# COMMAND ----------

total = df_dim_cliente_final.count()
sk_distintas = df_dim_cliente_final.select("sk_cliente").distinct().count()
cliente_distintos = df_dim_cliente_final.select("customer_unique_id").distinct().count()

print(f"Total: {total}")
print(f"sk_cliente distintas: {sk_distintas}")
print(f"customer_unique_id distintos: {cliente_distintos}")

assert total == sk_distintas == cliente_distintos, "Inconsistência na Dim Cliente"

(
    df_dim_cliente_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.dim_cliente")
)

print("Tabela olist_lakehouse.gold.dim_cliente escrita com sucesso.")