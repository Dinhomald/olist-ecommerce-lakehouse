# Databricks notebook source
from pyspark.sql.functions import col, to_date, date_format

df_payments = spark.table("olist_lakehouse.silver.payments")
df_orders = spark.table("olist_lakehouse.silver.orders").select(
    "order_id", "customer_id", "order_purchase_timestamp"
)
df_customers_silver = spark.table("olist_lakehouse.silver.customers").select(
    "customer_id", "customer_unique_id"
)
df_dim_cliente = spark.table("olist_lakehouse.gold.dim_cliente")

df_pagamentos_base = (
    df_payments
    .join(df_orders, on="order_id", how="inner")
    .join(df_customers_silver, on="customer_id", how="inner")
    .join(df_dim_cliente, on="customer_unique_id", how="left")
    .withColumn("sk_data", date_format(to_date(col("order_purchase_timestamp")), "yyyyMMdd").cast("int"))
)

print(f"Linhas na base de pagamentos: {df_pagamentos_base.count()}")

sem_cliente = df_pagamentos_base.filter(col("sk_cliente").isNull()).count()
print(f"Pagamentos sem cliente correspondente: {sem_cliente}")

# COMMAND ----------

df_fato_pagamentos = (
    df_pagamentos_base
    .select(
        col("order_id"),
        col("payment_sequential"),
        col("sk_cliente"),
        col("sk_data"),
        col("payment_type"),
        col("payment_installments"),
        col("payment_value"),
    )
)

display(df_fato_pagamentos.limit(5))

# COMMAND ----------

total = df_fato_pagamentos.count()
chave_composta_distinta = df_fato_pagamentos.select("order_id", "payment_sequential").distinct().count()

print(f"Total: {total}")
print(f"Chave composta (order_id, payment_sequential) distinta: {chave_composta_distinta}")

assert total == 103886, "Total não bate com payments original"
assert total == chave_composta_distinta, "Chave composta duplicada — fan-out em algum join"

(
    df_fato_pagamentos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.fato_pagamentos")
)

print("Tabela olist_lakehouse.gold.fato_pagamentos escrita com sucesso.")