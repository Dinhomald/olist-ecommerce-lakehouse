# Databricks notebook source
from pyspark.sql.functions import col, coalesce, lit, to_date

df_order_items = spark.table("olist_lakehouse.silver.order_items")
df_orders = spark.table("olist_lakehouse.silver.orders").select(
    "order_id", "customer_id", "order_purchase_timestamp"
)
df_dim_produto = spark.table("olist_lakehouse.gold.dim_produto")
df_dim_cliente = spark.table("olist_lakehouse.gold.dim_cliente")
df_dim_vendedor = spark.table("olist_lakehouse.gold.dim_vendedor")
df_dim_data = spark.table("olist_lakehouse.gold.dim_data")
df_customers_silver = spark.table("olist_lakehouse.silver.customers").select(
    "customer_id", "customer_unique_id"
)

df_base = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), on="order_id", how="inner")
    .join(df_customers_silver.alias("c"), on="customer_id", how="inner")
)

print(f"Linhas na base (order_items + data + cliente): {df_base.count()}")

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce, lit

df_dim_produto_intervalo = df_dim_produto.withColumn(
    "data_fim_vigencia_ajustada",
    coalesce(col("data_fim_vigencia"), to_date(lit("9999-12-31")))
)

df_com_produto = (
    df_base.alias("b")
    .join(
        df_dim_produto_intervalo.alias("dp"),
        on=(
            (col("b.product_id") == col("dp.product_id")) &
            (to_date(col("b.order_purchase_timestamp")) >= col("dp.data_inicio_vigencia")) &
            (to_date(col("b.order_purchase_timestamp")) < col("dp.data_fim_vigencia_ajustada"))
        ),
        how="left"
    )
)

print(f"Linhas após join com dim_produto: {df_com_produto.count()}")

sem_produto = df_com_produto.filter(col("dp.sk_produto").isNull()).count()
print(f"Itens sem versão de produto correspondente: {sem_produto}")

# COMMAND ----------

from pyspark.sql.functions import date_format

df_com_cliente = (
    df_com_produto
    .join(df_dim_cliente, on="customer_unique_id", how="left")
)

df_com_vendedor = (
    df_com_cliente
    .join(df_dim_vendedor, on="seller_id", how="left")
)

df_com_data = (
    df_com_vendedor
    .withColumn("sk_data_venda", date_format(to_date(col("order_purchase_timestamp")), "yyyyMMdd").cast("int"))
)

print(f"Linhas após todos os joins: {df_com_data.count()}")

sem_cliente = df_com_data.filter(col("sk_cliente").isNull()).count()
sem_vendedor = df_com_data.filter(col("sk_vendedor").isNull()).count()
print(f"Itens sem cliente correspondente: {sem_cliente}")
print(f"Itens sem vendedor correspondente: {sem_vendedor}")

# COMMAND ----------

df_fato_vendas = (
    df_com_data
    .select(
        col("order_id"),
        col("order_item_id"),
        col("sk_produto"),
        col("sk_cliente"),
        col("sk_vendedor"),
        col("sk_data_venda").alias("sk_data"),
        col("price"),
        col("freight_value"),
    )
)

display(df_fato_vendas.limit(5))

# COMMAND ----------

total = df_fato_vendas.count()
chave_composta_distinta = df_fato_vendas.select("order_id", "order_item_id").distinct().count()

print(f"Total: {total}")
print(f"Chave composta (order_id, order_item_id) distinta: {chave_composta_distinta}")

assert total == 112650, "Total não bate com order_items original"
assert total == chave_composta_distinta, "Chave composta duplicada — fan-out em algum join"

(
    df_fato_vendas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.fato_vendas")
)

print("Tabela olist_lakehouse.gold.fato_vendas escrita com sucesso.")