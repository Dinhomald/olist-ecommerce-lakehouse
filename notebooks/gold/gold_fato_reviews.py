# Databricks notebook source
from pyspark.sql.functions import col, to_date, date_format

df_reviews = spark.table("olist_lakehouse.silver.reviews")
df_orders = spark.table("olist_lakehouse.silver.orders").select(
    "order_id", "customer_id"
)
df_customers_silver = spark.table("olist_lakehouse.silver.customers").select(
    "customer_id", "customer_unique_id"
)
df_dim_cliente = spark.table("olist_lakehouse.gold.dim_cliente")

df_reviews_base = (
    df_reviews
    .join(df_orders, on="order_id", how="inner")
    .join(df_customers_silver, on="customer_id", how="inner")
    .join(df_dim_cliente, on="customer_unique_id", how="left")
    .withColumn("sk_data_review", date_format(to_date(col("review_creation_date")), "yyyyMMdd").cast("int"))
)

print(f"Linhas na base de reviews: {df_reviews_base.count()}")

sem_cliente = df_reviews_base.filter(col("sk_cliente").isNull()).count()
sem_data = df_reviews_base.filter(col("sk_data_review").isNull()).count()
print(f"Reviews sem cliente correspondente: {sem_cliente}")
print(f"Reviews sem sk_data_review resolvida: {sem_data}")

# COMMAND ----------

df_fato_reviews = (
    df_reviews_base
    .select(
        col("review_id"),
        col("order_id"),
        col("sk_cliente"),
        col("sk_data_review").alias("sk_data"),
        col("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
    )
)

display(df_fato_reviews.limit(5))

# COMMAND ----------

total = df_fato_reviews.count()
chave_composta_distinta = df_fato_reviews.select("review_id", "order_id").distinct().count()

print(f"Total: {total}")
print(f"Chave composta (review_id, order_id) distinta: {chave_composta_distinta}")

assert total == 99224, "Total não bate com reviews original"
assert total == chave_composta_distinta, "Chave composta duplicada — fan-out em algum join"

(
    df_fato_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.fato_reviews")
)

print("Tabela olist_lakehouse.gold.fato_reviews escrita com sucesso.")