# Databricks notebook source
df_sellers_silver = spark.table("olist_lakehouse.silver.sellers")

total_sellers = df_sellers_silver.count()
seller_id_distintos = df_sellers_silver.select("seller_id").distinct().count()

print(f"Total: {total_sellers}")
print(f"seller_id distintos: {seller_id_distintos}")

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id

df_dim_vendedor = (
    df_sellers_silver
    .select(
        col("seller_id"),
        col("seller_city").alias("cidade"),
        col("seller_state").alias("estado"),
        col("seller_zip_code_prefix").alias("zip_code_prefix"),
    )
    .withColumn("sk_vendedor", monotonically_increasing_id())
    .select("sk_vendedor", "seller_id", "cidade", "estado", "zip_code_prefix")
)

display(df_dim_vendedor.limit(5))

# COMMAND ----------

total = df_dim_vendedor.count()
sk_distintas = df_dim_vendedor.select("sk_vendedor").distinct().count()
seller_distintos = df_dim_vendedor.select("seller_id").distinct().count()

print(f"Total: {total}")
print(f"sk_vendedor distintas: {sk_distintas}")
print(f"seller_id distintos: {seller_distintos}")

assert total == sk_distintas == seller_distintos, "Inconsistência na Dim Vendedor"

(
    df_dim_vendedor.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.dim_vendedor")
)

print("Tabela olist_lakehouse.gold.dim_vendedor escrita com sucesso.")