# Databricks notebook source
from pyspark.sql.functions import (
    explode, sequence, to_date, col, date_format,
    year, month, dayofmonth, dayofweek, quarter, weekofyear
)

df_calendario = (
    spark.sql("SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) AS data_completa")
)

df_dim_data = (
    df_calendario
    .withColumn("sk_data", date_format(col("data_completa"), "yyyyMMdd").cast("int"))
    .withColumn("ano", year(col("data_completa")))
    .withColumn("mes", month(col("data_completa")))
    .withColumn("dia", dayofmonth(col("data_completa")))
    .withColumn("trimestre", quarter(col("data_completa")))
    .withColumn("semana_do_ano", weekofyear(col("data_completa")))
    .withColumn("dia_da_semana", dayofweek(col("data_completa")))
    .withColumn("nome_mes", date_format(col("data_completa"), "MMMM"))
    .withColumn("nome_dia_semana", date_format(col("data_completa"), "EEEE"))
    .withColumn("fim_de_semana", dayofweek(col("data_completa")).isin(1, 7))
)

print(f"Total de dias gerados: {df_dim_data.count()}")
df_dim_data.printSchema()
display(df_dim_data.limit(5))

# COMMAND ----------

# Validação: sem sk_data duplicada, contagem batendo com o range de dias
total = df_dim_data.count()
distintos = df_dim_data.select("sk_data").distinct().count()

print(f"Total: {total}")
print(f"sk_data distintas: {distintos}")

assert total == distintos, "sk_data duplicada — não deveria acontecer com sequence() de datas únicas"

(
    df_dim_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.dim_data")
)

print("Tabela olist_lakehouse.gold.dim_data escrita com sucesso.")