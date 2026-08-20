# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

df_geo_silver = spark.table("olist_lakehouse.silver.geolocation")

# Janela particionada por prefixo de CEP, ordenada por lat/lng como critério
# determinístico de desempate (não há timestamp de captura na fonte).
window_spec = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
    col("geolocation_lat"), col("geolocation_lng")
)

df_dim_geografia = (
    df_geo_silver
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
    .withColumnRenamed("geolocation_lat", "latitude")
    .withColumnRenamed("geolocation_lng", "longitude")
    .withColumnRenamed("geolocation_city", "cidade")
    .withColumnRenamed("geolocation_state", "estado")
)

print(f"Total de prefixos de CEP únicos: {df_dim_geografia.count()}")
df_dim_geografia.printSchema()

# COMMAND ----------

total = df_dim_geografia.count()
distintos = df_dim_geografia.select("zip_code_prefix").distinct().count()

print(f"Total: {total}")
print(f"zip_code_prefix distintos: {distintos}")

assert total == distintos, "zip_code_prefix duplicado — window function falhou em algum caso"

(
    df_dim_geografia.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("olist_lakehouse.gold.dim_geografia")
)

print("Tabela olist_lakehouse.gold.dim_geografia escrita com sucesso.")