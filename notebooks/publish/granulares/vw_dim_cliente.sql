# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG olist_lakehouse;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_dim_cliente AS
# MAGIC SELECT
# MAGIC   sk_cliente,
# MAGIC   customer_unique_id,
# MAGIC   cidade,
# MAGIC   estado,
# MAGIC   zip_code_prefix AS cep_prefixo
# MAGIC FROM olist_lakehouse.gold.dim_cliente;