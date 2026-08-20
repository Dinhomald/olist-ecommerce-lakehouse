-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_dim_vendedor AS
SELECT
  sk_vendedor,
  seller_id,
  cidade,
  estado,
  zip_code_prefix AS cep_prefixo
FROM olist_lakehouse.gold.dim_vendedor;
