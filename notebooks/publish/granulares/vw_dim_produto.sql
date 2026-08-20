-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_dim_produto AS
SELECT
  sk_produto,
  product_id,
  product_category_name AS categoria,
  product_weight_g AS peso_gramas,
  product_length_cm AS comprimento_cm,
  product_height_cm AS altura_cm,
  product_width_cm AS largura_cm,
  data_inicio_vigencia,
  data_fim_vigencia,
  flag_vigente
FROM olist_lakehouse.gold.dim_produto;
