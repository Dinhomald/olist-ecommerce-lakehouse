-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_top_vendedores AS
SELECT
  dv.seller_id,
  dv.estado,
  ROUND(SUM(fv.price), 2) AS receita_total,
  COUNT(*) AS itens_vendidos
FROM olist_lakehouse.gold.fato_vendas fv
JOIN olist_lakehouse.gold.dim_vendedor dv ON fv.sk_vendedor = dv.sk_vendedor
GROUP BY dv.seller_id, dv.estado;
