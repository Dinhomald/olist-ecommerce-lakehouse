-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_top_categorias AS
SELECT
  dp.product_category_name AS categoria,
  ROUND(SUM(fv.price), 2) AS receita_total,
  COUNT(*) AS itens_vendidos
FROM olist_lakehouse.gold.fato_vendas fv
JOIN olist_lakehouse.gold.dim_produto dp ON fv.sk_produto = dp.sk_produto
GROUP BY dp.product_category_name;
