-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_satisfacao_categoria AS
SELECT
  dp.product_category_name AS categoria,
  ROUND(AVG(fr.review_score), 2) AS nota_media,
  COUNT(*) AS total_reviews
FROM olist_lakehouse.gold.fato_reviews fr
JOIN olist_lakehouse.gold.fato_vendas fv ON fr.order_id = fv.order_id
JOIN olist_lakehouse.gold.dim_produto dp ON fv.sk_produto = dp.sk_produto
GROUP BY dp.product_category_name;
