-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_fato_reviews AS
SELECT
  review_id,
  order_id,
  sk_cliente,
  sk_data,
  review_score AS nota,
  review_comment_title AS titulo_comentario,
  review_comment_message AS comentario
FROM olist_lakehouse.gold.fato_reviews;
