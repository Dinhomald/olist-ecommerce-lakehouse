-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_fato_vendas AS
SELECT
  order_id,
  order_item_id,
  sk_produto,
  sk_cliente,
  sk_vendedor,
  sk_data,
  price AS valor_produto,
  freight_value AS valor_frete
FROM olist_lakehouse.gold.fato_vendas;
