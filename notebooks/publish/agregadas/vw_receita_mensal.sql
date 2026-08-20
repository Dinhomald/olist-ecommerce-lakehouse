-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_receita_mensal AS
SELECT
  dd.ano,
  dd.mes,
  dd.nome_mes,
  ROUND(SUM(fv.price), 2) AS receita_produtos,
  ROUND(SUM(fv.freight_value), 2) AS receita_frete,
  COUNT(DISTINCT fv.order_id) AS total_pedidos,
  ROUND(SUM(fv.price) / COUNT(DISTINCT fv.order_id), 2) AS ticket_medio
FROM olist_lakehouse.gold.fato_vendas fv
JOIN olist_lakehouse.gold.dim_data dd ON fv.sk_data = dd.sk_data
GROUP BY dd.ano, dd.mes, dd.nome_mes;
