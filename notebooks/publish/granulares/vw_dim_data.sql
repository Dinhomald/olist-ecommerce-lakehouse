-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_dim_data AS
SELECT
  sk_data,
  data_completa,
  ano,
  mes,
  dia,
  trimestre,
  semana_do_ano,
  nome_mes,
  nome_dia_semana,
  fim_de_semana
FROM olist_lakehouse.gold.dim_data;
