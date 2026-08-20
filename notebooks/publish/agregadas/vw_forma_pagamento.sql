-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_forma_pagamento AS
SELECT
  payment_type,
  COUNT(*) AS total_pagamentos,
  ROUND(SUM(payment_value), 2) AS valor_total,
  ROUND(AVG(payment_installments), 1) AS media_parcelas
FROM olist_lakehouse.gold.fato_pagamentos
GROUP BY payment_type;
