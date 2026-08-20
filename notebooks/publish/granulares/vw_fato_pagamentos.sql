-- Databricks notebook source
USE CATALOG olist_lakehouse;

-- COMMAND ----------

CREATE OR REPLACE VIEW olist_lakehouse.publish.vw_fato_pagamentos AS
SELECT
  order_id,
  payment_sequential,
  sk_cliente,
  sk_data,
  payment_type AS forma_pagamento,
  payment_installments AS parcelas,
  payment_value AS valor_pagamento
FROM olist_lakehouse.gold.fato_pagamentos;
