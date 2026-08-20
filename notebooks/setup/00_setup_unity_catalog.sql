-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS olist_lakehouse
MANAGED LOCATION 'abfss://bronze@adlsolistdatalake.dfs.core.windows.net//';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.bronze
MANAGED LOCATION 'abfss://bronze@adlsolistdatalake.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.silver
MANAGED LOCATION 'abfss://silver@adlsolistdatalake.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.gold
MANAGED LOCATION 'abfss://gold@adlsolistdatalake.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS olist_lakehouse.publish;
