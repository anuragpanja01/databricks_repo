-- Databricks notebook source
-- MAGIC %run "../shortcuts/configurations"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_silver
LOCATION "/mnt/fsilver";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_gold
LOCATION "/mnt/fgold";

-- COMMAND ----------

DESC DATABASE formula1_gold;
