-- Databricks notebook source
DROP DATABASE IF EXISTS formula1_silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_silver
LOCATION "/mnt/fsilver";

-- COMMAND ----------

DROP DATABASE IF EXISTS formula1_gold CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_gold
LOCATION "/mnt/fgold";
