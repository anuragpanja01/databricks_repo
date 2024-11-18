-- Databricks notebook source
SELECT *
  FROM formula1_gold.calculated_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Dominant Drivers for the decade 2011 - 2021**

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
 GROUP BY driver_name
 HAVING total_races > 50
 ORDER BY avg_points DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Dominant Drivers for the decade 2001 - 2010**

-- COMMAND ----------

SELECT driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
 GROUP BY driver_name
 HAVING total_races > 50
 ORDER BY avg_points DESC;
