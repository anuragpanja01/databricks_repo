-- Databricks notebook source
-- DBTITLE 1,itle
-- MAGIC %python
-- MAGIC html = """<h1 style= "color:Black;text-align:center;font-family:Ariel">Formula 1 Dominant Drivers Report</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_dominant_drivers 
AS
SELECT driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER (ORDER BY  AVG(calculated_points) DESC) AS driver_rank
  FROM formula1_gold.calculated_race_results
 GROUP BY driver_name
 HAVING total_races >= 50
 ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10

-- COMMAND ----------

SELECT race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
 GROUP BY race_year,driver_name
 ORDER BY race_year ASC,avg_points DESC, total_points DESC;

-- COMMAND ----------

SELECT race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
 GROUP BY race_year,driver_name
 ORDER BY race_year ASC,avg_points DESC, total_points DESC;

-- COMMAND ----------

SELECT race_year,
       driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM vw_dominant_drivers WHERE driver_rank <= 10)
 GROUP BY race_year,driver_name
 ORDER BY race_year ASC,avg_points DESC, total_points DESC;

-- COMMAND ----------


