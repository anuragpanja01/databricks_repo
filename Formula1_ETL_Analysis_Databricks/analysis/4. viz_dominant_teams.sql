-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style= "color:Black;text-align:center;font-family:Ariel">Formula 1 Dominant Teams Report</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_dominant_teams 
AS
SELECT team,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK() OVER (ORDER BY  AVG(calculated_points) DESC) AS team_rank
  FROM formula1_gold.calculated_race_results
 GROUP BY team
 HAVING total_races >= 100
 ORDER BY avg_points DESC;

-- COMMAND ----------

SELECT * FROM vw_dominant_teams;

-- COMMAND ----------

SELECT race_year,
       team,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE team IN (SELECT team FROM vw_dominant_teams WHERE team_rank <= 5)
 GROUP BY race_year,team
 ORDER BY race_year ASC,avg_points DESC, total_points DESC;

-- COMMAND ----------

SELECT race_year,
       team,
       COUNT(1) as total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM formula1_gold.calculated_race_results
  WHERE team IN (SELECT team FROM vw_dominant_teams WHERE team_rank <= 5)
 GROUP BY race_year,team
 ORDER BY race_year ASC,avg_points DESC, total_points DESC;
