# Databricks notebook source
# MAGIC %sql
# MAGIC USE formula1_silver;

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

spark.sql(f""" 
          CREATE TABLE IF NOT EXISTS formula1_gold.calculated_race_results (
              race_year INT,
              team STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              race_name STRING,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA  
          """)

# COMMAND ----------

spark.sql(f""" 
          CREATE OR REPLACE TEMP VIEW race_results_updated
            AS
            SELECT races.race_year, constructors.name AS team, drivers.name AS driver_name, results.position, results.points,
                11 - results.position AS calculated_points
                FROM results
                JOIN drivers ON (results.driver_id = drivers.driver_id)
                JOIN constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN races ON (results.race_id = races.race_id)
                WHERE results.position <= 10
                AND results.file_date = '{file_date}';
      """)

# COMMAND ----------

spark.sql(f"""
          MERGE INTO formula1_gold.calculated_race_results tgt
            USING race_results_updated upd
            ON (upd.race_id = tgt.race_id AND upd.driver_id = tgt.driver_id )
                    WHEN MATCHED THEN 
                    UPDATE SET tgt.position = upd.position,
                            tgt.points = upd.points,
                            tgt.calculated_points = upd.calculated_points
                            tgt.updated_date = current_timestamp()
                    WHEN NOT MATCHED THEN
            INSERT (race_year,team,driver_id ,driver_name, race_id ,race_name ,position ,points, calculated_points,created_date) VALUES(race_year, team, driver_id, driver_name, race_id ,race_name , position ,points, calculated_points,current_timestamp())
            """)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_gold.calculated_race_results;
