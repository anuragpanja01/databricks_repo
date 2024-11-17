# Databricks notebook source
# MAGIC %md
# MAGIC **Driver and Constructor Standings Logic**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.functions import count, sum, desc, rank, when, col
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{gold_path}/race_results/")\
    .filter(f"file_date = '{file_date}'")\
    .select("race_year").distinct()\
    .collect()

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
print(race_results_list)

race_results_df = spark.read.format("delta").load(f"{gold_path}/race_results/")\
    .filter(col("race_year").isin(race_year_list))

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Driver Standings**

# COMMAND ----------

position_df = update_position(race_results_df)
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings_df = position_df\
                .groupBy("race_year", "driver_name","driver_nationality")\
                .agg(
                    sum("points").alias("total_points"),
                    count(when(col("position")== 1, True)).alias("wins")
                ).orderBy(desc("total_points"))
driver_standings_df = driver_standings_df.withColumn("total_points", col("total_points").cast("double"))
driver_standings_df = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))
display(driver_standings_df)
driver_standings_df.printSchema()
merge_condition = "target.race_year = source.race_year AND target.driver_name = source.driver_name"
perform_mergeOperation(driver_standings_df, "driver_name", "race_year", "formula1_gold", "driver_standings", gold_path, merge_condition)


# COMMAND ----------

# %sql
# -- DROP TABLE IF EXISTS formula1_gold.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE formula1_gold.driver_standings

# COMMAND ----------

# MAGIC %md
# MAGIC **Constructor Standings**

# COMMAND ----------

position_df = update_position(race_results_df)
constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructor_standings_df = position_df\
                .groupBy("race_year" ,"team")\
                .agg(
                    sum("points").alias("total_points"),
                    count(when(col("position")== 1, True)).alias("wins")
                ).orderBy(desc("total_points"))
        
constructor_standings_df = constructor_standings_df .withColumn("rank", rank().over(constructorRankSpec))
display(constructor_standings_df)

merge_condition = "target.race_year = source.race_year AND target.team = source.team"
perform_mergeOperation(constructor_standings_df, "team", "race_year", "formula1_gold", "constructor_standings", gold_path, merge_condition)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS formula1_gold.constructor_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE formula1_gold.constructor_standings;
