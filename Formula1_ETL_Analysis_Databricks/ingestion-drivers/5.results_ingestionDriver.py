# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Results.JSON file from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

result_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = (spark.read \
               .schema(result_schema)\
               .json(f"{raw_path}/{file_date}/results.json"))             

# COMMAND ----------

results_dfs = results_df.drop("statusId")
results_df_renamed = (
    results_dfs.withColumnRenamed("resultId", "result_id")
               .withColumnRenamed("raceId", "race_id")
               .withColumnRenamed("driverId", "driver_id")
               .withColumnRenamed("constructorId", "constructor_id")
               .withColumnRenamed("positionText", "position_text")
               .withColumnRenamed("positionOrder", "position_order")
               .withColumnRenamed("fastestLap", "fastest_lap")
               .withColumnRenamed("fastestLapTime", "fastest_lap_time")
               .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
               .withColumn("source", lit(data_source))
               .withColumn("file_date", lit(file_date))
)

results_df_id = set_ingestion_date(results_df_renamed)
results_df_arranged = rearrange_columns(results_df_id,"race_id")
results_df_dedup = results_df_arranged.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_condition = "target.result_id = source.result_id AND target.race_id = source.race_id"
perform_mergeOperation(results_df_dedup, "result_id", "race_id", "formula1_silver", "results", silver_path, merge_condition)

# COMMAND ----------

# perform_incremental(results_df_arranged,"race_id","formula1_silver.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/results/"

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT race_id, COUNT(1) 
# MAGIC   FROM formula1_silver.results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS formula1_silver.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS formula1_gold.race_results;
# MAGIC
