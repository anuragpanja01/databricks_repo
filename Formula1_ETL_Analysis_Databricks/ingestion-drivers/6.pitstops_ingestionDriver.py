# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Pitstops.JSON multi line json file from Raw Layer**

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

pitstops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)  
])

# COMMAND ----------

 pitstops_df = (spark.read \
               .schema(pitstops_schema)\
                .option("multiLine", True)\
               .json(f"{raw_path}/{file_date}/pit_stops.json"))             

# COMMAND ----------

 pitstops_df.printSchema()

# COMMAND ----------

pitstops_df_renamed = (pitstops_df.withColumnRenamed("raceId","race_id")
                                .withColumnRenamed("driverId","driver_id")
                                .withColumn("source",lit(data_source)) \
                                .withColumn("file_date",lit(file_date))                          
                                    )
pitstops_df_final = set_ingestion_date(pitstops_df_renamed) 
display(pitstops_df_final)

# COMMAND ----------

merge_condition = "target.race_id = source.race_id AND target.driver_id = source.driver_id AND target.stop = source.stop"
perform_mergeOperation(pitstops_df_final, "driver_id", "race_id", "formula1_silver", "pitstops", silver_path, merge_condition)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/pitstops/"

# COMMAND ----------

dbutils.notebook.exit("Success")
