# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Laptimes multiple csv files from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

laptimes_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)  
])

# COMMAND ----------

laptimes_df = (spark.read \
               .schema(laptimes_schema)\
               .csv(f"{raw_path}/{file_date}/lap_times/lap_times_split*.csv"))             

# COMMAND ----------

laptimes_df.printSchema()

# COMMAND ----------

laptimes_df_renamed = (laptimes_df.withColumnRenamed("raceId","race_id")
                                .withColumnRenamed("driverId","driver_id")
                                .withColumn("source",lit(data_source))                            
                                    )
laptimes_df_final = set_ingestion_date(laptimes_df_renamed)
                                                                           
display(laptimes_df_final)

# COMMAND ----------

merge_condition = "target.race_id = source.race_id AND target.driver_id = source.driver_id AND target.lap = source.lap"
perform_mergeOperation(laptimes_df_final, "driver_id", "race_id", "formula1_silver", "lap_times", silver_path, merge_condition)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/pitstops/"

# COMMAND ----------

dbutils.notebook.exit("Success")
