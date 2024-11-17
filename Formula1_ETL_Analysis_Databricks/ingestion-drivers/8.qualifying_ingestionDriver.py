# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Qualifying.JSON multi line multiple json files from Raw Layer**

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

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

qualify_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)  
])

# COMMAND ----------

qualifying_df = (spark.read \
               .schema(qualify_schema)\
                .option("multiLine", True)\
               .json(f"{raw_path}/{file_date}/qualifying/qualifying_split*.json"))             

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

qualifying_df_renamed = (qualifying_df.withColumnRenamed("qualifyId","qualify_id")
                            .withColumnRenamed("raceId","race_id")
                            .withColumnRenamed("driverId","driver_id")
                            .withColumnRenamed("constructorId","constructor_id")
                            .withColumn("source",lit(data_source))                             
                                    )
qualifying_df_final = set_ingestion_date(qualifying_df_renamed)
                                                                    
display(qualifying_df_final)

# COMMAND ----------

merge_condition = "target.qualify_id = source.qualify_id"
perform_mergeOperation(qualifying_df_final, "qualify_id", "race_id", "formula1_silver", "qualifying", silver_path, merge_condition)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/qualifying/"

# COMMAND ----------

dbutils.notebook.exit("Success")
