# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Races.CSV file from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-10-14")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

races_schema = StructType(fields =[StructField("raceId", IntegerType(),False),
                                      StructField("year", IntegerType(),True),
                                      StructField("round", IntegerType(),True),
                                      StructField("circuitId", IntegerType(),True),
                                      StructField("name", StringType(),True),
                                      StructField("date", DateType(),True),
                                      StructField("time", StringType(),True),
                                      StructField("url", StringType(),True) 
                                      ])

# COMMAND ----------

races_df = (spark.read \
               .options(header=True) \
               .schema(races_schema)\
               .csv(f"{raw_path}/{file_date}/races.csv"))             

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

col_list = races_df.columns
races_dfs = races_df.select(col_list)
races_dfs  = races_df.drop("url")


# COMMAND ----------

races_df_renamed = (races_dfs.withColumnRenamed("raceId","race_id")\
                              .withColumnRenamed("circuitId", "circuit_id")\
                              .withColumnRenamed("year","race_year")\
                              .withColumn("source",lit(data_source))\
                              .withColumn("file_date",lit(file_date))                                    
                                    )
races_df_renamed = (races_df_renamed.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                                        )
races_df_added_df = set_ingestion_date(races_df_renamed)                                       
races_df_final = races_df_added_df.drop("date").drop("time")                                      
display(races_df_final)

# COMMAND ----------

races_df_final.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("formula1_silver.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/races/"

# COMMAND ----------

dbutils.notebook.exit("Success")
