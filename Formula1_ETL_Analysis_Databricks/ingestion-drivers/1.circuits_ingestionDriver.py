# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Circuits.CSV file from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-10-14")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

circuits_schema = StructType(fields =[StructField("circuitId", IntegerType(),False),
                                      StructField("circuitRef", StringType(),True),
                                      StructField("name", StringType(),True),
                                      StructField("location", StringType(),True),
                                      StructField("country", StringType(),True),
                                      StructField("lat", DoubleType(),True),
                                      StructField("lng", DoubleType(),True),
                                      StructField("alt", IntegerType(),True),
                                      StructField("url", StringType(),True) 
                                      ])

# COMMAND ----------

circuits_df = (spark.read \
               .options(header=True) \
               .schema(circuits_schema)\
               .csv(f"{raw_path}/{file_date}/circuits.csv"))             

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

col_list = circuits_df.columns
circuits_dfs = circuits_df.select(col_list)
circuits_dfs  = circuits_dfs.drop("url")


# COMMAND ----------

circuits_df_renamed = (circuits_dfs.withColumnRenamed("circuitId","circuit_id")\
                                    .withColumnRenamed("circuitRef", "circuit_ref")\
                                    .withColumnRenamed("lat","latitude")\
                                    .withColumnRenamed("lng","longitude")\
                                    .withColumnRenamed("alt","altitude")\
                                    .withColumn("source",lit(data_source))\
                                    .withColumn("file_date",lit(file_date))
                                    )
circuits_df_final = set_ingestion_date(circuits_df_renamed)                                       
display(circuits_df_final)

# COMMAND ----------

circuits_df_final.write.mode("overwrite").format("delta").saveAsTable("formula1_silver.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/circuits/"

# COMMAND ----------

dbutils.notebook.exit("Success")
