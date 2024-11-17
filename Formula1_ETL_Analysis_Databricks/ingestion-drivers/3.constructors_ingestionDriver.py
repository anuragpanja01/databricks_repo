# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Constructors.JSON file from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-10-14")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = (spark.read \
               .schema(constructors_schema)\
               .json(f"{raw_path}/{file_date}/constructors.json"))             

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

col_list = constructors_df.columns
constructors_dfs= constructors_df.select(col_list)
constructors_dfs  = constructors_dfs.drop("url")


# COMMAND ----------

constructors_df_renamed = (constructors_dfs.withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("constructorRef","constructor_ref")\
                                    .withColumn("source",lit(data_source))\
                                    .withColumn("file_date",lit(file_date))                                 
                                    )
constructors_df_final = set_ingestion_date(constructors_df_renamed)                                 
display(constructors_df_final)

# COMMAND ----------

constructors_df_final.write.mode("overwrite").format("delta").saveAsTable("formula1_silver.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/constructors/"

# COMMAND ----------

dbutils.notebook.exit("Success")
