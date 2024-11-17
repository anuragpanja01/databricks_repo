# Databricks notebook source
# MAGIC %md
# MAGIC **Ingestion Driver for Drivers.JSON nested json file from Raw Layer**

# COMMAND ----------

# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

dbutils.widgets.text("source","")
data_source = dbutils.widgets.get("source")

dbutils.widgets.text("file_date","2021-10-14")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)
                                   ])
drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema, True),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

drivers_df = (spark.read \
               .schema(drivers_schema)\
               .json(f"{raw_path}/{file_date}/drivers.json"))             

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

col_list = drivers_df.columns
drivers_dfs= drivers_df.select(col_list)
drivers_dfs  = drivers_dfs.drop("url")


# COMMAND ----------

drivers_df_renamed = (drivers_dfs.withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_ref")
                                    .withColumn("source",lit(data_source))\
                                    .withColumn("file_date",lit(file_date))                                   
                                    )
drivers_df_renamed = (drivers_df_renamed.withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname"))))

drivers_df_final = set_ingestion_date(drivers_df_renamed)                                                                     
display(drivers_df_final)

# COMMAND ----------

drivers_df_final.write.mode("overwrite").format("delta").saveAsTable("formula1_silver.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/fsilver/drivers/"

# COMMAND ----------

dbutils.notebook.exit("Success")
