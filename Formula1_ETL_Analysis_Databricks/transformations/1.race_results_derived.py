# Databricks notebook source
# MAGIC %run "../shortcuts/configurations"

# COMMAND ----------

# MAGIC %run "../shortcuts/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import count, sum, desc, rank, col
from pyspark.sql.window import Window

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{silver_path}/circuits/")
circuits_df = circuits_df.withColumnRenamed("name","circuit_name")\
                         .withColumnRenamed("location","circuit_location")

races_df = spark.read.format("delta").load(f"{silver_path}/races/")
races_df = races_df.withColumnRenamed("name","race_name")\
                 .withColumnRenamed("race_timestamp", "race_date")


drivers_df = spark.read.format("delta").load(f"{silver_path}/drivers/")
drivers_df = drivers_df.withColumnRenamed("name","driver_name")\
                     .withColumnRenamed("number","driver_number")\
                     .withColumnRenamed("nationality","driver_nationality")

constructors_df = spark.read.format("delta").load(f"{silver_path}/constructors/")
constructors_df = constructors_df.withColumnRenamed("name","team")

results_df = spark.read.format("delta").load(f"{silver_path}/results/")\
            .filter(f"file_date = '{file_date}'")\
            .withColumnRenamed("time","race_time")\
            .withColumnRenamed("race_id", "result_race_id")\
            .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df["circuit_id"] ==  circuits_df["circuit_id"],"inner")
race_circuits_df = race_circuits_df.select(races_df.race_id,races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id,"inner")\
                        .join(drivers_df,results_df.driver_id == drivers_df.driver_id, "inner")\
                        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

final_df = race_results_df.select("result_race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date", )
final_df = final_df.withColumn("created_date",current_timestamp())\
                .withColumnRenamed("result_file_date", "file_date")\
                .withColumnRenamed("result_race_id", "race_id")

final_df = final_df.withColumn("race_date", col("race_date").cast("timestamp")) \
                   .withColumn("driver_number", col("driver_number").cast("int")) \
                   .withColumn("grid", col("grid").cast("int")) \
                   .withColumn("points", col("points").cast("float")) \
                   .withColumn("created_date", col("created_date").cast("timestamp")) \
                   .withColumn("race_id", col("race_id").cast("int"))

final_df.printSchema()


# COMMAND ----------

merge_condition = "target.race_id = source.race_id AND target.driver_name = source.driver_name"
perform_mergeOperation(final_df, "driver_name", "race_id", "formula1_gold", "race_results", gold_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC   FROM formula1_gold.race_results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

display(final_df.filter(col("race_id").isNull()))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE formula1_gold.race_results;

# COMMAND ----------

# display(final_df.filter("race_year = 2020 AND race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df["points"].desc()))

# COMMAND ----------

# # Group By function
# demo_df = final_df.filter("race_year = 2020") \
#     .groupBy("driver_name", "driver_number", "driver_nationality", "team") \
#     .agg(
#         count("race_name").alias("total_races"), 
#         sum("points").alias("total_points")
#     ) \
#     .orderBy(desc("total_points"))

# display(demo_df)

# COMMAND ----------

# # Window functions
# window_spec_driverRank = Window.partitionBy("race_year").orderBy(desc("total_points"))

# grouped_df = final_df.filter("race_year IN (2019, 2020)") \
#     .groupBy("driver_name","race_year") \
#     .agg(
#         count("race_name").alias("total_races"), 
#         sum("points").alias("total_points")
#     ) \
#     .orderBy(desc("total_points"))

# window_df = grouped_df.withColumn("rank", rank().over(window_spec_driverRank))
# display(window_df)
