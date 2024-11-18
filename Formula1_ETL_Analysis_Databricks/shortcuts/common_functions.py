# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import functions as F

# COMMAND ----------

def set_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def update_position(spark_df):
    return spark_df.withColumn("position", 
                               F.when(F.col("position") == 1, 1)
                               .otherwise(0))

# COMMAND ----------

def rearrange_columns(spark_df, partition_col):
    column_list = []
    column_names = spark_df.schema.names
    for column in column_names:
        if column != partition_col:
            column_list.append(column)
    column_list.append(partition_col)
    print(column_list)
    output_df = spark_df.select(column_list)
    return output_df

# COMMAND ----------

def perform_incremental(spark_df, incremental_col, table_name):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    partition_list = [row[incremental_col] for row in spark_df.select(incremental_col).distinct().collect()]
    
    if spark._jsparkSession.catalog().tableExists(table_name):
        existing_partitions = [row[incremental_col] for row in spark.sql(f"SELECT DISTINCT {incremental_col} FROM {table_name}").collect()]
        
        for partition_col in partition_list:
            if partition_col in existing_partitions:
                spark_df.filter(F.col(incremental_col) == partition_col).write.mode("overwrite").insertInto(f"{table_name}")
            else:
                spark_df.filter(F.col(incremental_col) == partition_col).write.mode("append").format("parquet").partitionBy(incremental_col).saveAsTable(f"{table_name}")
        
        if partition_list:
            for partition in partition_list:
                print(f"Data overwritten into partition: {partition}")
    else:
        spark_df.write.mode("overwrite").partitionBy(incremental_col).format("parquet").saveAsTable(f"{table_name}")
        if partition_list:
            for partition in partition_list:
                print(f"Data written as a new partition: {partition}")

# COMMAND ----------

def perform_mergeOperation(spark_df, primary_col, partition_col, db_name, table_name, folder_path, merge_condition):
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("target") \
            .merge(spark_df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        # Determine which partitions are affected
        updated_partitions = deltaTable.toDF().filter(col(primary_col).isin(
            [getattr(row, primary_col) for row in spark_df.select(primary_col).collect()]
        )).select(partition_col).distinct().collect()
        
        inserted_partitions = spark_df.filter(~col(primary_col).isin(
            [getattr(row, primary_col) for row in deltaTable.toDF().select(primary_col).collect()]
        )).select(partition_col).distinct().collect()
        
        print(f"Existing records are updated in the table: {db_name}.{table_name}")
        print(f"Updated partitions (with existing records modified): {[getattr(row, partition_col) for row in updated_partitions]}")
        print(f"Inserted partitions (with all new records): {[getattr(row, partition_col) for row in inserted_partitions]}")
    else:
        spark_df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")
        print(f"Data overwritten as a new table: {db_name}.{table_name}")
