# Databricks notebook source
notebook_status = dbutils.notebook.run("1.circuits_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status = dbutils.notebook.run("2.races_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status= dbutils.notebook.run("3.constructors_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status= dbutils.notebook.run("4.drivers_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

dbutils.notebook.run("5.results_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status = dbutils.notebook.run("6.pitstops_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status = dbutils.notebook.run("7.laptimes_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)

# COMMAND ----------

notebook_status = dbutils.notebook.run("8.qualifying_ingestionDriver", 0, {"source":"ERGAST API", "file_date": "2021-04-18"})
print(notebook_status)
