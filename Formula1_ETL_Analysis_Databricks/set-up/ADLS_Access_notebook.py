# Databricks notebook source
# Accessing ADLS  using Service Principal
# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.dlsformula1dev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlsformula1dev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlsformula1dev.dfs.core.windows.net", dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientId"))
spark.conf.set("fs.azure.account.oauth2.client.secret.dlsformula1dev.dfs.core.windows.net", dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientSecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlsformula1dev.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenantId')}/oauth2/token")


# COMMAND ----------

# Accessing Secrets with dbutils.secrets

dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list(scope ="formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope",key="formula1-dl-SAS-token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dlsformula1dev.dfs.core.windows.net")
