# Databricks notebook source
mountPointList = ["demo","fraw","fgold","fsilver"]
dls_accname = "dlsformula1dev" 

# COMMAND ----------

 def setConfigurations():
      configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientId"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientSecret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='formula1-scope',key='formula1-app-tenantId')}/oauth2/token"}
      print("ADLS Access Successful\n")
      return configs



# COMMAND ----------

def mountContainers():
    configs = setConfigurations()
    for container in mountPointList:
        try:
            dbutils.fs.mount(
                source=f"abfss://{container}@{dls_accname}.dfs.core.windows.net/",
                mount_point=f"/mnt/{container}",
                extra_configs = configs
            )
            print(f"Mounting done for {container}\n")
        except Exception as e:
            print(f"Error mounting {container}: {str(e)}\n")


# COMMAND ----------

mountContainers()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/fraw/"))
