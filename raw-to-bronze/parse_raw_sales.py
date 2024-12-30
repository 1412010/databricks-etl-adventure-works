# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

SECRET_SCOPE_NAME = "azure-storage-secret-scope"
AZURE_KEY_NAME = "nda-databricks-service-prinicipal-secret"
STORAGE_ACCOUNT = "ndastorageaccount333"
CONTAINTER_NAME = "synapsecontainer"
APPLICATION_ID = "0e1cfa4d-20d6-481a-87fc-186d49f2edf6" # of service principal
DIRECTORY_ID = "40127cd4-45f3-49a3-b05d-315a43a9f033" # of service principal

service_credential = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key=AZURE_KEY_NAME)

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", APPLICATION_ID)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{DIRECTORY_ID}/oauth2/token")

# COMMAND ----------

path = f"bronze_aw/Sales/Customer"
# CONNECTION_URI = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/synapsecontainer/bronze_aw/Sales/Customer"
CONNECTION_URI = f"abfss://{CONTAINTER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"

print(CONNECTION_URI)
dbutils.fs.ls(CONNECTION_URI)

# COMMAND ----------

df = spark.read.parquet(CONNECTION_URI)
display(df)

# COMMAND ----------

CATALOG = "DEV"
SCHEMA = "bronze"
table_name = "Customer"

df.write.format("delta").mode("overwrite").saveAsTable(f".Volumes.{CATALOG}.{SCHEMA}.{table_name}")