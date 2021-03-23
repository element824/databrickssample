# Databricks notebook source
# DBTITLE 1,Configure a storage account access key for authentication
spark.conf.set(
  "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net", 
  dbutils.secrets.get(scope="<scope-name", key="storage-account-access-key-name"))

# COMMAND ----------

# DBTITLE 1,Create a storage container
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,Write IoT Devices JSON
df.write.json("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2 filesystem
df2 = spark.read.json("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")
display(df2)