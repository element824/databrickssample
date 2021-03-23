# Databricks notebook source
# DBTITLE 1,Configure passthrough
configs = { 
"fs.azure.account.auth.type": "CustomAccessToken",
"fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

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

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2
df2 = spark.read.json("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,Mount filesystem
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,List mount
dbutils.fs.ls("/mnt/<mount-name>") 

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from mount point
df2 = spark.read.json("/mnt/<mount-name>/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,Create a new database in the mounted filesystem
# MAGIC %sql
# MAGIC CREATE DATABASE <db-name>
# MAGIC LOCATION "/mnt/<mount-name>"

# COMMAND ----------

# DBTITLE 1,Create a table from the IoT Devices DataFrame
df.write.saveAsTable("<name")

# COMMAND ----------

# DBTITLE 1,Query the table
# MAGIC %sql
# MAGIC SELECT * FROM <name> LIMIT 10

# COMMAND ----------

# DBTITLE 1,Clean up table
# MAGIC %sql
# MAGIC DROP TABLE <name>

# COMMAND ----------

# DBTITLE 1,Unmount filesystem
dbutils.fs.unmount("/mnt/<mount-name>") 