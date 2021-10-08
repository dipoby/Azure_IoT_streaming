# Databricks notebook source
# Create widgets for storage account name and key
# for manual entering - not used

#dbutils.widgets.text("accountName", "", "Account Name")
#dbutils.widgets.text("accountKey", "", "Account Key")


# COMMAND ----------

# Get values entered into widgets
#accountName = dbutils.widgets.get("accountName")
#accountKey = dbutils.secrets.get(scope = "databricks-scope", key = "work")
container = "dv-papko"

spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(accountName), accountKey)




# COMMAND ----------

# Mount the blob storage account at /mnt/smartmeters. 
#This assumes your container name is smartmeters, and you have a folder named smartmeters within that container, as specified in the exercises above.
if not any(mount.mountPoint == '/mnt/smartmeters-iot' for mount in dbutils.fs.mounts()): 
  dbutils.fs.mount(
   source = "wasbs://{0}@{1}.blob.core.windows.net/dv-papko".format(container, accountName),
   mount_point = "/mnt/smartmeters-iot/",
   extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(accountName): accountKey}
  )



# COMMAND ----------

# Inspect the file structure
display(dbutils.fs.ls("/mnt/smartmeters-iot/"))

#Note: Mounting Azure Blob storage directly to DBFS allows you to access files as if they were on the local file system. Once your blob storage account is mounted, you can access them with Databricks #Utilities, dbutils.fs commands.


# COMMAND ----------

# Create a Dataframe containing data from all the files in blob storage, regardless of the folder they are located within.
df = spark.read.options(header='true', inferSchema='true').csv("dbfs:/mnt/smartmeters-iot/smartmeter-iot/*/*/*/*/*.csv",header=True)
print(df.dtypes)

#!!!Note: In some rare cases, you may receive an error that the dbfs:/mnt/smartmeters-iot/*/*/*.csv path is incorrect. If this happens, change the path in the cell to the following: #dbfs:/mnt/smartmeters/*/*/*/*/*.csv


# COMMAND ----------

df.show(10)

# COMMAND ----------

#Now, you can save the Dataframe to a global table in Databricks. This will make the table accessible to all users and clusters in your Databricks workspace
df.write.mode("overwrite").saveAsTable("SmartMetersN")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, COUNT(*) AS count, AVG(temp) AS averageTemp FROM SmartMeters GROUP BY id ORDER BY id

# COMMAND ----------

# Query the table to create a Dataframe containing the summary
summary = spark.sql("SELECT id, COUNT(*) AS count, AVG(temp) AS averageTemp FROM SmartMeters GROUP BY id ORDER BY id")

# Save the new pre-computed table
summary.write.mode("overwrite").saveAsTable("DeviceSummary")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DeviceSummary
