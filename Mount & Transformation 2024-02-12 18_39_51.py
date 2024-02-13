# Databricks notebook source
# Mounting code
dbutils.fs.mount(
  source="wasbs://input@storageaccountprm.blob.core.windows.net/",
  mount_point="/mnt/my-mount",
  extra_configs={
    "fs.azure.account.key.storageaccountprm.blob.core.windows.net": "tV28EEP1WZotDVi4Vhmz7SGzkCR+BEDyastOuyo4IwihnqQE6bWG5E+/7xc/jCDx4sgrcy3PPQ7K+AStIB6gOw=="
  }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/my-mount")

# COMMAND ----------

df = spark.read.options(header=True).csv('dbfs:/mnt/my-mount/sample.csv')

# COMMAND ----------

from pyspark.sql.functions import currentDate

df = df.withColumn("currentDate", currentDate())
df_writer = df.write.partitionBy("currentDate")
df_writer.save('dbfs:/mnt/my-mount/sample_out.csv')

# COMMAND ----------


