# Databricks notebook source
# # Mounting code
# dbutils.fs.mount(
#   source="wasbs://input@storageaccountprm.blob.core.windows.net/",
#   mount_point="/mnt/my-mount",
#   extra_configs={
#     "fs.azure.account.key.storageaccountprm.blob.core.windows.net": "tV28EEP1WZotDVi4Vhmz7SGzkCR+BEDyastOuyo4IwihnqQE6bWG5E+/7xc/jCDx4sgrcy3PPQ7K+AStIB6gOw=="
#   }
# )

# COMMAND ----------

dbutils.widgets.text("filename", "sample.csv", "Destination File Name")
destination_filename = dbutils.widgets.get("filename")

file_path = f'dbfs:/mnt/my-mount/{destination_filename}'  # Using f-string formatting

df = spark.read.options(header=True).csv(file_path)

# COMMAND ----------

dbutils.fs.ls("/mnt/my-mount")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

df = df.withColumn("Parameterisation", lit("PRM"))
df_writer = df.write.partitionBy("Country")
df_writer.format("csv").option("header", "true").mode("overwrite").save('dbfs:/mnt/my-mount/sample_out.csv')

# COMMAND ----------


