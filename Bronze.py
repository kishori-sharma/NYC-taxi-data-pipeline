# Databricks notebook source
# MAGIC %md
# MAGIC **Bronze** **layer**

# COMMAND ----------

# from datetime import date
# message = f"/Volumes/workspace/default/nyc1/yellow_tripdata_{date.today():%y-%m}.parquet"

# df = spark.read.parquet(message)
# display(df)
# df.printSchema()


# COMMAND ----------

# display(message)

# COMMAND ----------

df = spark.read.parquet("/Volumes/workspace/default/nyc1/yellow_tripdata_2023-01.parquet")
display(df)
df.printSchema()

df.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("bronze_taxi_trips")
from pyspark.sql.functions import col