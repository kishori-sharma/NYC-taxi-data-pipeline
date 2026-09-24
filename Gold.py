# Databricks notebook source
# MAGIC %md
# MAGIC **Gold Layer**

# COMMAND ----------

df = spark.table("silver_taxi_trips")
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggrigation**

# COMMAND ----------

from pyspark.sql.functions import sum, round

revenue_per_hour = df.groupBy("trip_duration_hour","VendorID"
).agg(round(sum("total_amount"), 2).alias("total_revenue")).orderBy("trip_duration_hour","VendorID"
)

display(revenue_per_hour)

# COMMAND ----------

# from pyspark.sql.functions import count

# count=df.groupBy("PULocationID") .agg(count("*").alias("total_count")) .orderBy("PULocationID") \
#   .display(count)

# COMMAND ----------

df.count()

# COMMAND ----------

# total_trips_per_hour.write \
# .format("delta") \
# .mode("overwrite") \
# .saveAsTable("gold_taxi_trips_per_hour_1")

# COMMAND ----------

from pyspark.sql.functions import count

total_trips_per_hour = df.groupBy("trip_duration_hour") \
    .agg(count("*").alias("total_trips")) \
    .orderBy("trip_duration_hour")

display(total_trips_per_hour)

# COMMAND ----------

from pyspark.sql.functions import sum, round

revenue_per_day = df.groupBy("pickup_day","VendorID"
).agg( round(sum("total_amount"), 2).alias("total_revenue")
).orderBy("pickup_day","VendorID"
)

display(revenue_per_day)

# COMMAND ----------

from pyspark.sql.functions import sum

revenue_per_vendor = df.groupBy("VendorID") \
    .agg(sum("total_amount").alias("total_revenue_vendor"))

display(revenue_per_vendor)

# COMMAND ----------

from pyspark.sql.functions import col, count
top10_pickup = df.groupBy("PULocationID") \
    .agg(count("*").alias("total_trips")) \
    .orderBy(col("total_trips").desc()) \
    .limit(10)

display(top10_pickup)

# COMMAND ----------

from pyspark.sql.functions import col, avg
avg_tip = df.groupBy("payment_type") \
    .agg(avg("tip_percentage").alias("avg_tip_percentage"))

display(avg_tip)


# COMMAND ----------

# MAGIC %md
# MAGIC **Save**

# COMMAND ----------

revenue_per_day.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_taxi_trips_per_day")

# COMMAND ----------

revenue_per_hour.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_taxi_trips_per_hour")

# COMMAND ----------

display(spark.table("gold_taxi_trips_per_day"))

# COMMAND ----------

display(spark.table("gold_taxi_trips_per_hour"))