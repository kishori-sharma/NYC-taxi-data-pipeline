# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver** **Layer**

# COMMAND ----------

df = spark.table("bronze_taxi_trips")
display(df)
df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter** **Null** **Values**

# COMMAND ----------

from pyspark.sql.functions import col
NotNullVendorIds = df.filter(col("VendorID").isNotNull())
NonNullPickupAndDrop = NotNullVendorIds.filter((col("tpep_pickup_datetime").isNotNull()) & (col("tpep_dropoff_datetime").isNotNull()))


# COMMAND ----------

silver_table= NonNullPickupAndDrop.filter(
    (col("VendorID").isNotNull()) &
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    (col("passenger_count").isNotNull()) &
    (col("passenger_count")>0) &
    (col("trip_distance").isNotNull()) &
    (col("trip_distance")>0) &
    (col("fare_amount").isNotNull()) &
    (col("fare_amount")>0) &
    (col("RatecodeID").isNotNull()) &
    (col("store_and_fwd_flag").isNotNull()) &
    (col("PULocationID").isNotNull()) &
    (col("DOLocationID").isNotNull()) &
    (col("extra").isNotNull()) &
    (col("mta_tax").isNotNull()) &
    (col("tip_amount").isNotNull()) &
    (col("tolls_amount").isNotNull()) &
    (col("improvement_surcharge").isNotNull()) &
    (col("total_amount").isNotNull()) &
    (col("congestion_surcharge").isNotNull()) &
    (col("airport_fee").isNotNull())
     )

# COMMAND ----------

# MAGIC %md
# MAGIC **Add** **Col**

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col, round

TripMin = silver_table.withColumn(
    "trip_duration_min",
    round(
        (
            unix_timestamp(col("tpep_dropoff_datetime")) -
            unix_timestamp(col("tpep_pickup_datetime"))
        ) / 60,
        2
    )
)

# COMMAND ----------

display(TripMin)

# COMMAND ----------

from pyspark.sql.functions import dayofweek, col

PicDay=TripMin.withColumn(
    "pickup_day",
    ((dayofweek(col("tpep_pickup_datetime"))))
)

# COMMAND ----------

display(PicDay)

# COMMAND ----------

PicHour = PicDay.withColumn(
    "trip_duration_hour",
    round(
    (
        unix_timestamp(col("tpep_dropoff_datetime")) -
        unix_timestamp(col("tpep_pickup_datetime"))
    ) / 3600
    )
)

# COMMAND ----------

display(PicHour)

# COMMAND ----------

from pyspark.sql.functions import when

TipPer = PicHour.withColumn(
    "tip_percentage",
    round(
    when(
        col("fare_amount") > 0,
        (col("tip_amount") / col("fare_amount")) * 100
    )
    )
)   

# COMMAND ----------

display(TipPer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Save** **table**

# COMMAND ----------

TipPer.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_taxi_trips")

# COMMAND ----------

display(spark.table("silver_taxi_trips"))