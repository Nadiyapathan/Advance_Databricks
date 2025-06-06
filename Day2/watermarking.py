# Databricks notebook source
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType

# COMMAND ----------


# Start Spark session
spark = SparkSession.builder.appName("WatermarkExample").getOrCreate()


# COMMAND ----------


# Sample data: mix of on-time and late events
data = [
    ("nadi1", datetime.now() - timedelta(minutes=2)),  # late
    ("nadi2", datetime.now() - timedelta(seconds=30)), # on-time
    ("nadi3", datetime.now()),                         # current
]


# COMMAND ----------

schema = StructType() \
    .add("user", StringType()) \
    .add("event_time", TimestampType())

# COMMAND ----------

df = spark.createDataFrame(data, schema)


# COMMAND ----------


# Write data to CSV to simulate a stream
df.write.mode("append").option("header", True).csv("/tmp/input_stream/")

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Pipeline with Watermark**

# COMMAND ----------

from pyspark.sql.functions import window

# Define schema
schema = StructType() \
    .add("user", StringType()) \
    .add("event_time", TimestampType())

# COMMAND ----------


# Read stream from the folder
stream_df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .option("maxFilesPerTrigger", 1) \
    .csv("/tmp/input_stream/")

# COMMAND ----------


# Apply watermarking to ignore events older than 1 minute
result = stream_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window("event_time", "1 minute"),
        "user"
    ).count()


# COMMAND ----------


# Write output to console
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()