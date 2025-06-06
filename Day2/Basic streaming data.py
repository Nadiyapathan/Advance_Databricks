# Databricks notebook source
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType

# COMMAND ----------


spark = SparkSession.builder.appName("WriteSampleCSV").getOrCreate()

# COMMAND ----------


# Create some sample data
data = [
    ("hello", datetime.now()),
    ("world", datetime.now()),
    ("hello", datetime.now()),
]

# COMMAND ----------

schema = StructType() \
    .add("word", StringType()) \
    .add("event_time", TimestampType())

# COMMAND ----------


df = spark.createDataFrame(data, schema)

# COMMAND ----------


# Write as CSV to simulate a stream (write one file at a time)
df.write.mode("append").option("header", True).csv("/tmp/stream_input")

# COMMAND ----------

# MAGIC %md
# MAGIC **Build the Structured Streaming Pipeline**

# COMMAND ----------

from pyspark.sql.functions import window


# COMMAND ----------

# Define schema
schema = StructType() \
    .add("word", StringType()) \
    .add("event_time", TimestampType())

# COMMAND ----------


# Read CSV files as a stream
stream_df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .option("maxFilesPerTrigger", 1) \
    .csv("/tmp/stream_input")



# COMMAND ----------


# Streaming transformation: count words per minute
result = stream_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute"), "word") \
    .count()

# COMMAND ----------


# Write to console
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()