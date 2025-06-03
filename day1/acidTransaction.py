# Databricks notebook source
# MAGIC %md
# MAGIC STEP1:create  a Delta table
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# Start Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaACIDExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create initial DataFrame
data = [(1, "Ayesha", 25),
        (2, "nadi", 30),
        (3, "Ayaan", 35)]

df = spark.createDataFrame(data, ["id", "name", "age"])

# Save as Delta table
df.write.format("delta").mode("overwrite").save("/tmp/delta/people")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC step2:perfrom ACID Transactions

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/tmp/delta/people")

# Update age where name is nadi
deltaTable.update(
    condition="name = 'nadi'",
    set={"age": "40"}
)
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Insert (atomic+consistent)

# COMMAND ----------

new_data = [(4, "Jabivul", 28)]
df_new = spark.createDataFrame(new_data, ["id", "name", "age"])

df_new.write.format("delta").mode("append").save("/tmp/delta/people")


df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Delete ()

# COMMAND ----------

deltaTable.delete(condition="name = 'Jabivul'")


# COMMAND ----------

# MAGIC %md
# MAGIC step4:TimeTravel(Durability + isolation)

# COMMAND ----------

# Get history
deltaTable.history().show()

# Read previous version using time travel
df_old = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta/people")
df_old.show()
