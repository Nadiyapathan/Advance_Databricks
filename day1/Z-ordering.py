# Databricks notebook source
/FileStore/tables/customers.csv

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/customers.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show(20)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/customers.csv", header=True, inferSchema=True)
df_clean = df.toDF(*[c.strip().replace(" ", "_").replace("(", "").replace(")", "") for c in df.columns])


# COMMAND ----------

df_clean.write.format("delta").mode("overwrite").save("/delta/customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers;

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS customers")
spark.sql("""
CREATE TABLE customers
USING DELTA
LOCATION '/delta/customers'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers;
# MAGIC
# MAGIC CREATE TABLE customers
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/customers';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE customers
# MAGIC ZORDER BY (City); -- replace with your actual column name
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE customers
# MAGIC ZORDER BY (Date_Of_Birth);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers WHERE City = 'Newyork';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers;
# MAGIC