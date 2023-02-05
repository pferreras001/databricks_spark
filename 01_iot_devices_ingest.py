# Databricks notebook source
json_location = f"/devices_lakehouse_demo/json/processing_json/day03"

# COMMAND ----------

from pyspark.sql.types import *

json_schema = StructType([
    StructField("device_id",LongType(),True),
    StructField("device_type",StringType(),True),
    StructField("ip",StringType(),True),
    StructField("cca3",StringType(),True),
    StructField("cn",StringType(),True),
    StructField("temp",LongType(),True),
    StructField("signal",LongType(),True),
    StructField("battery_level",LongType(),True),
    StructField("c02_level",LongType(),True),
    StructField("timestamp",TimestampType(),True),
])

# COMMAND ----------

devices_df = spark.read.schema(json_schema).json(json_location)

# COMMAND ----------

username = "pferreras"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
devices = f"/devices_lakehouse_demo/devices"

# COMMAND ----------

import time
execution = "INGDEV01"
executed_in = "databricks"
description = "Deployment of bronze-silver-gold database structure using IoT devices dataset"
start_time = time.time()

# COMMAND ----------

dbutils.fs.rm(f"{devices}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_bronze
"""
)

(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices}/bronze")
)

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_bronze
    USING DELTA
    LOCATION "{devices}/bronze"    
"""
)

# COMMAND ----------

dbutils.fs.rm(f"{devices}/silver", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_silver
"""
)

devices_bronze_df = spark.read.format("delta").load(f"{devices}/bronze")

from pyspark.sql.functions import *
import datetime

devices_silver_df = devices_bronze_df.select(
    col("device_id").cast('int').alias("id"),
    col("device_type").cast('string'),
    col("cca3").cast('string'),
    col("cn").cast("string"),
    col("temp").cast('int'),
    col("battery_level").cast('int'),
    col('c02_level').cast('int'),
    col('timestamp').cast('timestamp'),
)

(
    devices_silver_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices}/silver")
)

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_silver
    USING DELTA
    LOCATION "{devices}/silver"
"""
)

# COMMAND ----------

devices_silver_df.display()

# COMMAND ----------

dbutils.fs.rm(f"{devices}/gold", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_gold
"""
)

devices_silver_df = spark.read.format("delta").load(f"{devices}/silver")

from pyspark.sql.functions import col, avg, max

devices_gold_df = (

    devices_silver_df
    .groupby("cca3")
    .agg(
        avg(col("temp")).alias("temp_avg"),
        avg(col("battery_level")).alias("battery_level_avg"),
    )
    .orderBy("cca3")
)

(
    devices_gold_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices}/gold")
)

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_gold
    USING DELTA
    LOCATION "{devices}/gold"
"""
)

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

columns = ["execution", "executed_in", "ex_time", "description"]
data = [(execution,executed_in,time.time()-start_time, description)]
exec_df = spark.createDataFrame(data, columns)
exec_df = exec_df.withColumn("ex_date",current_timestamp())

exec_df.write.mode("append").json("/exec_lakehouse/exec_db_02")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM devices_gold

# COMMAND ----------

dbutils.fs.rm(f"{devices}/gold", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_gold
"""
)

# COMMAND ----------

