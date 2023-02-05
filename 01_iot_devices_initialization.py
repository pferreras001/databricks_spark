# Databricks notebook source
import time
execution = "INIDEV01"
executed_in = "databricks"
description = "Initialization of conexion to bucket creation of database, first table, and initial dataframe"
start_time = time.time()

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

username = "pferreras"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
devices = f"/devices_lakehouse_demo/devices"

# COMMAND ----------

json_location = f"/devices_lakehouse_demo/json/processing_json/day03"
devices_df = spark.read.schema(json_schema).json(json_location)

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

from pyspark.sql.functions import *
import datetime

columns = ["execution", "executed_in", "ex_time", "description"]
data = [(execution,executed_in,time.time()-start_time, description)]
exec_df = spark.createDataFrame(data, columns)
exec_df = exec_df.withColumn("ex_date",current_timestamp())

exec_df.write.mode("append").json("/exec_lakehouse/exec_db_02")

# COMMAND ----------

dbutils.fs.rm(f"{devices}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_bronze
"""
)