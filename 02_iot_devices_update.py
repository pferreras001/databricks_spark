# Databricks notebook source
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

devices_df = spark.read.schema(json_schema).json("/devices_lakehouse_demo/json/all_json")

# COMMAND ----------

username = "pferreras"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
devices_update = f"/devices_lakehouse_demo/devices_update"

# COMMAND ----------

dbutils.fs.rm(f"{devices_update}/bronze", recurse=True)
 
spark.sql(
    f"""
DROP TABLE IF EXISTS devices_update_bronze
"""
)
 
(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_update}/bronze")
)
 
spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_update_bronze
    USING DELTA
    LOCATION "{devices_update}/bronze"    
"""
)

# COMMAND ----------

import time
execution = "UPDDEV02"
executed_in = "databricks"
description = "Vulk UPDATE into IoT devices table using pyspark daraframes"
start_time = time.time()

# COMMAND ----------

from pyspark.sql.functions import when

df = spark.read.load(f"{devices_update}/bronze")
df = df.withColumn("device_type", when(df.device_type == "sensor-ipad","s-ip").when(df.device_type == "sensor-igauge","s-ig").when(df.device_type == "sensor-istick","s-is").when(df.device_type == "sensor-inest","s-in"))

# COMMAND ----------

(
    df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_update}/bronze")
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

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_update_bronze
"""
)