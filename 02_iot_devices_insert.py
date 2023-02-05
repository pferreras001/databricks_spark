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

devices_df = spark.read.schema(json_schema).json("/devices_lakehouse_demo/json/all_json/devices-1.json")

# COMMAND ----------

username = "pferreras"
spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse_{username}")
spark.sql(f"USE lakehouse_{username}")
devices_insert = f"/devices_lakehouse_demo/devices_insert"

# COMMAND ----------

dbutils.fs.rm(f"{devices_insert}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_insert_bronze
"""
)

(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_insert}/bronze")
)

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_insert_bronze
    USING DELTA
    LOCATION "{devices_insert}/bronze"    
"""
)

# COMMAND ----------

import time
execution = "INSDEV02"
executed_in = "databricks"
description = "Vulk INSERT into IoT devices table using pyspark dataframes"
start_time = time.time()

# COMMAND ----------

from pyspark.sql.functions import *

devices_df = spark.read.format("delta").load(f"{devices_insert}/bronze")

for i in range(20):
    new_device = spark.createDataFrame([
            (99999, 'sensor_ipdad', '68.161.225.145', 'EUS', 'Euskal Herria', 22, 22, 8, 917),  # create your data here, be consistent in the types.
            ],
        ["device_id", "device_type", "ip", "cca3", "cn", "temp", "signal", "battery_level", "c02_level,timestamp"]  # add your column names here
    )
    new_device = new_device.withColumn("timestamp",current_timestamp())
    devices_df = devices_df.union(new_device)

# COMMAND ----------

(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_insert}/bronze")
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

dbutils.fs.rm(f"{devices_insert}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_insert_bronze
"""
)