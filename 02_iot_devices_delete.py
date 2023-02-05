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
devices_delete = f"/devices_lakehouse_demo/devices_delete"

# COMMAND ----------

dbutils.fs.rm(f"{devices_delete}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_delete_bronze
"""
)

(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_delete}/bronze")
)

spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_delete_bronze
    USING DELTA
    LOCATION "{devices_delete}/bronze"    
"""
)

# COMMAND ----------

import time
execution = "DELDEV02"
executed_in = "databricks"
description = "Vulk DELETE into IoT devices table using pyspark dataframes"
start_time = time.time()

# COMMAND ----------

df = spark.read.load(f"{devices_delete}/bronze")

# COMMAND ----------

from pyspark.sql.functions import *

for i in range(20):
    df = df.filter(df.device_id!=i)

# COMMAND ----------

(
    df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_delete}/bronze")
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

dbutils.fs.rm(f"{devices_delete}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_delete_bronze
"""
)