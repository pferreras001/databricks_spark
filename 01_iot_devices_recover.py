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
devices_recover = f"/devices_lakehouse_demo/devices_recover"

# COMMAND ----------

dbutils.fs.rm(f"{devices_recover}/bronze", recurse=True)
 
spark.sql(
    f"""
DROP TABLE IF EXISTS devices_recover_bronze
"""
)
 
(
    devices_df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_recover}/bronze")
)
 
spark.sql(
f"""
    CREATE TABLE IF NOT EXISTS devices_recover_bronze
    USING DELTA
    LOCATION "{devices_recover}/bronze"    
"""
)

# COMMAND ----------

from pyspark.sql.functions import when

df = spark.read.load(f"{devices_recover}/bronze")
df = df.withColumn("device_type", when(df.device_type == "sensor-ipad","s-ip").when(df.device_type == "sensor-igauge","s-ig").when(df.device_type == "sensor-istick","s-is").when(df.device_type == "sensor-inest","s-in"))

# COMMAND ----------

(
    df.write.mode("overwrite")
    .format("delta")
    .save(f"{devices_recover}/bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM devices_recover_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM devices_recover_bronze
# MAGIC VERSION AS OF 0

# COMMAND ----------

import time
execution = "RECDEV01"
executed_in = "databricks"
description = "Recover an updated table to it's first state"
start_time = time.time()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC RESTORE TABLE devices_recover_bronze TO VERSION AS OF 0

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
# MAGIC SELECT * FROM devices_recover_bronze

# COMMAND ----------

dbutils.fs.rm(f"{devices_recover}/bronze", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS devices_recover_bronze
"""
)