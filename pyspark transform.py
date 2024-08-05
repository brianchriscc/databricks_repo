# Databricks notebook source
from pyspark.sql.functions import col,expr

# COMMAND ----------

fire_df = spark.read.csv("dbfs:/databricks-datasets/learning-spark-v2/sf-fire/", header=True, inferSchema=True) #sf-fire-calls.csv

# COMMAND ----------

df_fire = spark.sparkContext.parallelize(fire_df.select("Call Final Disposition"))

# COMMAND ----------

data = [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
columns = ["col1", "col2"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
cov_rec = df.cov('col1', 'col2')
df.display()

# COMMAND ----------

cov_rec

# COMMAND ----------

fire_df.display()

# COMMAND ----------



# COMMAND ----------

fire_df.cache()

# COMMAND ----------

fire_call_distinct_1 = fire_df.where("CallType IS NOT NULL")
fire_call_distinct_1 = fire_call_distinct_1.select((expr('CallType AS FINAL_CALLTYPE'))).distinct()
fire_call_distinct_10hgfdsa
.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

fire_df.groupby(col('CallType')).agg(max('Call Number').alias('First_call')).display()

# COMMAND ----------

fire_df.where('CallType IS NOT NULL').select('CallType').groupby('CallType').count().orderBy('count',ascending = False).display()

# COMMAND ----------

fire_df.where('CallType IS NOT NULL').SELECT('CallType')
