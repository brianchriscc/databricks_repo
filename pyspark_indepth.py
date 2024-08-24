# Databricks notebook source
from pyspark.sql.functions import col,when,lit,substr,split,sum,max,min

# COMMAND ----------

df_original = spark.read.format("csv").option("header", "true")\
    .option('inferSchema', 'true').load("dbfs:/FileStore/data/original.csv")


# COMMAND ----------

df_original = df_original.limit(100)

# COMMAND ----------

df_original.count()

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType,IntegerType

# Data
data = [
    Row(id=1, first_name='Brian', last_name='Shilburne', gender='Female', City='Nowa Ruda', 
        JobTitle='Assistant Professor', Salary='$57438.18', Latitude=50.5774075, Longitude=16.4967184)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("City", StringType(), True),
    StructField("JobTitle", StringType(), True),
    StructField("Salary", StringType(), True),  # Using DecimalType to handle currency
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True)
])

# Create DataFrame
df2 = spark.createDataFrame(data, schema)


# COMMAND ----------

df_original.display()

# COMMAND ----------

df_original_dropdiplicate = df_original.dropDuplicates()

# COMMAND ----------

df_original_sel = df_original.select('id','first_name','last_name','gender','City','JobTitle','Salary')
df_original_sel = df_original_sel.na.drop()

# COMMAND ----------

df_original_sel.display()


# COMMAND ----------

df_original_dropdiplicate.display()

# COMMAND ----------

df_original.head(5)

# COMMAND ----------

df_original.take(5)

# COMMAND ----------

for s in df_original.dtypes:
    print(s[0])

# COMMAND ----------

df_original = df_original.withColumn("City_updated",when(col('City').isNull(),"City to be added").otherwise(lit(col('City'))))

# COMMAND ----------

df_original = df_original.filter(col('JobTitle').isNotNull())

# COMMAND ----------

df_original = df_original.withColumn('clean_salary',(col('Salary')).substr(2,10000).cast('float'))

# COMMAND ----------

mean = df_original.groupBy().avg('clean_salary').toDF('Average_salary')

# COMMAND ----------

mean.take(1)[0][0]

# COMMAND ----------

latitude = df_original.filter(col('latitude').isNotNull()).select('latitude')

# COMMAND ----------

import numpy as np

# COMMAND ----------

median_lat = np.median(latitude.collect())
latitude_median = latitude.withColumn('median_lat', lit(median_lat))

# COMMAND ----------

latitude_median.display()

# COMMAND ----------

df_original = df_original.withColumn("new_column" ,split("City"," ").getItem(0))




# COMMAND ----------

df_original.select("new_column").display()

# COMMAND ----------

df_original.display()

# COMMAND ----------

df_agg_fn = df_original.groupBy("gender","City").agg(max(col('clean_salary')).alias("gender_wise_salary"))

# COMMAND ----------

df_original.write.saveAsTable("chris_catalog.brian_db.df_original")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,dense_rank() OVER(ORDER BY clean_salary DESC ) AS res_1 FROM chris_catalog.brian_db.df_original

# COMMAND ----------

o
