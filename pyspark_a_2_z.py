# Databricks notebook source
location_details = spark.sql('SHOW EXTERNAL LOCATIONS')
location_details = location_details.filter(location_details.name == 'data_location').select('url').collect()
mount_path = location_details[0]['url']


# COMMAND ----------

df_sales = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
    .load(f'{mount_path}/sales_records.csv')

# COMMAND ----------

spark.sql('USE DATABASE brian_database')

# COMMAND ----------

path = location_details = spark.sql('SHOW EXTERNAL LOCATIONS')

# COMMAND ----------

path.take(1)[0][1]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Preparation

# COMMAND ----------

text = "My name is Brian Chris and I am working for Tiger Analytics an Anlaytics company for Brian".split(" ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a word RDD

# COMMAND ----------

text_rdd = spark.sparkContext.parallelize(text)

# COMMAND ----------

data_rdd = text_rdd.collect()

# COMMAND ----------

data_rdd_distinct = text_rdd.distinct().collect()

# COMMAND ----------

def WordStart(word,letter):
    return word.startswith(letter)


B_only = text_rdd.filter(lambda x: WordStart(x.lower(),'b')).collect()

# COMMAND ----------

y = lambda y : y+5


