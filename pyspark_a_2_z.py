# Databricks notebook source
from pyspark.sql.functions import col,udf,expr,concat,lit,expr,concat_ws
from pyspark.sql.types import StringType,StructField,IntegerType,StructType,ArrayType,BooleanType,FloatType,DateType

# COMMAND ----------

location_details = spark.sql('SHOW EXTERNAL LOCATIONS')
location_details = location_details.filter(location_details.name == 'data_location').select('url').collect()
mount_path = location_details[0]['url']


# COMMAND ----------

df_sales = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
    .load(f'{mount_path}/sales_records.csv')

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
def start_withi(name):
    if name.lower().startswith('i'):
        return 'might_be_India'
    else:
        return name
# Define a UDF that takes a string and returns a string (for example, the same value in this case)
identity_udf = udf(start_withi)

# Apply the UDF to the 'Country' column and create a new column 'map_check'
df_sale = df_sales.withColumn("map_check", identity_udf('Country'))

# COMMAND ----------

df_sale.describe().display()

# COMMAND ----------

df_sale.select("Country","map_check").display()

# COMMAND ----------

from pyspark.sql.functions import expr

df_xpr_check = df_sale.withColumn("comp_country",concat(col("Country"),lit(" "),col('map_check')))

# COMMAND ----------

df_xpr_check.select("comp_country").display()

# COMMAND ----------

df_sale.filter(col("Country").startswith("I")).select("Country","map_check").display()

# COMMAND ----------

spark.sql('USE CATALOG brian_catalog')
spark.sql('USE DATABASE brian_database')

# COMMAND ----------

path = location_details = spark.sql('SHOW EXTERNAL LOCATIONS')

# COMMAND ----------

path =  path.take(1)[0][1]

# COMMAND ----------

tables = spark.sql('SHOW TABLES')
delete_tables = [tb for tb in tables.select('tableName').collect()]
for tb in delete_tables:
    spark.sql(f'DROP TABLE {tb.tableName}')
    print("Sucess")

# COMMAND ----------

from pyspark.sql.functions import col

# List of current column names in your DataFrame
current_columns = df_sales.columns

# Replace invalid characters in column names
# This example replaces spaces with underscores and removes other invalid characters
# You might need to adjust the logic based on your specific column names
new_columns = [c.replace(" ", "_").replace(",", "").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "") for c in current_columns]

# Rename the columns
df_sales_renamed = df_sales.select([col(c).alias(nc) for c, nc in zip(current_columns, new_columns)])

# Now you can attempt to save the DataFrame as a Delta table with the sanitized column names
table_name  = 'sales_records'
try:
    
    df_sales_renamed.write.format("delta").saveAsTable("brian_catalog.brian_database.sales_records")
except Exception as e:
    print("table already exists \n")
    print("Dropping table to create new")
    spark.sql(f'DROP TABLE {table_name}')
    df_sales_renamed.write.format("delta").saveAsTable("brian_catalog.brian_database.sales_records")
    print("Table created")
else:
    print("Table created")
    


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



# COMMAND ----------

# MAGIC %md
# MAGIC #### map and flatmap Transformation

# COMMAND ----------

num_list = [*range(1,21)]

num_rdd = spark.sparkContext.parallelize(num_list)
num_rdd.collect()

# COMMAND ----------

def even_checker(n):
    if n% 2 == 0:
        return (n , "Even")
    else:
        return (n,"odd")

# COMMAND ----------

# DBTITLE 1,Map()
num_rdd_map = num_rdd.map(lambda nst : (nst,'even') if nst %2 == 0 else (nst ,"odd"))
num_rdd_map.collect()


# COMMAND ----------

countries_list = [("India",1),('USA',2),["India",3]]
countries_RDD = spark.sparkContext.parallelize(countries_list)

# COMMAND ----------

# sorting the RDD with key country
sorted_country = countries_RDD.sortByKey().collect()

# COMMAND ----------

# sorting the RDD with key country
# There is  no direct method to so, We use map functionality to replcae the key and value and do so
#sorted_country_value_desc = countries_RDD.map(lambda x,y :(x ,y)).sortByKey(ascending=False).collect()

sorted_country_value_desc = countries_RDD.map(lambda x:(x[1],x[0])).sortByKey(ascending=False).collect()

# COMMAND ----------

# reduceByKey
reduceKey = countries_RDD.reduceByKey(lambda x,y : (x+y)).collect()

# COMMAND ----------

reduce_check = spark.sparkContext.parallelize(range(1,500)).reduce(lambda x,y: x if x>y else y)

# COMMAND ----------

value_list = spark.sparkContext.parallelize(range(1,500))

# COMMAND ----------

print(str(value_list.first()), "\n" + str(value_list.min()))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import numpy as np

# Define the days and generate random temperature values
days = [f"Day_{i}" for i in range(6)]
np.random.seed(0)  # For reproducibility
temperature_values = [59, 57.2, 53.6, 55.4, 51.8, 53.6]

# Ensure data is a flat list of values for each column
data = [[59, 57.2, 53.6, 55.4, 51.8, 53.6]]  # Tuple or list of values
data = spark.sparkContext.parallelize(data)


#Create a PySpark DataFrame with one row and multiple columns
columns = days
spark_df = spark.createDataFrame(data, columns)
#spark_df_mod = spark.createDataFrame(data_dd, columns)

# Display the PySpark DataFrame
display(spark_df)

# COMMAND ----------

def F_to_C(t):
    return (t - 32) * 5/9
    


# COMMAND ----------

data = [59, 57.2, 53.6, 55.4, 51.8, 53.6]  # Tuple or list of values
data = spark.sparkContext.parallelize(data)
modified_data = data.map(F_to_C)

# COMMAND ----------

modified_data.collect()

# COMMAND ----------

final = modified_data.filter(lambda x : x >= 13)

# COMMAND ----------

data2001List = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']

data2002List = ['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']

data2003List = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

# COMMAND ----------

data2001List_RDD  = spark.sparkContext.parallelize(data2001List)
data2002List_RDD = spark.sparkContext.parallelize(data2002List)
data2003List_RDD = spark.sparkContext.parallelize(data2003List)



# COMMAND ----------

data2001List_RDD.collect()

# COMMAND ----------

Result_rdd = data2001List_RDD.union(data2002List_RDD).union(data2003List_RDD)

# COMMAND ----------


Result_rdd.collect()
Result_rdd.count()

# COMMAND ----------

diff = Result_rdd = data2001List_RDD.intersection(data2002List_RDD)

# COMMAND ----------

diff.collect()

# COMMAND ----------

diff_1 = Result_rdd = data2001List_RDD.subtract(data2002List_RDD)
diff_1.collect()

# COMMAND ----------

from collections import Counter
Counter(data2001List)

# COMMAND ----------

# working with pyspark dataframes

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema_1 = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname",StringType(),True),
    StructField("Id",StringType(),True),
    StructField("Gender",StringType(),True),
    StructField("Salary",IntegerType(),True)
])

data_df = spark.createDataFrame(data,schema_1)


# COMMAND ----------

data_df.display()

# COMMAND ----------

load_path = f'{path}fire_incident/fire-incidents.csv'
fire_incident_df = spark.read.csv(load_path, header=True)

# COMMAND ----------

fire_incident_df.coalesce(1).write.format("parquet").mode("overwrite").save(f'{path}fire_incident/fire_parquet')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CURRENT DATABASE;

# COMMAND ----------

df_table = spark.table('brian_catalog.brian_database.sales_records')
df_table.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Reading JSON file

# COMMAND ----------

#abfss://data2@bcstore1103.dfs.core.windows.net/
location_detail = spark.sql("SHOW EXTERNAL LOCATIONS")

# COMMAND ----------

location_details = location_detail.filter(location_detail.name == 'json_location').select('url').collect()
location_details = location_details[0]['url']
location_details

# COMMAND ----------

person_schema = StructType([
StructField("id",StringType(),False),
StructField("first_name",StringType(),True),
StructField("last_name",StringType(),True),
StructField("fav_movies",ArrayType(StringType()),True),
StructField("salary",FloatType(),True),
StructField("image_url",StringType(),True),
StructField("date_of_birth",DateType(),True),
StructField("active",BooleanType(),True)])

json_file_df = spark.read.format('json').schema(person_schema).option('header', 'true').option('inferSchema', 'true')\
        .option("multiline",True).load(f'{location_details}/persons.json')

# COMMAND ----------

json_file_df.limit(2).display()
