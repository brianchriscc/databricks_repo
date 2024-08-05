# Databricks notebook source
import pandas as pd

data = {
    'Department': ['HR', 'Finance', 'IT', 'HR', 'Finance', 'IT', 'IT'],
    'Employee': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace'],
    'Salary': [70000, 80000, 75000, 72000, 85000, 78000, 77000],
    'Years': [5, 6, 4, 7, 5, 3, 6]
}

df = pd.DataFrame(data)
df2 = df.groupby('Department').groups
print(df2)


# COMMAND ----------

df
df_check = df.groupby('Department')['Salary'].max()

# COMMAND ----------

df

# COMMAND ----------

spark_df =spark.createDataFrame(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE brian_db

# COMMAND ----------

spark_df.write.format('delta').mode('overwrite').saveAsTable('pandas_table')

# COMMAND ----------

df_pad = spark.sql('SELECT * FROM chris_catalog.brian_db.pandas_table')


# COMMAND ----------

df_pad.freqItems([`Departmemnt`,`Years`])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT *, DENSE_RANK() OVER (PARTITION BY Department ORDER BY Salary DESC) AS SALARY_DETAILS
# MAGIC FROM chris_catalog.brian_db.pandas_table )
# MAGIC WHERE SALARY_DETAILS = 1

# COMMAND ----------

class Body():
    @classmethod
    def details(cls):
        print("Welcome to body")
    @staticmethod
    def checker():
        print("checker activated with")
    def __init__(self, organs):
        self.x = organs
    def __str__(self):
        return f'the organ is {self.x}'

    def action(self,y):
        print(y)
        if self.x.lower() == 'heart':
            self.checker()
            return "It pumps blood "
        else:
            return f'{self.x} is a organ is your body'
    
    @staticmethod
    def tail():
        print("Thats it")

# COMMAND ----------

import random
random.seed(19)
a = random.randint(1,9)

# COMMAND ----------

from collections import namedtuple


person = namedtuple('persons', 'name age gender')
mani = person('maani', 25, 'female')

# COMMAND ----------

mani.personshow to crerwtaegive`

# COMMAND ----------

a = (1,2,3,8,9,7,4,5)
print(a[5])

# COMMAND ----------

,,,brian = Body('heArt')
b,,,rian.details()
brian.action("hi")

# COMMAND ----------



# COMMAND ----------

name = '''my name 

is brian'''
print(name)

# COMMAND ----------

def fn_check(*ar,**hwargs):
    print('hwargs'  + 'ar')
    print(ar)
    return True
a = {'name':'brian','lastname':'chris'}
b = [1,2,3,45]
fn_check(*b,**a)

# COMMAND ----------

from pyspark.sql import Row

# Create a DataFrame from a list of Row objects
data = [
    Row(id=1, name="Alice"),
    Row(id=2, name="Bob"),
    Row(id=3, name="Charlie")
]

df = spark.createDataFrame(data)


# COMMAND ----------

pip install databricks-koalas

# COMMAND ----------


import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session


# Create a PySpark DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
columns = ["name", "age"]
spark_df = spark.createDataFrame(data, columns)

# Convert PySpark DataFrame to Koalas DataFrame
koalas_df = spark_df.to_koalas()

# Perform operations using the pandas-like API
print(koalas_df)



# COMMAND ----------

df_rdd = df.rdd

# COMMAND ----------

def print_element(x):
    print(f"Element: {x}")


# COMMAND ----------

# Initialize Spark session

data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)

# Define a function to process each element
def process_element(x):
    return f"Processing element: {x}"

# Apply the map action and collect the results
processed_data = rdd.map(process_element).collect()

# Print the results
for element in processed_data:
    print(element)

# Stop Spark session

