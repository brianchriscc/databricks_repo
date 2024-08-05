# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG chris_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME chris_catalog.brian_db.DLT_vol_landing

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/chris_catalog/brian_db/dlt_vol_landing/invoices

# COMMAND ----------

dbutils.fs.cp\
    ('abfss://externaldata@chrispalavilaistorage.dfs.core.windows.net/CH9-Data Files/dataset_ch9/invoices_2022.csv',
     '/Volumes/chris_catalog/brian_db/dlt_vol_landing/invoices/')
