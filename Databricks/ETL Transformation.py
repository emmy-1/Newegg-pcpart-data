# Databricks notebook source
display (dbutils.fs.ls("abfss://neweggfile@neweggdb.dfs.core.windows.net/"))

# COMMAND ----------

# Read the CSV file
df = spark.read.csv("abfss://neweggfile@neweggdb.dfs.core.windows.net/ScrapeDb/gpu.csv", header=True)

# Display the DataFrame
display(df)
