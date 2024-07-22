# Databricks notebook source
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace

Dataframe = spark.read.csv('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Bronz_layer/Gpu.csv', header=True)


def extract_number(df, column_name):
    pattern = r'\d+'  # Regex pattern to extract numeric values
    new_df = df.withColumn(column_name, regexp_extract(df[column_name], pattern, 0))
    return new_df


def Create_Database(DataFrame, Name):
    SQLDB = DataFrame.createOrReplaceTempView(Name)
    return SQLDB

Database = Create_Database(Dataframe, 'Gpu')

def Run_pipline(table_name):
    query = f"""
        WITH gpu_cte AS (
            SELECT Title, GPU, Memory, `Suggested PSU` AS PSU, `Length`, Ratings, Price, `Image URL`, Tips,
            ROW_NUMBER() OVER (PARTITION BY Title ORDER BY (SELECT NULL)) AS rn
            FROM {table_name}
        )
        SELECT DISTINCT Title, GPU, Memory, PSU, `Length`, Ratings, Price, `Image URL`, Tips
        FROM gpu_cte
        WHERE rn = 1
    """
    data = spark.sql(query)
    Data2 = extract_number(data, 'Ratings')
    return Data2

Result = Run_pipline('Gpu')
display(Result)



    

