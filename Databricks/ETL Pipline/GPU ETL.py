# Databricks notebook source
from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace

Dataframe = spark.read.csv('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/Bronz_layer/Gpu.csv', header=True)


def extract_number(df, column_name):
    pattern = r'\d+'  # Regex pattern to extract numeric values
    new_df = df.withColumn(column_name, regexp_extract(df[column_name], pattern, 0))
    return new_df

def extract_number2(df, column_name):
    pattern = r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)'  # Regex pattern to extract numeric values
    new_df = df.withColumn(column_name, regexp_extract(df[column_name], pattern, 1))
    new_df = new_df.withColumn(column_name, regexp_replace(new_df[column_name], ',', ''))
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
    extract_rating_no = extract_number(data, 'Ratings')
    extract_tips_no = extract_number2(extract_rating_no, 'Tips')
    extract_memory_no = extract_number(extract_tips_no, 'Memory')
    extract_PSU_no = extract_number(extract_memory_no, 'PSU')
    extract_Length_no = extract_number(extract_PSU_no, 'Length')
    return extract_Length_no

Result = Run_pipline('Gpu')
Result.coalesce(1).write.mode("overwrite").csv('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/DLT_Bronze_layer/', header=True)

# COMMAND ----------

display(Result)
