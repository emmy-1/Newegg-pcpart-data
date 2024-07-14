# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import regexp_extract#
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Transformation Function

# COMMAND ----------

def split_colum(Newcolumn, oldcolumn, character):
    Newdata = df.withColumn(Newcolumn, split(df[oldcolumn], character).getItem(1))
    return Newdata

def extract_number(df, column_name):
    pattern = r'\d+'  # Regex pattern to extract numeric values
    new_df = df.withColumn(column_name, regexp_extract(df[column_name], pattern, 0))
    return new_df

def remove_character(df, column_name, character):
    newddb = df.withColumn(column_name, regexp_replace(df[column_name], character, ''))
    return newddb



# COMMAND ----------

# MAGIC %md
# MAGIC # Define Data Pipline

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

df = spark.read.csv("abfss://neweggfile@neweggdb.dfs.core.windows.net/ScrapeDb/gpu.csv", header=True)

def run_pipeline(dataset):
    # Splitting the column 'Model_No' based on 'Model #:' and creating a new column 'model_no'
    Remove_model = split_colum('Model_No', 'model_no', 'Model #:')

    # Extracting numeric values from the 'No_rating' column and converting it to IntegerType
    extractNo = extract_number(Remove_model, 'No_rating').withColumn('No_rating', col('No_rating').cast(IntegerType()))

    # Removing the '£' character from the 'price' column and converting it to FloatType
    remove_sign = remove_character(extractNo, 'price', '£').withColumn('price', col('price').cast(FloatType()))

    # Removing the '–' character from the 'price' column and converting it to FloatType
    remove_dash = remove_character(remove_sign, 'price', '–').withColumn('price', col('price').cast(FloatType()))

    # Removing the '£' character from the 'stickthrough_price' column and converting it to FloatType
    Sremove_sign = remove_character(remove_dash, 'stickthrough_price', '£').withColumn('stickthrough_price', col('stickthrough_price').cast(FloatType()))

    # Creating a temporary view 'gpuCard' for further SQL operations
    createtb = Sremove_sign.createOrReplaceTempView("gpuCard")

    # Running SQL query to select required columns and handle NULL values in the strickthroughpirce
    strickthroughprice = spark.sql("""
        SELECT Model_No, Gpu_name,
            CASE WHEN No_rating IS NULL THEN 0 ELSE No_rating END AS num_rating,
            CASE WHEN ratings IS NULL THEN 0 ELSE ratings END AS Ratings_out_of_5,
            price,
            CASE WHEN stickthrough_price IS NULL THEN 0.0 ELSE stickthrough_price END AS stickthrough_price,
            Product_link
        FROM gpuCard
    """)

    return strickthroughprice

# Running the pipeline and displaying the result
test_run = run_pipeline(df)
test_run.coalesce(1).write.mode("overwrite").csv(path='abfss://neweggfile@neweggdb.dfs.core.windows.net/ScrapRaw/Sliver_Db', header=True)
