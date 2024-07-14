-- Databricks notebook source
-- MAGIC %md
-- MAGIC # CREATE STREAMING LIVE TABLE

-- COMMAND ----------

CREATE STREAMING LIVE TABLE GPU
COMMENT 'Streaming table for gpu cards extracted from new_egg csv format'
AS SELECT * FROM cloud_files("abfss://neweggfile@neweggdb.dfs.core.windows.net/ScrapRaw/Sliver_Db/", "csv" )

-- COMMAND ----------

CREATE STREAMING LIVE TABLE gpu_save(
  CONSTRAINT valid_no EXPECT (Model_No IS NOT NULL and Gpu_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'This sliver table sets constraint that ensures the model no and graphic card is not null.'
AS SELECT Model_No, Gpu_name, CAST(num_rating AS INT), CAST(Ratings_out_of_5 AS INT), CAST(price AS FLOAT), CAST(stickthrough_price AS FLOAT), Product_link
FROM STREAM (LIVE.GPU)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE Newegg
AS SELECT *
FROM Stream(Live.gpu_save)
