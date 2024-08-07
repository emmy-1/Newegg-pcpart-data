-- Databricks notebook source
CREATE STREAMING LIVE TABLE gpu
COMMENT 'Streaming table for gpu cards extracted from new_egg csv format'
AS SELECT Title, GPU, CAST(Memory AS INT), CAST(PSU AS INT), CAST(`Length` AS INT), CAST(Ratings AS INT), CAST(Price AS INT),ImageURL,CAST(Tips AS INT)
FROM cloud_files('abfss://pcpart@neweggdb.dfs.core.windows.net/Dataset/bronz/', "csv")


-- COMMAND ----------

CREATE STREAMING LIVE TABLE Sliver_Gpu(
  CONSTRAINT valid_title EXPECT(Title IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'This sliver table sets constraint that ensures the Title column is not null.'
AS SELECT Title, GPU, Memory as MemorySize , PSU as RequiredPSUWattage,`Length` as ItemLength, Ratings as RatingCount,Price,ImageURL as ImageURL,Tips as CustomersAdded
FROM STREAM(Live.gpu)

-- COMMAND ----------

CREATE STREAMING Live TABLE grahic_card
AS SELECT * FROM STREAM (Live.Sliver_Gpu)
