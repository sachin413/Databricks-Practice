-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Windows Function with Range
-- MAGIC - Aggregate sales amount for month with range using SQL and PySpark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data preparation and SQL queries

-- COMMAND ----------

-- DBTITLE 1,List file from DBFS -- check if source file is present
-- MAGIC %fs
-- MAGIC ls /FileStore/tables/practice_sales_data.csv

-- COMMAND ----------

-- DBTITLE 1,Create table by loading csv file using temp view
CREATE OR REPLACE TEMP VIEW temp_sales_data 
(SalesOrderNumber STRING ,SalesAmount FLOAT,OrderDate DATE)
USING CSV
OPTIONS (
  path='/FileStore/tables/practice_sales_data.csv',
  header='true',
  delimiter=','
);

CREATE TABLE sales_jan_2013  AS(
  SELECT * FROM temp_sales_data
);

SELECT * FROM sales_jan_2013;



-- COMMAND ----------

-- DBTITLE 1,Running total with range - SQL
SELECT
		SalesOrderNumber
		,OrderDate
		,SalesAmount
		,SUM(SalesAmount) OVER (ORDER BY OrderDate 
			RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS running_total
FROM sales_jan_2013;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Queries using PySpark

-- COMMAND ----------

-- DBTITLE 1,Create dataframe from existing table
-- MAGIC %python
-- MAGIC
-- MAGIC df_sales_in_jan= spark.sql('SELECT * FROM sales_jan_2013')

-- COMMAND ----------

-- DBTITLE 1,View dataframe
-- MAGIC %python
-- MAGIC df_sales_in_jan.display()

-- COMMAND ----------

-- DBTITLE 1,unboundedPreceding, currentRow
-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import col,avg,sum,min,max,row_number 
-- MAGIC windowSpec= Window.orderBy("OrderDate")
-- MAGIC df_sales_in_jan.withColumn(
-- MAGIC     "running_total", sum(col("SalesAmount")).over(windowSpec))\
-- MAGIC         .select("SalesOrderNumber","OrderDate","SalesAmount","running_total").display()

-- COMMAND ----------

-- DBTITLE 1,unboundedPreceding, unboundedFollowing
-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import col,avg,sum,min,max,row_number 
-- MAGIC windowSpec= Window.orderBy("OrderDate").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
-- MAGIC df_sales_in_jan.withColumn(
-- MAGIC     "running_total", sum(col("SalesAmount")).over(windowSpec))\
-- MAGIC         .select("SalesOrderNumber","OrderDate","SalesAmount","running_total").display()

-- COMMAND ----------

-- DBTITLE 1,unboundedFollowing, currentRow
-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import col,avg,sum,min,max,row_number 
-- MAGIC windowSpec= Window.orderBy("OrderDate").rowsBetween(Window.currentRow ,Window.unboundedFollowing)
-- MAGIC df_sales_in_jan.withColumn(
-- MAGIC     "running_total", sum(col("SalesAmount")).over(windowSpec))\
-- MAGIC         .select("SalesOrderNumber","OrderDate","SalesAmount","running_total").display()

-- COMMAND ----------

-- DBTITLE 1,Custom range with currentRow
-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import col,avg,sum,min,max,row_number 
-- MAGIC windowSpec= Window.orderBy("OrderDate").rowsBetween(Window.currentRow-1 ,Window.currentRow+1)
-- MAGIC df_sales_in_jan.withColumn(
-- MAGIC     "running_total", sum(col("SalesAmount")).over(windowSpec))\
-- MAGIC         .select("SalesOrderNumber","OrderDate","SalesAmount","running_total").display()
