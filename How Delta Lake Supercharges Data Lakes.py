# Databricks notebook source
# MAGIC %md
# MAGIC # How Delta Lake Supercharges Data Lakes
# MAGIC 
# MAGIC Delta Lake’s transaction log brings high reliability, performance, and ACID compliant transactions to data lakes. But exactly how does it accomplish this?
# MAGIC Working through concrete examples, we will take a close look at how the transaction logs are managed and leveraged by Delta to supercharge data lakes.
# MAGIC 
# MAGIC This tutorial notebook was developed using open source Delta Lake in an open source environment.
# MAGIC 
# MAGIC ### Environment used to develop and run this notebook
# MAGIC * [Centos 7.8](https://www.centos.org/download/)
# MAGIC * [Spark 3.0](http://spark.apache.org/docs/latest/)
# MAGIC * [Delta Lake 0.7.0](https://github.com/delta-io/delta/releases)
# MAGIC * [Scala 2.12](https://www.scala-lang.org/download/2.12.8.html)
# MAGIC * [Jupyterlab 2.1.5](https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html)
# MAGIC 
# MAGIC ### Installing Delta Lake
# MAGIC * [Download 0.7.0 jar file](https://mvnrepository.com/artifact/io.delta/delta-core_2.12/0.7.0)
# MAGIC * Move jar file to $SPARK_HOME/jars
# MAGIC * More Details here: [Setting up Apache Spark with Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC 
# MAGIC The data used in this tutorial is a modified version of the public data from [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets/Online+Retail#). This dataset contains transactional data from a UK online retailer and it spans January 12, 2010 to September 12, 2011. For a full view of the data please view the data dictionary available [here](http://archive.ics.uci.edu/ml/datasets/Online+Retail#).

# COMMAND ----------

# MAGIC %md
# MAGIC <br>
# MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>  
# MAGIC 
# MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box.
# MAGIC 
# MAGIC <img src="https://www.evernote.com/l/AAF4VIILJtFNZLuvZjGGhZTr2H6Z0wh6rOYB/image.png" width=800px align="center">

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup, Helpers, Config & APIs

# COMMAND ----------

# Required Classes
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, rand, when, count, col

# COMMAND ----------

# Create Spark session & configure it for Delta Lake version 0.7.0

# Load delta library - .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
# Enable sql (Delta Lake specific) support within Apache Spark - .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# Enable integration with Catalog APIs (since 3.0) - .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = SparkSession.builder.appName("DeltaLake Transaction Logs") \
    .master("local[4]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Main class for programmatically intereacting with Delta Tables.
from delta.tables import *

# COMMAND ----------

# Set configurations for our Spark Session
# Adjust to your environment, e.g # cores on cluster

spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.default.parallelism", 4)

# COMMAND ----------

# Credits: 
# Antoine L. Pobux
# Gist: https://gist.github.com/Pobux/0c474672b3acd4473d459d3219675ad8
# Referenced 7/15/2020

def get_printable_size(byte_size):
    """
    A bit is the smallest unit, it's either 0 or 1
    1 byte = 1 octet = 8 bits
    1 kB = 1 kilobyte = 1000 bytes = 10^3 bytes
    1 KiB = 1 kibibyte = 1024 bytes = 2^10 bytes
    1 KB = 1 kibibyte OR kilobyte ~= 1024 bytes ~= 2^10 bytes (it usually means 1024 bytes but sometimes it's 1000... ask the sysadmin ;) )
    1 kb = 1 kilobits = 1000 bits (this notation should not be used, as it is very confusing)
    1 ko = 1 kilooctet = 1000 octets = 1000 bytes = 1 kB
    Also Kb seems to be a mix of KB and kb, again it depends on context.
    In linux, a byte (B) is composed by a sequence of bits (b). One byte has 256 possible values.
    More info : http://www.linfo.org/byte.html
    """
    BASE_SIZE = 1024.00
    MEASURE = ["B", "KB", "MB", "GB", "TB", "PB"]

    def _fix_size(size, size_index):
        if not size:
            return "0"
        elif size_index == 0:
            return str(size)
        else:
            return "{:.0f}".format(size)

    current_size = byte_size
    size_index = 0

    while current_size >= BASE_SIZE and len(MEASURE) != size_index:
        current_size = current_size / BASE_SIZE
        size_index = size_index + 1

    size = _fix_size(current_size, size_index)
    measure = MEASURE[size_index]
    return size + measure
  

import glob
import os
import datetime

def files_in_dir(dir_path, file_ext):
    files = glob.glob(f'{dir_path}/*.{file_ext}')
    files.sort(key=os.path.getmtime)
    
    for f in files:
        x = os.path.getmtime(f)
        x = datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
        print(f'{get_printable_size(os.path.getsize(f)):8} {x}  {os.path.basename(f)}')

    numFiles = os.popen(f'ls -p {dir_path}/*.{file_ext} | egrep -v /$ | wc -l').read().strip()
    totalSize = os.popen(f'! du -sh {dir_path} | cut -f1').read().strip()
    
    print("")
    print(f"Number of file/s: {numFiles} | Total size: {totalSize}")

# COMMAND ----------

#%mkdir /dbfs/data


# COMMAND ----------

# MAGIC %md
# MAGIC # Define Data File Paths

# COMMAND ----------

# Source data
# Change paths to match your environment!

inputPath    = "/dbfs/source/"
sourceData   = inputPath + "online-retail-dataset.csv"

# Base location for all saved data
basePath     = "/dbfs/data" 

# Path for Parquet formatted data
parquetPath  = basePath + "/parquet/online_retail_data"

# Path for Delta formatted data
deltaPath    = basePath + "/delta/online_retail_data"
deltaLogPath = deltaPath + "/_delta_log"

# Clean up from last run.
! rm -Rf $deltaPath 2>/dev/null
print("Deleted path " + deltaPath)

! rm -Rf $parquetPath 2>/dev/null
print("Deleted path " + parquetPath)

# COMMAND ----------

# MAGIC %ls /home/spark/data/source/

# COMMAND ----------

# MAGIC %md
# MAGIC # Download and Stage Source Data

# COMMAND ----------

# Data sourced from "Spark - The Definitive Guide", located at: https://github.com/databricks/Spark-The-Definitive-Guide
# Data origin: http://archive.ics.uci.edu/ml/datasets/Online+Retail
import os.path

file_exists = os.path.isfile(f'{sourceData}')
 
if not file_exists:
    print("-> Downloading dataset.")
    os.system(f'curl https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv -o {sourceData}')
    file_exists = os.path.isfile(f'{sourceData}')
    
if file_exists:
    print("-> Dataset is present.\n")
    
    fileSize = ! du -m "$sourceData" | cut -f1 # Posix compliant
    print(f"File [{sourceData}] is {fileSize} MB in size.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Schema for Source Data

# COMMAND ----------

# Let's take a peek at the data as a text file
! head -n 5 $sourceData 2>/dev/null

# COMMAND ----------

# Provide schema for source data
# Schema source: http://archive.ics.uci.edu/ml/datasets/Online+Retail#

# SQL DDL method
schemaDDL = """InvoiceNo Integer, StockCode String, Description String, Quantity Integer, 
               InvoiceDate String, UnitPrice Double, CustomerID Integer, Country String """


# You could also use the StructType method.
# Libraries needed to define schemas
# from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

#inputSchema = StructType([
#  StructField("InvoiceNo", IntegerType(), True),
#  StructField("StockCode", StringType(), True),
#  StructField("Description", StringType(), True),
#  StructField("Quantity", IntegerType(), True),
#  StructField("InvoiceDate", StringType(), True),
#  StructField("UnitPrice", DoubleType(), True),
#  StructField("CustomerID", IntegerType(), True),
#  StructField("Country", StringType(), True)
#])


# COMMAND ----------

# MAGIC %ls /dbfs

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean and Explore Data

# COMMAND ----------

# Create retail sales data dataframe

rawSalesDataDF = (
    spark.read
    .format("csv")
    .option("header","true")
    .schema(schemaDDL)
    .load("dbfs:/source/online-retail-dataset.csv")
)

# Count rows and partitions
rowCount = rawSalesDataDF.count() 
partCount = rawSalesDataDF.rdd.getNumPartitions()

print(f'Row Count: {rowCount} Partition Count: {partCount}')

# COMMAND ----------

# Identify columns with null values

print("Columns with null values")
rawSalesDataDF.select([count(when(col(c).isNull(), c)).alias(c) for c in rawSalesDataDF.columns]).show()

# COMMAND ----------

# Remove rows where important columns are null. In our case: InvoiceNo and CustomerID

cleanSalesDataDF = rawSalesDataDF.where(col("InvoiceNo").isNotNull() & col("CustomerID").isNotNull())
cleanSalesDataCount = cleanSalesDataDF.count()
# POO cleanSalesDataDF = cleanSalesDataDF.where(col("CustomerID").isNotNull())

# All rows with null values should be gone
print("null values")
cleanSalesDataDF.select([count(when(col(c).isNull(), c)).alias(c) for c in rawSalesDataDF.columns]).show()

print(f' RowsRemoved: {rowCount-cleanSalesDataCount}\n Final Row Count: {cleanSalesDataCount}')

# COMMAND ----------

# Define new dataframe based on cleansed data but only use a subset of the data to make things run faster

# Random sample of 25%, with seed and without replacement
retailSalesData1 = cleanSalesDataDF.sample(withReplacement=False, fraction=.25, seed=75)

# Count rows and partitions
rowCount = retailSalesData1.count() 
partCount = retailSalesData1.rdd.getNumPartitions()

print(f'Row Count: {rowCount} Partition Count: {partCount}')

# COMMAND ----------

# Peek at the dataframe

retailSalesData1.show(3, truncate = False)

# COMMAND ----------

# MAGIC %sql drop database deltademo cascade

# COMMAND ----------

# MAGIC %md
# MAGIC # HIVE Metastore Database Setup

# COMMAND ----------

# Create database to hold demo objects
spark.sql("CREATE DATABASE IF NOT EXISTS deltademo")
spark.sql("SHOW DATABASES").show()

# Current DB should be deltademo
spark.sql("USE deltademo")
spark.sql("SELECT CURRENT_DATABASE()").show()
spark.sql("DESCRIBE DATABASE deltademo").show(truncate = False)

# Clean-up from last run
spark.sql("DROP TABLE IF EXISTS SalesParquetFormat")
spark.sql("DROP TABLE IF EXISTS SalesDeltaFormat")
spark.sql("DROP TABLE IF EXISTS tbl_CheckpointFile")
spark.sql("SHOW TABLES").show()


# COMMAND ----------

print(parquetPath)

# COMMAND ----------

# MAGIC %ls /dbfs/data/parquet/online_retail_data

# COMMAND ----------

# MAGIC %sql drop table SalesParquetFormat

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with Parquet Files

# COMMAND ----------

# Save data as a table in Parquet format
#dbfs:/source/online-retail-dataset.csv

#retailSalesData1.write.saveAsTable('SalesParquetFormat', format='parquet', mode='overwrite',path=parquetPath)
retailSalesData1.write.saveAsTable('SalesParquetFormat', format='parquet', mode='overwrite',path='dbfs:/data/parquet/online_retail_data/online-retail-dataset.csv')

# COMMAND ----------

# Let's peek into the catalog and verify that our table was created

spark.catalog.listTables()

# SQL method - not as informative
# spark.sql("show tables").show()

# COMMAND ----------

# Files and size on disk

files_in_dir(parquetPath, "parquet")

# COMMAND ----------

spark.sql("describe extended SalesParquetFormat").show(100,truncate = False)

# COMMAND ----------

# Use Spark SQL to query the newly created table

spark.sql("SELECT * FROM SalesParquetFormat;").show(3, truncate = False)

# You can directly query the directory too.
# spark.sql(f"SELECT * FROM parquet.`{parquetPath}` limit 5 ").show(truncate = False)

# COMMAND ----------

# Add one row of data to the table
# Parquet being immutable necessitates the creation of an additional Parquet file

spark.sql(""" 
             INSERT INTO SalesParquetFormat
              VALUES(963316, 2291, "WORLD'S BEST JAM MAKING SET", 5, "08/13/2011 07:58", 1.45, 15358, "United Kingdom")
          """)

# COMMAND ----------

files_in_dir(parquetPath, "parquet")

# COMMAND ----------

# Let's have a peek at the new file

# spark.read.load(f"{parquetPath}/part-00000-806d3f4d-6f4e-4d04-a464-f08a1cda3b2f-c000.snappy.parquet").show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC - Above we saw how to create a table structure using Parquet as the underlying data file format.<br>
# MAGIC - Using sql we were able to query the table and even insert new data using sql.<br>
# MAGIC - However, no history was kept of these operations.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/AAFxQkg_SzRC06GvVeatDBnNbDL7wUUgCg4B.png" alt="Delta Lake" width="600" align="left"/>

# COMMAND ----------

# Save retailSalesData1 to Delta

retailSalesData1.write.mode("overwrite").format("delta").save(deltaPath)

# Query delta directory directly
spark.sql(f"SELECT * FROM delta.`{deltaPath}` limit 3 ").show(truncate = False)

# COMMAND ----------

# Create variable for a path based Delta table

deltaTable = DeltaTable.forPath(spark, deltaPath)

# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
history = deltaTable.history().select('version','timestamp','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(truncate = False)

# COMMAND ----------

# files and size on disk
# Notice the sub-directory "_delta_log"

! ls -thl $deltaPath

# COMMAND ----------

# # files and size on disk
# We can see that parquet files were added but now there is a trx log

files_in_dir(deltaLogPath, "json")

# COMMAND ----------

# Let's have a peek inside the trx log

spark.read.format("json").load(deltaLogPath + "/00000000000000000000.json").collect()

# COMMAND ----------

# Create a new dataframe with fraction of original data.
# Random sample of 25%, with seed and without replacement

retailSalesData2 = cleanSalesDataDF.sample(withReplacement=False, fraction=.25, seed=31)
retailSalesData2.count()

# COMMAND ----------

# Add to our Delta Lake table by appending retailSalesData2

retailSalesData2.write.mode("append").format("delta").save(deltaPath)

# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
# Reference for full history schema: https://docs.delta.io/latest/delta-utility.html
history = deltaTable.history().select('version','timestamp','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(truncate = False)

# COMMAND ----------

# Data Files and size on disk

files_in_dir(deltaPath, "parquet")

# COMMAND ----------

# Transaction logs and size on disk

files_in_dir(deltaLogPath, "json")

# COMMAND ----------

# Peek inside the new transaction log

logDF = spark.read.format("json").load(deltaLogPath + "/00000000000000000001.json")
logDF.collect()

# COMMAND ----------

# Create SQL table to make life easier
# Stick with SQL from here on out, where possible.

spark.sql("""
    DROP TABLE IF EXISTS SalesDeltaFormat
  """)
spark.sql("""
    CREATE TABLE SalesDeltaFormat
    USING DELTA
    LOCATION '{}'
  """.format(deltaPath))

# COMMAND ----------

# Let's peek into the catalog and verify that our table was created.
spark.catalog.listTables()

# COMMAND ----------

spark.sql("describe extended SalesDeltaFormat").show(100, truncate = False)

# COMMAND ----------

# Let's find a Invoice with only 1 count and use it to test DML.
oneRandomInvoice = spark.sql(""" SELECT InvoiceNo, count(*)
                                 FROM SalesDeltaFormat
                                 GROUP BY InvoiceNo
                                 ORDER BY 2 asc
                                 LIMIT 1
                             """).collect()[0][0]

print(f"Random Invoice # => {oneRandomInvoice}")

# COMMAND ----------

# Before DML (insert)

spark.sql(f"""
              SELECT SUBSTRING(input_file_name(), -67, 67) AS FileName,
                     * FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

# Let's add some data to our table

spark.sql(f"""
               INSERT INTO SalesDeltaFormat
               VALUES({oneRandomInvoice}, 2291, "WORLD'S BEST JAM MAKING SET", 5, "08/13/2011 07:58", 1.45, 15358, "France");
          """)

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

# Schema details: https://docs.delta.io/latest/delta-utility.html

logDF = spark.read.format("json").load(deltaLogPath + "/00000000000000000002.json")
#dfLog.printSchema()
logDF.collect()

# COMMAND ----------

# After DML (insert)

spark.sql(f"""
              SELECT SUBSTRING(input_file_name(), -67, 67) AS FileName, *
                     FROM SalesDeltaFormat 
                     WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

# Update one invoice

spark.sql(f"""
              UPDATE SalesDeltaFormat
              SET Quantity = Quantity + 1000
              WHERE InvoiceNo = {oneRandomInvoice}
           """)

#deltaTable.update(
#    condition=("InvoiceNo = oneRandomInvoice"),
#    set={"Quantity": expr("Quantity + 1000")}
#)

# COMMAND ----------

# After Update

spark.sql(f"""
              SELECT 
              SUBSTRING(input_file_name(), -67, 67) AS FileName, *
              FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

print("####### HISTORY ########")

# Show which datafile was removed.

# Observe history of actions taken on a Delta table
# spark.sql not supported in 0.7.0 OSS Delta
history = deltaTable.history().select('version','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(truncate = False)

# COMMAND ----------

files_in_dir(deltaPath, "parquet")

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

# Schema details: https://docs.delta.io/latest/delta-utility.html

logDF = spark.read.format("json").load(deltaLogPath + "/00000000000000000003.json")
#dfLog.printSchema()
logDF.collect()

# COMMAND ----------

# Before DML (delete)

spark.sql(f"""select 
          substring(input_file_name(), -67, 67) as FileName,
          * from SalesDeltaFormat 
          where InvoiceNo = {oneRandomInvoice}""").show(truncate = False)

# COMMAND ----------

# https://github.com/delta-io/delta/blob/master/examples/python/quickstart.py
# Delete and invoice (two records)

# This results in one new file being created.  One file had just the one record so it does not have to be re-created
# Each of the two records were in two different files. One of those files had only one record so it did not have to be re-created.

spark.sql(f"DELETE FROM SalesDeltaFormat WHERE InvoiceNo = {oneRandomInvoice}")

# deltaTable.delete(
#    condition=("InvoiceNo = {537617}")
# )

# COMMAND ----------

# After DML (delete)

spark.sql(f"""
              SELECT 
              SUBSTRING(input_file_name(), -67, 67) as FileName, *
              FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
          """).show(truncate = False)

# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
history = deltaTable.history().select('version','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(truncate = False)

# COMMAND ----------

files_in_dir(deltaPath, "parquet")

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

logDF = spark.read.format("json").load(deltaLogPath + "/00000000000000000004.json")
#dfLog.printSchema()
logDF.collect()

# COMMAND ----------

# Randomy update 5 random invoices to force a checkpoint

count = 0
anInvoice = retailSalesData2.select("InvoiceNo").orderBy(rand()).limit(1).collect()[0][0]

while (count <= 5):
  deltaTable.update(
    condition=(f"InvoiceNo = {anInvoice}"),
    set={"Quantity": expr("Quantity + 100")})

  count = count + 1
  anInvoice = retailSalesData2.select("InvoiceNo").orderBy(rand()).limit(1).collect()[0][0]

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

checkPointDF = spark.read.format("parquet").load(deltaLogPath + "/00000000000000000010.checkpoint.parquet")
checkPointDF.show(100, truncate = False)

# COMMAND ----------

checkPointFile10 =(
    checkPointDF.select(col("add.path").alias("FileAdded"),
                        col("add.modificationTime").alias("DateAdded"),
                        col("remove.path").alias("FileDeleted"),
                        col("remove.deletionTimestamp").alias("DateDeleted"))
                .orderBy(["DateAdded","DateDeleted"], ascending=[True,False])
)

spark.sql("DROP TABLE IF EXISTS tbl_checkpointfile")
spark.sql("CREATE TABLE IF NOT EXISTS tbl_checkpointfile (Action string, filename string, ActionDate Long)")

checkPointFile10.createOrReplaceTempView("vw_checkpointfile")

# COMMAND ----------

spark.sql("select * from vw_checkpointfile limit 100").show(100, truncate=False)

# COMMAND ----------

spark.sql("""
          INSERT INTO tbl_checkpointfile
          SELECT "Add", FileAdded, DateAdded
          FROM vw_checkpointfile
          WHERE FileAdded IS NOT NULL
          """)

spark.sql("""
          INSERT INTO tbl_checkpointfile
          SELECT "Delete", FileDeleted, DateDeleted
          FROM vw_checkpointfile
          WHERE FileDeleted IS NOT NULL
          """)

# COMMAND ----------

spark.sql("""
           SELECT Action, 
                  filename as `File Name`, 
                  from_unixtime(actiondate/1e3) AS `ActionDate`
           FROM tbl_checkpointfile 
           order by ActionDate asc
          """).show(200, truncate=False)

# COMMAND ----------

# Let's add some data to our table by doing a merge
# Do this to show how json logs pick up after checkpoint

# Create a tiny dataframe to use with merge
mergeSalesData= cleanSalesDataDF.sample(withReplacement=False, fraction=.0001, seed=13)
mergeSalesData.createOrReplaceTempView("vw_mergeSalesData")

# User-defined commit metadata
spark.sql(f"""
               SET spark.databricks.delta.commitInfo.userMetadata=08-25-2020 Data Merge;
          """)

spark.sql("""
          MERGE INTO SalesDeltaFormat
          USING vw_mergeSalesData
          ON SalesDeltaFormat.StockCode = vw_mergeSalesData.StockCode
           AND SalesDeltaFormat.InvoiceNo = vw_mergeSalesData.InvoiceNo
          WHEN MATCHED THEN
            UPDATE SET *
          WHEN NOT MATCHED
            THEN INSERT *
          """)


# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
history = deltaTable.history().select('version','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(truncate = False)

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

logDF = spark.read.format("json").load(deltaLogPath + "/00000000000000000011.json")
#dfLog.printSchema()
logDF.collect()

# COMMAND ----------

# Count files in deltaPath

numFiles = ! ls $deltaPath/*parquet | wc -l
print(f"There are {numFiles} files.")

# COMMAND ----------

spark.sql("""
           SELECT Action, 
                  COUNT(Action) as `CountAction`
           FROM tbl_checkpointfile 
           GROUP BY ACTION WITH ROLLUP
          """).show(200, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake File Compaction

# COMMAND ----------

# Create an artificial "small file" problem

(spark.read
.format("delta")
.load(deltaPath)
.repartition(1000)
.write
.option("dataChange", True)
.format("delta")
.mode("overwrite")
.save(deltaPath)
)


# COMMAND ----------

# Count files in deltaPath

numFiles = ! ls $deltaPath/*parquet | wc -l
print(f"There are {numFiles} files.")

# COMMAND ----------

# MAGIC %%time
# MAGIC 
# MAGIC # spark.sql("select * from SalesDeltaFormat limit 2").show()
# MAGIC 
# MAGIC rowCount = spark.sql(""" SELECT CustomerID, count(Country) AS num_countries
# MAGIC                          FROM SalesDeltaFormat
# MAGIC                          GROUP BY CustomerID 
# MAGIC                      """).count()
# MAGIC 
# MAGIC print(f"Row Count => {rowCount}\n")

# COMMAND ----------

# Compact 1000 files to 4

(spark.read
.format("delta")
.load(deltaPath)
.repartition(4)
.write
.option("dataChange", False)
.format("delta")
.mode("overwrite")
.save(deltaPath)
)

# COMMAND ----------

# Count files in deltaPath

numFiles = ! ls $deltaPath/*parquet | wc -l
print(f"There are {numFiles} files.")

# COMMAND ----------

# MAGIC %%time
# MAGIC 
# MAGIC # spark.sql("select * from SalesDeltaFormat limit 2").show()
# MAGIC rowCount = spark.sql(""" select CustomerID, count(Country) as num_countries
# MAGIC                          from SalesDeltaFormat
# MAGIC                         group by CustomerID """).count()
# MAGIC 
# MAGIC print(f"Row Count => {rowCount}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Time Travel Queries

# COMMAND ----------

# Time Travel Queries

#POO currentVersion = deltaTable.history(1).select("version").collect()[0][0]
# Determine latest version of the Delta table
currentVersion = spark.sql("DESCRIBE HISTORY SalesDeltaFormat LIMIT 1").collect()[0][0]

# Query table as of the current version to attain row count
currentRowCount = spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count()

print(f"Row Count: {currentRowCount} as of table version {currentVersion}")
print("")

# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
history = deltaTable.history().select('version','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(100, truncate = False)

# COMMAND ----------

# Determine difference in record count between the current version and the original version of the table.

origRowCount = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath).count()
print(f"There are {currentRowCount-origRowCount} more rows in version [{currentVersion}] than version [0] of the table.")

# COMMAND ----------

# Roll back current table to version 0 (original).
(
    spark
    .read
    .format("delta")
    .option("versionAsOf",0)
    .load(deltaPath)
    .write
    .format("delta")
    .mode("overwrite")
    .save(deltaPath)
)

# COMMAND ----------

# Current version should have same record count as version 0.

currentVersion = spark.sql("DESCRIBE HISTORY SalesDeltaFormat LIMIT 1").collect()[0][0]
# If equal it will return "true"
spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count() == spark.read.format("delta").option("versionAsOf", 0).load(deltaPath).count()

# COMMAND ----------

print("####### HISTORY ########")

# Observe history of actions taken on a Delta table
history = deltaTable.history().select('version','operation', 'operationParameters', \
                                      'operationMetrics') \
                              .withColumnRenamed("version", "ver")

history.show(100, truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Vacuum - Data Retention

# COMMAND ----------

# MAGIC %md
# MAGIC delta.logRetentionDuration - default 30 days
# MAGIC <br>
# MAGIC delta.deletedFileRetentionDuration - default 30 days
# MAGIC 
# MAGIC * Don't need to set them to be the same.  You may want to keep the log files around after the tombstoned files are purged.
# MAGIC * Time travel in order of months/years infeasible
# MAGIC * Initially desinged to correct mistakes

# COMMAND ----------

# files_in_dir(deltaPath,"parquet")
# Count files in deltaPath

numFiles = ! ls $deltaPath/*parquet | wc -l
print(f"There are {numFiles} files.")

# COMMAND ----------

# Attempt to vacuum table with default settings

spark.sql("vacuum SalesDeltaFormat retain 0 hours dry run").show(100, truncate = False)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %%time
# MAGIC # Vacuum Delta table to remove all history
# MAGIC 
# MAGIC spark.sql("VACUUM SalesDeltaFormat RETAIN 0 HOURS").show(truncate = False)
# MAGIC # ! Can use deltaTable.vacuum(0) against directory

# COMMAND ----------

files_in_dir(deltaPath,"parquet")

# COMMAND ----------

files_in_dir(deltaLogPath,"*")

# COMMAND ----------

spark.read.format("json").load(deltaLogPath + "/00000000000000000014.json").collect()

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count()

# COMMAND ----------

# Configure Delta table to keep around 7 days of deleted data and 7 days of older log files
spark.sql("alter table SalesDeltaFormat set tblproperties ('delta.logRetentionDuration' = 'interval 7 days', 'delta.deletedFileRetentionDuration' = 'interval 7 days')")

# COMMAND ----------

# Verify our changes
spark.sql("describe extended SalesDeltaFormat").show(truncate = False)

# COMMAND ----------

# spark.stop()
