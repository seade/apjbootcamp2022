# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Environment Setup

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Batch", 0).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

bronze_table_path = f"{dbfs_data_path}tables/bronze"
# silver_table_path = f"{dbfs_data_path}tables/silver"
# gold_table_path = f"{dbfs_data_path}tables/gold"

autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"

# Remove all files from location in case there were any
dbutils.fs.rm(bronze_table_path, recurse=True)
# dbutils.fs.rm(silver_table_path, recurse=True)
# dbutils.fs.rm(gold_table_path, recurse=True)

print("Local data path is {}".format(local_data_path))
print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))

print("Brone Table Location is {}".format(bronze_table_path))
# print("Silver Table Location is {}".format(silver_table_path))
# print("Gold Table Location is {}".format(gold_table_path))

spark.sql(f"USE {database_name};")

# COMMAND ----------

# MAGIC %run ./Utils/Define-Functions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Prepare for the first autoloader run - as this is an example Notebook, we can delete all the files and tables before running it.

# COMMAND ----------

import pyspark.sql.functions as F

checkpoint_path = f'{local_data_path}/_checkpoints'
schema_path = f'{local_data_path}/_schema'
write_path = f'{bronze_table_path}/bronze_sales'

spark.sql("drop table if exists bronze_sales")

refresh_autoloader_datasets = True

if refresh_autoloader_datasets:
  # Run these only if you want to start a fresh run!
  dbutils.fs.rm(checkpoint_path,True)
  dbutils.fs.rm(schema_path,True)
  dbutils.fs.rm(write_path,True)
  dbutils.fs.rm(autoloader_ingest_path, True)
  
  dbutils.fs.mkdirs(autoloader_ingest_path)
  
  # Land a few files; these are used to create the table
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202110.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202111.json", autoloader_ingest_path)
  dbutils.fs.cp(f"{dbfs_data_path}/sales_202112.json", autoloader_ingest_path)

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, exported_ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 

batch_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .trigger(once=True) \
  .start(write_path)

batch_autoloader.awaitTermination()

spark.sql(f"create table if not exists bronze_sales location '{write_path}'")
# The above executes Autoloader on the three file that were added above and creates the table we will use in the subsequent invocation.

# COMMAND ----------

# We can confirm that at this point there is just the three files in the autoloader_ingest_path location
dbutils.fs.ls(autoloader_ingest_path)

# COMMAND ----------

# We are going to add some more data here, we will see shortly that this comes from some other
# process and is not just a plain json file like it was for the initial file.
get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-01')

# COMMAND ----------

# Let's see what the new data looks like now
dbutils.fs.ls(f"{autoloader_ingest_path}")
# Shows: dbfs:/FileStore/scott_eade/deltademoasset/autoloader_ingest/SYD01/
# Note that the data added in the previous cell is actually a folder.

# COMMAND ----------

# Let's look in the folder (spoiler, we will need to go three folders deep)
dbutils.fs.ls(f"{autoloader_ingest_path}/SYD01/")
# Shows: dbfs:/FileStore/scott_eade/deltademoasset/autoloader_ingest/SYD01/2022-01-01/

# COMMAND ----------

# Another folder - let's look in there
dbutils.fs.ls(f"{autoloader_ingest_path}/SYD01/2022-01-01/")
# Shows: dbfs:/FileStore/scott_eade/deltademoasset/autoloader_ingest/SYD01/2022-01-01/daily_sales.json/

# COMMAND ----------

# Another folder (named after the json file) - let's look in there (spoiler, it is the last one)
dbutils.fs.ls(f"{autoloader_ingest_path}/SYD01/2022-01-01/daily_sales.json/")
# This looks like the output of some other process, it contains 4 files, the last of which is actual data
# _SUCCESS
# _committed_2175735148423210524
# _started_2175735148423210524
# part-00000-tid-2175735148423210524-4b5de05b-14bd-4e32-a921-62ceb590d399-33132-1-c000.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now let's run Autoloader in **streaming mode**.

# COMMAND ----------

# Set up the stream to begin reading incoming files from the autoloader_ingest_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaHints", "ts long, exported_ts long, SaleID string") \
  .option('cloudFiles.schemaLocation', schema_path) \
  .load(autoloader_ingest_path) \
  .withColumn("file_path",F.input_file_name()) \
  .withColumn("inserted_at", F.current_timestamp()) 

streaming_autoloader = df.writeStream \
  .format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .option("mergeSchema", "true") \
  .option("path", write_path) \
  .table('bronze_sales')

# The default for writeStream is
# .trigger(processingTime='500 milliseconds') \
# We might reduce the hits on the file listing service by increasing processingTime to 10s.
# .trigger(processingTime='10 seconds') \

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can see the data has actually been processed
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path

# COMMAND ----------

# Lets add a further set of data that uses the same three levels of folder nesting.
get_incremental_data(autoloader_ingest_path, 'SYD01','2022-01-02')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Stop streaming autoloader to allow our cluster to shut down.

# COMMAND ----------

streaming_autoloader.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A final look at the data that has been processed
# MAGIC select file_path, count(*) number_of_records
# MAGIC from bronze_sales
# MAGIC group by file_path

# COMMAND ----------


