# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest race.csv file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

race_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])


race_df = spark.read \
.option("header", True) \
.schema(race_schema) \
.csv(f'{raw_folder_path}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Transform & add new columns

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp

race_transformed_df = race_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

race_new_df = add_ingestion_date(race_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Select required columns

# COMMAND ----------

race_selected_df = race_new_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('race_timestamp'), col('ingestion_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 : Write to file

# COMMAND ----------

race_selected_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit("Success")
