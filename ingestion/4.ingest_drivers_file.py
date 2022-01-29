# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Rename columns & add new columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat

drivers_temp_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('driverRef', 'driver_ref') \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

drivers_transformed_df = add_ingestion_date(drivers_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Drop unwanted column

# COMMAND ----------

drivers_final_df = drivers_transformed_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 : Write to output file

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
