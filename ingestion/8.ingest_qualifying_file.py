# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read multiple json files from qualifying folder

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('multiLine', True) \
.json(f'{raw_folder_path}/qualifying/qualifying_split*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Rename & add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

qualifying_temp_df = qualifying_df.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('qualifyId', 'qualify_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Write to parquet file

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

dbutils.notebook.exit("Success")
