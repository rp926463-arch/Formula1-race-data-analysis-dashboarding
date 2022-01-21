# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read pit_stops.json file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

pitstops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('stop', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('time', StringType(), False),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

pitstops_df = spark.read \
.schema(pitstops_schema) \
.option('multiLine', True) \
.json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Rename & add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

pitstops_temp_df = pitstops_df.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Write to parquet file

# COMMAND ----------

pitstops_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/pit_stops')

# COMMAND ----------

dbutils.notebook.exit("Success")
