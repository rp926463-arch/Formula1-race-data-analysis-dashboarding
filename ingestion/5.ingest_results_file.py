# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read results.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

results_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),  
    StructField('constructorId', IntegerType(), False),  
    StructField('number', IntegerType(), True),  
    StructField('grid', IntegerType(), False),  
    StructField('position', IntegerType(), True),  
    StructField('positionText', StringType(), False),  
    StructField('positionOrder', IntegerType(), False),  
    StructField('points', DoubleType(), False),  
    StructField('laps', IntegerType(), False),  
    StructField('time', StringType(), True),  
    StructField('milliseconds', IntegerType(), True),  
    StructField('fastestLap', IntegerType(), True),  
    StructField('rank', IntegerType(), True),  
    StructField('fastestLapTime', StringType(), True),  
    StructField('fastestLapSpeed', StringType(), True),  
    StructField('statusId', IntegerType(), False)
])

results_df = spark.read \
.schema(results_schema) \
.json(f'{raw_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

results_temp_df = results_df.withColumnRenamed('resultId', 'result_id') \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'drive_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('positionText', 'position_text') \
.withColumnRenamed('positionOrder', 'position_order') \
.withColumnRenamed('fastestLap', 'fastest_lap') \
.withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

results_final_df = add_ingestion_date(results_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : write to parquet file

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formula1dl199/processed/results')

# COMMAND ----------

dbutils.notebook.exit("Success")
