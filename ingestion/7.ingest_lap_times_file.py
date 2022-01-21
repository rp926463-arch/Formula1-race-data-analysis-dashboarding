# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : Read all csv files under lap_times folder

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

laptimes_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

laptimes_df = spark.read \
.schema(laptimes_schema) \
.csv(f'{raw_folder_path}/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Rename & add new column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

laptimes_temp_df = laptimes_df.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

laptimes_final_df = add_ingestion_date(laptimes_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Write to parquet file

# COMMAND ----------

laptimes_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

dbutils.notebook.exit("Success")
