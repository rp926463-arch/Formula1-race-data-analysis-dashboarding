# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 : read json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 : Drop unwanted columns

# COMMAND ----------

constructor_droppped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 : Rename & add required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

constructor_temp_df = constructor_droppped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                              .withColumnRenamed('constructorRef', 'constructor_ref') \
                                              .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 : Write data to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")
