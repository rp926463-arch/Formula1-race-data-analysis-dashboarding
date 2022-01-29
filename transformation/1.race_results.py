# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits/') \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers/') \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors/') \
.withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races/') \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results/') \
.withColumnRenamed("time", "race_time")

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.drive_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality','team','grid','points','race_time','fastest_lap', 'position') \
.withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM f1_presentation.race_results

# COMMAND ----------


