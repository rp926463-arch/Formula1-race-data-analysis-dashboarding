# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, col, count, when

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum('points').alias('total_points'),count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year == 2020'))

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings

# COMMAND ----------


