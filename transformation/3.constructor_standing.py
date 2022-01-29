# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

constructor_standing_df = race_results_df \
.groupBy('race_year', 'team') \
.agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standing_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter('race_year == 2020'))

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings

# COMMAND ----------


