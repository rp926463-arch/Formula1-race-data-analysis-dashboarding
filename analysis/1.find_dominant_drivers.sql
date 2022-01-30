-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Dominant drivers of all time

-- COMMAND ----------

SELECT driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dominant drivers of last decade

-- COMMAND ----------

SELECT driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------


