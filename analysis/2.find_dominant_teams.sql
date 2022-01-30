-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Dominant teams of all time

-- COMMAND ----------

SELECT team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dominant teams of last decade

-- COMMAND ----------

SELECT team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------


