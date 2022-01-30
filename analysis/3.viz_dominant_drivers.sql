-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points,
    RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING COUNT(1) >= 50
  ORDER BY avg_points DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Get Information for each of driver per year

-- COMMAND ----------

SELECT race_year, 
    driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year, 
    driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year, 
    driver_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, avg_points DESC

-- COMMAND ----------


