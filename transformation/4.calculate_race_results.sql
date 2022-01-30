-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results
  JOIN drivers ON (results.drive_id = drivers.driver_id)
  JOIN constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN races ON (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC --1954 max points for 1st postition is 8<br>
-- MAGIC --While in 1953 max points for 1st position is 9<br>
-- MAGIC --2012 points for 1st position is 25<br>
-- MAGIC --So there is no consistency in points

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------


