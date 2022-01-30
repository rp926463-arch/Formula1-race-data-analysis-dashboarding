-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points,
    RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS teams_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY team_name
  HAVING COUNT(1) >= 100
  ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, 
    team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE teams_rank <= 10)
  GROUP BY race_year,team_name
  ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year, 
    team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE teams_rank <= 5)
  GROUP BY race_year,team_name
  ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year, 
    team_name, 
    SUM(calculated_points) as total_points,
    COUNT(1) as total_races,
    AVG(calculated_points) as avg_points
  FROM f1_presentation.calculated_race_results
  WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE teams_rank <= 10
  )
  GROUP BY race_year,team_name
  ORDER BY race_year,avg_points DESC

-- COMMAND ----------


