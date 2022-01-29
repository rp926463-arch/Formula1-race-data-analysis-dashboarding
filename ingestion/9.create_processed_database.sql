-- Databricks notebook source
DESC DATABASE f1_raw;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl199/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------


