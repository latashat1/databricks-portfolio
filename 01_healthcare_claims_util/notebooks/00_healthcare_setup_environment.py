# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment Setup: Healthcare Claims & Utilization
# MAGIC
# MAGIC **Project:** Healthcare Claims & Utilization Analytics
# MAGIC **Author:** Latasha Terry
# MAGIC **Last updated:** 8/14/2026
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Creates the Unity Catalog schema and Volumes used by every other notebook in this
# MAGIC project. This mirrors the "platform setup" step a data engineer does before any
# MAGIC pipeline work begins - defining where data will live and how it's organized.
# MAGIC
# MAGIC ## Objects created
# MAGIC - Schema: `main.healthcare_claims_util`
# MAGIC - Volume: `main.healthcare_claims_util.raw_data` (landing zone for source files)
# MAGIC - Volume: `main.healthcare_claims_util.checkpoints` (Structured Streaming checkpoints)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm which catalog you actually have write access to in Free Edition
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.healthcare_claims_util
# MAGIC COMMENT 'Medicare-style claims, beneficiary, and provider fraud data for utilization and claims analytics';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.healthcare_claims_util.raw_data
# MAGIC COMMENT 'Landing zone for raw Kaggle CSV extracts';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.healthcare_claims_util.checkpoints
# MAGIC COMMENT 'Autoloader and Structured Streaming checkpoint locations';

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/main/healthcare_claims_util/raw_data"))