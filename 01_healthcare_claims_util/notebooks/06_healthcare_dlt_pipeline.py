# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Lakeflow Declarative Pipeline: Healthcare Claims (Alternate Implementation)
# MAGIC
# MAGIC **Project:** Healthcare Claims & Utilization Analytics
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Re-implements the bronze -> silver ingestion for the beneficiary data as a
# MAGIC Lakeflow Declarative Pipeline instead of manually orchestrated notebooks +
# MAGIC a Job. Declarative pipelines let you describe *what* each table should
# MAGIC contain; the pipeline engine figures out execution order, dependency
# MAGIC management, and incremental processing on its own.
# MAGIC
# MAGIC ## What's different from the notebook-based approach (02 + 03)
# MAGIC - Tables are defined with `@dlt.table` decorators instead of explicit writes.
# MAGIC - Data quality rules are declared with `@dlt.expect*` instead of manual filters.
# MAGIC - Table dependencies are declared by referencing another table's name, not by
# MAGIC   building a Job DAG with task dependencies.
# MAGIC
# MAGIC ## Important
# MAGIC This notebook is NOT run with the notebook's own "Run All" button. It is
# MAGIC attached to a Lakeflow pipeline (Workflows -> Jobs & Pipelines -> Create ->
# MAGIC ETL Pipeline) and executed by the pipeline engine.
# MAGIC
# MAGIC ## Scope
# MAGIC This alternate implementation covers the beneficiary table only, as a
# MAGIC representative example of the pattern - not a full re-implementation of
# MAGIC every table in the project.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="bronze_beneficiary_dlt",
    comment="Raw beneficiary data ingested via Autoloader inside a Lakeflow pipeline",
)
def bronze_beneficiary_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/main/healthcare_claims_util/raw_data/Train_Beneficiarydata*.csv")
        .withColumn("_ingested_at", current_timestamp())
    )