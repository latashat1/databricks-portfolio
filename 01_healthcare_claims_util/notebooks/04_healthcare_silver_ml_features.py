# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver ML Feature Table: Provider Risk Features
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Aggregates claims and beneficiary data up to the provider grain to produce a
# MAGIC feature table data scientists can use directly for fraud-risk modeling, without
# MAGIC redoing this aggregation themselves.
# MAGIC
# MAGIC ## Class balance note for downstream modeling
# MAGIC ~9.4% of providers (506 of 5,410) carry a PotentialFraud = Yes label. This is a
# MAGIC moderately imbalanced classification problem - plain accuracy will be a
# MAGIC misleading evaluation metric here. Precision/recall or AUC-PR are more
# MAGIC appropriate.
# MAGIC
# MAGIC ## Table created
# MAGIC - `main.healthcare_claims_util.silver_provider_ml_features` (grain: one row per provider)

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum as _sum, avg, countDistinct, max as _max

silver_claims = spark.table("main.healthcare_claims_util.silver_claims")
silver_beneficiary = spark.table("main.healthcare_claims_util.silver_beneficiary")

# Join claims to beneficiary attributes so we can compute chronic-condition
# prevalence per provider - a known fraud signal: a provider whose patient
# panel shows implausibly high rates of multiple chronic conditions warrants
# a closer look.
claims_with_bene = silver_claims.join(silver_beneficiary, on="bene_id", how="left")

provider_features = (
    claims_with_bene.groupBy("provider_id")
    .agg(
        count("claim_id").alias("total_claims"),
        countDistinct("bene_id").alias("distinct_beneficiaries"),
        _sum("claim_amount_reimbursed").alias("total_reimbursed_amt"),
        avg("claim_amount_reimbursed").alias("avg_claim_amount"),
        _max("claim_amount_reimbursed").alias("max_claim_amount"),
        avg(when(col("claim_type") == "Inpatient", 1).otherwise(0)).alias("pct_inpatient_claims"),
        avg(col("has_diabetes").cast("int")).alias("pct_patients_diabetes"),
        avg(col("has_heart_failure").cast("int")).alias("pct_patients_heart_failure"),
        avg(col("age")).alias("avg_patient_age"),
        _max("provider_potential_fraud_flag").alias("potential_fraud_label"),
    )
)

(
    provider_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.healthcare_claims_util.silver_provider_ml_features")
)

print("Row count:", spark.table("main.healthcare_claims_util.silver_provider_ml_features").count())