# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Layer: Cleaning & Conformance
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Transforms bronze (raw, as-landed) tables into clean, conformed silver tables:
# MAGIC - Casts date strings to proper DATE types
# MAGIC - Converts chronic condition flags (1/2 encoding) into readable booleans
# MAGIC - Unions inpatient + outpatient claims into one `silver_claims` table with a
# MAGIC   `claim_type` column, since most downstream utilization analysis wants to look
# MAGIC   at both together
# MAGIC - Joins provider fraud labels onto claims for convenience
# MAGIC
# MAGIC ## Tables created
# MAGIC - `main.healthcare_claims_util.silver_beneficiary`
# MAGIC - `main.healthcare_claims_util.silver_claims`

# COMMAND ----------

from pyspark.sql.functions import col, try_to_date, when, datediff, current_date

bronze_beneficiary = spark.table("main.healthcare_claims_util.bronze_beneficiary")

silver_beneficiary = (
    bronze_beneficiary
    .withColumn("dob", try_to_date("DOB", "yyyy-MM-dd"))
    .withColumn("dod", try_to_date("DOD", "yyyy-MM-dd"))
    .withColumn("is_deceased", col("dod").isNotNull())
    .withColumn("age", (datediff(current_date(), col("dob")) / 365.25).cast("int"))
    .withColumn("has_diabetes", when(col("ChronicCond_Diabetes") == 1, True).otherwise(False))
    .withColumn("has_heart_failure", when(col("ChronicCond_Heartfailure") == 1, True).otherwise(False))
    .withColumn("has_kidney_disease", when(col("ChronicCond_KidneyDisease") == 1, True).otherwise(False))
    .withColumn("has_cancer", when(col("ChronicCond_Cancer") == 1, True).otherwise(False))
    .withColumn("has_ischemic_heart", when(col("ChronicCond_IschemicHeart") == 1, True).otherwise(False))
    .withColumn("has_stroke", when(col("ChronicCond_stroke") == 1, True).otherwise(False))
    .withColumnRenamed("BeneID", "bene_id")
    .withColumnRenamed("Gender", "gender_code")
    .withColumnRenamed("Race", "race_code")
    .withColumnRenamed("State", "state_code")
    .withColumnRenamed("County", "county_code")
    .select(
        "bene_id", "dob", "dod", "is_deceased", "age", "gender_code", "race_code",
        "state_code", "county_code", "has_diabetes", "has_heart_failure",
        "has_kidney_disease", "has_cancer", "has_ischemic_heart", "has_stroke",
        "IPAnnualReimbursementAmt", "OPAnnualReimbursementAmt",
    )
)

(
    silver_beneficiary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.healthcare_claims_util.silver_beneficiary")
)

print("Row count:", spark.table("main.healthcare_claims_util.silver_beneficiary").count())

# COMMAND ----------

from pyspark.sql.functions import lit, try_to_date, datediff

bronze_inpatient = spark.table("main.healthcare_claims_util.bronze_inpatient")
bronze_outpatient = spark.table("main.healthcare_claims_util.bronze_outpatient")
bronze_fraud_labels = spark.table("main.healthcare_claims_util.bronze_fraud_labels")

inpatient_std = (
    bronze_inpatient
    .withColumn("claim_type", lit("Inpatient"))
    .withColumn("claim_start_dt", try_to_date("ClaimStartDt", "yyyy-MM-dd"))
    .withColumn("claim_end_dt", try_to_date("ClaimEndDt", "yyyy-MM-dd"))
    .withColumn("admission_dt", try_to_date("AdmissionDt", "yyyy-MM-dd"))
    .withColumn("discharge_dt", try_to_date("DischargeDt", "yyyy-MM-dd"))
    .withColumn("length_of_stay_days", datediff(col("discharge_dt"), col("admission_dt")))
    .select(
        "ClaimID", "BeneID", "Provider", "claim_type", "claim_start_dt", "claim_end_dt",
        "InscClaimAmtReimbursed", "DeductibleAmtPaid", "AttendingPhysician",
        "ClmDiagnosisCode_1", "length_of_stay_days",
    )
)

outpatient_std = (
    bronze_outpatient
    .withColumn("claim_type", lit("Outpatient"))
    .withColumn("claim_start_dt", try_to_date("ClaimStartDt", "yyyy-MM-dd"))
    .withColumn("claim_end_dt", try_to_date("ClaimEndDt", "yyyy-MM-dd"))
    .withColumn("length_of_stay_days", lit(None).cast("int"))  # not applicable to outpatient visits
    .select(
        "ClaimID", "BeneID", "Provider", "claim_type", "claim_start_dt", "claim_end_dt",
        "InscClaimAmtReimbursed", "DeductibleAmtPaid", "AttendingPhysician",
        "ClmDiagnosisCode_1", "length_of_stay_days",
    )
)

silver_claims = (
    inpatient_std.unionByName(outpatient_std)
    .join(bronze_fraud_labels.select("Provider", "PotentialFraud"), on="Provider", how="left")
    .withColumnRenamed("ClaimID", "claim_id")
    .withColumnRenamed("BeneID", "bene_id")
    .withColumnRenamed("Provider", "provider_id")
    .withColumnRenamed("InscClaimAmtReimbursed", "claim_amount_reimbursed")
    .withColumnRenamed("DeductibleAmtPaid", "deductible_amt_paid")
    .withColumnRenamed("AttendingPhysician", "attending_physician_id")
    .withColumnRenamed("ClmDiagnosisCode_1", "primary_diagnosis_code")
    .withColumnRenamed("PotentialFraud", "provider_potential_fraud_flag")
)

(
    silver_claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("main.healthcare_claims_util.silver_claims")
)

print("Row count:", spark.table("main.healthcare_claims_util.silver_claims").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE main.healthcare_claims_util.silver_claims IS
# MAGIC 'Unified inpatient and outpatient Medicare claims, one row per claim, joined with provider fraud status.';
# MAGIC
# MAGIC ALTER TABLE main.healthcare_claims_util.silver_claims ALTER COLUMN claim_type COMMENT 'Whether the claim was for an inpatient (hospital admission) or outpatient (visit only) encounter';
# MAGIC ALTER TABLE main.healthcare_claims_util.silver_claims ALTER COLUMN claim_amount_reimbursed COMMENT 'Total dollar amount reimbursed by the insurance program for this claim';
# MAGIC ALTER TABLE main.healthcare_claims_util.silver_claims ALTER COLUMN provider_potential_fraud_flag COMMENT 'Yes/No flag indicating whether the billing provider was labeled as a potential fraud risk';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT claim_type, COUNT(*) AS claim_count, SUM(claim_amount_reimbursed) AS total_reimbursed
# MAGIC FROM main.healthcare_claims_util.silver_claims
# MAGIC GROUP BY claim_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT provider_potential_fraud_flag, COUNT(*) AS claim_count, COUNT(DISTINCT provider_id) AS provider_count
# MAGIC FROM main.healthcare_claims_util.silver_claims
# MAGIC GROUP BY provider_potential_fraud_flag;