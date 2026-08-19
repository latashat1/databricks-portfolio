# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold Layer: Business Aggregates
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Builds the final, business-consumable gold tables that power the dashboard and
# MAGIC Genie space. Each table answers a specific business question and is
# MAGIC pre-aggregated so dashboard queries are fast and simple (no joins needed at
# MAGIC query time).
# MAGIC
# MAGIC ## Tables created
# MAGIC - `main.healthcare_claims_util.gold_provider_utilization_summary`
# MAGIC - `main.healthcare_claims_util.gold_chronic_disease_prevalence`
# MAGIC - `main.healthcare_claims_util.gold_claims_cost_trends`
# MAGIC - `main.healthcare_claims_util.gold_fraud_risk_summary`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.healthcare_claims_util.gold_provider_utilization_summary
# MAGIC COMMENT 'Claim volume and cost by provider and claim type, for utilization review'
# MAGIC AS
# MAGIC SELECT
# MAGIC   provider_id,
# MAGIC   claim_type,
# MAGIC   COUNT(claim_id)                AS total_claims,
# MAGIC   COUNT(DISTINCT bene_id)        AS distinct_patients,
# MAGIC   SUM(claim_amount_reimbursed)   AS total_reimbursed,
# MAGIC   ROUND(AVG(claim_amount_reimbursed), 2) AS avg_claim_amount,
# MAGIC   ROUND(AVG(length_of_stay_days), 1)     AS avg_length_of_stay_days
# MAGIC FROM main.healthcare_claims_util.silver_claims
# MAGIC GROUP BY provider_id, claim_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference table: SSA state codes
# MAGIC Medicare/CMS data uses Social Security Administration (SSA) 2-digit state
# MAGIC codes, not the more commonly known FIPS codes - a different numbering system
# MAGIC entirely. This reference table translates SSA codes to readable state names
# MAGIC so gold_chronic_disease_prevalence isn't stuck showing raw numbers.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.healthcare_claims_util.ref_ssa_state_codes
# MAGIC COMMENT 'Lookup table: SSA (Social Security Administration) 2-digit state codes to state names, as used in CMS/Medicare data. Codes 54+ represent non-US/foreign residence buckets per SSA convention; grouped here as Other/Foreign rather than asserting a specific region, since this is synthetic data.'
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Alabama'), (2, 'Alaska'), (3, 'Arizona'), (4, 'Arkansas'), (5, 'California'),
# MAGIC   (6, 'Colorado'), (7, 'Connecticut'), (8, 'Delaware'), (9, 'District of Columbia'),
# MAGIC   (10, 'Florida'), (11, 'Georgia'), (12, 'Hawaii'), (13, 'Idaho'), (14, 'Illinois'),
# MAGIC   (15, 'Indiana'), (16, 'Iowa'), (17, 'Kansas'), (18, 'Kentucky'), (19, 'Louisiana'),
# MAGIC   (20, 'Maine'), (21, 'Maryland'), (22, 'Massachusetts'), (23, 'Michigan'), (24, 'Minnesota'),
# MAGIC   (25, 'Mississippi'), (26, 'Missouri'), (27, 'Montana'), (28, 'Nebraska'), (29, 'Nevada'),
# MAGIC   (30, 'New Hampshire'), (31, 'New Jersey'), (32, 'New Mexico'), (33, 'New York'),
# MAGIC   (34, 'North Carolina'), (35, 'North Dakota'), (36, 'Ohio'), (37, 'Oklahoma'), (38, 'Oregon'),
# MAGIC   (39, 'Pennsylvania'), (40, 'Puerto Rico'), (41, 'Rhode Island'), (42, 'South Carolina'),
# MAGIC   (43, 'South Dakota'), (44, 'Tennessee'), (45, 'Texas'), (46, 'Utah'), (47, 'Vermont'),
# MAGIC   (48, 'Virgin Islands'), (49, 'Virginia'), (50, 'Washington'), (51, 'West Virginia'),
# MAGIC   (52, 'Wisconsin'), (53, 'Wyoming'), (54, 'Other/Foreign')
# MAGIC AS state_lookup(ssa_state_code, state_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.healthcare_claims_util.gold_chronic_disease_prevalence
# MAGIC COMMENT 'Chronic condition prevalence rates by state, for population health analysis'
# MAGIC AS
# MAGIC SELECT
# MAGIC   b.state_code AS ssa_state_code,
# MAGIC   s.state_name,
# MAGIC   COUNT(*)                                            AS total_beneficiaries,
# MAGIC   ROUND(AVG(CAST(b.has_diabetes AS INT)) * 100, 1)     AS pct_diabetes,
# MAGIC   ROUND(AVG(CAST(b.has_heart_failure AS INT)) * 100, 1) AS pct_heart_failure,
# MAGIC   ROUND(AVG(CAST(b.has_kidney_disease AS INT)) * 100, 1) AS pct_kidney_disease,
# MAGIC   ROUND(AVG(CAST(b.has_cancer AS INT)) * 100, 1)        AS pct_cancer,
# MAGIC   ROUND(AVG(b.age), 1)                                  AS avg_age
# MAGIC FROM main.healthcare_claims_util.silver_beneficiary b
# MAGIC LEFT JOIN main.healthcare_claims_util.ref_ssa_state_codes s
# MAGIC   ON CAST(b.state_code AS INT) = s.ssa_state_code
# MAGIC GROUP BY b.state_code, s.state_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.healthcare_claims_util.gold_chronic_disease_prevalence ORDER BY total_beneficiaries DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.healthcare_claims_util.gold_claims_cost_trends
# MAGIC COMMENT 'Monthly claims cost and volume trends by claim type'
# MAGIC AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('MONTH', claim_start_dt) AS claim_month,
# MAGIC   claim_type,
# MAGIC   COUNT(claim_id)                     AS claim_count,
# MAGIC   SUM(claim_amount_reimbursed)        AS total_reimbursed,
# MAGIC   ROUND(AVG(claim_amount_reimbursed), 2) AS avg_claim_amount
# MAGIC FROM main.healthcare_claims_util.silver_claims
# MAGIC WHERE claim_start_dt IS NOT NULL
# MAGIC GROUP BY DATE_TRUNC('MONTH', claim_start_dt), claim_type
# MAGIC ORDER BY claim_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.healthcare_claims_util.gold_fraud_risk_summary
# MAGIC COMMENT 'Provider fraud label distribution with average claim behavior, for fraud/SIU review dashboards'
# MAGIC AS
# MAGIC SELECT
# MAGIC   provider_potential_fraud_flag AS fraud_label,
# MAGIC   COUNT(DISTINCT provider_id)   AS provider_count,
# MAGIC   COUNT(claim_id)               AS total_claims,
# MAGIC   SUM(claim_amount_reimbursed)  AS total_reimbursed,
# MAGIC   ROUND(AVG(claim_amount_reimbursed), 2) AS avg_claim_amount
# MAGIC FROM main.healthcare_claims_util.silver_claims
# MAGIC GROUP BY provider_potential_fraud_flag;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'gold_provider_utilization_summary' AS table_name, COUNT(*) AS row_count FROM main.healthcare_claims_util.gold_provider_utilization_summary
# MAGIC UNION ALL
# MAGIC SELECT 'gold_chronic_disease_prevalence', COUNT(*) FROM main.healthcare_claims_util.gold_chronic_disease_prevalence
# MAGIC UNION ALL
# MAGIC SELECT 'gold_claims_cost_trends', COUNT(*) FROM main.healthcare_claims_util.gold_claims_cost_trends
# MAGIC UNION ALL
# MAGIC SELECT 'gold_fraud_risk_summary', COUNT(*) FROM main.healthcare_claims_util.gold_fraud_risk_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT claim_type, MIN(claim_month) AS earliest_month, MAX(claim_month) AS latest_month, COUNT(*) AS month_count
# MAGIC FROM main.healthcare_claims_util.gold_claims_cost_trends
# MAGIC GROUP BY claim_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE main.healthcare_claims_util.gold_provider_utilization_summary IS
# MAGIC 'Claim volume, patient counts, and reimbursement by provider and claim type (inpatient/outpatient). One row per provider per claim type.';
# MAGIC
# MAGIC COMMENT ON TABLE main.healthcare_claims_util.gold_chronic_disease_prevalence IS
# MAGIC 'Chronic condition prevalence rates by state (using CMS numeric state codes). One row per state.';
# MAGIC
# MAGIC COMMENT ON TABLE main.healthcare_claims_util.gold_claims_cost_trends IS
# MAGIC 'Monthly claim volume and reimbursement cost trends, split by inpatient/outpatient, covering Nov 2008 - Dec 2009.';
# MAGIC
# MAGIC COMMENT ON TABLE main.healthcare_claims_util.gold_fraud_risk_summary IS
# MAGIC 'Provider counts and claim behavior grouped by potential fraud label (Yes/No), for fraud/SIU review.';