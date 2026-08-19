# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze Layer: Autoloader Ingestion
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Uses Databricks Autoloader (`cloudFiles`) to incrementally ingest the raw CSV
# MAGIC files from the `raw_data` Volume into bronze Delta tables. Autoloader tracks
# MAGIC which files have already been processed via a checkpoint, so re-running this
# MAGIC notebook does NOT reprocess files already loaded - this is what makes it
# MAGIC production-safe for a scheduled job instead of a one-off script.
# MAGIC
# MAGIC ## Tables created
# MAGIC - `main.healthcare_claims_util.bronze_beneficiary`
# MAGIC - `main.healthcare_claims_util.bronze_inpatient`
# MAGIC - `main.healthcare_claims_util.bronze_outpatient`
# MAGIC - `main.healthcare_claims_util.bronze_fraud_labels`

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp, col

def autoload_csv_to_bronze(source_path: str, table_name: str, schema: StructType):
    """
    Incrementally ingest CSV files from a Volume path into a bronze Delta table
    using Autoloader (cloudFiles). trigger(availableNow=True) processes all
    files currently sitting in the source path and then stops the stream -
    ideal for a scheduled job rather than an always-on cluster.
    """
    checkpoint_path = f"/Volumes/main/healthcare_claims_util/checkpoints/{table_name}"
    schema_path = f"/Volumes/main/healthcare_claims_util/checkpoints/{table_name}_schema"

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .schema(schema)
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(f"main.healthcare_claims_util.{table_name}")
    )

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

beneficiary_schema = StructType([
    StructField("BeneID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("DOD", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Race", StringType(), True),
    StructField("RenalDiseaseIndicator", StringType(), True),
    StructField("State", StringType(), True),
    StructField("County", StringType(), True),
    StructField("NoOfMonths_PartACov", IntegerType(), True),
    StructField("NoOfMonths_PartBCov", IntegerType(), True),
    StructField("ChronicCond_Alzheimer", IntegerType(), True),
    StructField("ChronicCond_Heartfailure", IntegerType(), True),
    StructField("ChronicCond_KidneyDisease", IntegerType(), True),
    StructField("ChronicCond_Cancer", IntegerType(), True),
    StructField("ChronicCond_ObstrPulmonary", IntegerType(), True),
    StructField("ChronicCond_Depression", IntegerType(), True),
    StructField("ChronicCond_Diabetes", IntegerType(), True),
    StructField("ChronicCond_IschemicHeart", IntegerType(), True),
    StructField("ChronicCond_Osteoporasis", IntegerType(), True),
    StructField("ChronicCond_rheumatoidarthritis", IntegerType(), True),
    StructField("ChronicCond_stroke", IntegerType(), True),
    StructField("IPAnnualReimbursementAmt", DoubleType(), True),
    StructField("IPAnnualDeductibleAmt", DoubleType(), True),
    StructField("OPAnnualReimbursementAmt", DoubleType(), True),
    StructField("OPAnnualDeductibleAmt", DoubleType(), True),
])

# Shared by both claim files, and in the SAME order in both, so it's safe to reuse.
shared_claim_prefix = [
    StructField("BeneID", StringType(), True),
    StructField("ClaimID", StringType(), True),
    StructField("ClaimStartDt", StringType(), True),
    StructField("ClaimEndDt", StringType(), True),
    StructField("Provider", StringType(), True),
    StructField("InscClaimAmtReimbursed", DoubleType(), True),
    StructField("AttendingPhysician", StringType(), True),
    StructField("OperatingPhysician", StringType(), True),
    StructField("OtherPhysician", StringType(), True),
]

# Also shared and in the same order in both files - built once, reused in both schemas.
diagnosis_and_procedure_codes = (
    [StructField(f"ClmDiagnosisCode_{i}", StringType(), True) for i in range(1, 11)]
    + [StructField(f"ClmProcedureCode_{i}", StringType(), True) for i in range(1, 7)]
)

# INPATIENT: admission/discharge fields come BEFORE the diagnosis/procedure codes
inpatient_schema = StructType(
    shared_claim_prefix
    + [
        StructField("AdmissionDt", StringType(), True),
        StructField("ClmAdmitDiagnosisCode", StringType(), True),
        StructField("DeductibleAmtPaid", DoubleType(), True),
        StructField("DischargeDt", StringType(), True),
        StructField("DiagnosisGroupCode", StringType(), True),
    ]
    + diagnosis_and_procedure_codes
)

# OUTPATIENT: diagnosis/procedure codes come FIRST, then DeductibleAmtPaid and
# ClmAdmitDiagnosisCode at the end - different order than inpatient, confirmed
# against the actual file, not assumed.
outpatient_schema = StructType(
    shared_claim_prefix
    + diagnosis_and_procedure_codes
    + [
        StructField("DeductibleAmtPaid", DoubleType(), True),
        StructField("ClmAdmitDiagnosisCode", StringType(), True),
    ]
)

fraud_label_schema = StructType([
    StructField("Provider", StringType(), True),
    StructField("PotentialFraud", StringType(), True),
])

# COMMAND ----------

autoload_csv_to_bronze(
    source_path="/Volumes/main/healthcare_claims_util/raw_data/Train_Beneficiarydata*.csv",
    table_name="bronze_beneficiary",
    schema=beneficiary_schema,
)

autoload_csv_to_bronze(
    source_path="/Volumes/main/healthcare_claims_util/raw_data/Train_Inpatientdata*.csv",
    table_name="bronze_inpatient",
    schema=inpatient_schema,
)

autoload_csv_to_bronze(
    source_path="/Volumes/main/healthcare_claims_util/raw_data/Train_Outpatientdata*.csv",
    table_name="bronze_outpatient",
    schema=outpatient_schema,
)

autoload_csv_to_bronze(
    source_path="/Volumes/main/healthcare_claims_util/raw_data/Train-*.csv",
    table_name="bronze_fraud_labels",
    schema=fraud_label_schema,
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_beneficiary' AS table_name, COUNT(*) AS row_count FROM main.healthcare_claims_util.bronze_beneficiary
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_inpatient', COUNT(*) FROM main.healthcare_claims_util.bronze_inpatient
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_outpatient', COUNT(*) FROM main.healthcare_claims_util.bronze_outpatient
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_fraud_labels', COUNT(*) FROM main.healthcare_claims_util.bronze_fraud_labels;

# COMMAND ----------

inpatient_preview = spark.read.option("header", "true").csv(
    "/Volumes/main/healthcare_claims_util/raw_data/Train_Inpatientdata-1542865627584.csv"
)
print(inpatient_preview.columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL main.healthcare_claims_util.bronze_beneficiary;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/main/healthcare_claims_util/checkpoints/bronze_beneficiary"))