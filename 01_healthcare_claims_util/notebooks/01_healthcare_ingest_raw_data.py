# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest Raw Data: Healthcare Claims & Utilization
# MAGIC
# MAGIC **Project:** Healthcare Claims & Utilization Analytics
# MAGIC
# MAGIC ## What this notebook does
# MAGIC Documents the source and acquisition method for the raw claims data, and
# MAGIC confirms the files landed correctly in the raw_data Volume before bronze
# MAGIC ingestion runs.
# MAGIC
# MAGIC ## Source
# MAGIC Kaggle: healthcare-provider-fraud-detection-analysis (rohitrox)
# MAGIC https://www.kaggle.com/datasets/rohitrox/healthcare-provider-fraud-detection-analysis
# MAGIC
# MAGIC ## Acquisition method
# MAGIC Manually downloaded via browser and uploaded through Catalog Explorer, rather
# MAGIC than the Kaggle API - Databricks Free Edition blocks outbound network calls
# MAGIC from notebooks, so the CLI/API approach isn't available here.
# MAGIC
# MAGIC ## Output
# MAGIC Raw CSVs confirmed present in `/Volumes/main/healthcare_claims_util/raw_data/`

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/main/healthcare_claims_util/raw_data")
for f in files:
    print(f"{f.name:45} {f.size / 1024 / 1024:.2f} MB")

# COMMAND ----------

beneficiary_preview = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/main/healthcare_claims_util/raw_data/Train_Beneficiarydata*.csv")
)
beneficiary_preview.printSchema()
display(beneficiary_preview.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

print("Total rows:", beneficiary_preview.count())
print("Non-null BeneID rows:", beneficiary_preview.filter(col("BeneID").isNotNull()).count())

# COMMAND ----------

files = dbutils.fs.ls("/Volumes/main/healthcare_claims_util/raw_data")
for f in files:
    print(f.path)

# COMMAND ----------

for name in ["Train-1542865627584.csv",
             "Train_Inpatientdata-1542865627584.csv",
             "Train_Outpatientdata-1542865627584.csv"]:
    path = f"/Volumes/main/healthcare_claims_util/raw_data/{name}"
    df = spark.read.option("header", "true").csv(path)
    print(name, "-> total rows:", df.count(), "| non-empty rows:", df.dropna(how="all").count())

# COMMAND ----------

path = "/Volumes/main/healthcare_claims_util/raw_data/Train_Beneficiarydata-1542865627584.csv"
df = spark.read.option("header", "true").csv(path)
print("total rows:", df.count(), "| non-empty rows:", df.dropna(how="all").count())
df.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality note
# MAGIC The initial upload of Train_Beneficiarydata included a malformed file
# MAGIC (`-selected-columns` suffix) with a valid header but entirely empty rows.
# MAGIC Root cause: exported from Kaggle's in-browser data preview/column-selector
# MAGIC rather than the main dataset Download button. Fixed by re-downloading via
# MAGIC the primary Download link and re-uploading. All 4 source files now verified:
# MAGIC
# MAGIC | File | Rows | Status |
# MAGIC |---|---|---|
# MAGIC | Train_Beneficiarydata | 138,556 | ✅ verified |
# MAGIC | Train_Inpatientdata | 40,474 | ✅ verified |
# MAGIC | Train_Outpatientdata | 517,737 | ✅ verified |
# MAGIC | Train (fraud labels) | 5,410 | ✅ verified |