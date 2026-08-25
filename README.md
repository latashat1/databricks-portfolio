# Databricks Data Engineering Portfolio

End-to-end data engineering projects built on Databricks, demonstrating medallion
architecture, Autoloader, Structured Streaming, Lakeflow Declarative Pipelines,
Databricks Asset Bundles (CI/CD), Workflows/Jobs orchestration, and Genie/Lakeview
dashboards.

## Projects

### [01 - Healthcare Claims & Utilization](./01_healthcare_claims_util)
Medicare-style claims and utilization analytics: bronze/silver/gold medallion
pipeline, provider fraud-risk ML feature table, scheduled Job orchestration,
Lakeflow Declarative Pipeline (alternate implementation), Lakeview dashboard,
and a Genie natural-language query space.

![Healthcare Dashboard](./01_healthcare_claims_util/dashboards/healthcare_dashboard.png)

### [02 - Property & Casualty Underwriting & Claims](./02_pc_underwriting_claims)
Underwriting risk factor and loss experience analytics on real Wisconsin
government property insurance data: Lakeflow Declarative Pipeline as the
primary implementation, AI-assisted dashboard authoring, DABs pipeline
deployment, and a Genie space.

![P&C Dashboard](./02_pc_underwriting_claims/dashboards/pc_dashboard.png)
