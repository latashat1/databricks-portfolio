"""
Connectivity check for Project 5.

Proves that tasks running INSIDE Airflow can reach Salesforce and Databricks
through the encrypted Airflow Connections. Manual trigger only, read-only:
nothing is written to either platform.
"""
from datetime import datetime

from airflow.sdk import BaseHook, dag, task

SALESFORCE_CONN_ID = "salesforce_dev_org"
DATABRICKS_CONN_ID = "databricks_free_edition"


@dag(
    dag_id="sf_databricks_connectivity_check",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["project5", "connectivity"],
)
def sf_databricks_connectivity_check():

    @task
    def check_salesforce() -> dict:
        # Heavy imports live inside the task so the scheduler's DAG parsing stays fast.
        from simple_salesforce import Salesforce

        conn = BaseHook.get_connection(SALESFORCE_CONN_ID)
        host = (conn.host or "").replace("https://", "").replace("http://", "").strip("/")
        domain = host.removesuffix(".salesforce.com")   # e.g. <org>-dev-ed.develop.my

        sf = Salesforce(
            consumer_key=conn.login,
            consumer_secret=conn.password,
            domain=domain,
        )
        counts = {}
        for obj in ["Account", "Contact", "Lead", "Opportunity", "Campaign"]:
            counts[obj] = sf.query(f"SELECT COUNT() FROM {obj}")["totalSize"]
        print("Salesforce record counts:", counts)
        return counts

    @task
    def check_databricks() -> int:
        from airflow.providers.databricks.hooks.databricks import DatabricksHook

        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        jobs = hook.list_jobs(limit=25)
        names = sorted(j.get("settings", {}).get("name", "(unnamed)") for j in jobs)
        print(f"Databricks jobs visible to this token: {len(jobs)}")
        for name in names:
            print("  -", name)
        return len(jobs)

    # No dependency between them: they run in parallel and fail independently.
    check_salesforce()
    check_databricks()


sf_databricks_connectivity_check()
