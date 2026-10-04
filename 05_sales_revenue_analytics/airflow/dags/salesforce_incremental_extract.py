"""
Incremental Salesforce extraction for Project 5.

For each Salesforce object (mapped tasks, run in parallel):
  1. read the object's watermark from an Airflow Variable
  2. query records changed since then (everything on the first run or a full refresh),
     INCLUDING soft-deleted records (the IsDeleted flag is preserved)
  3. upload the new records as JSON Lines to the Databricks volume (Files API)
  4. only AFTER a successful upload, advance the watermark
A final task confirms each landed file exists with the expected size.

Watermark = newest SystemModstamp landed + the record Ids landed at exactly that
timestamp. Salesforce timestamps have one-second resolution, so many records can
share the newest second; remembering their Ids lets us query with >= (never miss a
record) without re-landing the ones we already have (no repeats).
"""
import json
import re
from datetime import datetime, timedelta

import requests
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task

SALESFORCE_CONN_ID = "salesforce_dev_org"
DATABRICKS_CONN_ID = "databricks_free_edition"
# Numeric Databricks job id for sales_revenue_analytics_etl_job (dev target), from
# `databricks bundle summary -t dev` after deployment. DABs adds a "[dev <user>]" prefix
# to the job's display name in dev mode, so triggering by exact name is fragile; the
# numeric id is stable regardless of that prefix.
DATABRICKS_JOB_ID = "56774962363249"
OBJECTS = ["Account", "Contact", "Lead", "Opportunity", "Campaign"]
VOLUME_ROOT = "/Volumes/main/sales_revenue_analytics/raw_data/salesforce"
SKIP_FIELD_TYPES = {"address", "location", "base64"}   # compound / binary fields


# ---- Connections -----------------------------------------------------------
def strip_host(host):
    """Return a bare hostname (no scheme, no path, no trailing slash), the same
    way the Databricks provider's own DatabricksHook._parse_host does it, so any
    reasonable stored form (with scheme, without scheme, with a typo'd or missing
    scheme, with a trailing slash) is handled the same, robust way instead of by
    a fragile literal string replace.
    """
    from urllib.parse import urlsplit

    raw = (host or "").strip()
    hostname = urlsplit(raw).hostname if "://" in raw else urlsplit("//" + raw).hostname
    clean = hostname or raw.strip("/")
    if not clean or "://" in clean or "." not in clean:
        raise ValueError(
            f"Databricks connection host does not look like a real hostname: {raw!r} "
            f"(parsed to {clean!r}). Check the Host field on the Airflow Connection."
        )
    return clean


def salesforce_client():
    from simple_salesforce import Salesforce

    conn = BaseHook.get_connection(SALESFORCE_CONN_ID)
    domain = strip_host(conn.host).removesuffix(".salesforce.com")
    return Salesforce(consumer_key=conn.login, consumer_secret=conn.password, domain=domain)


def databricks_credentials():
    conn = BaseHook.get_connection(DATABRICKS_CONN_ID)
    return strip_host(conn.host), conn.password


# ---- Query building --------------------------------------------------------
def queryable_fields(describe):
    names = [f["name"] for f in describe["fields"] if f["type"] not in SKIP_FIELD_TYPES]
    for required in ("Id", "SystemModstamp", "IsDeleted"):
        if required not in names:
            raise ValueError(f"required field {required} missing from describe output")
    return names


def soql_datetime(stamp):
    """Salesforce returns 2026-09-19T20:00:00.000+0000; SOQL literals need ...Z, unquoted."""
    return stamp.replace("+0000", "Z")


def build_soql(sobject, fields, state):
    soql = f"SELECT {', '.join(fields)} FROM {sobject}"
    if state:
        soql += f" WHERE SystemModstamp >= {soql_datetime(state['stamp'])}"
    return soql + " ORDER BY SystemModstamp"


# ---- Watermark logic -------------------------------------------------------
def watermark_key(sobject):
    return f"sf_watermark_{sobject}"


def select_new_records(records, state):
    """Drop records already landed at the boundary timestamp."""
    if not state:
        return list(records)
    seen = set(state["ids"])
    return [r for r in records
            if not (r["SystemModstamp"] == state["stamp"] and r["Id"] in seen)]


def next_state(records):
    stamp = max(r["SystemModstamp"] for r in records)
    return {"stamp": stamp,
            "ids": sorted(r["Id"] for r in records if r["SystemModstamp"] == stamp)}


# ---- Serialisation and landing --------------------------------------------
def clean_records(records):
    return [{k: v for k, v in r.items() if k != "attributes"} for r in records]


def to_jsonl(records):
    return ("\n".join(json.dumps(r, ensure_ascii=True) for r in records) + "\n").encode("utf-8")


def safe_name(run_id):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def landing_path(sobject, run_id):
    name = sobject.lower()
    return f"{VOLUME_ROOT}/{name}/{name}_{safe_name(run_id)}.jsonl"


def upload_file(host, token, path, payload):
    headers = {"Authorization": f"Bearer {token}"}
    directory = path.rsplit("/", 1)[0] + "/"
    r = requests.put(f"https://{host}/api/2.0/fs/directories{directory}",
                     headers=headers, timeout=60)
    if not r.ok:
        raise RuntimeError(f"creating {directory} failed: {r.status_code} {r.text[:200]}")
    r = requests.put(f"https://{host}/api/2.0/fs/files{path}",
                     headers={**headers, "Content-Type": "application/octet-stream"},
                     params={"overwrite": "true"}, data=payload, timeout=120)
    if not r.ok:
        raise RuntimeError(f"upload to {path} failed: {r.status_code} {r.text[:200]}")


def landed_size(host, token, path):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://{host}/api/2.0/fs/files{path}"
    r = requests.head(url, headers=headers, timeout=60)
    if r.ok and "Content-Length" in r.headers:
        return int(r.headers["Content-Length"])
    r = requests.get(url, headers=headers, timeout=120)      # fallback: measure the bytes
    return len(r.content) if r.ok else -1


# ---- The two units of work -------------------------------------------------
def extract_object(sf_object, run_id, full_refresh):
    key = watermark_key(sf_object)
    state = None if full_refresh else Variable.get(key, default=None, deserialize_json=True)

    sf = salesforce_client()
    fields = queryable_fields(getattr(sf, sf_object).describe())
    soql = build_soql(sf_object, fields, state)
    print(f"{sf_object}: {'FULL' if state is None else 'incremental from ' + state['stamp']}"
          f" | {len(fields)} fields")

    fetched = clean_records(sf.query_all(soql, include_deleted=True)["records"])
    new = select_new_records(fetched, state)
    summary = {"object": sf_object, "fetched": len(fetched), "landed": len(new),
               "deleted_flagged": sum(1 for r in new if r["IsDeleted"]),
               "path": None, "bytes": 0,
               "watermark": state["stamp"] if state else None}

    if not new:
        print(f"{sf_object}: no new or changed records; nothing to land.")
        return summary

    payload = to_jsonl(new)
    path = landing_path(sf_object, run_id)
    host, token = databricks_credentials()
    upload_file(host, token, path, payload)

    new_state = next_state(fetched)              # only reached if the upload succeeded
    Variable.set(key, new_state, serialize_json=True)
    summary.update(path=path, bytes=len(payload), watermark=new_state["stamp"])
    print(f"{sf_object}: landed {len(new)} records "
          f"({summary['deleted_flagged']} soft-deleted) -> {path}")
    return summary


def verify_summaries(summaries):
    host, token = databricks_credentials()
    problems = []
    print(f"{'object':<12}{'fetched':>8}{'landed':>8}{'deleted':>8}{'bytes':>10}")
    for s in summaries:
        print(f"{s['object']:<12}{s['fetched']:>8}{s['landed']:>8}"
              f"{s['deleted_flagged']:>8}{s['bytes']:>10}")
        if s["landed"] == 0:
            continue
        size = landed_size(host, token, s["path"])
        if size != s["bytes"]:
            problems.append(f"{s['object']}: expected {s['bytes']} bytes at {s['path']}, found {size}")
    if problems:
        raise RuntimeError("landing verification failed: " + "; ".join(problems))
    print("All landed files verified.")
    return summaries


# ---- The DAG ---------------------------------------------------------------
@dag(
    dag_id="salesforce_incremental_extract",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,            # overlapping runs could corrupt the watermarks
    params={"full_refresh": Param(False, type="boolean",
                                  description="Ignore watermarks and re-extract everything")},
    tags=["project5", "salesforce", "incremental"],
)
def salesforce_incremental_extract():

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract_and_land(sf_object: str) -> dict:
        ctx = get_current_context()
        return extract_object(sf_object, ctx["run_id"],
                              bool(ctx["params"].get("full_refresh", False)))

    @task
    def verify_landing(summaries):
        return verify_summaries(list(summaries))

    landed = verify_landing(extract_and_land.expand(sf_object=OBJECTS))

    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="trigger_databricks_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        wait_for_termination=True,   # task stays running until bronze->silver->gold finishes
        polling_period_seconds=30,
    )

    landed >> trigger_databricks_job


salesforce_incremental_extract()
