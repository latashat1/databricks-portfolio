"""
Load the synthetic CSVs into Salesforce (Project 5).

Modes:
  py load_synthetic_to_salesforce.py --test      ~10 opps + their accounts + 5 leads
  py load_synthetic_to_salesforce.py --full      everything in synthetic_output/
  py load_synthetic_to_salesforce.py --cleanup   delete every record this script loaded

Credentials come from the same environment variables as the earlier test scripts:
SF_CONSUMER_KEY, SF_CONSUMER_SECRET, SF_DOMAIN.
Every record created is logged to loaded_ids.json so cleanup is exact.
"""
import argparse
import csv
import json
import os
import sys

CSV_DIR = "synthetic_output"
IDS_FILE = "loaded_ids.json"
BATCH = 200          # composite sObject collections accept up to 200 records per call


def connect():
    from simple_salesforce import Salesforce
    return Salesforce(
        consumer_key=os.environ["SF_CONSUMER_KEY"],
        consumer_secret=os.environ["SF_CONSUMER_SECRET"],
        domain=os.environ["SF_DOMAIN"],
    )


def read_csv(name):
    with open(os.path.join(CSV_DIR, name), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---- Row -> Salesforce payload ---------------------------------------------
def build_account(r):
    return {"Name": r["Name"], "Industry": r["Industry"],
            "CreatedDate": r["CreatedDate"], "Description": r["Description"]}


def build_opp(r, account_id):
    rec = {"Name": r["Name"], "AccountId": account_id, "StageName": r["StageName"],
           "CloseDate": r["CloseDate"], "CreatedDate": r["CreatedDate"],
           "Sales_Rep__c": r["Sales_Rep__c"], "Description": r["Description"]}
    if r["Amount"] != "":
        rec["Amount"] = float(r["Amount"])
    if r["LeadSource"] != "":
        rec["LeadSource"] = r["LeadSource"]
    return rec


def build_lead(r):
    return {"FirstName": r["FirstName"], "LastName": r["LastName"], "Company": r["Company"],
            "Email": r["Email"], "Status": r["Status"], "LeadSource": r["LeadSource"],
            "CreatedDate": r["CreatedDate"], "Description": r["Description"]}


# ---- ID bookkeeping --------------------------------------------------------
def load_ids():
    if os.path.exists(IDS_FILE):
        with open(IDS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {"Account": [], "Opportunity": [], "Lead": []}


def save_ids(loaded):
    with open(IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(loaded, f, indent=2)


def fail(msg):
    print("\nSTOPPED: " + msg)
    print("Records loaded before this point are logged in " + IDS_FILE +
          ". Run with --cleanup to remove them.")
    sys.exit(1)


# ---- Insert / delete -------------------------------------------------------
def insert_records(sf, sobject, records, loaded):
    ids = []
    for n, i in enumerate(range(0, len(records), BATCH), start=1):
        chunk = records[i:i + BATCH]
        payload = {"allOrNone": True,
                   "records": [dict(r, attributes={"type": sobject}) for r in chunk]}
        try:
            results = sf.restful("composite/sobjects", method="POST", json=payload)
        except Exception as e:
            fail(f"{sobject} batch {n} raised an error: {getattr(e, 'content', e)}")
        bad = [r for r in results if not r.get("success")]
        if bad:
            errors = [(e.get("statusCode"), e.get("message"), e.get("fields"))
                      for r in bad for e in r.get("errors", [])]
            real = [e for e in errors if e[0] != "ALL_OR_NONE_OPERATION_ROLLED_BACK"]
            shown = sorted(set(map(str, real or errors)))[:5]
            fail(f"{sobject} batch {n}: {len(bad)} record(s) rejected, batch rolled back. "
                 f"Errors: {shown}")
        batch_ids = [r["id"] for r in results]
        ids.extend(batch_ids)
        loaded[sobject].extend(batch_ids)
        save_ids(loaded)
        print(f"  {sobject}: batch {n} inserted ({len(batch_ids)} records)")
    return ids


def delete_records(sf, sobject, ids):
    ok = 0
    problems = []
    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i + BATCH]
        results = sf.restful("composite/sobjects", method="DELETE",
                             params={"ids": ",".join(chunk), "allOrNone": "false"})
        for r in results:
            if r.get("success"):
                ok += 1
            else:
                problems.append(r.get("errors"))
    print(f"  {sobject}: deleted {ok} of {len(ids)}")
    return problems


# ---- Modes -----------------------------------------------------------------
def run_load(sf, test):
    if os.path.exists(IDS_FILE):
        print(f"{IDS_FILE} already exists, so a load has already happened.\n"
              "Run with --cleanup first, then load again.")
        sys.exit(1)

    accounts = read_csv("accounts.csv")
    opps = read_csv("opportunities.csv")
    leads = read_csv("leads.csv")
    if test:
        opps = opps[:10]
        needed = sorted({int(o["account_idx"]) for o in opps})
        leads = leads[:5]
    else:
        needed = list(range(len(accounts)))
    by_idx = {int(a["account_idx"]): a for a in accounts}

    loaded = {"Account": [], "Opportunity": [], "Lead": []}
    save_ids(loaded)
    print(f"Mode: {'TEST' if test else 'FULL'} | accounts={len(needed)} "
          f"opps={len(opps)} leads={len(leads)}")

    acct_ids = insert_records(sf, "Account", [build_account(by_idx[i]) for i in needed], loaded)
    id_for = dict(zip(needed, acct_ids))
    insert_records(sf, "Opportunity",
                   [build_opp(o, id_for[int(o["account_idx"])]) for o in opps], loaded)
    insert_records(sf, "Lead", [build_lead(l) for l in leads], loaded)
    verify(sf, loaded)


def verify(sf, loaded):
    print("\nVerification (queried back from Salesforce):")
    ids = "','".join(loaded["Account"])
    n_acct = sf.query(f"SELECT COUNT() FROM Account WHERE Id IN ('{ids}')")["totalSize"]
    print(f"  Accounts loaded and found: {n_acct} of {len(loaded['Account'])}")
    n_opp = sf.query("SELECT COUNT() FROM Opportunity WHERE Sales_Rep__c != null")["totalSize"]
    print(f"  Opportunities with a Sales Rep: {n_opp} (loaded {len(loaded['Opportunity'])})")
    n_lead = sf.query("SELECT COUNT() FROM Lead WHERE Email LIKE '%@example.com'")["totalSize"]
    print(f"  Leads with example.com email: {n_lead} (loaded {len(loaded['Lead'])})")
    for r in sf.query("SELECT StageName, COUNT(Id) cnt FROM Opportunity "
                      "WHERE Sales_Rep__c != null GROUP BY StageName")["records"]:
        print(f"    {r['StageName']}: {r['cnt']}")
    r = sf.query("SELECT MIN(CreatedDate) mn, MAX(CreatedDate) mx FROM Opportunity "
                 "WHERE Sales_Rep__c != null")["records"][0]
    print(f"  Opportunity CreatedDate range: {r['mn']} to {r['mx']}")


def run_cleanup(sf):
    if not os.path.exists(IDS_FILE):
        print(f"No {IDS_FILE} found, so there is nothing to clean up.")
        return
    loaded = load_ids()
    print("Cleaning up records logged in " + IDS_FILE)
    problems = []
    for sobject in ("Opportunity", "Lead", "Account"):     # children before parents
        if loaded.get(sobject):
            problems += delete_records(sf, sobject, loaded[sobject])
    if problems:
        print("Some deletes failed; keeping " + IDS_FILE + ". Details: " + str(problems[:3]))
        sys.exit(1)
    os.remove(IDS_FILE)
    print("Cleanup complete; " + IDS_FILE + " removed.")


def main(argv=None):
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--test", action="store_true")
    g.add_argument("--full", action="store_true")
    g.add_argument("--cleanup", action="store_true")
    a = p.parse_args(argv)
    sf = connect()
    if a.cleanup:
        run_cleanup(sf)
    else:
        run_load(sf, test=a.test)


if __name__ == "__main__":
    main()
