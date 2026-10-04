"""
Simulate a day of CRM activity, to prove the incremental Airflow extraction
only lands what changed (Project 5).

  py simulate_crm_activity.py             PREVIEW ONLY - decides what it would do, changes nothing
  py simulate_crm_activity.py --apply      makes the changes in Salesforce

What it does, and why each part is safe to run against the real org:
  - Closes some open Opportunities as Won or Lost (an UPDATE -> bumps SystemModstamp)
  - Deletes a couple of open Opportunities (a plain delete, no parent Account touched,
    so nothing repeats the hard-delete-by-cascade we found after the first full run)
  - Inserts a few brand-new Opportunities, attached to existing Accounts
  - Moves a few Leads to a new Status (an UPDATE)
  - Inserts a couple of brand-new Leads
No Account is ever created, updated, or deleted by this script.
Every changed/created/deleted Id is logged to sim_activity_log.json for reference.
"""
import json
import os
import random
import sys
from datetime import date, timedelta

SEED = 7
N_CLOSE = 8
N_DELETE_OPPS = 2
N_NEW_OPPS = 5
N_LEAD_STATUS_CHANGES = 5
N_NEW_LEADS = 3
LOG_FILE = "sim_activity_log.json"
DESC = "Synthetic demo data (seed 42) - simulated activity"

OPEN_STAGES = ["Prospecting", "Qualification", "Needs Analysis", "Value Proposition",
               "Id. Decision Makers", "Perception Analysis", "Proposal/Price Quote",
               "Negotiation/Review"]
REPS = ["Avery Chen", "Marcus Bell", "Priya Nair", "Diego Alvarez", "Hannah Okafor", "Tom Brennan"]
SOURCES = ["Employee Referral", "External Referral", "Partner", "Phone Inquiry",
          "Web", "Trade Show", "Purchased List"]
LEAD_STATUSES_FORWARD = {          # a plausible next status for each current one
    "Open - Not Contacted": "Working - Contacted",
    "Working - Contacted": "Closed - Converted",
}
FIRST = ["Jordan", "Riley", "Sam", "Kai", "Reese"]
LAST = ["Park", "Donnelly", "Abara", "Lindqvist", "Marsh"]

rng = random.Random(SEED)


def connect():
    from simple_salesforce import Salesforce
    return Salesforce(
        consumer_key=os.environ["SF_CONSUMER_KEY"],
        consumer_secret=os.environ["SF_CONSUMER_SECRET"],
        domain=os.environ["SF_DOMAIN"],
    )


def plan(sf):
    open_opps = sf.query_all(
        "SELECT Id, Name, Amount FROM Opportunity WHERE StageName NOT IN "
        "('Closed Won','Closed Lost') ORDER BY Id"
    )["records"]
    if len(open_opps) < N_CLOSE + N_DELETE_OPPS:
        raise SystemExit(f"Only {len(open_opps)} open opportunities found; "
                         f"need at least {N_CLOSE + N_DELETE_OPPS}.")
    pool = open_opps[:]
    rng.shuffle(pool)
    to_close = pool[:N_CLOSE]
    to_delete = pool[N_CLOSE:N_CLOSE + N_DELETE_OPPS]

    account_ids = [r["Id"] for r in sf.query_all("SELECT Id FROM Account")["records"]]
    new_opps = []
    for i in range(N_NEW_OPPS):
        new_opps.append({
            "Name": f"Simulated New Deal {i + 1:02d}",
            "AccountId": rng.choice(account_ids),
            "StageName": rng.choice(OPEN_STAGES[:3]),   # early-stage, since these are brand new
            "CloseDate": (date.today() + timedelta(days=rng.randint(20, 90))).isoformat(),
            "Amount": round(rng.uniform(8000, 200000) / 500) * 500,
            "LeadSource": rng.choice(SOURCES),
            "Sales_Rep__c": rng.choice(REPS),
            "Description": DESC,
        })

    lead_soql = (
        "SELECT Id, Status FROM Lead "
        "WHERE Status IN ('Open - Not Contacted', 'Working - Contacted') "
        "ORDER BY Id"
    )
    open_leads = sf.query_all(lead_soql)["records"]
    lead_updates = []
    for r in rng.sample(open_leads, min(N_LEAD_STATUS_CHANGES, len(open_leads))):
        nxt = LEAD_STATUSES_FORWARD.get(r["Status"])
        if nxt:
            lead_updates.append({"Id": r["Id"], "Status": nxt})

    new_leads = []
    for i in range(N_NEW_LEADS):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        new_leads.append({
            "FirstName": first, "LastName": last, "Company": f"New Prospect {i + 1:02d} Inc",
            "Email": f"{first.lower()}.{last.lower()}.sim{i}@example.com",
            "Status": "Open - Not Contacted", "LeadSource": rng.choice(SOURCES),
            "Description": DESC,
        })

    return {"close": to_close, "delete": to_delete, "new_opps": new_opps,
            "lead_updates": lead_updates, "new_leads": new_leads}


def describe(p):
    print(f"Opportunities to CLOSE:  {len(p['close'])}")
    for r in p["close"]:
        print(f"    {r['Id']}  {r['Name']}")
    print(f"Opportunities to DELETE: {len(p['delete'])}  (Accounts are never touched)")
    for r in p["delete"]:
        print(f"    {r['Id']}  {r['Name']}")
    print(f"New Opportunities to INSERT: {len(p['new_opps'])}")
    for r in p["new_opps"]:
        print(f"    {r['Name']}  ({r['Sales_Rep__c']}, ${r['Amount']:,.0f})")
    print(f"Leads to move forward a status: {len(p['lead_updates'])}")
    print(f"New Leads to INSERT: {len(p['new_leads'])}")


def apply(sf, p):
    log = {"closed": [], "deleted": [], "new_opportunities": [],
           "lead_status_changes": [], "new_leads": []}

    for r in p["close"]:
        won = rng.random() < 0.35
        sf.Opportunity.update(r["Id"], {"StageName": "Closed Won" if won else "Closed Lost",
                                        "CloseDate": date.today().isoformat()})
        log["closed"].append({"Id": r["Id"], "result": "Won" if won else "Lost"})
    print(f"Closed {len(p['close'])} opportunities.")

    for r in p["delete"]:
        sf.Opportunity.delete(r["Id"])
        log["deleted"].append(r["Id"])
    print(f"Deleted {len(p['delete'])} opportunities.")

    for rec in p["new_opps"]:
        res = sf.Opportunity.create(rec)
        log["new_opportunities"].append(res["id"])
    print(f"Inserted {len(p['new_opps'])} new opportunities.")

    for u in p["lead_updates"]:
        sf.Lead.update(u["Id"], {"Status": u["Status"]})
        log["lead_status_changes"].append(u)
    print(f"Updated {len(p['lead_updates'])} lead statuses.")

    for rec in p["new_leads"]:
        res = sf.Lead.create(rec)
        log["new_leads"].append(res["id"])
    print(f"Inserted {len(p['new_leads'])} new leads.")

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)
    print(f"\nActivity log written to {LOG_FILE}")

    touched = (len(p["close"]) + len(p["delete"]) + len(p["new_opps"])
              + len(p["lead_updates"]) + len(p["new_leads"]))
    print(f"\nExpect the next incremental run to land approximately {touched} changed/new "
          f"records across Opportunity and Lead, and 0 for Account, Contact, and Campaign.")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    sf = connect()
    p = plan(sf)
    describe(p)
    if "--apply" not in argv:
        print("\nPreview only. Nothing was changed. Re-run with --apply to proceed.")
        return
    apply(sf, p)


if __name__ == "__main__":
    main()
