"""
Remove the Developer Edition sample Opportunities and Leads (Project 5).

  py delete_seed_data.py             PREVIEW ONLY - reads, shows what would be deleted
  py delete_seed_data.py --delete    deletes, but only if counts match the profile

What counts as "seed" data:
  Opportunity: Sales_Rep__c is blank (all synthetic opportunities have a rep)
  Lead:        Email does not end in @example.com (all synthetic leads use example.com)
Accounts, Contacts and Campaigns are NOT touched.
"""
import os
import sys
from collections import Counter

EXPECTED_OPPS = 31     # from the earlier profile of the org
EXPECTED_LEADS = 22
BATCH = 200


def connect():
    from simple_salesforce import Salesforce
    return Salesforce(
        consumer_key=os.environ["SF_CONSUMER_KEY"],
        consumer_secret=os.environ["SF_CONSUMER_SECRET"],
        domain=os.environ["SF_DOMAIN"],
    )


def find_seed(sf):
    opps = [r for r in sf.query_all(
        "SELECT Id, Name, StageName, CreatedDate, Sales_Rep__c FROM Opportunity")["records"]
        if not r["Sales_Rep__c"]]
    leads = [r for r in sf.query_all(
        "SELECT Id, Name, Email, Status, CreatedDate FROM Lead")["records"]
        if not (r["Email"] or "").endswith("@example.com")]
    return opps, leads


def preview(opps, leads):
    print(f"Seed opportunities found: {len(opps)} (expected {EXPECTED_OPPS})")
    print("  by stage:", dict(Counter(o["StageName"] for o in opps)))
    if opps:
        print("  CreatedDate range:", min(o["CreatedDate"] for o in opps), "to",
              max(o["CreatedDate"] for o in opps))
    print(f"Seed leads found: {len(leads)} (expected {EXPECTED_LEADS})")
    print("  by status:", dict(Counter(l["Status"] for l in leads)))
    print("  first few:", [(l["Name"], l["Email"]) for l in leads[:4]])


def delete(sf, sobject, ids):
    ok, problems = 0, []
    for i in range(0, len(ids), BATCH):
        chunk = ids[i:i + BATCH]
        for r in sf.restful("composite/sobjects", method="DELETE",
                            params={"ids": ",".join(chunk), "allOrNone": "false"}):
            if r.get("success"):
                ok += 1
            else:
                problems.append(r.get("errors"))
    print(f"  {sobject}: deleted {ok} of {len(ids)}")
    return problems


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    sf = connect()
    opps, leads = find_seed(sf)
    preview(opps, leads)
    if "--delete" not in argv:
        print("\nPreview only. Nothing was deleted. Re-run with --delete to proceed.")
        return
    if len(opps) != EXPECTED_OPPS or len(leads) != EXPECTED_LEADS:
        print("\nSTOPPED: counts do not match what was profiled earlier. Nothing deleted.")
        sys.exit(1)
    print("\nDeleting...")
    problems = delete(sf, "Opportunity", [o["Id"] for o in opps])
    problems += delete(sf, "Lead", [l["Id"] for l in leads])
    if problems:
        print("Some deletes failed:", problems[:3])
    left_o, left_l = find_seed(sf)
    print(f"\nAfter deletion: seed opportunities left={len(left_o)}, seed leads left={len(left_l)}")


if __name__ == "__main__":
    main()
