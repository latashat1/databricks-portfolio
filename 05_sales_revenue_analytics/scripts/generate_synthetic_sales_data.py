"""
Dry-run generator for synthetic Salesforce sales data (Project 5).
Writes CSV files locally and prints summary statistics. Does NOT touch Salesforce.
Standard library only. Fixed random seed => reproducible.
"""
import csv
import math
import os
import random
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

SEED = 42
AS_OF = date(2026, 9, 19)          # "today" for the dataset
OPP_START = date(2025, 1, 1)
OPP_END = AS_OF - timedelta(days=2)
RECENT_SHARE = 0.22                 # share of deals created in the last 100 days
RECENT_DAYS = 100
N_ACCOUNTS = 55
N_OPPS = 400
N_LEADS = 500
OUT_DIR = "synthetic_output"
DESC = "Synthetic demo data (seed 42)"

rng = random.Random(SEED)

# ---- Reference data --------------------------------------------------------
REPS = [
    # name, win-rate multiplier, deal-size multiplier, share of deals
    ("Avery Chen",     1.25, 1.10, 0.22),
    ("Marcus Bell",    1.10, 1.00, 0.20),
    ("Priya Nair",     1.00, 1.30, 0.18),
    ("Diego Alvarez",  0.95, 0.90, 0.16),
    ("Hannah Okafor",  0.85, 1.00, 0.14),
    ("Tom Brennan",    0.75, 0.80, 0.10),
]
# lead source, base win rate, share of deals (values exist in this org's picklist)
SOURCES = [
    ("Employee Referral", 0.48, 0.10),
    ("External Referral", 0.40, 0.13),
    ("Partner",           0.38, 0.15),
    ("Phone Inquiry",     0.30, 0.10),
    ("Web",               0.28, 0.25),
    ("Trade Show",        0.24, 0.12),
    ("Purchased List",    0.12, 0.15),
]
NULL_SOURCE_RATE = 0.05
NULL_SOURCE_WIN = 0.22
OPEN_STAGES = ["Prospecting", "Qualification", "Needs Analysis", "Value Proposition",
               "Id. Decision Makers", "Perception Analysis", "Proposal/Price Quote",
               "Negotiation/Review"]
INDUSTRIES = ["Technology", "Finance", "Healthcare", "Retail", "Manufacturing",
              "Energy", "Education", "Insurance"]
PREFIX = ["Summit", "Blue Harbor", "Ironwood", "Clearwater", "Redwood", "Silverline",
          "Pioneer", "Brightpath", "Granite", "Lakeshore", "Evergreen", "Cobalt",
          "Meridian", "Harborview", "Northgate", "Falcon", "Willow", "Crestview"]
SUFFIX = ["Systems", "Partners", "Industries", "Logistics", "Health", "Financial",
          "Energy", "Retail Group", "Labs", "Holdings", "Networks", "Foods"]
FIRST = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Drew",
         "Cameron", "Quinn", "Sam", "Robin", "Avery", "Kai", "Noel", "Reese"]
LAST = ["Nguyen", "Patel", "Garcia", "Kim", "Johnson", "Rossi", "Okoye", "Silva",
        "Miller", "Haddad", "Novak", "Larsen", "Ibrahim", "Cohen", "Reyes", "Walsh"]


def iso(d, hour=None):
    h = rng.randint(8, 18) if hour is None else hour
    m = rng.randint(0, 59)
    return datetime(d.year, d.month, d.day, h, m, 0).strftime("%Y-%m-%dT%H:%M:%SZ")


def rand_date(start, end, mode=None):
    span = (end - start).days
    frac = rng.triangular(0, 1, mode) if mode is not None else rng.random()
    return start + timedelta(days=int(frac * span))


def weighted(items, weight_index):
    return rng.choices(items, weights=[i[weight_index] for i in items], k=1)[0]


def company_names(n):
    combos = [f"{p} {s}" for p in PREFIX for s in SUFFIX]
    rng.shuffle(combos)
    return combos[:n]


# ---- Accounts --------------------------------------------------------------
accounts = []
for i, name in enumerate(company_names(N_ACCOUNTS)):
    accounts.append({
        "account_idx": i,
        "Name": name,
        "Industry": rng.choice(INDUSTRIES),
        "CreatedDate": iso(rand_date(date(2024, 9, 1), date(2024, 12, 31))),
        "Description": DESC,
    })

# ---- Opportunities ---------------------------------------------------------
opps = []
for i in range(N_OPPS):
    rep = weighted(REPS, 3)
    if rng.random() < NULL_SOURCE_RATE:
        source, base_win = "", NULL_SOURCE_WIN
    else:
        s = weighted(SOURCES, 2)
        source, base_win = s[0], s[1]

    if rng.random() < RECENT_SHARE:
        created = rand_date(AS_OF - timedelta(days=RECENT_DAYS), OPP_END)
    else:
        created = rand_date(OPP_START, OPP_END)
    amount = round(rng.lognormvariate(math.log(60000), 0.9) * rep[2] / 500) * 500
    amount = max(5000, min(900000, amount))

    win_p = base_win * rep[1]
    if amount > 250000:
        win_p *= 0.8
    elif amount < 20000:
        win_p *= 1.1
    win_p = max(0.03, min(0.85, win_p))
    will_win = rng.random() < win_p

    cycle = rng.lognormvariate(math.log(50), 0.5) * (amount / 60000) ** 0.15
    if not will_win:
        cycle *= 0.8
    cycle = int(max(7, min(240, cycle)))
    close = created + timedelta(days=cycle)

    if close <= AS_OF:
        stage = "Closed Won" if will_win else "Closed Lost"
    else:
        progress = (AS_OF - created).days / cycle
        idx = int(progress * len(OPEN_STAGES) + rng.uniform(-0.5, 0.5))
        stage = OPEN_STAGES[max(0, min(len(OPEN_STAGES) - 1, idx))]
        if stage in ("Prospecting", "Qualification") and rng.random() < 0.20:
            amount = ""          # early-stage deals sometimes have no amount yet

    opps.append({
        "Name": "",
        "account_idx": rng.randrange(N_ACCOUNTS),
        "StageName": stage,
        "Amount": amount,
        "CloseDate": close.isoformat(),
        "LeadSource": source,
        "Sales_Rep__c": rep[0],
        "CreatedDate": iso(created),
        "Description": DESC,
    })
# name each opportunity after its own account
for n, o in enumerate(opps, start=1):
    o["Name"] = f"{accounts[o['account_idx']]['Name']} - Opp {n:03d}"

# ---- Leads -----------------------------------------------------------------
leads = []
lead_companies = company_names(N_LEADS)
for i in range(N_LEADS):
    created = rand_date(date(2025, 1, 1), date(2026, 9, 10))
    age = (AS_OF - created).days
    if age > 180:
        weights = [0.05, 0.40, 0.55]
    elif age > 60:
        weights = [0.20, 0.55, 0.25]
    else:
        weights = [0.50, 0.45, 0.05]
    status = rng.choices(["Open - Not Contacted", "Working - Contacted",
                          "Closed - Not Converted"], weights=weights, k=1)[0]
    first, last = rng.choice(FIRST), rng.choice(LAST)
    leads.append({
        "FirstName": first,
        "LastName": last,
        "Company": lead_companies[i % len(lead_companies)],
        "Email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "Status": status,
        "LeadSource": weighted(SOURCES, 2)[0],
        "CreatedDate": iso(created),
        "Description": DESC,
    })

# ---- Write CSVs ------------------------------------------------------------
os.makedirs(OUT_DIR, exist_ok=True)
for fname, rows in [("accounts.csv", accounts), ("opportunities.csv", opps), ("leads.csv", leads)]:
    with open(os.path.join(OUT_DIR, fname), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

# ---- Summary ---------------------------------------------------------------
print(f"Accounts: {len(accounts)} | Opportunities: {len(opps)} | Leads: {len(leads)}")
closed = [o for o in opps if o["StageName"].startswith("Closed")]
won = [o for o in closed if o["StageName"] == "Closed Won"]
print(f"\nClosed: {len(closed)} (won {len(won)}, lost {len(closed) - len(won)}) | "
      f"Open: {len(opps) - len(closed)}")
print(f"Overall win rate (closed only): {len(won) / len(closed):.1%}")

print("\nWin rate by lead source (closed deals):")
by = defaultdict(lambda: [0, 0])
for o in closed:
    k = o["LeadSource"] or "(none)"
    by[k][0] += 1
    by[k][1] += o["StageName"] == "Closed Won"
for k, (n, w_) in sorted(by.items(), key=lambda kv: -kv[1][1] / kv[1][0]):
    print(f"  {k:<18} closed={n:>3}  won={w_:>3}  win rate={w_ / n:.0%}")

print("\nWin rate by rep (closed deals):")
byr = defaultdict(lambda: [0, 0, 0])
for o in closed:
    byr[o["Sales_Rep__c"]][0] += 1
    byr[o["Sales_Rep__c"]][1] += o["StageName"] == "Closed Won"
    if o["StageName"] == "Closed Won":
        byr[o["Sales_Rep__c"]][2] += int(o["Amount"])
for k, (n, w_, amt) in sorted(byr.items()):
    print(f"  {k:<15} closed={n:>3}  won={w_:>3}  win rate={w_ / n:.0%}  won $={amt:,}")

print("\nOpen deals by stage:")
oc = Counter(o["StageName"] for o in opps if not o["StageName"].startswith("Closed"))
for s in OPEN_STAGES:
    print(f"  {s:<22} {oc.get(s, 0)}")

cyc = []
for o in won:
    c = datetime.strptime(o["CreatedDate"], "%Y-%m-%dT%H:%M:%SZ").date()
    cyc.append((date.fromisoformat(o["CloseDate"]) - c).days)
cyc.sort()
print(f"\nWon-deal cycle days: min={cyc[0]} median={cyc[len(cyc)//2]} max={cyc[-1]}")
print(f"Opps with blank LeadSource: {sum(1 for o in opps if not o['LeadSource'])}")
print(f"Opps with blank Amount: {sum(1 for o in opps if o['Amount'] == '')}")
lc = Counter(l["Status"] for l in leads)
print(f"Lead statuses: {dict(lc)}")
mo = Counter(o["CreatedDate"][:7] for o in opps)
print(f"Opps created per month: min={min(mo.values())} max={max(mo.values())} "
      f"(months={len(mo)})")
print(f"\nCSV files written to ./{OUT_DIR}/")
