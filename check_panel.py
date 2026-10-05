"""Report which assets and months in a panel.csv are measured, missing or unreliable.

Usage: python3 check_panel.py paper_research/panel.csv
"""

import csv
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "paper_research/panel.csv"
rows = list(csv.DictReader(open(path, encoding="utf-8")))
by_asset = defaultdict(list)
for row in rows:
    by_asset[row["symbol"]].append(row)

complete = True
for symbol, months in by_asset.items():
    missing = [r["window_start"][:7] for r in months if r["missing"]]
    unreconciled = months[0]["reconciled"] not in ("true", "True")
    measured = len(months) - len(missing)
    status = "OK"
    if missing:
        status, complete = f"MISSING {len(missing)} months ({months[0]['missing']})", False
    elif unreconciled:
        status, complete = "measured, but supply check FAILED (ownership columns empty)", False
    print(f"{symbol:8} {measured}/{len(months)} months measured   {status}")

print("\nAll assets complete." if complete else "\nNot complete yet: rerun the paper command.")
