#!/usr/bin/env python3
"""
Checks for the 2026-11-11 Confidence rules (Tala's hand-check of the top 10).
Synthetic names and data only (the repo is public). No network, no database.

    python tests/test_morbius_confidence.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import morbius_sync as m  # noqa: E402

fails = []
NOW = datetime.now(timezone.utc)
FRESH = NOW.isoformat()
FUTURE = (NOW + timedelta(days=200)).date().isoformat()


def check(label, cond):
    if not cond:
        fails.append(label)


def conf(tags, raw, owner="DOE JANE Q"):
    return m.confidence({"source_tags": tags, "owner_name": owner, "review_reasons": []}, raw)


tax_raw = {"tax_sale": {"fetched_at": FRESH},
           "redemption_period": {"fetched_at": FRESH, "sale_year": 2026, "redemption_deadline": FUTURE}}

# never 100 without a hand check
s, _ = conf(["tax_sale", "probate", "pre_foreclosure"],
            dict(tax_raw, probate={"decedent_name": "DOE , JANE QUINN", "fetched_at": FRESH}))
check("auto score capped at 90", s <= 90)

# hand-checked override wins
s, why = conf(["tax_sale"], dict(tax_raw, morbius_override={"confidence": 25, "note": "heirs"}))
check("override sets the score", s == 25 and "hand-checked" in why)

# taxes paid after the sale -> tax signal gone
s, why = conf(["tax_sale"], dict(tax_raw, tax_paid={"tax_year": 2025, "paid_on": "10/23/2026"}))
check("paid taxes drop the tax signal", s <= 10 and "paid" in why)
sold = dict(tax_raw, redemption_period=dict(tax_raw["redemption_period"], sale_date="2026-09-25"))
s, why = conf(["tax_sale"], dict(sold, tax_paid={"tax_year": 2025, "paid_on": "10/23/2026"}))
check("paid after the sale is ambiguous, not resolved", 10 < s <= 60 and "AFTER the tax sale" in why)
s, why = conf(["tax_sale"], dict(sold, tax_paid={"tax_year": 2025, "paid_on": "09/01/2026"}))
check("paid before the sale resolves it", s <= 10)
s2, _ = conf(["tax_sale"], dict(tax_raw, tax_paid={"tax_year": 2025, "paid_on": None}))
check("unpaid taxes still count", s2 > 10)

# probate for someone not on the deed
raw_p = {"probate": {"decedent_name": "DOE , MARY ANN", "fetched_at": FRESH}}
s, why = conf(["probate"], raw_p, owner="DOE JOHN P (JTWROS)")
check("probate for a relative does not count", s <= 10 and "NOT on the deed" in why)
s, _ = conf(["probate"], {"probate": {"decedent_name": "DOE , JOHN PAUL", "fetched_at": FRESH}}, owner="DOE JOHN P")
check("probate for the owner counts", s > 40)

# PropStream alone
s, why = conf(["pre_foreclosure"], {})
check("PropStream alone max 40", s <= 40 and "PropStream only" in why)

# permit alone
s, _ = conf(["permit_expired"], {"building_permits": {"fetched_at": FRESH}})
check("permit alone max 20", s <= 20)

# recent full-price purchase
recent = (NOW - timedelta(days=200)).date().isoformat()
s, why = conf(["tax_sale"], dict(tax_raw, absentee_owner={"sale_price": 236000, "deed_date": recent}))
check("recent full-price buy max 30", s <= 30 and "full-price" in why)

if fails:
    print("FAILED:\n  " + "\n  ".join(fails))
    sys.exit(1)
print("all confidence checks passed")
