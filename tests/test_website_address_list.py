#!/usr/bin/env python3
"""
Checks for the nightly website address list (2026-11-19).
Synthetic data only (the repo is public). No network, no database.

    python tests/test_website_address_list.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import website_address_list as w  # noqa: E402

fails = []


def check(label, cond):
    if not cond:
        fails.append(label)


TODAY = "2026-11-19"

rows = [
    {"address": "123  Sample  St", "city": "Greenville", "zip": "29601", "owner_name": "DOE JANE",
     "source_tags": ["tax_sale", "code_violation"], "tax_amount_due": "2,140.50",
     "tax_sale_date": "2026-12-01", "foreclosure_sale_date": None, "probate_file_date": None},
    {"address": "45 Example Rd", "city": None, "zip": None, "owner_name": "ROE RICHARD",
     "source_tags": ["foreclosure_mie", "probate"], "tax_amount_due": None, "tax_sale_date": None,
     "foreclosure_sale_date": "12/07/2026", "probate_file_date": "1/2/2026"},
    {"address": "Highway 11 Tract", "owner_name": "X", "source_tags": ["tax_sale"]},       # no number
    {"address": "9 Old Lane", "owner_name": "OLD", "source_tags": ["tax_sale"],
     "tax_sale_date": "2025-10-20", "tax_amount_due": "x"},
    {"address": "77 Other Ave", "owner_name": "NOPE", "source_tags": ["code_violation"]},  # not a list we send
    {"address": "45 EXAMPLE ROAD", "owner_name": "ROE MARY", "source_tags": ["tax_sale"],
     "tax_sale_date": "2026-12-01", "tax_amount_due": "10"},
]

out = w.build_entries(rows, today=TODAY)
by = {e["house_number"] + " " + e["street"]: e for e in out}

check("4 rows skipped/merged -> 3 entries", len(out) == 3)
check("no-number row skipped", not any("HIGHWAY" in e["address"] for e in out))
check("code-violation-only row skipped", "77 OTHER" not in by)

a = by.get("123 SAMPLE", {})
check("street suffix stripped", a.get("street") == "SAMPLE")
check("spaces squeezed + upper", a.get("address") == "123 SAMPLE ST")
check("tax sale amount parsed", a.get("lists", [{}])[0].get("amount_due") == 2140.5)
check("upcoming sale not after_sale", a.get("lists", [{}])[0].get("after_sale") is False)
check("only tax sale listed", [l["list"] for l in a.get("lists", [])] == ["tax sale"])

b = by.get("45 EXAMPLE", {})
names = [l["list"] for l in b.get("lists", [])]
check("merged duplicate house: 3 lists", names == ["foreclosure", "probate", "tax sale"])
check("foreclosure date to ISO", b["lists"][0].get("sale_date") == "2026-12-07")
check("probate date to ISO", b["lists"][1].get("filed_date") == "2026-01-02")
check("both owners kept", b.get("owner_name") == "ROE RICHARD; ROE MARY")

c = by.get("9 OLD", {})
check("past sale = after_sale", c.get("lists", [{}])[0].get("after_sale") is True)
check("junk amount -> None", c.get("lists", [{}])[0].get("amount_due") is None)

check("empty input -> empty list", w.build_entries([], today=TODAY) == [])
check("iso junk -> None", w.iso_date("soon") is None)

if fails:
    print("FAILED:\n  " + "\n  ".join(fails))
    sys.exit(1)
print("All website address list checks passed.")
