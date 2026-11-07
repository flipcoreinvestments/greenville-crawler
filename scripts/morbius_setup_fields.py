"""
MORBIUS SETUP: creates Tala's Unstuck Homes GHL contact custom fields (added 2026-11-07).

Run once from GitHub Actions (Morbius setup). Safe to run again: any field that
already exists (same name, any letter case) is skipped, never duplicated or changed.
Uses the GHL_MORBIUS_TOKEN private-integration key + GHL_LOCATION_ID.
Logs field names and counts only. No contact data is read or written.
"""
import os
import sys
import time

import requests

BASE = "https://services.leadconnectorhq.com"
TOKEN = os.environ.get("GHL_MORBIUS_TOKEN", "").strip()
LOC = os.environ.get("GHL_LOCATION_ID", "").strip()
HEAD = {"Authorization": f"Bearer {TOKEN}", "Version": "2021-07-28", "Accept": "application/json"}

# (name, GHL dataType, intended built-in folder)
FIELDS = [
    # Additional Info
    ("Crawler ID", "TEXT", "Additional Info"),
    ("Their Story", "LARGE_TEXT", "Additional Info"),
    ("Skip Trace Info", "LARGE_TEXT", "Additional Info"),
    ("Mailing Address", "TEXT", "Additional Info"),
    ("County", "TEXT", "Additional Info"),
    ("Best Time to Call", "TEXT", "Additional Info"),
    ("SMS Consent", "TEXT", "Additional Info"),
    ("Phone 2", "PHONE", "Additional Info"),
    ("Phone 3", "PHONE", "Additional Info"),
    ("Phone 4", "PHONE", "Additional Info"),
    ("Phone 5", "PHONE", "Additional Info"),
    ("Name 2", "TEXT", "Additional Info"),
    ("Name 3", "TEXT", "Additional Info"),
    ("Name 4", "TEXT", "Additional Info"),
    ("Name 5", "TEXT", "Additional Info"),
    ("Distress Signals", "TEXT", "Additional Info"),
    ("Days on List", "NUMERICAL", "Additional Info"),
    ("Next Deadline", "TEXT", "Additional Info"),
    ("Heirs", "LARGE_TEXT", "Additional Info"),
    ("Other Properties Owned", "LARGE_TEXT", "Additional Info"),
    # General Info
    ("ARV", "MONETORY", "General Info"),
    ("Rehab Estimate", "MONETORY", "General Info"),
    ("Rehab Description", "TEXT", "General Info"),
    ("Payoff", "MONETORY", "General Info"),
    ("Total Liens", "MONETORY", "General Info"),
    ("Equity", "MONETORY", "General Info"),
    ("Years Owned", "NUMERICAL", "General Info"),
    ("Offer", "MONETORY", "General Info"),
    ("Contract Price", "MONETORY", "General Info"),
    ("Assignment Fee", "MONETORY", "General Info"),
]


def main():
    if not TOKEN or not LOC:
        print("Missing GHL_MORBIUS_TOKEN or GHL_LOCATION_ID secret.")
        sys.exit(1)
    r = requests.get(f"{BASE}/locations/{LOC}/customFields", headers=HEAD, params={"model": "contact"}, timeout=30)
    if r.status_code != 200:
        print(f"Could not read existing custom fields: HTTP {r.status_code}. "
              "Check that the Morbius key has View Custom Fields + Edit Custom Fields.")
        sys.exit(1)
    existing = {f.get("name", "").strip().lower() for f in r.json().get("customFields", [])}
    made, skipped, failed = [], [], []
    for name, dtype, folder in FIELDS:
        if name.lower() in existing:
            skipped.append(name)
            continue
        body = {"name": name, "dataType": dtype, "model": "contact"}
        resp = requests.post(f"{BASE}/locations/{LOC}/customFields", headers=HEAD, json=body, timeout=30)
        if resp.status_code in (200, 201):
            made.append(f"{name} ({folder})")
        else:
            failed.append(f"{name}: HTTP {resp.status_code} {resp.text[:160]}")
        time.sleep(0.4)
    print(f"Created {len(made)}:", *made, sep="\n  ")
    print(f"Already existed {len(skipped)}:", *skipped, sep="\n  ")
    if failed:
        print(f"FAILED {len(failed)}:", *failed, sep="\n  ")
        sys.exit(1)


if __name__ == "__main__":
    main()
