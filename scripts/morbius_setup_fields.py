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
    ("Confidence", "NUMERICAL", "Additional Info"),
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
    ("Sellers Name on Deed", "TEXT", "General Info"),
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


def _folder_ids(fields):
    """GHL folder ids by name (2026-11-11). A folder shows up either as its own entry
    (documentType 'folder') or as the parentId most of its known fields share."""
    ids = {}
    for f in fields:
        if str(f.get("documentType", "")).lower() == "folder" and f.get("name"):
            ids[f["name"].strip().lower()] = f.get("id")
    by_name = {f.get("name", "").strip().lower(): f.get("parentId") for f in fields}
    for folder in {fo for _, _, fo in FIELDS}:
        if folder.lower() in ids:
            continue
        votes = {}
        for name, _, fo in FIELDS:
            pid = by_name.get(name.lower())
            if fo == folder and pid:
                votes[pid] = votes.get(pid, 0) + 1
        if votes:
            ids[folder.lower()] = max(votes, key=votes.get)
    return ids


def _all_fields():
    r = requests.get(f"{BASE}/locations/{LOC}/customFields", headers=HEAD, params={"model": "contact"}, timeout=30)
    return r, (r.json().get("customFields", []) if r.status_code == 200 else [])


def main():
    if not TOKEN or not LOC:
        print("Missing GHL_MORBIUS_TOKEN or GHL_LOCATION_ID secret.")
        sys.exit(1)
    r, current = _all_fields()
    if r.status_code != 200:
        print("GHL said:", r.text[:300].replace(TOKEN, "***"))
        c = requests.get(f"{BASE}/contacts/", headers=HEAD, params={"locationId": LOC, "limit": 1}, timeout=30)
        print(f"Key check: contacts read = HTTP {c.status_code}; key length {len(TOKEN)}; starts with 'pit-': {TOKEN.startswith('pit-')}")
        print(f"Could not read existing custom fields: HTTP {r.status_code}. "
              "Check that the Morbius key has View Custom Fields + Edit Custom Fields.")
        sys.exit(1)
    folders = _folder_ids(current)
    print("Folders found:", ", ".join(f"{k} = {'yes' if v else 'no'}" for k, v in sorted(folders.items())) or "none")
    existing = {f.get("name", "").strip().lower(): f for f in current}
    made, skipped, failed = [], [], []
    for name, dtype, folder in FIELDS:
        if name.lower() in existing:
            skipped.append(name)
            continue
        body = {"name": name, "dataType": dtype, "model": "contact"}
        pid = folders.get(folder.lower())
        resp = None
        if pid:
            resp = requests.post(f"{BASE}/locations/{LOC}/customFields", headers=HEAD,
                                 json=dict(body, parentId=pid), timeout=30)
        if resp is None or resp.status_code not in (200, 201):
            resp = requests.post(f"{BASE}/locations/{LOC}/customFields", headers=HEAD, json=body, timeout=30)
        if resp.status_code in (200, 201):
            made.append(f"{name} ({folder})")
        else:
            failed.append(f"{name}: HTTP {resp.status_code} {resp.text[:160]}")
        time.sleep(0.4)
    print(f"Created {len(made)}:", *made, sep="\n  ")
    print(f"Already existed {len(skipped)}:", *skipped, sep="\n  ")
    # verify: every field exists, and say which folder it sits in
    _, after = _all_fields()
    now = {f.get("name", "").strip().lower(): f for f in after}
    wrong = []
    for name, _, folder in FIELDS:
        f = now.get(name.lower())
        if f is None:
            failed.append(f"{name}: not found after setup")
            continue
        pid = folders.get(folder.lower())
        # only move a field this run created (or the new deed-name field); never re-file
        # a field Tala may have placed somewhere on purpose
        mine = any(m_.startswith(name + " (") for m_ in made) or name == "Sellers Name on Deed"
        if mine and pid and f.get("parentId") != pid:
            put = requests.put(f"{BASE}/locations/{LOC}/customFields/{f['id']}", headers=HEAD,
                               json={"name": f.get("name"), "parentId": pid}, timeout=30)
            time.sleep(0.4)
            _, again = _all_fields()
            f2 = next((x for x in again if x.get("id") == f["id"]), {})
            if f2.get("parentId") != pid:
                wrong.append(f"{name} (should be in {folder}; move answered HTTP {put.status_code})")
    print("VERIFY Sellers Name on Deed:",
          "exists" if "sellers name on deed" in now else "MISSING",
          "| in General Info:", "unknown" if not folders.get("general info") else
          ("yes" if (now.get("sellers name on deed") or {}).get("parentId") == folders.get("general info")
           or not any(w.startswith("Sellers Name on Deed") for w in wrong) else "no"))
    if wrong:
        print(f"In the wrong folder {len(wrong)} (drag them in GHL Settings > Custom Fields):", *wrong, sep="\n  ")
    if failed:
        print(f"FAILED {len(failed)}:", *failed, sep="\n  ")
        sys.exit(1)


if __name__ == "__main__":
    main()
