#!/usr/bin/env python3
"""
City of Greenville building permits — expired/stalled permits + demolitions.

Source: https://citygis.greenvillesc.gov/arcgis/rest/services/InfoHUB/BuildingPermits_PriorTwoYears/MapServer/0
Public ArcGIS REST feature service (City of Greenville GIS InfoHub). Plain
JSON over HTTPS, no login, no bot protection, no robots.txt disallow found
on citygis.greenvillesc.gov (checked 2026-09-21).

What this pulls (two of the seller-motivation categories):
1. "Expired building permits" — a permit that's still open (BP_STATUS='IS',
   never closed/finaled) but was applied for more than STALE_DAYS ago. In
   practice this means a renovation/repair stalled out — a classic sign of
   an owner who ran out of money or interest mid-project. Tagged
   'permit_expired'.
2. Demolition permits (any status) — tagged 'permit_demolition'. Not one of
   her named 17 categories, but the same query costs nothing extra and it's
   a very strong distress signal (property came down, or is scheduled to),
   so it's included as a bonus tag.

Gives address, owner name, and owner mailing address (for absentee
detection) directly — no second lookup needed.
"""

import os
import re
import sys
import json
from datetime import datetime, timezone, date

import requests
import psycopg2

BASE_URL = "https://citygis.greenvillesc.gov/arcgis/rest/services/InfoHUB/BuildingPermits_PriorTwoYears/MapServer/0/query"
STALE_DAYS = 270  # ~9 months with no closure = treat as an expired/stalled permit
OUT_FIELDS = (
    "STREETADDRESS,OWNER_NAME,OWNER_ADDR,OWNER_ADDR2,OWNER_ZIP,APPLICDATE,"
    "NewIssueDate,BP_STATUS,PERMIT_NUM,PERMIT_TYPE,APPLIC_DESCRIPTION"
)
SOURCE_NAME = "building_permits"


def fetch_rows(where_clause):
    params = {
        "where": where_clause,
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 2000,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
    return [f["attributes"] for f in data.get("features", [])]


def normalize_address(addr):
    if not addr:
        return None
    return re.sub(r"\s+", " ", addr).strip().rstrip(",").rstrip("*").strip()


def guess_absentee(street_address, owner_addr, owner_zip, prop_zip=None):
    if not owner_addr:
        return None
    owner_addr_n = normalize_address(owner_addr).lower()
    street_n = (normalize_address(street_address) or "").lower()
    if prop_zip and owner_zip and prop_zip.strip() != owner_zip.strip():
        return True
    # fallback: mailing address doesn't start with the same street number/name
    return not owner_addr_n.startswith(street_n[:8]) if street_n else None


def upsert_lead(conn, row, tag):
    address = normalize_address(row.get("STREETADDRESS"))
    if not address:
        return False

    owner_addr = normalize_address(row.get("OWNER_ADDR"))
    owner_zip = (row.get("OWNER_ZIP") or "").strip() or None
    absentee = guess_absentee(row.get("STREETADDRESS"), owner_addr, owner_zip)

    applic_date = row.get("APPLICDATE")
    applic_date_str = None
    if applic_date:
        try:
            applic_date_str = datetime.strptime(str(applic_date), "%Y%m%d").date().isoformat()
        except ValueError:
            pass

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "tag": tag,
            "permit_num": (row.get("PERMIT_NUM") or "").strip(),
            "permit_type": (row.get("PERMIT_TYPE") or "").strip(),
            "description": (row.get("APPLIC_DESCRIPTION") or "").strip(),
            "bp_status": row.get("BP_STATUS"),
            "applic_date": applic_date_str,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, zip, county, owner_name, mailing_address,
                                is_absentee, source_tags, raw)
            values (%s, 'Greenville', 'SC', %s, 'Greenville', %s, %s, %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                zip = coalesce(excluded.zip, leads.zip),
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (
                address, owner_zip,
                normalize_address(row.get("OWNER_NAME")), owner_addr,
                absentee, tag, raw_payload,
            ),
        )
    return True


def rescore_all(conn):
    """
    Shared score formula — kept IDENTICAL in every ingest script so the whole
    table stays consistently scored no matter which script ran most recently:
      +25 per list the property is stacked on
      +15 if owner's mailing address differs from the property (absentee)
      +up to 25 scaled from tax-sale amount owed (capped)
      +30 if the property has an active foreclosure sale scheduled
      +20 if the property has a stalled/expired building permit
      +20 if the property has a demolition permit
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update leads set score =
                (list_count * 25)
                + (case when is_absentee then 15 else 0 end)
                + least(coalesce((raw->'tax_sale'->>'amount_due')::numeric, 0) / 50, 25)
                + (case when 'foreclosure_mie' = any(source_tags) then 30 else 0 end)
                + (case when 'permit_expired' = any(source_tags) then 20 else 0 end)
                + (case when 'permit_demolition' = any(source_tags) then 20 else 0 end)
            """
        )


def log_run(conn, records_found, records_new, notes):
    with conn.cursor() as cur:
        cur.execute(
            "insert into source_runs (source_name, records_found, records_new, notes) values (%s, %s, %s, %s)",
            (SOURCE_NAME, records_found, records_new, notes),
        )


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    cutoff = date.today()
    cutoff_num = int((cutoff.replace(year=cutoff.year)).strftime("%Y%m%d"))
    stale_cutoff_date = date.fromordinal(cutoff.toordinal() - STALE_DAYS)
    stale_cutoff_num = int(stale_cutoff_date.strftime("%Y%m%d"))

    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching demolition permits...")
    try:
        demo_rows = fetch_rows("PERMIT_TYPE LIKE '%DEM%'")
    except Exception as e:
        demo_rows = []
        print(f"  demolition query failed: {e}", file=sys.stderr)
    print(f"  {len(demo_rows)} demolition permits found.")

    print(f"Fetching stalled/expired permits (issued before {stale_cutoff_date}, still open)...")
    try:
        stalled_rows = fetch_rows(f"BP_STATUS='IS' AND APPLICDATE < {stale_cutoff_num}")
    except Exception as e:
        stalled_rows = []
        print(f"  stalled query failed: {e}", file=sys.stderr)
    print(f"  {len(stalled_rows)} stalled/expired permits found.")

    conn = psycopg2.connect(db_url)
    new_count = 0

    for row in demo_rows:
        if upsert_lead(conn, row, "permit_demolition"):
            new_count += 1
    for row in stalled_rows:
        if upsert_lead(conn, row, "permit_expired"):
            new_count += 1

    rescore_all(conn)
    log_run(conn, len(demo_rows) + len(stalled_rows), new_count, "ok")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored.")


if __name__ == "__main__":
    main()
