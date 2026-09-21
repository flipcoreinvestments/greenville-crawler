#!/usr/bin/env python3
"""
Absentee owner + tired landlord detection — Greenville County assessor parcel data.

Sources (both public ArcGIS REST Feature Services, no login, no bot protection):
1. City of Greenville GIS "Parcels with Ownership" layer
   https://citygis.greenvillesc.gov/arcgis/rest/services/AddressSearch/Property/MapServer/3
2. Greenville County base parcel data (mirrored on ArcGIS Online)
   https://services3.arcgis.com/YQLyddqtM8cTAr6Y/arcgis/rest/services/GreenvilleCountyBaseData/FeatureServer/2

Both layers expose the SAME assessor schema: STREET/CITY/STATE/ZIP5 is the
OWNER'S MAILING address, while STRNUM + LOCATE identify the property's own
site address. Comparing the two is exactly what's needed to flag absentee
owners with zero manual lookups (confirmed against sample records 2026-09-21,
e.g. an owner mailing from Cary, NC on a Greenville rental property).

What this pulls (two of the seller-motivation categories):
1. "Absentee owner" — owner's mailing address doesn't match the property's
   own site address (out-of-state owners are the strongest signal; in-state
   owners whose mailing street doesn't match the property are also flagged).
   Tagged 'absentee_owner'.
2. "Tired landlords" — the SAME owner name shows up on 3+ separate parcels
   county-wide. Tagged 'tired_landlord' (in addition to absentee_owner if
   applicable — most tired landlords are also absentee, but not required).

NOTE on address quality: LOCATE is the assessor's street/subdivision name
WITHOUT the street-type suffix (Dr/Rd/Cir/etc.), so the address this script
writes (e.g. "509 Hampton Townes") won't always string-match an address from
another source that includes the suffix (e.g. "509 Hampton Townes Dr"). That
means some genuine stacking matches will be missed until address matching is
made suffix-tolerant — flagged here as a known limitation, not silently
hidden.
"""

import os
import re
import sys
import json
import time
from collections import Counter
from datetime import datetime, timezone

import requests
import psycopg2

SOURCE_NAME = "absentee_owner"

LAYERS = [
    {
        "name": "city_greenville",
        "url": "https://citygis.greenvillesc.gov/arcgis/rest/services/AddressSearch/Property/MapServer/3/query",
        "page_size": 2000,
    },
    {
        "name": "county_base",
        "url": "https://services3.arcgis.com/YQLyddqtM8cTAr6Y/arcgis/rest/services/GreenvilleCountyBaseData/FeatureServer/2/query",
        "page_size": 2000,
    },
]

OUT_FIELDS = "PIN,OWNAM1,OWNAM2,STREET,CITY,STATE,ZIP5,STRNUM,LOCATE,DEEDTE,SLPRICE,TOTTAX,LANDUSE"
TIRED_LANDLORD_THRESHOLD = 3


def fetch_all(layer):
    out = []
    offset = 0
    page_size = layer["page_size"]
    while True:
        params = {
            "where": "1=1",
            "outFields": OUT_FIELDS,
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": page_size,
            "resultOffset": offset,
        }
        for attempt in range(3):
            try:
                resp = requests.get(layer["url"], params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  [{layer['name']}] failed at offset {offset}: {e}", file=sys.stderr)
                    return out
                time.sleep(2)
        if "error" in data:
            print(f"  [{layer['name']}] ArcGIS error at offset {offset}: {data['error']}", file=sys.stderr)
            break
        feats = data.get("features", [])
        out.extend(f["attributes"] for f in feats)
        if len(feats) < page_size:
            break
        offset += page_size
    return out


def normalize_owner(name):
    if not name:
        return None
    n = re.sub(r"\s+", " ", name).strip().upper()
    n = re.sub(r"[.,]", "", n)
    return n or None


def normalize_addr(strnum, locate):
    if not strnum or not locate or locate.strip().upper() in ("SYMBOLIC", ""):
        return None
    addr = f"{strnum.strip()} {locate.strip()}"
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr.title() if addr else None


def is_absentee(mailing_street, mailing_state, prop_strnum, prop_locate):
    if mailing_state and mailing_state.strip().upper() != "SC":
        return True
    if not mailing_street or not prop_strnum or not prop_locate:
        return None
    mail_n = re.sub(r"\s+", " ", mailing_street).strip().upper()
    prop_n = f"{prop_strnum.strip()} {prop_locate.strip()}".upper()
    return not mail_n.startswith(prop_n[:8]) if prop_n else None


def upsert_lead(conn, address, owner_name, mailing_addr, mailing_zip, absentee, tags, extra):
    raw_payload = json.dumps({SOURCE_NAME: extra})
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, zip, county, owner_name, mailing_address,
                                is_absentee, source_tags, raw)
            values (%s, 'Greenville', 'SC', %s, 'Greenville', %s, %s, %s, %s::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, mailing_zip, owner_name, mailing_addr, absentee, tags, raw_payload),
        )


def rescore_all(conn):
    """
    Shared score formula — kept IDENTICAL in every ingest script:
      +25 per list the property is stacked on
      +15 if owner's mailing address differs from the property (absentee)
      +up to 25 scaled from tax-sale amount owed (capped)
      +30 if the property has an active foreclosure sale scheduled
      +20 if the property has a stalled/expired building permit
      +20 if the property has a demolition permit
      +15 if the same owner holds 3+ properties county-wide (tired landlord)
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
                + (case when 'tired_landlord' = any(source_tags) then 15 else 0 end)
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

    all_parcels = {}
    for layer in LAYERS:
        print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching {layer['name']}...")
        rows = fetch_all(layer)
        print(f"  {len(rows)} parcels fetched from {layer['name']}.")
        for r in rows:
            pin = r.get("PIN")
            if pin and pin not in all_parcels:
                all_parcels[pin] = r

    print(f"Total unique parcels across both layers: {len(all_parcels)}")

    # Pass 1: normalize + classify absentee
    processed = []
    owner_counts = Counter()
    for pin, r in all_parcels.items():
        address = normalize_addr(r.get("STRNUM"), r.get("LOCATE"))
        if not address:
            continue
        owner_name = normalize_owner(r.get("OWNAM1"))
        absentee = is_absentee(r.get("STREET"), r.get("STATE"), r.get("STRNUM"), r.get("LOCATE"))
        processed.append({
            "pin": pin,
            "address": address,
            "owner_name": owner_name,
            "mailing_address": (r.get("STREET") or "").strip() or None,
            "mailing_zip": (r.get("ZIP5") or "").strip() or None,
            "absentee": absentee,
            "deed_date": r.get("DEEDTE"),
            "sale_price": r.get("SLPRICE"),
            "total_tax": r.get("TOTTAX"),
            "landuse": r.get("LANDUSE"),
        })
        if owner_name:
            owner_counts[owner_name] += 1

    tired_owners = {name for name, cnt in owner_counts.items() if cnt >= TIRED_LANDLORD_THRESHOLD}
    print(f"Owners with {TIRED_LANDLORD_THRESHOLD}+ parcels (tired landlords): {len(tired_owners)}")

    absentee_rows = [p for p in processed if p["absentee"] is True]
    tired_only_rows = [p for p in processed if p["absentee"] is not True and p["owner_name"] in tired_owners]
    print(f"Absentee-owner parcels to upsert: {len(absentee_rows)}")
    print(f"Additional tired-landlord-only parcels (owner-occupied but 3+ properties): {len(tired_only_rows)}")

    conn = psycopg2.connect(db_url)
    new_count = 0

    for p in absentee_rows + tired_only_rows:
        tags = ["absentee_owner"] if p["absentee"] is True else []
        if p["owner_name"] in tired_owners:
            tags.append("tired_landlord")
        if not tags:
            continue
        extra = {
            "pin": p["pin"],
            "deed_date": p["deed_date"],
            "sale_price": p["sale_price"],
            "total_tax": p["total_tax"],
            "landuse": p["landuse"],
            "owner_parcel_count": owner_counts.get(p["owner_name"], 1),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        upsert_lead(conn, p["address"], p["owner_name"], p["mailing_address"],
                    p["mailing_zip"], p["absentee"], tags, extra)
        new_count += 1

    rescore_all(conn)
    log_run(conn, len(processed), new_count,
            f"ok: {len(absentee_rows)} absentee, {len(tired_only_rows)} tired-only, "
            f"{len(tired_owners)} tired-landlord owners")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored.")


if __name__ == "__main__":
    main()
