#!/usr/bin/env python3
"""
Fills the property CITY and ZIP on active leads (added 2026-09-30).

Why: the county parcel feed has no site city/zip (see refresh_needs_review
in absentee_owner_ingest.py -- 'incomplete_address'), and skip-trace tools
(PropStream, BatchData, BatchDialer) want street + city + state + zip.

Source: U.S. Census Bureau Geocoder (free, public, no key):
  https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress
Query "<street>, SC" and accept a match ONLY if it lies in Greenville
County (state 45, county 045). Two different Greenville County matches
with different zips = ambiguous = left blank. Nothing is guessed.
Verified 2026-09-30: "412 Simsbury Way, SC" -> GREER, SC 29650, county 45045.

Only leads on a list (list_count > 0) that are not sold/duplicates, a few
hundred a night; a miss is not retried for 30 days.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests

GEOCODER = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
STATE_FIPS, COUNTY_FIPS = "45", "045"
NIGHTLY_LIMIT = int(os.environ.get("CITY_ZIP_LIMIT", "1000"))
DELAY = 0.3


def geocode(street, session):
    r = session.get(GEOCODER, params={
        "address": f"{street}, SC", "benchmark": "Public_AR_Current",
        "vintage": "Current_Current", "layers": "Counties", "format": "json"}, timeout=30)
    r.raise_for_status()
    return pick(r.json())


def pick(payload):
    """(city, zip) for a single unambiguous Greenville County match, else None."""
    found = set()
    for m in (payload.get("result") or {}).get("addressMatches") or []:
        counties = (m.get("geographies") or {}).get("Counties") or []
        if not any(c.get("STATE") == STATE_FIPS and c.get("COUNTY") == COUNTY_FIPS for c in counties):
            continue
        comp = m.get("addressComponents") or {}
        if comp.get("zip"):
            found.add(((comp.get("city") or "").title(), comp["zip"]))
    zips = {z for _, z in found}
    if len(zips) != 1:
        return None
    return sorted(found)[0]


def todo(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, address from leads
            where list_count > 0 and is_sold = false and is_duplicate = false
              and coalesce(zip, '') = '' and address ~ '^\\s*\\d+\\S*\\s+\\S'
              and coalesce((raw->'geocode'->>'tried_at')::timestamptz, 'epoch') < now() - interval '30 days'
            order by score desc
            limit %s
            """, (limit,))
        return cur.fetchall()


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    rows = todo(conn, NIGHTLY_LIMIT)
    print(f"{len(rows)} lead(s) need a property city/zip")
    s = requests.Session()
    filled = missed = errors = 0
    for lead_id, address in rows:
        try:
            hit = geocode(address, s)
        except Exception as e:  # network/5xx: stop quietly, retry tomorrow
            errors += 1
            print(f"  geocoder error on {address!r}: {e}")
            if errors >= 5:
                print("  5 geocoder errors -- stopping for tonight")
                break
            continue
        now = datetime.now(timezone.utc).isoformat()
        with conn.cursor() as cur:
            if hit:
                city, zip_ = hit
                cur.execute("update leads set city = %s, zip = %s, "
                            "raw = coalesce(raw, '{}'::jsonb) || %s::jsonb where id = %s",
                            (city, zip_, json.dumps({"geocode": {"source": "census", "tried_at": now}}), lead_id))
                filled += 1
            else:
                cur.execute("update leads set raw = coalesce(raw, '{}'::jsonb) || %s::jsonb where id = %s",
                            (json.dumps({"geocode": {"source": "census", "tried_at": now, "match": False}}), lead_id))
                missed += 1
        if (filled + missed) % 50 == 0:
            conn.commit()
        time.sleep(DELAY)
    conn.commit()
    conn.close()
    print(f"Done. city/zip filled for {filled}, no single Greenville County match for {missed}, errors {errors}.")


if __name__ == "__main__":
    main()
