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
import math
import os
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests

GEOCODER = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
STATE_FIPS, COUNTY_FIPS = "45", "045"
NIGHTLY_LIMIT = int(os.environ.get("CITY_ZIP_LIMIT", "1000"))
# 2026-10-10: tie-breaker for streets that exist twice in the county
# (verified: "10 Virginia Ave" and "37 Hunt St" are each in both Greenville
# 29611 and Travelers Rest 29690). The county parcel map gives the parcel's
# real location by PIN; the Census match next to it wins. Same two public
# layers absentee_owner_ingest.py reads.
PARCEL_LAYERS = [
    "https://citygis.greenvillesc.gov/arcgis/rest/services/AddressSearch/Property/MapServer/3/query",
    "https://services3.arcgis.com/YQLyddqtM8cTAr6Y/arcgis/rest/services/GreenvilleCountyBaseData/FeatureServer/2/query",
]
NEAR_KM = 1.5      # winner must be this close to the parcel
FAR_RATIO = 4      # and the runner-up at least 4x farther away
GEOCODE_VERSION = 2
DELAY = 0.3


def geocode(street, session, pin=None):
    r = session.get(GEOCODER, params={
        "address": f"{street}, SC", "benchmark": "Public_AR_Current",
        "vintage": "Current_Current", "layers": "Counties", "format": "json"}, timeout=30)
    r.raise_for_status()
    hit, cands = pick(r.json())
    if hit or not cands or not pin:
        return hit, None
    point = parcel_point(pin, session)
    if not point:
        return None, None
    return nearest(cands, point), "parcel_map"


def pick(payload):
    """((city, zip), []) for a single unambiguous Greenville County match;
    (None, candidates) when the county has the street in 2+ zips -- each
    candidate is (city, zip, lon, lat) for the parcel-map tie-breaker."""
    found, cands = set(), []
    for m in (payload.get("result") or {}).get("addressMatches") or []:
        counties = (m.get("geographies") or {}).get("Counties") or []
        if not any(c.get("STATE") == STATE_FIPS and c.get("COUNTY") == COUNTY_FIPS for c in counties):
            continue
        comp = m.get("addressComponents") or {}
        if comp.get("zip"):
            city = (comp.get("city") or "").title()
            found.add((city, comp["zip"]))
            xy = m.get("coordinates") or {}
            if xy.get("x") is not None and xy.get("y") is not None:
                cands.append((city, comp["zip"], float(xy["x"]), float(xy["y"])))
    zips = {z for _, z in found}
    if len(zips) == 1:
        return sorted(found)[0], []
    return None, (cands if len(zips) > 1 else [])


def parcel_point(pin, session):
    """(lon, lat) of a parcel by PIN from the county parcel map, else None."""
    for url in PARCEL_LAYERS:
        try:
            r = session.get(url, params={
                "where": f"PIN = '{pin}'", "outFields": "PIN", "returnGeometry": "true",
                "outSR": "4326", "f": "json"}, timeout=30)
            r.raise_for_status()
            feats = r.json().get("features") or []
        except Exception:
            continue
        for f in feats:
            rings = (f.get("geometry") or {}).get("rings") or []
            pts = [p for ring in rings for p in ring]
            if pts:
                return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))
    return None


def km(a, b):
    dx = (a[0] - b[0]) * 111.32 * math.cos(math.radians((a[1] + b[1]) / 2))
    dy = (a[1] - b[1]) * 110.57
    return math.hypot(dx, dy)


def nearest(cands, point):
    """The candidate next to the parcel, only if it's clearly the one."""
    by_dist = sorted(cands, key=lambda c: km((c[2], c[3]), point))
    best = by_dist[0]
    d1 = km((best[2], best[3]), point)
    others = [km((c[2], c[3]), point) for c in by_dist[1:] if c[1] != best[1]]
    if d1 <= NEAR_KM and (not others or min(others) >= FAR_RATIO * max(d1, 0.05)):
        return best[0], best[1]
    return None


def todo(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, address,
                   coalesce(nullif(pin, ''),
                            nullif(regexp_replace(coalesce(raw->'tax_sale'->>'map_number', ''), '\\D', '', 'g'), ''))
            from leads
            where list_count > 0 and is_sold = false and is_duplicate = false
              and coalesce(zip, '') = '' and address ~ '^\\s*\\d+\\S*\\s+\\S'
              and (coalesce((raw->'geocode'->>'tried_at')::timestamptz, 'epoch') < now() - interval '30 days'
                   -- misses from before the parcel-map tie-breaker get one retry now
                   or coalesce((raw->'geocode'->>'v')::int, 1) < %s)
            order by score desc
            limit %s
            """, (GEOCODE_VERSION, limit))
        return cur.fetchall()


# 2026-10-10 (lead re-check audit): 52 live leads still had no city/zip
# after the geocoder -- rural addresses the Census can't place. When the
# owner's mailing address IS the property (same house number + street),
# the mailing city/zip is the property's. Greenville County zips are 296xx.
TOWNS = sorted(["Greenville", "Greer", "Simpsonville", "Mauldin", "Fountain Inn", "Travelers Rest",
                "Taylors", "Piedmont", "Pelzer", "Marietta", "Landrum", "Gray Court", "Easley",
                "Cleveland", "Slater", "Tigerville", "Duncan", "Belton", "Honea Path"], key=len, reverse=True)
_MAIL_RE = re.compile(r"^(?P<street>.*?),?\s*SC\s+(?P<zip>296\d\d)(?:-\d{4})?\s*$", re.I)


def mailing_fallback(conn):
    with conn.cursor() as cur:
        cur.execute("""select id, address, mailing_address from leads
                       where list_count > 0 and is_sold = false and is_duplicate = false
                         and coalesce(zip, '') = '' and coalesce(mailing_address, '') <> ''""")
        rows = cur.fetchall()
        filled = 0
        for lead_id, address, mailing in rows:
            m = _MAIL_RE.match((mailing or "").strip())
            if not m:
                continue
            body = m.group("street").strip(" ,")
            town = next((t for t in TOWNS if body.upper().endswith(" " + t.upper())), None)
            p, q = (address or "").upper().split(), body.upper().split()
            if not town or len(p) < 2 or len(q) < 2 or p[0] != q[0] or p[1][:4] != q[1][:4]:
                continue
            cur.execute("update leads set city = %s, zip = %s, raw = coalesce(raw, '{}'::jsonb) || %s::jsonb "
                        "where id = %s",
                        (town, m.group("zip"), json.dumps({"geocode": {"source": "owner_mailing_same_address",
                                                                        "tried_at": datetime.now(timezone.utc).isoformat(),
                                                                        "v": GEOCODE_VERSION}}), lead_id))
            filled += 1
    conn.commit()
    print(f"  city/zip filled from the owner's same-address mailing for {filled} lead(s)")


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
    for lead_id, address, pin in rows:
        try:
            hit, method = geocode(address, s, pin)
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
                            (city, zip_, json.dumps({"geocode": {"source": "census", "tried_at": now,
                                                                  "v": GEOCODE_VERSION,
                                                                  "method": method or "single_match"}}), lead_id))
                filled += 1
            else:
                cur.execute("update leads set raw = coalesce(raw, '{}'::jsonb) || %s::jsonb where id = %s",
                            (json.dumps({"geocode": {"source": "census", "tried_at": now, "match": False,
                                                     "v": GEOCODE_VERSION}}), lead_id))
                missed += 1
        if (filled + missed) % 50 == 0:
            conn.commit()
        time.sleep(DELAY)
    conn.commit()
    conn.close()
    print(f"Done. city/zip filled for {filled}, no single Greenville County match for {missed}, errors {errors}.")
    conn = psycopg2.connect(db_url)
    mailing_fallback(conn)
    conn.close()


if __name__ == "__main__":
    main()
