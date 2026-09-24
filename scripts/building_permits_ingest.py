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
3. INSURANCE/STORM DAMAGE (added 2026-09-22, T Dawg's "utmost importance"
   category). No clean address-level public source exists purely for
   insurance claims (court "Fraud/Bad Faith" filings have no address field
   without OCR'ing complaint PDFs; FEMA/NOAA data is redacted to
   zip/census-block; county Assessor has no casualty-loss track). Instead,
   this reuses the SAME feed already pulled above and keyword-filters the
   free-text APPLIC_DESCRIPTION/PERMIT_COMMENTS fields for damage/repair
   language — a repair permit for storm/fire/water damage is the strongest
   real proxy that (a) something happened and (b) the owner is/was dealing
   with it, whether or not insurance covered it fully. Tagged
   'insurance_damage'. CAVEAT: this ArcGIS layer lives on
   citygis.greenvillesc.gov (City of Greenville InfoHub) — unverified
   whether it covers unincorporated Greenville County too (county permitting
   may run through a separate eTrakit system). Ship it for what it covers
   now; revisit county-wide coverage separately.

BUG FIX 2026-09-22: the insurance_damage query was silently returning 0
results in every production run despite real matches existing (confirmed
144 real damage-repair permits live, including explicit Hurricane Helene
damage). A WAF/CDN in front of citygis.greenvillesc.gov blocks the long,
repeated "LIKE '%...%' OR LIKE '%...%' OR ..." GET querystring this WHERE
clause produces (looks like a SQLi signature to it) -- see the comment on
fetch_rows() for the full diagnosis. Fixed by sending all queries in this
script as POST instead of GET; confirmed working with real data.

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
    "NewIssueDate,BP_STATUS,PERMIT_NUM,PERMIT_TYPE,APPLIC_DESCRIPTION,PERMIT_COMMENTS"
)
SOURCE_NAME = "building_permits"

# Free-text damage/repair keywords for the insurance_damage signal. Matched
# case-insensitively against APPLIC_DESCRIPTION and PERMIT_COMMENTS. Kept
# broad on purpose (repair permits are the proxy, not a legal insurance
# determination) -- a false positive here just means a normal repair permit
# picks up a bonus tag, which is harmless; a false negative means a real
# distress lead gets missed entirely, which is worse. Err toward recall.
DAMAGE_KEYWORDS = [
    "fire damage", "fire repair", "storm damage", "storm repair",
    "water damage", "flood damage", "flood repair", "wind damage",
    "hail damage", "roof damage", "tornado damage", "tree damage",
    "tree fell", "collapse", "structural damage", "smoke damage",
    "burned", "burnt", "hurricane", "helene",
]

# BUG FIX (found 2026-09-2x, T Dawg's own spot-check on 12 Wakefield St):
# BP_STATUS stays 'IS' (issued, never formally closed) on some permits long
# after the actual work is done -- a county paperwork-closeout lag, not an
# active/abandoned project. Confirmed on permit #2400004633 (Hurricane
# Helene porch repair): BP_STATUS was still 'IS' months later, but the
# permit's OWN PERMIT_COMMENTS said "Repaired like for like", and T Dawg
# separately confirmed the house is rehabbed and currently rented. Tagging
# that permit_expired (implies stalled/abandoned) or insurance_damage
# (implies live, unresolved damage) is actively misleading. When the
# permit's own free text says the work is finished, skip both tags for that
# permit -- the raw permit record is simply not written as a lead in that
# case (no fabricated distress), rather than tagging a home that's already
# fixed as a current lead.
COMPLETION_KEYWORDS = [
    "repaired", "repair complete", "repair completed", "complete",
    "completed", "finaled", "final inspection", "like for like",
    "restored", "rebuilt",
]


def looks_completed(description, comments):
    text = f"{description or ''} {comments or ''}".upper()
    return any(kw.upper() in text for kw in COMPLETION_KEYWORDS)


def build_damage_where_clause():
    """
    ArcGIS REST feature services accept a standard SQL WHERE against the
    underlying DB for string fields, so UPPER()/LIKE works here the same
    way it does in tax_sale_ingest.py's queries. OR-chain both free-text
    fields against every keyword.
    """
    clauses = []
    for kw in DAMAGE_KEYWORDS:
        kw_upper = kw.upper().replace("'", "''")
        clauses.append(f"UPPER(APPLIC_DESCRIPTION) LIKE '%{kw_upper}%'")
        clauses.append(f"UPPER(PERMIT_COMMENTS) LIKE '%{kw_upper}%'")
    return "(" + " OR ".join(clauses) + ")"


def fetch_rows(where_clause):
    params = {
        "where": where_clause,
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": 2000,
    }
    # BUG FIX 2026-09-22 (found via production validation: this query was
    # unconditionally reporting "0 damage-related permits found" every
    # night). Root cause: the insurance_damage WHERE clause OR-chains ~30
    # keyword LIKE conditions across two fields, which is a long, heavily
    # repeated "X LIKE '%...%' OR Y LIKE '%...%' OR ..." pattern in the
    # querystring. Something in front of citygis.greenvillesc.gov (a
    # WAF/CDN) matches that shape as a SQLi signature and silently drops
    # the GET request -- surfaced as a 404 to `requests`, and as an opaque
    # network failure when reproduced via browser fetch(). The exact same
    # WHERE clause succeeds (HTTP 200, real rows back) as a POST with the
    # where clause in the form body instead of the URL -- confirmed live:
    # 144 real storm/fire/water-damage repair permits came back, including
    # explicit Hurricane Helene tree-damage repairs. Using POST for every
    # query here (not just the long one) so the short demolition/stalled
    # queries take the same, now-proven-safe path.
    resp = requests.post(BASE_URL, data=params, timeout=30)
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
            "comments": (row.get("PERMIT_COMMENTS") or "").strip(),
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
      +15 if the same owner holds 3+ properties county-wide (tired landlord)
      +35 if the property is in an active tax-sale redemption period (owner
          is about to permanently lose the property if they don't act)
      +30 if a permit shows storm/fire/water/damage repair language
          (insurance_damage -- T Dawg's "utmost importance" category)
      +20 if the MIE foreclosure plaintiff is an HOA/COA (hoa_foreclosure)
      +20 if the property shows 15+ years of ownership or a last sale price
          well below current fair market value (high_equity proxy)

    FIXED 2026-09-21: was missing the tired_landlord bonus AND the
    `where is_sold = false` guard, so this script (runs before
    absentee_owner_ingest.py in nightly.yml) was un-zeroing already-sold
    leads' scores each night. Fixed so this script is independently
    correct regardless of run order.

    UPDATED 2026-09-21: added the redemption_period bonus alongside the new
    redemption_period_ingest.py script.

    UPDATED 2026-09-22: added insurance_damage, hoa_foreclosure, and
    high_equity bonuses alongside this round's new tags/scripts.
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
                + (case when 'redemption_period' = any(source_tags) then 35 else 0 end)
                + (case when 'insurance_damage' = any(source_tags) then 30 else 0 end)
                + (case when 'hoa_foreclosure' = any(source_tags) then 20 else 0 end)
                + (case when 'high_equity' = any(source_tags) then 20 else 0 end)
                + (case when 'code_violation' = any(source_tags) then 25 else 0 end)
            where is_sold = false
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

    print("Fetching insurance/storm-damage permits (keyword match on description/comments)...")
    try:
        damage_rows = fetch_rows(build_damage_where_clause())
    except Exception as e:
        damage_rows = []
        print(f"  damage-keyword query failed: {e}", file=sys.stderr)
    print(f"  {len(damage_rows)} damage-related permits found.")

    conn = psycopg2.connect(db_url)
    new_count = 0
    skipped_completed = 0

    for row in demo_rows:
        if upsert_lead(conn, row, "permit_demolition"):
            new_count += 1
    for row in stalled_rows:
        if looks_completed(row.get("APPLIC_DESCRIPTION"), row.get("PERMIT_COMMENTS")):
            skipped_completed += 1
            continue
        if upsert_lead(conn, row, "permit_expired"):
            new_count += 1
    for row in damage_rows:
        if looks_completed(row.get("APPLIC_DESCRIPTION"), row.get("PERMIT_COMMENTS")):
            skipped_completed += 1
            continue
        if upsert_lead(conn, row, "insurance_damage"):
            new_count += 1

    print(f"  skipped {skipped_completed} permit(s) whose own comments say the repair is already done.")

    rescore_all(conn)
    log_run(conn, len(demo_rows) + len(stalled_rows) + len(damage_rows), new_count,
            f"ok ({skipped_completed} skipped as already-completed repairs)")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored.")


if __name__ == "__main__":
    main()
