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
from datetime import datetime, timezone, date, timedelta

import requests
import psycopg2
from lead_common import ensure_schema, expire_by_address, mailing_is_address, remove_tag, rescore_all  # noqa: E402
import permit_inspections as pi  # noqa: E402

BASE_URL = "https://citygis.greenvillesc.gov/arcgis/rest/services/InfoHUB/BuildingPermits_PriorTwoYears/MapServer/0/query"
STALE_DAYS = pi.STALLED_DAYS  # 180: IRC/IBC 105.5 -- permit invalid after 180 days with no work (proven by inspections)
OUT_FIELDS = (
    "STREETADDRESS,OWNER_NAME,OWNER_ADDR,OWNER_ADDR2,OWNER_ZIP,APPLICDATE,"
    "NewIssueDate,BP_STATUS,PERMIT_NUM,PERMIT_TYPE,APPLIC_DESCRIPTION,PERMIT_COMMENTS,PERMIT_VALUATION"
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
# TIGHTENED: bare "complete"/"repaired" also matched "incomplete",
# "to complete unfinished basement", "roof to be repaired" -- which would
# silently drop REAL stalled/damage leads. Only past-tense, finished-work
# phrases count, matched on word boundaries, and any future/intent wording
# ("to be", "will", "need", "incomplete", "not complete") vetoes the skip.
COMPLETION_PATTERNS = [
    r"\bREPAIRED LIKE FOR LIKE\b",
    r"\bLIKE FOR LIKE\b",
    r"\bREPAIRS? (?:ARE |IS |WAS |WERE )?COMPLETED?\b",
    r"\bWORK (?:IS |WAS )?COMPLETED?\b",
    r"\bFINALED\b",
    r"\bPASSED FINAL(?: INSPECTION)?\b",
    r"\bFINAL INSPECTION (?:PASSED|APPROVED|COMPLETE[D]?)\b",
    r"\bHAS BEEN (?:REPAIRED|RESTORED|REBUILT)\b",
    r"\bWAS (?:REPAIRED|RESTORED|REBUILT)\b",
    r"^\s*REPAIRED\b|[.;]\s*REPAIRED\b",
]
NOT_DONE_PATTERNS = [
    r"\bINCOMPLETE\b", r"\bNOT (?:YET )?(?:COMPLETE[D]?|REPAIRED|FINALED)\b",
    r"\bTO BE (?:REPAIRED|COMPLETED|RESTORED|REBUILT)\b",
    r"\bTO COMPLETE\b", r"\bWILL BE\b", r"\bNEEDS?\b", r"\bPENDING\b",
    r"\bFAILED\b",
]


def looks_completed(description, comments):
    text = re.sub(r"\s+", " ", f"{description or ''}. {comments or ''}").upper()
    if any(re.search(p, text) for p in NOT_DONE_PATTERNS):
        return False
    return any(re.search(p, text) for p in COMPLETION_PATTERNS)


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
    if data.get("exceededTransferLimit"):
        # More matches than one page returns -- the list is incomplete, so
        # raise instead of letting tag expiry treat the missing rows as gone.
        raise RuntimeError("ArcGIS result truncated (exceededTransferLimit); pagination needed")
    return [f["attributes"] for f in data.get("features", [])]


def normalize_address(addr):
    if not addr:
        return None
    return re.sub(r"\s+", " ", addr).strip().rstrip(",").rstrip("*").strip()


def guess_absentee(street_address, owner_addr, owner_zip, prop_zip=None):
    if not owner_addr:
        return None
    # FIX 2026-09-25: owner mailing field can hold a NAME -> unknown, not True.
    if not mailing_is_address(owner_addr):
        return None
    owner_addr_n = normalize_address(owner_addr).lower()
    street_n = (normalize_address(street_address) or "").lower()
    if prop_zip and owner_zip and prop_zip.strip() != owner_zip.strip():
        return True
    # fallback: mailing address doesn't start with the same street number/name
    return not owner_addr_n.startswith(street_n[:8]) if street_n else None


def upsert_lead(conn, row, tag):
    # FIX 2026-09-25: OWNER_ZIP is the owner's MAILING zip, not the
    # property's -- it used to be written into leads.zip. Now kept only in
    # raw->building_permits->owner_zip. City is no longer hard-coded.
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
            "owner_zip": owner_zip,
            "applic_date": applic_date_str,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, state, county, owner_name, mailing_address,
                                is_absentee, source_tags, raw)
            values (%s, 'SC', 'Greenville', %s, %s, %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = coalesce(leads.raw, '{}'::jsonb) || excluded.raw,
                updated_at = now()
            """,
            (
                address,
                normalize_address(row.get("OWNER_NAME")), owner_addr,
                absentee, tag, raw_payload,
            ),
        )
    return True


# rescore_all now lives in lead_common.py (one shared formula for every script).

def log_run(conn, records_found, records_new, notes):
    with conn.cursor() as cur:
        cur.execute(
            "insert into source_runs (source_name, records_found, records_new, notes) values (%s, %s, %s, %s)",
            (SOURCE_NAME, records_found, records_new, notes),
        )


PAGE_SIZE = 2000
REBUILD_WINDOW_DAYS = 365
# A stalled window/fence/water-heater job isn't distress -- it's paperwork.
# Confirmed 2026-09-25: 925 Cleveland St had five open replacement-window
# permits ($3,480-$8,761) with zero inspections. 389 of 1,693 open city
# permits are under this line. Damage repairs are exempt (damage matters at
# any size).
MIN_STALLED_VALUATION = 15000
RECENT_FINAL_DAYS = 365  # a new building permit this close to a demolition = teardown/rebuild
TAG_PRIORITY = ("insurance_damage", "permit_demolition", "permit_expired")


def fetch_all_permits():
    """
    REWRITE 2026-09-25: pull the WHOLE two-year permit layer (~4,000 rows) in
    pages and classify locally, instead of three separate server-side
    queries. Classifying needs to see every permit at an address together
    (see classify_permits). Raises on any failure, so a partial pull can
    never drive tag expiry.
    """
    out, offset = [], 0
    while True:
        params = {
            "where": "1=1", "outFields": OUT_FIELDS, "returnGeometry": "false", "f": "json",
            "resultRecordCount": PAGE_SIZE, "resultOffset": offset, "orderByFields": "OBJECTID",
        }
        resp = requests.post(BASE_URL, data=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"ArcGIS error: {data['error']}")
        feats = [f["attributes"] for f in data.get("features", [])]
        out.extend(feats)
        if len(feats) < PAGE_SIZE and not data.get("exceededTransferLimit"):
            break
        if not feats:
            raise RuntimeError("ArcGIS returned an empty page while claiming more rows")
        offset += len(feats)
    return out


def _applic_date(row):
    try:
        return datetime.strptime(str(int(row.get("APPLICDATE"))), "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


def _has_damage_language(row):
    text = f"{row.get('APPLIC_DESCRIPTION') or ''} {row.get('PERMIT_COMMENTS') or ''}".lower()
    return any(kw in text for kw in DAMAGE_KEYWORDS)


def inspection_candidates(rows, today=None):
    """Open, non-demolition permits old enough that they COULD be stalled."""
    today = today or date.today()
    cutoff = today - timedelta(days=STALE_DAYS)
    out = []
    # also check every other permit at those addresses, so a recent
    # inspection/final on a companion permit can clear the address
    old_addrs = {normalize_address(p.get("STREETADDRESS")) for p in rows
                 if (p.get("BP_STATUS") or "").upper() == "IS"
                 and not (p.get("PERMIT_TYPE") or "").upper().startswith("DEM")
                 and _applic_date(p) and _applic_date(p) <= cutoff}
    for p in rows:
        if normalize_address(p.get("STREETADDRESS")) in old_addrs and p.get("PERMIT_NUM") \
                and not (p.get("PERMIT_TYPE") or "").upper().startswith("DEM"):
            out.append(str(p["PERMIT_NUM"]).strip())
    return list(dict.fromkeys(out))


def classify_permits(rows, today=None, checks=None):
    """
    Returns {tag: {normalized_address: permit_row}}.

    REVISED 2026-09-25 (after T Dawg asked how "stalled" is proven): open
    status alone proves nothing, so damage and stalled tags now REQUIRE the
    city's own inspection history (permit_inspections.py):
      insurance_damage  -- damage/repair wording, permit still OPEN, notes
                           don't say done, AND no inspection for 180+ days
                           (the repair stopped). A damage permit with recent
                           inspections is an active repair -> no tag.
      permit_expired    -- permit still OPEN, notes don't say done, no
                           approved final, AND no inspection for 180+ days
                           (building code 105.5 abandonment). Not counted
                           again if the same permit is already insurance_damage.
      permit_demolition -- demolition permit (DEMR/DEMC) with NO other
                           building permit at the same address within 365
                           days (demo + new build = teardown/rebuild).
    A permit with no inspection check yet is NOT tagged (never guessed).
    `checks` = {permit_num: check dict} from permit_inspections.load_checks.
    """
    today = today or date.today()
    checks = checks or {}
    by_addr = {}
    for r in rows:
        addr = normalize_address(r.get("STREETADDRESS"))
        if addr:
            by_addr.setdefault(addr, []).append(r)

    result = {t: {} for t in TAG_PRIORITY}
    for addr, permits in by_addr.items():
        builds = [p for p in permits if not (p.get("PERMIT_TYPE") or "").upper().startswith("DEM")]
        # ADDRESS ACTIVITY OVERRIDE (2026-09-25): 1124 Wembley Rd's tree-damage
        # permit had no inspection since 11/2024, but its companion permit at
        # the same address passed FINAL on 12/15/2025 -- the repair was done.
        # Any permit at the address applied for or inspected in the last 180
        # days, or finaled in the last year = owner is actively working on
        # the property -> nothing at this address counts as stalled.
        active = False
        for q in permits:
            qa = _applic_date(q)
            ck = checks.get(str(q.get("PERMIT_NUM") or "").strip()) or {}
            last = ck.get("last_inspection")
            if qa and (today - qa).days < STALE_DAYS:
                active = True
            if last and (today - last).days < STALE_DAYS:
                active = True
            if ck.get("final_approved") and last and (today - last).days <= RECENT_FINAL_DAYS:
                active = True
        for p in permits:
            ptype = (p.get("PERMIT_TYPE") or "").upper()
            is_open = (p.get("BP_STATUS") or "").upper() == "IS"
            done = looks_completed(p.get("APPLIC_DESCRIPTION"), p.get("PERMIT_COMMENTS"))
            applied = _applic_date(p)
            if ptype.startswith("DEM"):
                rebuild = any(
                    _applic_date(b) and applied and abs((_applic_date(b) - applied).days) <= REBUILD_WINDOW_DAYS
                    for b in builds
                )
                if not rebuild:
                    result["permit_demolition"].setdefault(addr, p)
                continue
            if not is_open or done or active:
                continue
            stalled = pi.is_stalled(checks.get(str(p.get("PERMIT_NUM") or "").strip()), applied, today)
            if stalled is not True:
                continue
            if _has_damage_language(p):
                result["insurance_damage"].setdefault(addr, p)
            elif (p.get("PERMIT_VALUATION") or 0) >= MIN_STALLED_VALUATION:
                result["permit_expired"].setdefault(addr, p)
    return result


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching the full two-year permit layer...")
    try:
        rows = fetch_all_permits()
        fetched_ok = True
    except Exception as e:
        rows, fetched_ok = [], False
        print(f"  permit fetch failed: {e}", file=sys.stderr)
    print(f"  {len(rows)} permits fetched.")

    conn = psycopg2.connect(db_url)
    ensure_schema(conn)
    pi.ensure_table(conn)
    conn.commit()

    # One-time: every permit_expired / insurance_damage tag written before
    # 2026-09-25 was based on "still open" alone (unproven). Clear them; the
    # rules below re-add only permits the city's inspection records prove.
    with conn.cursor() as cur:
        cur.execute("select 1 from pipeline_migrations where name = '2026_09_25_permits_need_inspection_proof'")
        if cur.fetchone() is None:
            for t in ("permit_expired", "insurance_damage"):
                n = remove_tag(conn, t, "true")
                print(f"  migration: cleared {n} unproven {t} tag(s)")
            cur.execute("insert into pipeline_migrations (name) values ('2026_09_25_permits_need_inspection_proof')")
    conn.commit()

    checks = {}
    if fetched_ok:
        candidates = inspection_candidates(rows)
        print(f"  {len(candidates)} open permit(s) are old enough to need an inspection check")
        pi.run_checks(conn, candidates)
        checks = pi.load_checks(conn)
    classified = classify_permits(rows, checks=checks) if fetched_ok else {t: {} for t in TAG_PRIORITY}
    for t in TAG_PRIORITY:
        print(f"  {t}: {len(classified[t])} address(es)")
    new_count = 0
    for tag in TAG_PRIORITY:
        for addr, permit in classified[tag].items():
            if upsert_lead(conn, permit, tag):
                new_count += 1

    # Expire every permit tag whose property no longer qualifies under the
    # rules above -- only after a complete, successful fetch.
    if fetched_ok and len(rows) > 0:
        for tag in TAG_PRIORITY:
            current = list(classified[tag].keys())
            if current:
                expire_by_address(conn, tag, current)
            else:
                print(f"  {tag}: 0 qualifying permits this run; expiry skipped as a safety check")
    else:
        print("  fetch failed or empty: permit tag expiry skipped")

    rescore_all(conn)
    log_run(conn, len(rows), new_count, "ok" if fetched_ok else "fetch failed")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} permit lead(s) upserted.")


if __name__ == "__main__":
    main()
