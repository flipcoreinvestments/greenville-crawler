#!/usr/bin/env python3
"""
*** 2026-09-25 REWRITE -- READ FIRST ***
This script no longer CREATES leads for absentee owners, tired landlords or
high equity. Those were owner-profile facts, not distress lists, and they
inflated the table to ~98k "leads" (see lead_common.py). It now:
  1. ENRICHES properties that are already on a real distress list with
     county owner facts (owner, mailing address, is_absentee,
     is_tired_landlord, is_long_term_owner, parcel number).
  2. Creates leads for ONE list only: out-of-state owners of vacant land.
  3. Marks sold properties, rescores, refreshes the '*' review flags.
The history notes below describe earlier versions and are kept for context.

Absentee owner + tired landlord detection — Greenville County assessor parcel data.

Sources (both public ArcGIS REST Feature Services, no login, no bot protection):
1. City of Greenville GIS "Parcels with Ownership" layer
   https://citygis.greenvillesc.gov/arcgis/rest/services/AddressSearch/Property/MapServer/3
2. Greenville County base parcel data (mirrored on ArcGIS Online)
   https://services3.arcgis.com/YQLyddqtM8cTAr6Y/arcgis/rest/services/GreenvilleCountyBaseData/FeatureServer/2

Both layers expose the SAME assessor schema: STREET/CITY/STATE/ZIP5 is the
OWNER'S MAILING address, while STRNUM + LOCATE identify the property's own
site address. Comparing the two is exactly what's needed to flag absentee
owners with zero manual lookups.

FIXED 2026-09-21: the two layers use DIFFERENT field names for the deed
date — city_greenville calls it DEEDTE, county_base calls it DEEDDATE.
Requesting "DEEDTE" from county_base threw a hard 400 ('outFields'
parameter is invalid) and silently zeroed out that entire layer (confirmed
via the layers' own /?f=json metadata). Each layer now requests its own
deed-date field name and normalizes it to DEED_DATE_NORM before merging.

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

PERFORMANCE (fixed 2026-09-21): the first version upserted one row at a
time via individual psycopg2 execute() calls — 33m30s for ~35,800 rows in
testing, too slow to run nightly. Rewritten to batch upserts via
psycopg2.extras.execute_values in chunks of 2000.

STALE-LEAD FIX (2026-09-21): confirmed twice in production (9 Monteith Cir,
108 Old Augusta Rd) that leads never got their tags removed once a property
actually sold, so already-sold houses kept surfacing as top leads forever.
This script touches EVERY parcel county-wide and already pulls each one's
deed date, so it's the natural place to close that gap: any parcel with a
deed recorded in the last SOLD_LOOKBACK_DAYS is cross-referenced against
existing leads by PIN (stored under raw->absentee_owner->pin or
raw->tax_sale->map_number, same assessor parcel ID either way) and flipped
to is_sold=true, score=0. Downstream queries should filter
`where is_sold = false` (or `score > 0`) to keep sold properties out of
lead lists automatically going forward.

CORRECTED 2026-09-21: the first version of this fix used a column called
'status' for this, not realizing 'leads.status' already existed as T Dawg's
CRM/GHL outreach-pipeline field (default 'new', alongside phone/email).
That would have silently clobbered her pipeline status the next time any
lead sold. Switched to a dedicated is_sold/sold_at pair so 'status' is
never touched by this script.

HIGH EQUITY / FREE-AND-CLEAR (added 2026-09-22): both layers already carry
TAXMKTVAL/FAIRMKTVAL (assessed value) alongside SLPRICE (last recorded sale
price) and the deed date this script already pulls -- no new data source
needed, just two more fields off the same query. There's no public lien-
balance data anywhere (confirmed: ROD's recorded-document index has no bulk
API), so this is a PROXY, not a confirmed "free and clear" fact: a parcel is
tagged 'high_equity' when EITHER (a) it's been owned 15+ years (matches the
15-years-ownership PropStream filter T Dawg was already using manually), OR
(b) the last recorded sale price is <= 50% of current FAIRMKTVAL (catches
long-paid-off or inherited-at-low-basis homes regardless of hold length).
equity_pct is populated as a rough 0-100 estimate (1 - SLPRICE/FAIRMKTVAL)
when both values exist; null otherwise -- treat it as directional, not exact,
since it can't see an actual mortgage balance.

HIGH_EQUITY REMOVED -> LONG_TERM_OWNER (T Dawg's call, 2026-09-24): the
high_equity proxy hit 92.5% of all leads and could not actually show equity
-- a 20-year owner may have pulled a cash-out refi last year, and no public
source gives a mortgage payoff balance or an ARV. So: the 'high_equity' tag
is gone, the equity_pct estimate is cleared (it was never a real equity
figure), and parcels owned 15+ years (deed date from the county) are tagged
'long_term_owner' -- a plain ownership-length fact, NOT an equity claim.
long_term_owner adds no score points by itself (length of ownership is not
distress). A verified 'free_and_clear' tag is only allowed from a per-
property Register of Deeds check (every recorded mortgage has a recorded
satisfaction), never from this bulk estimate.

NEEDS-REVIEW / ASTERISK FLAG (added 2026-09-21): T Dawg's explicit ask
after the 108 Old Augusta Rd stale-lead incident -- she doesn't want to
spend money reverse-searching/skip-tracing a lead unless it's been cross-
checked against more than one source. Added dedicated needs_review /
review_reasons columns (checked information_schema first -- neither
existed) plus a generated display_address column that prepends a literal
'*' to the address when needs_review is true, WITHOUT touching the real
`address` column that lower(address) conflict-matching depends on. This
script does a full-table pass every night, so it's the natural place to
keep this current for every lead, not just the ones it upserts this run.
"""

import os
import re
import sys
import json
import time
from collections import Counter
from datetime import datetime, timezone, timedelta, date

import requests
import psycopg2
from psycopg2.extras import execute_values

from lead_common import (
    address_core, ensure_schema, mailing_is_address, remove_tag, rescore_all, run_migrations,
)

SOURCE_NAME = "absentee_owner"
SOLD_LOOKBACK_DAYS = 270

OUT_FIELDS_BASE = "PIN,OWNAM1,OWNAM2,STREET,CITY,STATE,ZIP5,STRNUM,LOCATE,SLPRICE,TOTTAX,LANDUSE,TAXMKTVAL,FAIRMKTVAL,RRETCD"

# RRETCD = "2" means the county classes the parcel as the owner's LEGAL
# RESIDENCE (the 4% owner-occupied assessment). Verified 2026-09-25 against
# the county's own Real Property page ("Assessment Class: LR - Legal
# Residence") on 5 of 5 sampled parcels, including 1124 Wembley Rd, which
# the old mailing-address comparison had wrongly flagged absentee. Code "9"
# was mixed (1 OT, 3 LR on 4 samples), so it carries no rule. A legal
# residence is never absentee, whatever the mailing field says.
LEGAL_RESIDENCE_CODE = "2"

LONG_TERM_OWNER_YEARS = 15  # T Dawg's threshold for the long_term_owner tag

# OUT-OF-STATE VACANT LAND (added 2026-09-24): owner mails from outside SC
# AND the parcel's land-use code is one of the county's own vacant-land
# codes. Codes are NOT guessed -- all three were verified against the
# county's Real Property Search "Land Use" field in backfill_land_use.py's
# VERIFIED_CODES / CODE_VERIFICATION_LOG:
#   1180 = "Residential Vacant", 6800 = "Commercial Vacant", 9170 = "Ag Vacant"
# Tag only, no score points (same as long_term_owner). KNOWN LIMITATION:
# normalize_addr() skips parcels with no street number, and a lot of raw
# land has none, so some vacant parcels never become leads here.
VACANT_LAND_CODES = {"1180", "9170"}  # 6800 Commercial Vacant dropped 2026-09-26: residential only

LAYERS = [
    {
        "name": "city_greenville",
        "url": "https://citygis.greenvillesc.gov/arcgis/rest/services/AddressSearch/Property/MapServer/3/query",
        "page_size": 2000,
        "deed_field": "DEEDTE",
    },
    {
        "name": "county_base",
        "url": "https://services3.arcgis.com/YQLyddqtM8cTAr6Y/arcgis/rest/services/GreenvilleCountyBaseData/FeatureServer/2/query",
        "page_size": 2000,
        "deed_field": "DEEDDATE",
    },
]

TIRED_LANDLORD_THRESHOLD = 3
UPSERT_BATCH_SIZE = 2000
MIN_SANE_PARCELS = 50000  # full county is ~96k; fewer means a layer fetch failed


def normalize_deed_date(v):
    """
    ArcGIS REST returns date fields as epoch milliseconds (UTC) when f=json.
    Defensively also handle a plain date/datetime string in case a layer ever
    serializes differently. Returns an ISO date string ('YYYY-MM-DD') or None.
    """
    if v is None or v == "":
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc).date().isoformat()
        if isinstance(v, str):
            return v.strip()[:10] or None
    except Exception:
        return None
    return None


def fetch_all(layer):
    out = []
    offset = 0
    page_size = layer["page_size"]
    out_fields = f"{OUT_FIELDS_BASE},{layer['deed_field']}"
    while True:
        params = {
            "where": "1=1",
            "outFields": out_fields,
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
        for f in feats:
            attrs = f["attributes"]
            attrs["DEED_DATE_NORM"] = normalize_deed_date(attrs.pop(layer["deed_field"], None))
            out.append(attrs)
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
    if not strnum or not locate or locate.strip().upper() in ("SYMBOLIC", "", "NONE"):
        return None
    addr = f"{strnum.strip()} {locate.strip()}"
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr.title() if addr else None


def mailing_street_is_valid(mailing_street):
    """
    BUG FIX (found 2026-09-2x, T Dawg's own spot-check): the county's STREET
    field is documented as the owner's mailing address, but for ~430 of
    41,283 absentee-flagged parcels it actually contains a second owner's
    NAME instead of a street address (e.g. "SMITH JOHN R") -- a data-quality
    problem in the upstream county ArcGIS source, not a comparison bug in
    this script. Left unguarded, is_absentee() compared that name against
    the property address, it never matched, and the parcel got flagged
    absentee_owner even when the real owner lives right there.
    A genuine mailing street always starts with a house/box number. Anything
    that doesn't is untrustworthy for the absentee comparison -- return False
    here so is_absentee() backs off to "unknown" (None) instead of guessing.
    """
    if not mailing_street:
        return False
    s = mailing_street.strip().upper()
    if re.match(r"^\d", s):
        return True
    if re.match(r"^P\.?\s*O\.?\s*BOX\b", s):
        return True
    return False


def is_absentee(mailing_street, mailing_state, prop_strnum, prop_locate):
    # ORDER FIX 2026-09-24: the name-in-STREET check now runs FIRST. When the
    # county's STREET field holds a person's name, the mailing record is
    # misaligned, so its STATE can't be trusted either -- previously an
    # out-of-state STATE value short-circuited to True before this check ran.
    if not mailing_street_is_valid(mailing_street):
        # Can't safely compare a non-address string (e.g. a person's name)
        # against the property address -- don't guess. Caught separately by
        # refresh_needs_review()'s invalid_mailing_address flag below.
        return None
    if mailing_state and mailing_state.strip().upper() != "SC":
        return True
    if not prop_strnum or not prop_locate:
        return None
    mail_n = re.sub(r"\s+", " ", mailing_street).strip().upper()
    prop_n = f"{prop_strnum.strip()} {prop_locate.strip()}".upper()
    return not mail_n.startswith(prop_n[:8]) if prop_n else None


def parcel_absentee(r):
    """Legal residence (county RRETCD "2") is never absentee; otherwise
    fall back to the mailing-address comparison."""
    if str(r.get("RRETCD") or "").strip() == LEGAL_RESIDENCE_CODE:
        return False
    return is_absentee(r.get("STREET"), r.get("STATE"), r.get("STRNUM"), r.get("LOCATE"))


def is_long_term_owner(deed_date_str):
    """
    True if the current owner's deed is LONG_TERM_OWNER_YEARS+ old, False if
    newer, None if the county has no usable deed date. Ownership length only
    -- says nothing about equity (see module docstring).
    """
    if not deed_date_str:
        return None
    try:
        deed_date = datetime.strptime(deed_date_str[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    return (date.today() - deed_date).days / 365.25 >= LONG_TERM_OWNER_YEARS


def ensure_status_column(conn):
    """
    IMPORTANT: 'status' already exists on this table as a CRM/GHL pipeline
    field (default 'new' -- new/contacted/etc, alongside phone/email). An
    earlier version of this fix mistakenly reused that same column for
    sold/active tracking, which would have silently overwritten T Dawg's
    outreach-pipeline status the next time a lead resolved. Use a SEPARATE
    is_sold boolean + sold_at date instead so this never touches 'status'.
    """
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists is_sold boolean not null default false")
        cur.execute("alter table leads add column if not exists sold_at date")
        cur.execute("create index if not exists idx_leads_is_sold on leads(is_sold)")


def ensure_review_column(conn):
    """
    Checked information_schema.columns first -- 'needs_review',
    'review_reasons', and 'display_address' were all unused. display_address
    is a STORED GENERATED column (never written directly, always derived)
    so it can never drift out of sync and never interferes with the
    lower(address) upsert conflict target.

    TRIPLE-ASTERISK FLAG (added 2026-09-22, T Dawg's explicit ask): '***'
    prefix means "confirmed vacant AND behind on taxes" -- the opposite of
    the single-'*' needs_review flag (that one means "double check this,
    might be wrong"; '***' means "high-confidence, act on this one first").
    HONEST CAVEAT, don't remove this comment: is_vacant is NOT currently
    populated by any script in this pipeline. Exhaustive research (2026-09-22)
    into every public vacancy proxy for Greenville County -- USPS vacancy
    data (access restricted to government/nonprofit entities), Greenville
    Water / Blue Ridge Rural Water disconnect data (no public dataset), a
    vacant-property registration ordinance (none exists for the county or
    city) -- came back NOT VIABLE across the board. Being tax-delinquent
    does NOT by itself mean a property is vacant (plenty of occupied owners
    fall behind on taxes), so this pipeline will not guess at is_vacant.
    Generated-column logic is wired up and ready to fire the moment a real
    vacancy source is found and is_vacant starts getting set to true/false
    (not just left null) -- until then this will correctly show '***' on
    zero properties rather than fabricate confidence the data doesn't support.
    Recreated (drop+re-add) every run rather than "if not exists" so this
    expression always reflects the latest logic.
    """
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists needs_review boolean not null default false")
        cur.execute("alter table leads add column if not exists review_reasons text[] not null default '{}'")
        cur.execute("create index if not exists idx_leads_needs_review on leads(needs_review)")
        # FIX 2026-09-25: this used to DROP and re-ADD the column every
        # night. For a STORED generated column that rewrites every row in
        # the table -- a big reason the free-plan DB hit 352 MB. Now it is
        # only created when missing.
        cur.execute(
            """
            alter table leads add column if not exists display_address text generated always as (
                case
                    when is_vacant is true and (
                        'tax_sale' = any(source_tags) or 'redemption_period' = any(source_tags)
                    ) then '***' || address
                    when needs_review then '*' || address
                    else address
                end
            ) stored
            """
        )

def refresh_needs_review(conn):
    """
    Flags a lead for T Dawg to double-check before spending money on it.
    Any ONE of these triggers the '*':
      - single_source: RECORDED only, no '*' (changed 2026-09-30: 81% of the
        house list is on one list, so the '*' stopped meaning "check this
        address/owner" -- which is what T Dawg's '*' rule is for)
      - missing_owner: no owner name on file
      - incomplete_address: no city or zip -- RECORDED in review_reasons but
        does NOT by itself set the '*' (it's EXPECTED for most leads since the
        2026-09-25 fix -- the county parcel feed has no site city/zip, and the
        old values were the owner's mailing zip / a hard-coded 'Greenville',
        i.e. wrong. Address standardization (e.g. Smarty) will fill these.)
      - absentee_flag_but_same_address: flagged absentee but mailing == property
      - corrupted_address: the address contains the literal word "None"
        (county LOCATE field is sometimes the text "None")
      - invalid_mailing_address: county mailing field holds a NAME, not an
        address, so absentee status can't be judged
      - probate_owner_mismatch (added 2026-09-25): a probate lead whose
        county owner shares no name with the decedent -- the probate record's
        address may be where they LIVED (rental, relative, nursing home), not
        a property they owned
    Only real leads (list_count > 0) are flagged; non-leads are cleared.
    Only rows whose flag actually changes are written.
    """
    with conn.cursor() as cur:
        cur.execute(
            r"""
            with r as (
                select id, array_remove(array[
                    case when list_count <= 1 then 'single_source' end,
                    case when owner_name is null or owner_name = '' then 'missing_owner' end,
                    case when city is null or city = '' or zip is null or zip = '' then 'incomplete_address' end,
                    case when mailing_address is not null
                         and lower(regexp_replace(mailing_address, '[^a-zA-Z0-9]', '', 'g'))
                           = lower(regexp_replace(address, '[^a-zA-Z0-9]', '', 'g'))
                         and is_absentee = true then 'absentee_flag_but_same_address' end,
                    -- 2026-09-30: also no real street name ("9 A" from a PropStream row)
                    case when address ~* '\mnone\M' or address !~* '^\s*\d+\S*\s+.*[a-z]{2,}'
                         then 'corrupted_address' end,
                    case when mailing_address is not null
                         and mailing_address !~* '^\s*(\d|p\.?\s*o\.?\s*box\M)'
                         then 'invalid_mailing_address' end,
                    case when land_use is null or land_use !~ '^\s*\d' then 'land_use_unknown' end,
                    case when raw->'probate'->>'match' = 'owner_name' then 'probate_name_match' end,
                    case when 'probate' = any(source_tags)
                         and owner_name is not null
                         and not exists (
                             select 1 from regexp_split_to_table(
                                 upper(coalesce(raw->'probate'->>'decedent_name', '')), '[^A-Z]+') tok
                             where length(tok) >= 3 and upper(owner_name) like '%%' || tok || '%%')
                         then 'probate_owner_mismatch' end
                ], null) as reasons
                from leads
                where is_sold = false and is_duplicate = false and list_count > 0
            )
            update leads l set
                review_reasons = r.reasons,
                needs_review = cardinality(array_remove(array_remove(r.reasons, 'incomplete_address'), 'single_source')) > 0
            from r
            where l.id = r.id
              and (l.review_reasons is distinct from r.reasons
                   or l.needs_review is distinct from
                      (cardinality(array_remove(array_remove(r.reasons, 'incomplete_address'), 'single_source')) > 0))
            """
        )
        cur.execute(
            """
            update leads set needs_review = false, review_reasons = '{}'
            where (list_count = 0 or is_sold or is_duplicate) and (needs_review or cardinality(review_reasons) > 0)
            """
        )


def mark_sold_by_recent_deed(conn, all_parcels):
    """
    Cross-reference every parcel's deed date against existing leads. A deed
    recorded within SOLD_LOOKBACK_DAYS means the property changed hands, so
    whatever distress condition put it on a list is resolved. Matches by PIN,
    stored under either raw->absentee_owner->pin or raw->tax_sale->map_number
    depending on which source touched the row first — same assessor parcel ID.
    raw->absentee_owner->pin can be a single string OR a json array (when
    de-duped parcels share one address), so match with jsonb containment (@>)
    rather than ->>'pin' = text, which only works for the scalar case.
    Returns how many leads got flipped to sold.
    """
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=SOLD_LOOKBACK_DAYS)).isoformat()
    recent_sales = [
        (pin, r["DEED_DATE_NORM"])
        for pin, r in all_parcels.items()
        if r.get("DEED_DATE_NORM") and r["DEED_DATE_NORM"] >= cutoff
    ]
    if not recent_sales:
        return 0
    with conn.cursor() as cur:
        cur.execute("create temporary table pin_deed (pin text, deed_date date) on commit drop")
        execute_values(cur, "insert into pin_deed (pin, deed_date) values %s", recent_sales)
        cur.execute(
            """
            update leads set is_sold = true, sold_at = pd.deed_date, score = 0, updated_at = now()
            from pin_deed pd
            where pd.deed_date >= %s
              and (
                    leads.pin = pd.pin
                 or leads.raw->'absentee_owner'->'pin' @> to_jsonb(pd.pin)
                 or regexp_replace(coalesce(leads.raw->'tax_sale'->>'map_number', ''), '\\D', '', 'g') = pd.pin
              )
              and leads.is_sold is distinct from true
            """,
            (cutoff,),
        )
        return cur.rowcount



def enrich_existing_leads(conn, parcels, owner_counts, tired_owners):
    """
    ENRICH ONLY (2026-09-25 rewrite). This script used to CREATE a lead for
    every absentee / tired-landlord / high-equity parcel county-wide, which is
    how the table ballooned to ~98k rows that were not on any distress list.
    Now it only writes owner facts onto properties that are ALREADY real
    leads (on at least one distress list):
        owner_name, mailing_address, is_absentee, is_tired_landlord,
        is_long_term_owner, owner_parcel_count, pin, raw->absentee_owner
    Matching is by address core (address minus street-type suffix, since the
    county LOCATE field has no suffix). A core shared by parcels with
    DIFFERENT owners (e.g. "100 Main" in two towns) is ambiguous and skipped
    rather than guessed.
    """
    by_core = {}
    for p in parcels:
        core = address_core(p["address"])
        if not core:
            continue
        by_core.setdefault(core, []).append(p)

    rows = []
    ambiguous = 0
    for core, group in by_core.items():
        owners = {g["owner_name"] for g in group}
        if len(owners) > 1:
            ambiguous += 1
            continue
        g = group[0]
        absentee_vals = {x["absentee"] for x in group}
        absentee = True if True in absentee_vals else (False if absentee_vals == {False} else None)
        extra = {
            "pin": g["pin"] if len(group) == 1 else [x["pin"] for x in group],
            "deed_date": g["deed_date"],
            "sale_price": g["sale_price"],
            "total_tax": g["total_tax"],
            "landuse": g["landuse"],
            "owner_parcel_count": owner_counts.get(g["owner_name"], 1),
            "mailing_state": g["mailing_state"],
            "mailing_zip": g["mailing_zip"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        rows.append((
            core, g["pin"], g["owner_name"],
            g["mailing_address"] if mailing_is_address(g["mailing_address"]) else None,
            absentee, g["owner_name"] in tired_owners, g["long_term"],
            owner_counts.get(g["owner_name"], 1), json.dumps(extra),
        ))
    print(f"  enrichment candidates: {len(rows)} address cores ({ambiguous} ambiguous cores skipped)")

    with conn.cursor() as cur:
        cur.execute(
            """
            create temporary table parcel_attrs (
                core text primary key, pin text, owner_name text, mailing_address text,
                is_absentee boolean, is_tired boolean, is_long_term boolean,
                owner_parcel_count int, extra jsonb
            ) on commit drop
            """
        )
        execute_values(cur, "insert into parcel_attrs values %s", rows, page_size=5000)
        cur.execute(
            """
            update leads l set
                owner_name = coalesce(p.owner_name, l.owner_name),
                mailing_address = coalesce(p.mailing_address, l.mailing_address),
                is_absentee = coalesce(p.is_absentee, l.is_absentee),
                is_tired_landlord = p.is_tired,
                is_long_term_owner = p.is_long_term,
                owner_parcel_count = p.owner_parcel_count,
                pin = coalesce(l.pin, p.pin),
                raw = coalesce(l.raw, '{}'::jsonb) || jsonb_build_object('absentee_owner', p.extra),
                updated_at = now()
            from parcel_attrs p
            where address_core(l.address) = p.core
              and l.is_sold = false and l.is_duplicate = false and l.list_count > 0
            """
        )
        n = cur.rowcount
    print(f"  enriched {n} existing lead(s) with county owner facts")
    return n


def flag_out_of_state_land(conn, parcels, full_fetch):
    """
    CHANGED 2026-09-28 (T Dawg's call): out-of-state owners of vacant land
    used to be a distress LIST that created ~1,300 leads on its own. Being
    out of state proves nothing is wrong with the property -- it's an owner
    fact like absentee. Now it's the is_out_of_state_land column: set on
    parcels already in the table (matched by PIN), never creates a lead,
    never counts as a list. Cleared for parcels that no longer qualify, but
    only after a full county fetch.
    """
    pins = sorted({p["pin"] for p in parcels if p["out_of_state_land"] and p.get("pin")})
    with conn.cursor() as cur:
        cur.execute("update leads set is_out_of_state_land = true "
                    "where pin = any(%s::text[]) and is_out_of_state_land is not true", (pins,))
        n = cur.rowcount
        cleared = 0
        if full_fetch:
            cur.execute("update leads set is_out_of_state_land = null where is_out_of_state_land is true "
                        "and coalesce(pin, '') <> all(%s::text[])", (pins or ["__none__"],))
            cleared = cur.rowcount
    print(f"  out-of-state vacant land: {len(pins)} parcels, {n} newly flagged, {cleared} cleared")
    return pins


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
            # digits only, so it matches tax/code-violation parcel numbers
            pin = re.sub(r"\D", "", str(r.get("PIN") or ""))
            if pin and pin not in all_parcels:
                all_parcels[pin] = r
    print(f"Total unique parcels across both layers: {len(all_parcels)}")

    processed = []
    owner_counts = Counter()
    for pin, r in all_parcels.items():
        address = normalize_addr(r.get("STRNUM"), r.get("LOCATE"))
        if not address:
            continue
        owner_name = normalize_owner(r.get("OWNAM1"))
        mailing = (r.get("STREET") or "").strip() or None
        processed.append({
            "pin": pin,
            "address": address,
            "owner_name": owner_name,
            "mailing_address": mailing,
            "mailing_state": (r.get("STATE") or "").strip() or None,
            "mailing_zip": (r.get("ZIP5") or "").strip() or None,
            "absentee": parcel_absentee(r),
            "long_term": is_long_term_owner(r.get("DEED_DATE_NORM")),
            "out_of_state_land": (
                str(r.get("LANDUSE") or "").strip() in VACANT_LAND_CODES
                and mailing_street_is_valid(r.get("STREET"))
                and bool(r.get("STATE")) and r["STATE"].strip().upper() != "SC"
            ),
            "deed_date": r.get("DEED_DATE_NORM"),
            "sale_price": r.get("SLPRICE"),
            "total_tax": r.get("TOTTAX"),
            "landuse": r.get("LANDUSE"),
        })
        if owner_name:
            owner_counts[owner_name] += 1

    tired_owners = {n for n, c in owner_counts.items() if c >= TIRED_LANDLORD_THRESHOLD}
    print(f"Owners with {TIRED_LANDLORD_THRESHOLD}+ parcels: {len(tired_owners)}")

    conn = psycopg2.connect(db_url)
    ensure_status_column(conn)
    ensure_review_column(conn)
    ensure_schema(conn)
    conn.commit()
    run_migrations(conn)

    oosl_pins = flag_out_of_state_land(conn, processed, len(all_parcels) >= MIN_SANE_PARCELS)
    if len(all_parcels) < MIN_SANE_PARCELS:
        print(f"  only {len(all_parcels)} parcels fetched (< {MIN_SANE_PARCELS}); skipped clearing flags")

    # A partial county fetch would under-count parcels per owner and wrongly
    # clear is_tired_landlord -- only enrich from a full fetch.
    if len(all_parcels) >= MIN_SANE_PARCELS:
        enriched = enrich_existing_leads(conn, processed, owner_counts, tired_owners)
    else:
        enriched = 0
        print("  enrichment skipped: partial county fetch")
    conn.commit()

    sold_count = mark_sold_by_recent_deed(conn, all_parcels)
    print(f"Marked {sold_count} leads as sold (deed recorded in last {SOLD_LOOKBACK_DAYS} days).")

    rescore_all(conn)
    refresh_needs_review(conn)
    log_run(conn, len(processed), 0,
            f"ok: enriched {enriched} leads, {len(oosl_pins)} out-of-state land parcels flagged, "
            f"{len(tired_owners)} tired-landlord owners, {sold_count} marked sold")
    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
