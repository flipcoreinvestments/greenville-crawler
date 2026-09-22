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
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values

SOURCE_NAME = "absentee_owner"
SOLD_LOOKBACK_DAYS = 270

OUT_FIELDS_BASE = "PIN,OWNAM1,OWNAM2,STREET,CITY,STATE,ZIP5,STRNUM,LOCATE,SLPRICE,TOTTAX,LANDUSE"

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


def is_absentee(mailing_street, mailing_state, prop_strnum, prop_locate):
    if mailing_state and mailing_state.strip().upper() != "SC":
        return True
    if not mailing_street or not prop_strnum or not prop_locate:
        return None
    mail_n = re.sub(r"\s+", " ", mailing_street).strip().upper()
    prop_n = f"{prop_strnum.strip()} {prop_locate.strip()}".upper()
    return not mail_n.startswith(prop_n[:8]) if prop_n else None


def upsert_leads_batch(conn, rows):
    """
    Batched upsert via execute_values — replaces the old per-row execute()
    loop that took 33+ minutes for ~35,800 rows. Same conflict/merge logic,
    just sent in chunks of UPSERT_BATCH_SIZE instead of one row per round trip.
    rows: list of tuples (address, mailing_zip, owner_name, mailing_addr, absentee, tags, raw_json)
    """
    sql = """
        insert into leads (address, city, state, zip, county, owner_name, mailing_address,
                            is_absentee, source_tags, raw)
        values %s
        on conflict (lower(address)) do update set
            owner_name = coalesce(excluded.owner_name, leads.owner_name),
            mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
            is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
            source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
            raw = leads.raw || excluded.raw,
            updated_at = now()
    """
    template = "(%s, 'Greenville', 'SC', %s, 'Greenville', %s, %s, %s, %s::text[], %s::jsonb)"
    with conn.cursor() as cur:
        for i in range(0, len(rows), UPSERT_BATCH_SIZE):
            chunk = rows[i:i + UPSERT_BATCH_SIZE]
            execute_values(cur, sql, chunk, template=template, page_size=len(chunk))
            print(f"  upserted batch {i // UPSERT_BATCH_SIZE + 1} ({len(chunk)} rows)")


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
    is a STORED GENERATED column (never written directly, always derived from
    needs_review + address) so it can never drift out of sync and never
    interferes with the lower(address) upsert conflict target.
    """
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists needs_review boolean not null default false")
        cur.execute("alter table leads add column if not exists review_reasons text[] not null default '{}'")
        cur.execute("create index if not exists idx_leads_needs_review on leads(needs_review)")
        cur.execute(
            "select 1 from information_schema.columns where table_name='leads' and column_name='display_address'"
        )
        if not cur.fetchone():
            cur.execute(
                """
                alter table leads add column display_address text generated always as (
                    case when needs_review then '*' || address else address end
                ) stored
                """
            )


def refresh_needs_review(conn):
    """
    Flags a lead for T Dawg to manually double-check before she spends money
    reverse-searching/skip-tracing it. Any ONE of these triggers a flag:
      - single_source: only ever touched by one list (list_count <= 1) --
        never cross-referenced/stacked against a second source
      - missing_owner: no owner name on file
      - incomplete_address: missing city or zip
      - absentee_flag_but_same_address: flagged absentee but the mailing
        address string is actually identical to the property address --
        an internal contradiction worth a manual look
      - corrupted_address: the address string contains the literal word
        "none" -- confirmed 2026-09-21 that the county assessor's LOCATE
        field is sometimes literally the text "None" (not a null value,
        an actual 4-character string), which produces addresses like
        "24749 None". Catches this regardless of which ingest script wrote
        the row, since this function runs a full-table pass every night.
    Runs against every non-sold lead, not just rows this script upserted,
    since this script already does a full-table pass nightly.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update leads set
                needs_review = true,
                review_reasons = array_remove(array[
                    case when list_count <= 1 then 'single_source' end,
                    case when owner_name is null or owner_name = '' then 'missing_owner' end,
                    case when city is null or city = '' or zip is null or zip = '' then 'incomplete_address' end,
                    case when mailing_address is not null
                         and lower(regexp_replace(mailing_address, '[^a-zA-Z0-9]', '', 'g'))
                           = lower(regexp_replace(address, '[^a-zA-Z0-9]', '', 'g'))
                         and is_absentee = true then 'absentee_flag_but_same_address' end,
                    case when address ~* 'none' then 'corrupted_address' end
                ], null)
            where is_sold = false
            """
        )
        cur.execute(
            """
            update leads set needs_review = false, review_reasons = '{}'
            where is_sold = false
              and list_count > 1
              and owner_name is not null and owner_name <> ''
              and city is not null and city <> ''
              and zip is not null and zip <> ''
              and address !~* 'none'
              and not (mailing_address is not null
                       and lower(regexp_replace(mailing_address, '[^a-zA-Z0-9]', '', 'g'))
                         = lower(regexp_replace(address, '[^a-zA-Z0-9]', '', 'g'))
                       and is_absentee = true)
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
                    leads.raw->'absentee_owner'->'pin' @> to_jsonb(pd.pin)
                 or leads.raw->'tax_sale'->>'map_number' = pd.pin
              )
              and leads.is_sold is distinct from true
            """,
            (cutoff,),
        )
        return cur.rowcount


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
      +35 if the property is in an active tax-sale redemption period (owner
          is about to permanently lose the property if they don't act)
    Only touches is_sold=false rows so a property already flipped by
    mark_sold_by_recent_deed() stays at score 0 instead of being rescored
    back up. Does NOT touch 'status' -- that's T Dawg's CRM/GHL pipeline
    field (new/contacted/etc), completely separate from is_sold.

    UPDATED 2026-09-21: added the redemption_period bonus alongside the new
    redemption_period_ingest.py script.
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
            "deed_date": r.get("DEED_DATE_NORM"),
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
    ensure_status_column(conn)
    ensure_review_column(conn)

    # De-dupe by lower(address) BEFORE building the batch. LOCATE (the assessor's
    # street name) has no suffix, so two different PINs can normalize to the exact
    # same address string (e.g. subdivided/multi-unit parcels) -- sending both as
    # separate rows in the same execute_values() batch trips Postgres's
    # "ON CONFLICT DO UPDATE command cannot affect row a second time"
    # (CardinalityViolation), which is exactly what killed run #2. Merge duplicates
    # into one row instead of crashing the whole batch.
    merged = {}
    for p in absentee_rows + tired_only_rows:
        tags = {"absentee_owner"} if p["absentee"] is True else set()
        if p["owner_name"] in tired_owners:
            tags.add("tired_landlord")
        if not tags:
            continue
        key = p["address"].lower()
        entry = merged.get(key)
        if entry is None:
            merged[key] = {
                "address": p["address"],
                "mailing_zip": p["mailing_zip"],
                "owner_name": p["owner_name"],
                "mailing_address": p["mailing_address"],
                "absentee": p["absentee"],
                "tags": tags,
                "pins": [p["pin"]],
                "deed_date": p["deed_date"],
                "sale_price": p["sale_price"],
                "total_tax": p["total_tax"],
                "landuse": p["landuse"],
                "owner_parcel_count": owner_counts.get(p["owner_name"], 1),
            }
        else:
            entry["tags"] |= tags
            entry["pins"].append(p["pin"])
            if p["absentee"] is True:
                entry["absentee"] = True

    batch_rows = []
    for entry in merged.values():
        extra = {
            "pin": entry["pins"][0] if len(entry["pins"]) == 1 else entry["pins"],
            "deed_date": entry["deed_date"],
            "sale_price": entry["sale_price"],
            "total_tax": entry["total_tax"],
            "landuse": entry["landuse"],
            "owner_parcel_count": entry["owner_parcel_count"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        raw_payload = json.dumps({SOURCE_NAME: extra})
        batch_rows.append((
            entry["address"], entry["mailing_zip"], entry["owner_name"], entry["mailing_address"],
            entry["absentee"], sorted(entry["tags"]), raw_payload,
        ))

    dupes_merged = len(absentee_rows) + len(tired_only_rows) - len(batch_rows)
    if dupes_merged > 0:
        print(f"Merged {dupes_merged} duplicate-address parcels before upsert.")

    upsert_leads_batch(conn, batch_rows)

    sold_count = mark_sold_by_recent_deed(conn, all_parcels)
    print(f"Marked {sold_count} leads as sold (deed recorded in last {SOLD_LOOKBACK_DAYS} days).")

    rescore_all(conn)
    refresh_needs_review(conn)
    log_run(conn, len(processed), len(batch_rows),
            f"ok: {len(absentee_rows)} absentee, {len(tired_only_rows)} tired-only, "
            f"{len(tired_owners)} tired-landlord owners, {sold_count} marked sold")
    conn.commit()
    conn.close()
    print(f"Done. {len(batch_rows)} properties upserted and rescored. {sold_count} marked sold.")


if __name__ == "__main__":
    main()
