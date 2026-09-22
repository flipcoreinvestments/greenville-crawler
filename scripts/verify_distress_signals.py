#!/usr/bin/env python3
"""
Live cross-verification of distress signals for the highest-priority
(3+ list) leads.

WHY THIS EXISTS: every ingest script in this pipeline is additive-only --
once a tag (tax_sale, foreclosure_mie, etc.) is written onto a lead it is
NEVER removed, even after the underlying case/permit/sale resolves. T Dawg
asked, after seeing 13k+ leads stacked on 3+ lists, whether those signals
are still actually live right now, not just historically true. This script
answers that for real: it re-pulls the CURRENT live list from every
re-checkable source -- reusing the exact fetch/parse functions already
proven in each ingest script (imported directly, not reimplemented, so a
fix made there is automatically used here too) -- and checks every
list_count>=3 lead's tags against what's actually live today.

Matching key per source (chosen for reliability, see comments inline):
  tax_sale            -> map_number
  redemption_period   -> map_number
  foreclosure_mie /
  hoa_foreclosure     -> case_number
  permit_expired      -> address (against the live stalled-permit query)
  permit_demolition   -> address (against the live demolition query)
  insurance_damage    -> address (against the live damage-keyword query)
    NOTE: address, not permit_num, for these three. building_permits_ingest.py
    stores only ONE permit's data under raw->'building_permits' even when a
    property carries more than one tag from that script (later upsert
    overwrites the shared jsonb key), so permit_num isn't reliable to key on
    for whichever tag wasn't the last one written for that address. Address
    matching sidesteps that entirely.
  code_violation      -> case_number
  probate             -> targeted per-case re-fetch. This source has no
    "current full list" endpoint (it's a sequential case-number crawl), so
    each probate-tagged lead's own case_number is re-fetched directly and
    checked for a Closed Date.

NOT touched here (by design): is_absentee, tired_landlord, high_equity.
Those come from absentee_owner_ingest.py's full-county nightly sweep, which
recomputes EVERY parcel from live assessor data every run (not additive --
already continuously self-verifying). The workflow that runs this script
re-runs that ingest immediately before this one so those three are freshly
current too, without duplicating its logic here.

Writes results to three new leads columns (created if missing):
  distress_verification jsonb  -- {checked_at, live_tags, stale_tags, unverifiable_tags}
  distress_verified_at timestamptz
  distress_all_live boolean    -- every checkable tag on this lead confirmed still live
  distress_any_stale boolean   -- at least one tag no longer found on its live source

Never sets is_sold, never deletes anything, never removes a source tag --
what to do about a stale-flagged lead stays T Dawg's call.

Requires env var DATABASE_URL. Run manually via the "Verify distress
signals (3+ list leads)" GitHub Actions workflow.
"""

import os
import re
import sys
import time
import json
from datetime import datetime, timezone, date, timedelta

import requests
import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tax_sale_ingest as ts
import redemption_period_ingest as rp
import foreclosure_mie_ingest as fm
import building_permits_ingest as bp
import code_enforcement_ingest as ce
import probate_ingest as pb

PROBATE_REQUEST_DELAY_SECONDS = 0.4
SELF_VERIFYING_TAGS = {"absentee_owner", "tired_landlord", "high_equity"}


def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists distress_verification jsonb")
        cur.execute("alter table leads add column if not exists distress_verified_at timestamptz")
        cur.execute("alter table leads add column if not exists distress_all_live boolean")
        cur.execute("alter table leads add column if not exists distress_any_stale boolean")


def get_target_leads(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select id, address, source_tags, raw
            from leads
            where is_sold = false and list_count >= 3
            """
        )
        return cur.fetchall()


def norm_addr_simple(a):
    return re.sub(r"\s+", " ", (a or "")).strip().rstrip(",").lower()


def build_tax_sale_live_set():
    try:
        html = ts.fetch(ts.LIST_URL)
        rows = ts.parse_list(html)
        return {r["map_number"] for r in rows if r.get("map_number")}
    except Exception as e:
        print(f"  WARNING: tax_sale live pull failed: {e}", file=sys.stderr)
        return None


def build_redemption_live_set():
    live = set()
    today = date.today()
    try:
        any_year_resolved = False
        for year in (today.year, today.year - 1):
            url, html = rp.find_year_page(year)
            if not url:
                continue
            sale_date = rp.parse_sale_date(html)
            if not sale_date:
                continue
            redemption_deadline = sale_date + timedelta(days=rp.REDEMPTION_DAYS)
            if redemption_deadline < today:
                continue
            any_year_resolved = True
            rows = rp.parse_list(html)
            live.update(r["map_number"] for r in rows if r.get("map_number"))
        return live if any_year_resolved else set()
    except Exception as e:
        print(f"  WARNING: redemption_period live pull failed: {e}", file=sys.stderr)
        return None


def build_foreclosure_live_set(page, context):
    try:
        dates = fm.fetch_future_sale_dates(page)
        live_cases = set()
        for d in dates:
            html = fm.fetch_sale_list(context, d)
            rows = fm.parse_sale_list(html)
            live_cases.update(r["case_number"] for r in rows if r.get("case_number"))
            time.sleep(fm.REQUEST_DELAY_SECONDS)
        return live_cases
    except Exception as e:
        print(f"  WARNING: foreclosure_mie live pull failed: {e}", file=sys.stderr)
        return None


def build_permits_live_sets():
    cutoff = date.today()
    stale_cutoff_date = date.fromordinal(cutoff.toordinal() - bp.STALE_DAYS)
    stale_cutoff_num = int(stale_cutoff_date.strftime("%Y%m%d"))
    result = {}

    try:
        demo_rows = bp.fetch_rows("PERMIT_TYPE LIKE '%DEM%'")
        result["permit_demolition"] = {
            norm_addr_simple(bp.normalize_address(r.get("STREETADDRESS")))
            for r in demo_rows if r.get("STREETADDRESS")
        }
    except Exception as e:
        print(f"  WARNING: demolition live pull failed: {e}", file=sys.stderr)
        result["permit_demolition"] = None

    try:
        stalled_rows = bp.fetch_rows(f"BP_STATUS='IS' AND APPLICDATE < {stale_cutoff_num}")
        result["permit_expired"] = {
            norm_addr_simple(bp.normalize_address(r.get("STREETADDRESS")))
            for r in stalled_rows if r.get("STREETADDRESS")
        }
    except Exception as e:
        print(f"  WARNING: stalled-permit live pull failed: {e}", file=sys.stderr)
        result["permit_expired"] = None

    try:
        damage_rows = bp.fetch_rows(bp.build_damage_where_clause())
        result["insurance_damage"] = {
            norm_addr_simple(bp.normalize_address(r.get("STREETADDRESS")))
            for r in damage_rows if r.get("STREETADDRESS")
        }
    except Exception as e:
        print(f"  WARNING: damage-permit live pull failed: {e}", file=sys.stderr)
        result["insurance_damage"] = None

    return result


def build_code_violation_live_set():
    try:
        session = requests.Session()
        session.headers.update(ce.HEADERS)
        rows = ce.fetch_all_cases(session)
        return {r["case_number"] for r in rows if r.get("case_number")}
    except Exception as e:
        print(f"  WARNING: code_violation live pull failed: {e}", file=sys.stderr)
        return None


def check_probate_case_live(session, case_number):
    """Returns True (still open), False (closed), or None (couldn't determine)."""
    try:
        html = pb.fetch_case(session, case_number)
    except Exception:
        return None
    if html is None:
        return None
    try:
        parsed = pb.parse_case_detail(html, case_number)
    except Exception:
        return None
    if parsed is None:
        return None
    return not bool(parsed.get("closed_date"))


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    ensure_columns(conn)
    conn.commit()

    leads = get_target_leads(conn)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Verifying distress signals for "
          f"{len(leads)} leads on 3+ lists...")

    print("Pulling live tax sale list...")
    tax_sale_live = build_tax_sale_live_set()
    print(f"  -> {'FAILED' if tax_sale_live is None else len(tax_sale_live)} live tax-sale map numbers")

    print("Pulling live redemption-period list...")
    redemption_live = build_redemption_live_set()
    print(f"  -> {'FAILED' if redemption_live is None else len(redemption_live)} live redemption map numbers")

    print("Pulling live foreclosure (MIE) sale list via headless browser...")
    foreclosure_live = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context, page = fm.new_browser_context(browser)
            foreclosure_live = build_foreclosure_live_set(page, context)
            browser.close()
    except Exception as e:
        print(f"  WARNING: foreclosure_mie browser session failed: {e}", file=sys.stderr)
    print(f"  -> {'FAILED' if foreclosure_live is None else len(foreclosure_live)} live foreclosure case numbers")

    print("Pulling live building permits (demolition/stalled/damage)...")
    permits_live = build_permits_live_sets()
    for k, v in permits_live.items():
        print(f"  -> {k}: {'FAILED' if v is None else len(v)} live addresses")

    print("Pulling live code enforcement (unfit structures) list...")
    code_violation_live = build_code_violation_live_set()
    print(f"  -> {'FAILED' if code_violation_live is None else len(code_violation_live)} live case numbers")

    probate_session = requests.Session()
    probate_session.headers.update(pb.HEADERS)
    try:
        probate_session.get(pb.BASE_URL, timeout=30)
    except Exception:
        pass

    all_live = 0
    any_stale = 0
    could_not_verify = 0
    stale_by_tag = {}
    checked_count = 0
    probate_checks = 0
    updates = []

    print(f"Cross-checking {len(leads)} leads against live pulls...")
    for lead in leads:
        tags = lead["source_tags"] or []
        raw = lead["raw"] or {}
        addr_norm = norm_addr_simple(lead["address"])
        live_tags = []
        stale_tags = []
        unverifiable_tags = []

        for tag in tags:
            if tag in SELF_VERIFYING_TAGS:
                continue

            if tag == "tax_sale":
                mn = (raw.get("tax_sale") or {}).get("map_number")
                if tax_sale_live is None or not mn:
                    unverifiable_tags.append(tag)
                elif mn in tax_sale_live:
                    live_tags.append(tag)
                else:
                    stale_tags.append(tag)

            elif tag == "redemption_period":
                mn = (raw.get("redemption_period") or {}).get("map_number")
                if redemption_live is None or not mn:
                    unverifiable_tags.append(tag)
                elif mn in redemption_live:
                    live_tags.append(tag)
                else:
                    stale_tags.append(tag)

            elif tag in ("foreclosure_mie", "hoa_foreclosure"):
                cn = (raw.get("foreclosure_mie") or {}).get("case_number")
                if foreclosure_live is None or not cn:
                    unverifiable_tags.append(tag)
                elif cn in foreclosure_live:
                    live_tags.append(tag)
                else:
                    stale_tags.append(tag)

            elif tag in ("permit_expired", "permit_demolition", "insurance_damage"):
                live_set = permits_live.get(tag)
                if live_set is None:
                    unverifiable_tags.append(tag)
                elif addr_norm in live_set:
                    live_tags.append(tag)
                else:
                    stale_tags.append(tag)

            elif tag == "code_violation":
                cn = (raw.get("code_violation") or {}).get("case_number")
                if code_violation_live is None or not cn:
                    unverifiable_tags.append(tag)
                elif cn in code_violation_live:
                    live_tags.append(tag)
                else:
                    stale_tags.append(tag)

            elif tag == "probate":
                cn = (raw.get("probate") or {}).get("case_number")
                if not cn:
                    unverifiable_tags.append(tag)
                else:
                    still_open = check_probate_case_live(probate_session, cn)
                    probate_checks += 1
                    time.sleep(PROBATE_REQUEST_DELAY_SECONDS)
                    if still_open is None:
                        unverifiable_tags.append(tag)
                    elif still_open:
                        live_tags.append(tag)
                    else:
                        stale_tags.append(tag)

            else:
                unverifiable_tags.append(tag)

        checked_tags = live_tags + stale_tags
        is_all_live = bool(checked_tags) and not stale_tags
        has_any_stale = bool(stale_tags)

        if has_any_stale:
            any_stale += 1
            for t in stale_tags:
                stale_by_tag[t] = stale_by_tag.get(t, 0) + 1
        elif checked_tags:
            all_live += 1
        else:
            could_not_verify += 1

        verification = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "live_tags": live_tags,
            "stale_tags": stale_tags,
            "unverifiable_tags": unverifiable_tags,
        }
        updates.append((json.dumps(verification), is_all_live, has_any_stale, lead["id"]))

        checked_count += 1
        if checked_count % 1000 == 0:
            print(f"  ...{checked_count}/{len(leads)} leads checked ({probate_checks} probate re-fetches so far)")

    print(f"Writing verification results for {len(updates)} leads...")
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            update leads set
                distress_verification = %s::jsonb,
                distress_all_live = %s,
                distress_any_stale = %s,
                distress_verified_at = now()
            where id = %s
            """,
            updates,
            page_size=500,
        )
    conn.commit()
    conn.close()

    print("=" * 60)
    print(f"DONE. {len(leads)} leads on 3+ lists checked.")
    print(f"  Fully live (every checkable tag confirmed still active): {all_live}")
    print(f"  At least one stale tag (signal may already be resolved): {any_stale}")
    print(f"  No tag could be checked against a live source:           {could_not_verify}")
    if stale_by_tag:
        print("  Stale tags by type:")
        for t, c in sorted(stale_by_tag.items(), key=lambda x: -x[1]):
            print(f"    {t}: {c}")


if __name__ == "__main__":
    main()
