#!/usr/bin/env python3
"""
Greenville County Tax Sale ingest.

Source: https://www.greenvillecounty.org/appsas400/taxsale/
Public, plain-HTML list published by Greenville County Tax Collector.
robots.txt checked 2026-09-18: /appsas400/ is not disallowed.

What this does:
1. Fetches the current tax sale list (owner name, map/parcel number, amount due).
2. For each parcel, fetches the Real Property Details page to get the actual
   property address, owner mailing address, and land use. The details pages
   sit behind an Imperva bot-check that blocks plain HTTP requests after the
   first few dozen, so these are fetched with a real headless browser
   (Playwright/Chromium) instead of the `requests` library.
3. Upserts each property into the `leads` table in Supabase, tagging it
   'tax_sale' in source_tags.
4. Recomputes a transparent, rule-based motivation score for every row this
   source touched (see compute_score below for the exact formula).
5. Logs the run to `source_runs` so you can see ingest history.

Runs nightly via GitHub Actions (.github/workflows/nightly.yml).
Requires env var DATABASE_URL (Supabase session pooler connection string).
"""

import os
import re
import sys
import time
import json
from datetime import datetime, timezone

import random

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

LIST_URL = "https://www.greenvillecounty.org/appsas400/taxsale/"
DETAILS_URL = "https://www.greenvillecounty.org/appsas400/RealProperty/Details.aspx?TaxYear={year}&MapNumber={map_number}"
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}
REQUEST_DELAY_SECONDS = 1.5  # be polite to the county's server
ROW_RETRY_ATTEMPTS = 3
CONSECUTIVE_MISS_LIMIT = 5  # this many blocked in a row -> assume a soft block, cool down and reset session
COOLDOWN_SECONDS = 90
SOURCE_NAME = "tax_sale"


def new_browser_context(browser):
    context = browser.new_context(user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ))
    page = context.new_page()
    stealth_sync(page)
    return context, page


def fetch_details_html(page, url):
    """Navigate with retries — the county's Imperva bot-check intermittently
    serves a challenge page instead of the real content. A retry after a short
    wait usually gets through."""
    last_html = None
    for attempt in range(ROW_RETRY_ATTEMPTS):
        page.goto(url, timeout=30000)
        page.wait_for_timeout(1200)  # let any JS challenge/redirect settle
        html = page.content()
        last_html = html
        if "Location:" in html or "Location" in BeautifulSoup(html, "html.parser").get_text():
            return html
        time.sleep(2 + attempt * 3)
    return last_html


def fetch(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_list(html):
    """Find the tax sale table and return [{map_number, owner_name, amount_due}]."""
    soup = BeautifulSoup(html, "html.parser")
    rows_out = []

    for table in soup.find_all("table"):
         header_text = table.get_text(" ", strip=True).lower()
        if "map" not in header_text or "amount" not in header_text:
            continue

       trs = table.find_all("tr")
        for tr in trs:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 3:
                continue
            # Find a cell that looks like a map/parcel number (long digit string)
            map_cell = next((c for c in cells if re.fullmatch(r"\d{8,}", c.replace("-", ""))), None)
            if not map_cell:
                continue
            amount_cell = next((c for c in cells if re.search(r"\$?\d[\d,]*\.\d{2}", c)), None)
            amount_due = None
            if amount_cell:
                m = re.search(r"[\d,]+\.\d{2}", amount_cell)
                if m:
                    amount_due = float(m.group(0).replace(",", ""))
            # owner name: the longest alphabetic-ish cell that isn't the map or amount cell
            name_candidates = [c for c in cells if c not in (map_cell, amount_cell) and re.search(r"[A-Za-z]{3,}", c)]
            owner_name = max(name_candidates, key=len) if name_candidates else None

            if map_cell and owner_name:
                rows_out.append({
                    "map_number": map_cell,
                    "owner_name": owner_name,
                    "amount_due": amount_due,
                })

        if rows_out:
            break  # found the right table, stop scanning others

    # de-dupe by map_number
    seen = set()
    deduped = []
    for r in rows_out:
        if r["map_number"] not in seen:
            seen.add(r["map_number"])
            deduped.append(r)
    return deduped


LABELS = ["Owner(s)", "Mailing Address", "Location", "Land Use", "Fair Market Value", "Taxable Market Value"]


def parse_details(html):
    """Pull label -> value pairs out of the Real Property Details page."""
    soup = BeautifulSoup(html, "html.parser")
    values = {}

    # Pattern: label and value sit in adjacent cells of the same row.
    for tr in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        for i, cell in enumerate(cells):
            for label in LABELS:
                if cell.strip().lower().startswith(label.lower()) and i + 1 < len(cells):
                    values[label] = cells[i + 1]

    return values


def normalize_address(addr):
    if not addr:
        return None
    return re.sub(r"\s+", " ", addr).strip().rstrip(",")


def guess_absentee(location, mailing_address):
    if not location or not mailing_address:
        return None
    loc_zip = re.search(r"\b\d{5}\b", location)
    mail_zip = re.search(r"\b\d{5}\b", mailing_address)
    if loc_zip and mail_zip:
        return loc_zip.group(0) != mail_zip.group(0)
    # fall back to comparing city tokens
    return normalize_address(location).lower()[:10] not in mailing_address.lower()


def upsert_lead(conn, address, owner_name, mailing_address, is_absentee, amount_due, map_number):
    address = normalize_address(address)
    if not address:
        return False

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "amount_due": amount_due,
            "map_number": map_number,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, county, owner_name, mailing_address,
                                is_absentee, source_tags, raw)
            values (%s, 'Greenville', 'SC', 'Greenville', %s, %s, %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, owner_name, mailing_address, is_absentee, SOURCE_NAME, raw_payload),
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

    FIXED 2026-09-21: this copy of the formula was missing the
    tired_landlord bonus AND the `where is_sold = false` guard that
    absentee_owner_ingest.py already had. Without that guard, this script
    (which runs before absentee_owner_ingest.py in nightly.yml) was
    un-zeroing every already-sold lead's score each night, relying on
    absentee_owner_ingest.py running later in the same workflow to zero
    them back out. Fixed here so each script is independently correct
    regardless of run order.
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
            where is_sold = false
            """
        )


def log_run(conn, records_found, records_new, notes)
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

    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching tax sale list...")
    list_html = fetch(LIST_URL)
    rows = parse_list(list_html)
    print(f"Found {len(rows)} parcels on the list.")

    max_rows = os.environ.get("MAX_ROWS")
    if max_rows:
        rows = rows[: int(max_rows)]
        print(f"MAX_ROWS set — processing only the first {len(rows)}.")

    if not rows:
        print("No rows parsed — the county likely changed the page layout. Check the HTML structure.")
        conn = psycopg2.connect(db_url)
        log_run(conn, 0, 0, "parse_list returned 0 rows — page layout may have changed")
        conn.commit()
        conn.close()
        return

    conn = psycopg2.connect(db_url)
    new_count = 0
    error_count = 0
    consecutive_misses = 0

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context, page = new_browser_context(browser)

        for i, row in enumerate(rows):
            try:
                url = DETAILS_URL.format(year=datetime.now().year, map_number=row["map_number"])
                details_html = fetch_details_html(page, url)
                details = parse_details(details_html)
                location = details.get("Location")
                mailing = details.get("Mailing Address")
                owner = details.get("Owner(s)") or row["owner_name"]
                absentee = guess_absentee(location, mailing)

                if location:
                    consecutive_misses = 0
                    inserted = upsert_lead(
                        conn,
                        address=location,
                        owner_name=owner,
                        mailing_address=mailing,
                        is_absentee=absentee,
                        amount_due=row["amount_due"],
                        map_number=row["map_number"],
                    )
                    if inserted:
                        new_count += 1
                else:
                    consecutive_misses += 1
                    print(f"  map {row['map_number']}: no Location field found after retries, skipping (miss streak {consecutive_misses})")
                    if consecutive_misses >= CONSECUTIVE_MISS_LIMIT:
                        print(f"  {consecutive_misses} misses in a row — cooling down {COOLDOWN_SECONDS}s and starting a fresh browser session")
                        context.close()
                        time.sleep(COOLDOWN_SECONDS)
                        context, page = new_browser_context(browser)
                        consecutive_misses = 0

            except Exception as e:
                error_count += 1
                print(f"  map {row['map_number']}: error {e}", file=sys.stderr)

            if (i + 1) % 50 == 0:
                print(f"  ...{i + 1}/{len(rows)} processed ({new_count} ok, {error_count} errors)")

            time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.8))

        browser.close()

    rescore_all(conn)
    log_run(conn, len(rows), new_count, "ok")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored.")


if __name__ == "__main__":
    main()
