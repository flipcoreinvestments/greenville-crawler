#!/usr/bin/env python3
"""
Greenville County tax sale REDEMPTION PERIOD ingest.

Source: https://greenvillejournal.com/{year}-tax-sales/ (slug format has
changed year to year -- see YEAR_SLUG_CANDIDATES below). This is the same
domain the foreclosure_mie_ingest.py script already uses, and is where
Greenville County publishes its legally-required tax sale notice as a
plain HTML table (owner, map/parcel number, amount due), archived
indefinitely. robots.txt checked 2026-09-21: greenvillejournal.com
disallows nothing.

What this does and why it's a distinct lead category from tax_sale_ingest.py:
tax_sale_ingest.py tracks the UPCOMING sale list (properties about to be
auctioned). This script tracks properties whose tax sale has ALREADY
HAPPENED and are now sitting in South Carolina's statutory redemption
period -- state law gives the original owner "a year and a day" from the
sale date to redeem the property (pay off what's owed plus interest)
before the tax sale purchaser can get a deed. An owner in this window is
about to permanently lose the property and is a very strong, very
time-sensitive motivated-seller lead -- often more urgent than someone on
the upcoming sale list, since for them the clock has already started and
can't be paused.

1. Tries a handful of known URL slug patterns per year (the county/journal
   has used at least 3 different slug formats since 2022) to find that
   year's published sale-notice page.
2. Parses the sale date out of the notice's prose (e.g. "DECEMBER 16-17,
   2024" or "November 3 & 4, 2025" -- format varies by year) and computes
   redemption_deadline = sale_date + 366 days ("a year and a day").
3. Only processes years whose redemption_deadline is still in the future
   -- in practice this means the current year's sale and the prior year's
   sale are the only two that can ever still be active, so that's all we
   fetch.
4. Parses the same three-column table format as tax_sale_ingest.py (map
   number, owner, amount due).
5. For each still-in-window row, fetches the Real Property Details page
   (same Imperva-protected endpoint tax_sale_ingest.py already handles
   with a stealth headless browser) to get the actual property address,
   owner mailing address, and current owner of record.
6. Upserts into `leads`, tagging 'redemption_period', storing sale_date
   and redemption_deadline in raw so the CRM/GHL side can show a countdown.
7. Recomputes the shared score (see rescore_all) and logs the run.

Caveat: the journal's published list is the pre-sale notice, not a
certified post-sale results list -- a handful of parcels get paid off or
withdrawn at the last minute and won't show up again in a later reprint.
Practically this just means a small number of these leads may already be
resolved by the time they're pulled; the RealProperty Details lookup
(step 5) at least confirms the parcel still exists and pulls current
owner-of-record, which is the same verification tax_sale_ingest.py relies
on. Cross-check before spending money on skip tracing, same as every
other source in this system.

Runs nightly via GitHub Actions (.github/workflows/nightly.yml).
Requires env var DATABASE_URL (Supabase session pooler connection string).
"""

import os
import re
import sys
import time
import json
from datetime import datetime, timezone, date, timedelta

import random

import requests
from bs4 import BeautifulSoup
import psycopg2
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

DETAILS_URL = "https://www.greenvillecounty.org/appsas400/RealProperty/Details.aspx?TaxYear={year}&MapNumber={map_number}"
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}
REQUEST_DELAY_SECONDS = 1.5
ROW_RETRY_ATTEMPTS = 3
CONSECUTIVE_MISS_LIMIT = 5
COOLDOWN_SECONDS = 90
SOURCE_NAME = "redemption_period"
REDEMPTION_DAYS = 366  # SC gives "a year and a day" to redeem

# The journal's URL slug for the yearly tax sale notice has changed format
# more than once. Try each candidate for a given year until one resolves.
YEAR_SLUG_CANDIDATES = [
    "{year}-tax-sales",
    "{year}-greenville-county-sc-tax-sales",
    "{year}-greenville-county-tax-sales",
    "{year}-delinquent-tax-sale",
]

MONTH_NAMES = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
# Matches "DECEMBER 16-17, 2024" or "November 3 & 4, 2025" or a single
# "December 16, 2024" -- case-insensitive.
SALE_DATE_RE = re.compile(
    rf"({MONTH_NAMES})\s+(\d{{1,2}})(?:\s*(?:-|&|to|and)\s*(\d{{1,2}}))?,?\s*(\d{{4}})",
    re.IGNORECASE,
)


def find_year_page(year):
    """Return (url, html) for the first slug candidate that resolves, else (None, None)."""
    for slug_tpl in YEAR_SLUG_CANDIDATES:
        url = f"https://greenvillejournal.com/{slug_tpl.format(year=year)}/"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200 and "tax sale" in resp.text.lower():
                return url, resp.text
        except requests.RequestException:
            continue
    return None, None


def parse_sale_date(html):
    """Pull the published sale date out of the notice prose. Uses the LATER
    day when a range is given (e.g. "16-17" -> 17th), since a two-day sale's
    redemption clock is commonly measured from its conclusion."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = SALE_DATE_RE.search(text)
    if not m:
        return None
    month_name, day1, day2, year_str = m.groups()
    day = max(int(day1), int(day2)) if day2 else int(day1)
    try:
        return datetime.strptime(f"{month_name} {day} {year_str}", "%B %d %Y").date()
    except ValueError:
        return None


def parse_list(html):
    """Same three-column (map number, owner, amount due) table format as
    tax_sale_ingest.py's upcoming-sale list -- the journal publishes both
    the upcoming notice and this archived one with the same table shape."""
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
            map_cell = next((c for c in cells if re.fullmatch(r"\d{8,}", c.replace("-", ""))), None)
            if not map_cell:
                continue
            amount_cell = next((c for c in cells if re.search(r"\$?\d[\d,]*\.\d{2}", c)), None)
            amount_due = None
            if amount_cell:
                m = re.search(r"[\d,]+\.\d{2}", amount_cell)
                if m:
                    amount_due = float(m.group(0).replace(",", ""))
            name_candidates = [c for c in cells if c not in (map_cell, amount_cell) and re.search(r"[A-Za-z]{3,}", c)]
            owner_name = max(name_candidates, key=len) if name_candidates else None

            if map_cell and owner_name:
                rows_out.append({
                    "map_number": map_cell,
                    "owner_name": owner_name,
                    "amount_due": amount_due,
                })

        if rows_out:
            break

    seen = set()
    deduped = []
    for r in rows_out:
        if r["map_number"] not in seen:
            seen.add(r["map_number"])
            deduped.append(r)
    return deduped


def new_browser_context(browser):
    context = browser.new_context(user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ))
    page = context.new_page()
    stealth_sync(page)
    return context, page


def fetch_details_html(page, url):
    last_html = None
    for attempt in range(ROW_RETRY_ATTEMPTS):
        page.goto(url, timeout=30000)
        page.wait_for_timeout(1200)
        html = page.content()
        last_html = html
        if "Location:" in html or "Location" in BeautifulSoup(html, "html.parser").get_text():
            return html
        time.sleep(2 + attempt * 3)
    return last_html


LABELS = ["Owner(s)", "Mailing Address", "Location", "Land Use", "Fair Market Value", "Taxable Market Value"]


def parse_details(html):
    soup = BeautifulSoup(html, "html.parser")
    values = {}
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
    return normalize_address(location).lower()[:10] not in mailing_address.lower()


def upsert_lead(conn, address, owner_name, mailing_address, is_absentee, amount_due,
                 map_number, sale_year, sale_date, redemption_deadline, land_use=None):
    address = normalize_address(address)
    if not address:
        return False

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "amount_due": amount_due,
            "map_number": map_number,
            "sale_year": sale_year,
            "sale_date": sale_date.isoformat(),
            "redemption_deadline": redemption_deadline.isoformat(),
            "land_use": land_use,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, county, owner_name, mailing_address,
                                is_absentee, land_use, source_tags, raw)
            values (%s, 'Greenville', 'SC', 'Greenville', %s, %s, %s, %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                land_use = coalesce(excluded.land_use, leads.land_use),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, owner_name, mailing_address, is_absentee, land_use, SOURCE_NAME, raw_payload),
        )
    return True


def rescore_all(conn):
    """
    Shared score formula -- kept IDENTICAL in every ingest script so the whole
    table stays consistently scored no matter which script ran most recently:
      +25 per list the property is stacked on
      +15 if owner's mailing address differs from the property (absentee)
      +up to 25 scaled from tax-sale amount owed (capped)
      +30 if the property has an active foreclosure sale scheduled
      +20 if the property has a stalled/expired building permit
      +20 if the property has a demolition permit
      +15 if the same owner holds 3+ properties county-wide (tired landlord)
      +35 if the property is in an active tax-sale redemption period (about
          to permanently lose the property -- added 2026-09-21 alongside
          this script)
      +30 if a permit shows storm/fire/water/damage repair language
          (insurance_damage -- T Dawg's "utmost importance" category)
      +20 if the MIE foreclosure plaintiff is an HOA/COA (hoa_foreclosure)
      +20 if the property shows 15+ years of ownership or a last sale price
          well below current fair market value (high_equity proxy)

    Always scoped `where is_sold = false` so this script stays independently
    correct regardless of what order nightly.yml runs the scripts in.

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

    today = date.today()
    max_rows = os.environ.get("MAX_ROWS")

    active_years = []
    for year in (today.year, today.year - 1):
        print(f"[{datetime.now(timezone.utc).isoformat()}] Looking for {year} tax sale notice...")
        url, html = find_year_page(year)
        if not url:
            print(f"  no {year} notice page found under any known URL slug, skipping")
            continue
        sale_date = parse_sale_date(html)
        if not sale_date:
            print(f"  {url}: found the page but couldn't parse a sale date, skipping")
            continue
        redemption_deadline = sale_date + timedelta(days=REDEMPTION_DAYS)
        if redemption_deadline < today:
            print(f"  {url}: sale was {sale_date}, redemption deadline {redemption_deadline} already passed, skipping")
            continue
        rows = parse_list(html)
        print(f"  {url}: sale {sale_date}, redemption deadline {redemption_deadline}, {len(rows)} parcels")
        active_years.append((year, sale_date, redemption_deadline, rows))

    if not active_years:
        conn = psycopg2.connect(db_url)
        log_run(conn, 0, 0, "no active redemption-period years found (none resolved, or all deadlines passed)")
        conn.commit()
        conn.close()
        print("Done. No active redemption-period leads this run.")
        return

    conn = psycopg2.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists land_use text")
    conn.commit()
    new_count = 0
    error_count = 0
    consecutive_misses = 0
    total_rows = sum(len(rows) for _, _, _, rows in active_years)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context, page = new_browser_context(browser)

        i = 0
        for sale_year, sale_date, redemption_deadline, rows in active_years:
            if max_rows:
                rows = rows[: int(max_rows)]
            for row in rows:
                i += 1
                try:
                    url = DETAILS_URL.format(year=sale_year, map_number=row["map_number"])
                    details_html = fetch_details_html(page, url)
                    details = parse_details(details_html)
                    location = details.get("Location")
                    mailing = details.get("Mailing Address")
                    owner = details.get("Owner(s)") or row["owner_name"]
                    land_use = details.get("Land Use")
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
                            sale_year=sale_year,
                            sale_date=sale_date,
                            redemption_deadline=redemption_deadline,
                            land_use=land_use,
                        )
                        if inserted:
                            new_count += 1
                    else:
                        consecutive_misses += 1
                        print(f"  map {row['map_number']}: no Location field found after retries, skipping (miss streak {consecutive_misses})")
                        if consecutive_misses >= CONSECUTIVE_MISS_LIMIT:
                            print(f"  {consecutive_misses} misses in a row -- cooling down {COOLDOWN_SECONDS}s and starting a fresh browser session")
                            context.close()
                            time.sleep(COOLDOWN_SECONDS)
                            context, page = new_browser_context(browser)
                            consecutive_misses = 0

                except Exception as e:
                    error_count += 1
                    print(f"  map {row['map_number']}: error {e}", file=sys.stderr)

                if i % 50 == 0:
                    print(f"  ...{i}/{total_rows} processed ({new_count} ok, {error_count} errors)")

                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.8))

        browser.close()

    rescore_all(conn)
    log_run(conn, total_rows, new_count, "ok")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored.")


if __name__ == "__main__":
    main()
