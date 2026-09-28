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
from psycopg2.extras import execute_values
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from lead_common import ensure_schema, expire_by_raw_key, mailing_is_address, remove_tag, rescore_all  # noqa: E402

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
# FIX 2026-09-25: also accepts ordinals ("OCTOBER 19TH & 20TH, 2026") -- the
# 2026 notice writes it that way, the old pattern didn't match it, and the
# parser silently fell back to some OTHER date on the page.
SALE_DATE_RE = re.compile(
    rf"({MONTH_NAMES})\s+(\d{{1,2}})(?:st|nd|rd|th)?"
    rf"(?:\s*(?:-|&|to|and)\s*(\d{{1,2}})(?:st|nd|rd|th)?)?,?\s*(\d{{4}})",
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


def parse_sale_date(html, year=None):
    """
    Pull the sale date out of the notice prose. Uses the LATER day when a
    range is given (e.g. "16-17" -> 17th), since a two-day sale's redemption
    clock is commonly measured from its conclusion.

    FIX 2026-09-25 (found checking T Dawg's top 10): the old version took the
    FIRST date anywhere on the page. The 2026 page's header still says
    "November 3 & 4, 2025" (stale) while the body says the sale is
    "OCTOBER 19TH & 20TH, 2026", and the stored sale date came out as
    2026-09-25 -- none of those. Now every date on the page is collected and
    the one whose year matches the notice year wins (latest if several).
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    found = []  # (date, is_sale_context)
    for m in SALE_DATE_RE.finditer(text):
        month_name, day1, day2, year_str = m.groups()
        day = max(int(day1), int(day2)) if day2 else int(day1)
        try:
            d = datetime.strptime(f"{month_name} {day} {year_str}", "%B %d %Y").date()
        except ValueError:
            continue
        before = text[max(0, m.start() - 80):m.start()].lower()
        # FIX 2026-09-27: the site's "Latest Issue September 25, 2026" masthead
        # sits right after the "Tax Sales" menu link, so it passed as a sale
        # date and the CURRENT 2026 list was treated as last year's sale --
        # every tax-sale lead got repeat_tax_delinquent (1,222 of 1,222).
        stamp = re.search(r"(posted|updated|published|modified|last edited|latest issue|issue|edition)"
                          r"\W*(on\W*)?$", before)
        ctx = bool(re.search(r"\b(sell|sale|auction)\b", before)) and not stamp
        found.append((d, ctx))
    if not found:
        return None
    pool = [x for x in found if year is None or x[0].year == year]
    if not pool:
        return None  # notice for `year` with no date in `year` -> don't guess
    # prefer a date written right after "will sell"/"sale"/"auction" -- not a
    # "Posted"/"Updated" date elsewhere on the page
    in_context = [d for d, ctx in pool if ctx]
    return max(in_context) if in_context else max(d for d, _ in pool)

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
    # FIX 2026-09-25: the county mailing field sometimes holds a NAME, not an
    # address -- can't judge absentee from that, so unknown (None), not True.
    if not mailing_is_address(mailing_address):
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
            insert into leads (address, state, county, owner_name, mailing_address,
                                is_absentee, land_use, source_tags, raw, pin)
            values (%s, 'SC', 'Greenville', %s, %s, %s, %s, ARRAY[%s]::text[], %s::jsonb, nullif(regexp_replace(coalesce(%s, ''), '\\D', '', 'g'), ''))
            on conflict (lower(address)) do update set
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                mailing_address = coalesce(excluded.mailing_address, leads.mailing_address),
                is_absentee = coalesce(excluded.is_absentee, leads.is_absentee),
                land_use = coalesce(excluded.land_use, leads.land_use),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = coalesce(leads.raw, '{}'::jsonb) || excluded.raw,
                pin = coalesce(leads.pin, excluded.pin),
                updated_at = now()
            """,
            (address, owner_name, mailing_address, is_absentee, land_use, SOURCE_NAME, raw_payload, map_number),
        )
    return True


# rescore_all now lives in lead_common.py (one shared formula for every script).

def log_run(conn, records_found, records_new, notes):
    with conn.cursor() as cur:
        cur.execute(
            "insert into source_runs (source_name, records_found, records_new, notes) values (%s, %s, %s, %s)",
            (SOURCE_NAME, records_found, records_new, notes),
        )


REPEAT_TAG = "repeat_tax_delinquent"
PRIOR_SALE_MAX_AGE_DAYS = 400


def looks_like_current_list(conn, pins, threshold=0.95):
    """Backstop for a mis-read sale date: last year's notice can't contain
    95%+ of this year's delinquent parcels (only repeat offenders carry over)."""
    with conn.cursor() as cur:
        cur.execute("select regexp_replace(coalesce(raw->'tax_sale'->>'map_number', ''), '\\D', '', 'g') "
                    "from leads where 'tax_sale' = any(source_tags) and is_sold = false")
        current = {r[0] for r in cur.fetchall()} - {""}
    if len(current) < 50 or not pins:
        return False
    return len(current & pins) / len(current) >= threshold


def apply_repeat_tag(conn, prior):
    """prior = {digits-only map number: info} from the prior sale's notice.
    Tags leads already on the CURRENT tax sale list; expires the rest."""
    with conn.cursor() as cur:
        cur.execute("create temporary table prior_notice (pin text primary key, info jsonb) on commit drop")
        execute_values(cur, "insert into prior_notice values %s",
                       [(k, json.dumps(v)) for k, v in prior.items()])
        cur.execute(
            """
            update leads l set
                source_tags = array(select distinct unnest(l.source_tags || array[%s::text])),
                raw = coalesce(l.raw, '{}'::jsonb) || jsonb_build_object(%s::text, p.info),
                updated_at = now()
            from prior_notice p
            where 'tax_sale' = any(l.source_tags)
              and regexp_replace(coalesce(l.raw->'tax_sale'->>'map_number', ''), '\\D', '', 'g') = p.pin
              and not (%s::text = any(l.source_tags))
            """,
            (REPEAT_TAG, REPEAT_TAG, REPEAT_TAG),
        )
        added = cur.rowcount
    n = remove_tag(conn, REPEAT_TAG,
                   "not ('tax_sale' = any(source_tags)) "
                   "or regexp_replace(coalesce(raw->'tax_sale'->>'map_number', ''), '\\D', '', 'g') "
                   "<> all(%(pins)s::text[])", {"pins": sorted(prior)})
    return added, n


def main():
    """
    REWRITE 2026-09-25 -- "redemption_period" retired, "repeat_tax_delinquent"
    added.

    Why: this script used to tag every parcel on the PRIOR year's tax sale
    notice as "in its redemption period". But the notice is published BEFORE
    the sale and lists every parcel that MIGHT be sold; many pay first, some
    go to the Forfeited Land Commission, and the county publishes no list of
    what actually sold (confirmed on the Tax Collector's own FAQ,
    2026-09-25). Checked on the county record for 626 Cox St: 2024 taxes
    "paid", which is true whether the owner paid or a bidder bought it --
    redemption can't be proven from public data.

    What IS provable: the parcel was advertised for the prior sale AND is on
    the county's CURRENT tax sale list -- behind on taxes two years running.
    That's what gets tagged now, only on leads already tagged tax_sale (so it
    never creates a lead by itself).
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("select 1 from pipeline_migrations where name = '2026_09_25_redemption_unprovable'")
        if cur.fetchone() is None:
            n = remove_tag(conn, SOURCE_NAME, "true")
            print(f"  migration: removed {n} unprovable redemption_period tag(s)")
            cur.execute("insert into pipeline_migrations (name) values ('2026_09_25_redemption_unprovable')")
    conn.commit()

    today = date.today()
    prior = {}
    for year in (today.year, today.year - 1):
        url, html = find_year_page(year)
        if not url:
            print(f"  no {year} notice page found, skipping")
            continue
        sale_date = parse_sale_date(html, year)
        if not sale_date:
            print(f"  {url}: couldn't find the {year} sale date on the page, skipping")
            continue
        if sale_date >= today:
            print(f"  {url}: {year} sale is {sale_date} (not held yet) -- that's the current list, skipping")
            continue
        if (today - sale_date).days > PRIOR_SALE_MAX_AGE_DAYS:
            print(f"  {url}: {year} sale was {sale_date}, too old to count as 'last year', skipping")
            continue
        rows = parse_list(html)
        pins = {re.sub(r"\D", "", r.get("map_number") or "") for r in rows} - {""}
        if looks_like_current_list(conn, pins):
            print(f"  {url}: its parcels match the CURRENT tax sale list almost exactly -- "
                  f"this is this year's notice, not last year's; skipping")
            continue
        print(f"  {url}: prior sale {sale_date}, {len(rows)} parcels advertised")
        for r in rows:
            digits = re.sub(r"\D", "", r.get("map_number") or "")
            if digits:
                prior[digits] = {"sale_year": year, "sale_date": sale_date.isoformat(), "amount": r.get("amount_due")}

    if not prior:
        print("  no prior-year notice available this run; repeat_tax_delinquent left unchanged")
        rescore_all(conn)
        log_run(conn, 0, 0, "no prior notice resolved")
        conn.commit()
        conn.close()
        return

    added, n = apply_repeat_tag(conn, prior)
    print(f"  repeat_tax_delinquent: {added} added, {n} removed")

    rescore_all(conn)
    log_run(conn, len(prior), added, f"ok: {added} repeat-delinquent tagged, {n} expired")
    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
