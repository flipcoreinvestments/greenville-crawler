#!/usr/bin/env python3
"""
Greenville County Probate Court ingest — decedent / inherited-property leads.

Source: https://www.greenvillecounty.org/appsas400/Probate/
A county-run AS400/ASP.NET app, completely separate from the Imperva-
protected SC Public Index the foreclosure/tax-sale scripts deal with. No
login, no CAPTCHA found. Case numbers follow a sequential per-year scheme:
{YEAR}ES23{5-digit sequence} ('23' = Greenville's county code) -- confirmed
by spot-checking that consecutive sequence numbers are real, distinct
cases. The site's own search form only offers name search and exact
case-number search (no date-range/"recent filings" search), so this script
enumerates the current year's sequence directly against SearchDetails.aspx,
which is GET-addressable per case once a session cookie is picked up from
the search landing page (a direct deep-link without first hitting that
page returned a 404 in testing).

The case detail page hands over a property address directly for the
decedent -- no assessor/ROD cross-reference needed. Also captures the
Personal Representative/heir name (a live contact to reach about the
property), file date, and date of death. Tagged 'probate'.

SCOPE: only Case Type == 'Estate' rows are kept (Guardianship/Conservatorship
cases involve a living ward, not a decedent's property -- out of scope for
this category).

PROGRESS TRACKING: keeps a high-water mark (highest sequence number
confirmed to exist this year) in source_runs.notes as 'high_water_seq:N',
so each nightly run only walks forward from there instead of re-checking
the whole year from scratch. Stops a pass after CONSECUTIVE_MISS_LIMIT
consecutive not-yet-filed sequence numbers in a row (assumed to mean we've
caught up to today's filings), and caps total lookups per run at
MAX_LOOKUPS_PER_RUN so the first-ever backfill run doesn't run past a
GitHub Actions job timeout -- it just picks up again next night from
wherever it left off.

UNVERIFIED IN PRODUCTION YET (2026-09-22): the exact page-text layout for
label/value pairs (Name/Address/Date of Death/etc.) was described secondhand
by a research pass, not confirmed against raw HTML from this script's own
parser. parse_case_detail() extracts fields with an order-based label scan
that's tolerant of exact whitespace/newline differences, and main() logs the
raw extracted text for the first few cases so this can be verified/fixed
fast from real GitHub Actions run output if the layout doesn't match.
"""

import os
import re
import sys
import json
import time
from datetime import datetime, timezone, date

import requests
from bs4 import BeautifulSoup
import psycopg2

BASE_URL = "https://www.greenvillecounty.org/appsas400/Probate/"
DETAILS_URL = "https://www.greenvillecounty.org/appsas400/Probate/SearchDetails.aspx?CaseNumber={case_number}"
COUNTY_CODE = "23"
SEQ_WIDTH = 5
CONSECUTIVE_MISS_LIMIT = 25
REQUEST_DELAY_SECONDS = 0.4
SOURCE_NAME = "probate"
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}

LABELS_IN_ORDER = [
    "Name", "Party Type", "Address", "Date of Death", "Date of Birth", "Sex",
    "Status", "Case Type", "Case SubType", "File Date", "PR Appointed Date",
    "Closed Date", "PARTIES INVOLVED",
]


def new_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(BASE_URL, timeout=30)
    return s


def case_number(year, seq):
    return f"{year}ES{COUNTY_CODE}{seq:0{SEQ_WIDTH}d}"


def fetch_case(session, cn):
    url = DETAILS_URL.format(case_number=cn)
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=30)
            break
        except requests.RequestException as e:
            if attempt == 2:
                print(f"  {cn}: request failed after retries: {e}", file=sys.stderr)
                return None
            time.sleep(2)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        print(f"  {cn}: unexpected status {resp.status_code}", file=sys.stderr)
        return None
    if "Page Not Found" in resp.text[:3000]:
        return None
    return resp.text


def extract_labeled_fields(text):
    pattern = "|".join(re.escape(l) for l in LABELS_IN_ORDER)
    matches = list(re.finditer(pattern, text))
    result = {}
    for i, m in enumerate(matches):
        label = m.group(0)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        value = text[start:end].strip(" :\n\t\r")
        if label not in result:
            result[label] = value
    return result


def normalize_address(addr):
    if not addr:
        return None
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr or None


def parse_address_parts(full_addr):
    """Best-effort split of 'STREET CITY SC ZIP' -> (street, city, zip)."""
    m = re.match(r"^(.*?)\s+([A-Za-z .'-]+?)\s+SC\s+(\d{5})", full_addr)
    if m:
        return m.group(1).strip(), m.group(2).strip().title(), m.group(3)
    return full_addr, "Greenville", None


def parse_case_detail(html, cn):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ")
    text = re.sub(r"\s+", " ", text)
    fields = extract_labeled_fields(text)

    if (fields.get("Case Type") or "").strip().lower() != "estate":
        return None

    decedent_name = fields.get("Name")
    address = normalize_address(fields.get("Address"))
    if not decedent_name or not address:
        return None

    return {
        "case_number": cn,
        "decedent_name": decedent_name,
        "address": address,
        "date_of_death": fields.get("Date of Death") or None,
        "file_date": fields.get("File Date") or None,
        "pr_appointed_date": fields.get("PR Appointed Date") or None,
        "closed_date": fields.get("Closed Date") or None,
        "case_subtype": fields.get("Case SubType") or None,
        "parties": fields.get("PARTIES INVOLVED") or None,
    }


def upsert_lead(conn, row):
    street, city, zip_code = parse_address_parts(row["address"])
    if not street:
        return False

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "case_number": row["case_number"],
            "decedent_name": row["decedent_name"],
            "date_of_death": row["date_of_death"],
            "file_date": row["file_date"],
            "pr_appointed_date": row["pr_appointed_date"],
            "closed_date": row["closed_date"],
            "case_subtype": row["case_subtype"],
            "parties": row["parties"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, zip, county, owner_name, source_tags, raw)
            values (%s, %s, 'SC', %s, 'Greenville', %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                city = coalesce(excluded.city, leads.city),
                zip = coalesce(excluded.zip, leads.zip),
                owner_name = coalesce(leads.owner_name, excluded.owner_name),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (street, city, zip_code, row["decedent_name"], SOURCE_NAME, raw_payload),
        )
    return True


def rescore_all(conn):
    """
    Shared score formula -- kept IDENTICAL in every ingest script. See
    absentee_owner_ingest.py for the full running commentary; this copy
    adds no new bonus of its own (no dedicated 'probate' point value was
    specified -- it contributes only the flat +25 per-list list_count bonus
    like every other source, until T Dawg says otherwise).
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
            where is_sold = false
            """
        )


def get_high_water_seq(conn, year):
    with conn.cursor() as cur:
        cur.execute(
            """
            select notes from source_runs
            where source_name = %s and notes like %s
            order by id desc limit 1
            """,
            (SOURCE_NAME, f"%year:{year}%"),
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return 0
    m = re.search(r"high_water_seq:(\d+)", row[0])
    return int(m.group(1)) if m else 0


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

    max_lookups = int(os.environ.get("MAX_ROWS") or 3000)
    year = date.today().year

    conn = psycopg2.connect(db_url)
    start_seq = get_high_water_seq(conn, year) + 1
    print(f"[{datetime.now(timezone.utc).isoformat()}] Year {year}, starting at sequence {start_seq}, "
          f"cap {max_lookups} lookups this run.")

    session = new_session()
    seq = start_seq
    consecutive_misses = 0
    lookups = 0
    found = 0
    new_count = 0
    highest_confirmed = start_seq - 1
    debug_logged = 0

    while consecutive_misses < CONSECUTIVE_MISS_LIMIT and lookups < max_lookups:
        cn = case_number(year, seq)
        html = fetch_case(session, cn)
        lookups += 1

        if html is None:
            consecutive_misses += 1
            seq += 1
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        consecutive_misses = 0
        highest_confirmed = seq
        found += 1

        try:
            parsed = parse_case_detail(html, cn)
        except Exception as e:
            parsed = None
            print(f"  {cn}: parse error: {e}", file=sys.stderr)

        if parsed is None and debug_logged < 3:
            # Log a snippet so a real layout mismatch can be diagnosed and
            # fixed from actual GitHub Actions output rather than guessed at.
            soup = BeautifulSoup(html, "html.parser")
            snippet = re.sub(r"\s+", " ", soup.get_text(" "))[:800]
            print(f"  {cn}: could not extract address/estate fields, raw text snippet: {snippet}", file=sys.stderr)
            debug_logged += 1

        if parsed:
            if upsert_lead(conn, parsed):
                new_count += 1

        seq += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    rescore_all(conn)
    log_run(
        conn, found, new_count,
        f"ok: year:{year} high_water_seq:{highest_confirmed} lookups:{lookups} "
        f"found_cases:{found} estate_leads:{new_count} stopped_after_misses:{consecutive_misses}"
    )
    conn.commit()
    conn.close()
    print(f"Done. Checked {lookups} case numbers ({found} real cases found), "
          f"{new_count} estate/probate leads upserted. High-water mark now {highest_confirmed}.")


if __name__ == "__main__":
    main()
