#!/usr/bin/env python3
"""
Greenville County Master-in-Equity foreclosure sale list ingest.

Source: https://mie.greenvillejournal.com/printer-friendly-sale-list/
Public foreclosure auction listings for Greenville County, published by the
Greenville County Master-in-Equity Court via the Greenville Journal.
robots.txt checked 2026-09-18: /wp-content/ is not disallowed. No bot
protection observed on this site (plain nginx/WordPress) — uses `requests`
directly, no headless browser needed.

What this does:
1. Reads the sale-date dropdown on the site's own list page to find every
   FUTURE scheduled foreclosure sale date (past dates are already sold).
2. For each future sale date, POSTs to the site's own list-generator endpoint
   and gets back an HTML table of every case up for sale that date.
3. Skips any row marked "Withdrawn" (pulled from sale — no longer a lead).
4. Upserts each property into `leads`, tagging it 'foreclosure_mie'. The
   Defendant name is the homeowner facing foreclosure — a strong motivated
   seller signal on its own, and a very strong one when stacked with any
   other source (tax sale, etc.) for the same address.
5. Recomputes the shared score (see rescore_all) and logs the run.
"""

import os
import re
import sys
import time
import json
from datetime import datetime, timezone, date

import requests
from bs4 import BeautifulSoup
import psycopg2

BASE_URL = "https://mie.greenvillejournal.com"
LIST_PAGE_URL = f"{BASE_URL}/printer-friendly-sale-list/"
GENERATE_URL = f"{BASE_URL}/wp-content/plugins/master-in-equity/download-clerk-docs.php"
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}
REQUEST_DELAY_SECONDS = 1.5
SOURCE_NAME = "foreclosure_mie"


def fetch_future_sale_dates():
    resp = requests.get(LIST_PAGE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    select = soup.find("select", attrs={"name": "closedate"})
    if not select:
        return []
    today = date.today()
    dates = []
    for opt in select.find_all("option"):
        value = (opt.get("value") or "").strip()
        try:
            d = datetime.strptime(value, "%m/%d/%Y").date()
        except ValueError:
            continue
        if d >= today:
            dates.append(value)
    return dates


def fetch_sale_list(sale_date):
    resp = requests.post(
        GENERATE_URL,
        headers=HEADERS,
        data={"closedate": sale_date, "target": "Generate List"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


def parse_sale_list(html):
    """Columns: Withdrawn, Def waived after approved, Def waived in order,
    Def demanded in order, Sale#, Case#, Address, Att., Law Firm, Plaintiff,
    Defendant, Comments."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows_out = []
    trs = table.find_all("tr")
    for tr in trs[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 11:
            continue

        withdrawn = cells[0].get_text(strip=True)
        sale_num = cells[4].get_text(strip=True)
        case_num = cells[5].get_text(strip=True)
        address_cell = cells[6]
        attorney = cells[7].get_text(strip=True)
        law_firm = cells[8].get_text(strip=True)
        plaintiff = cells[9].get_text(strip=True)
        defendant = cells[10].get_text(strip=True)

        if withdrawn:
            continue  # pulled from sale, not a live lead

        addr_strings = list(address_cell.stripped_strings)
        if not addr_strings:
            continue
        street = addr_strings[0].strip()
        city = state = zip_code = None
        if len(addr_strings) > 1:
            m = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5})", addr_strings[1])
            if m:
                city, state, zip_code = m.group(1).strip(), m.group(2), m.group(3)

        if not street:
            continue

        rows_out.append({
            "street": street,
            "city": city,
            "state": state or "SC",
            "zip": zip_code,
            "case_number": case_num,
            "sale_number": sale_num,
            "plaintiff": plaintiff,
            "attorney": attorney,
            "law_firm": law_firm,
            "defendant": defendant,
        })

    return rows_out


def normalize_address(addr):
    if not addr:
        return None
    return re.sub(r"\s+", " ", addr).strip().rstrip(",")


def upsert_lead(conn, row, sale_date):
    address = normalize_address(row["street"])
    if not address:
        return False

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "sale_date": sale_date,
            "case_number": row["case_number"],
            "plaintiff": row["plaintiff"],
            "law_firm": row["law_firm"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, zip, county, owner_name, source_tags, raw)
            values (%s, %s, %s, %s, 'Greenville', %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                city = coalesce(excluded.city, leads.city),
                zip = coalesce(excluded.zip, leads.zip),
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, row["city"], row["state"], row["zip"], row["defendant"], SOURCE_NAME, raw_payload),
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
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update leads set score =
                (list_count * 25)
                + (case when is_absentee then 15 else 0 end)
                + least(coalesce((raw->'tax_sale'->>'amount_due')::numeric, 0) / 50, 25)
                + (case when 'foreclosure_mie' = any(source_tags) then 30 else 0 end)
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

    print(f"[{datetime.now(timezone.utc).isoformat()}] Finding upcoming foreclosure sale dates...")
    sale_dates = fetch_future_sale_dates()
    print(f"Found {len(sale_dates)} upcoming sale dates: {sale_dates}")

    conn = psycopg2.connect(db_url)

    if not sale_dates:
        log_run(conn, 0, 0, "no upcoming sale dates found — page structure may have changed")
        conn.commit()
        conn.close()
        return

    total_found = 0
    new_count = 0

    for sale_date in sale_dates:
        try:
            html = fetch_sale_list(sale_date)
            rows = parse_sale_list(html)
            print(f"  {sale_date}: {len(rows)} active listings")
            total_found += len(rows)
            for row in rows:
                if upsert_lead(conn, row, sale_date):
                    new_count += 1
        except Exception as e:
            print(f"  {sale_date}: error {e}", file=sys.stderr)

        time.sleep(REQUEST_DELAY_SECONDS)

    rescore_all(conn)
    log_run(conn, total_found, new_count, "ok")
    conn.commit()
    conn.close()
    print(f"Done. {new_count} properties upserted and rescored across {len(sale_dates)} sale dates.")


if __name__ == "__main__":
    main()
