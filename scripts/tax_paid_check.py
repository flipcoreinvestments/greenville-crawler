"""
Tax paid check (added 2026-11-11, from Tala's hand-check of the top 10 leads).

Four out of four tax-sale leads she checked had already PAID the delinquent
year after it was sold at the tax sale (likely redeemed), yet the crawler still
showed them as owing. This script opens the county Real Property Details page
for the delinquent tax year of every tax-sale lead that Morbius works (in GHL or
in the skip-trace queue) and records whether that year shows "paid on <date>".

Writes raw.tax_paid = {tax_year, paid_on, amount, fetched_at} (paid_on null when
unpaid). morbius_sync.confidence() drops the tax signal when it is paid.
Logs counts only.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

sys.path.insert(0, os.path.dirname(__file__))
from tax_sale_ingest import DETAILS_URL, cell_lines, fetch_details_html, new_browser_context  # noqa: E402

MAX_LEADS = int(os.environ.get("TAX_PAID_MAX") or 250)
DELAY = 1.5
PAID_RE = re.compile(r"\$?([\d,]+\.\d\d)\s*\(paid on\s*(\d{1,2}/\d{1,2}/\d{4})\)", re.I)


def parse_taxes(html):
    """Return (amount, paid_on) from the 'Taxes:' row; (None, None) if not shown."""
    soup = BeautifulSoup(html or "", "html.parser")
    for tr in soup.find_all("tr"):
        cells = [" ".join(cell_lines(td)) for td in tr.find_all(["td", "th"])]
        for i, c in enumerate(cells):
            if c.strip().lower().startswith("taxes") and i + 1 < len(cells):
                val = cells[i + 1]
                m = PAID_RE.search(val)
                if m:
                    return m.group(1), m.group(2)
                m = re.search(r"\$?([\d,]+\.\d\d)", val)
                return (m.group(1) if m else None), None
    return None, None


def delinquent_year(raw):
    rp = raw.get("redemption_period") or {}
    if rp.get("sale_year"):
        return int(rp["sale_year"]) - 1
    return datetime.now(timezone.utc).year - 1


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select l.id, l.raw, coalesce(l.pin, l.raw->'tax_sale'->>'map_number',
                                         l.raw->'redemption_period'->>'map_number') as map_number
            from leads l
            where (l.raw ? 'redemption_period' or l.raw ? 'tax_sale')
              and coalesce(l.lead_status, '') = ''
              and (l.id::text in (select lead_id::text from morbius_contacts where deleted_at is null)
                   or l.id::text in (select lead_id from morbius_skiptrace_queue))
            order by (l.id::text in (select lead_id::text from morbius_contacts where deleted_at is null)) desc
            limit %s
            """,
            (MAX_LEADS,),
        )
        leads = [r for r in cur.fetchall() if r["map_number"]]
    paid = unpaid = blank = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context, page = new_browser_context(browser)
        for r in leads:
            raw = r["raw"] or {}
            year = delinquent_year(raw)
            try:
                html = fetch_details_html(page, DETAILS_URL.format(year=year, map_number=r["map_number"]))
            except Exception:  # noqa: BLE001 -- one bad page never stops the run
                blank += 1
                continue
            amount, paid_on = parse_taxes(html)
            if amount is None and paid_on is None:
                blank += 1
                continue
            rec = {"tax_year": year, "paid_on": paid_on, "amount": amount,
                   "fetched_at": datetime.now(timezone.utc).isoformat()}
            with conn.cursor() as cur:
                cur.execute("update leads set raw = coalesce(raw, '{}'::jsonb) || jsonb_build_object('tax_paid', %s::jsonb) "
                            "where id = %s", (json.dumps(rec), r["id"]))
            if paid_on:
                paid += 1
            else:
                unpaid += 1
            time.sleep(DELAY)
        browser.close()
    print(f"tax paid check: {len(leads)} leads, {paid} paid, {unpaid} still unpaid, {blank} no data")


if __name__ == "__main__":
    main()
