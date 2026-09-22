#!/usr/bin/env python3
"""
Greenville County Master-in-Equity foreclosure sale list ingest.

Source: https://mie.greenvillejournal.com/printer-friendly-sale-list/
Public foreclosure auction listings for Greenville County, published by the
Greenville County Master-in-Equity Court via the Greenville Journal.
robots.txt checked 2026-09-18: /wp-content/ is not disallowed.

BUG FIX 2026-09-22 (round 2): the first fix (browser-like User-Agent on
`requests`) didn't work -- turned out to be a JS-executing bot-challenge
(the sgcaptcha plugin) that categorically requires a JS-capable client. That
was "fixed" by driving a real headless browser (Playwright + stealth) the
same way tax_sale_ingest.py handles the county's own Imperva check.

BUG FIX 2026-09-22 (round 3 -- TRUE ROOT CAUSE): the Playwright fix above
still returned 0 upcoming dates in production even though it worked
perfectly in a one-off local test. Added diagnostics, re-ran, and confirmed
via a side-by-side test that a real residential/office-IP Chrome session
sees 5 genuine future sale dates with zero challenge, while the exact same
Playwright+stealth code from a GitHub Actions runner gets served a distinct,
more severe Imperva/Incapsula "Robot Challenge Screen" that blocks the
`<select name='closedate'>` from ever rendering. This is IP-reputation/
behavioral scoring aimed at datacenter IPs specifically -- no amount of
browser fingerprint stealth fixes it, because the block isn't looking at the
browser, it's looking at the network the request came from.

FIX: this script no longer runs its own browser at all. It routes both the
list-page load and every sale-date POST through Scrapfly's Unblocker API
(https://scrapfly.io), a paid scraping proxy built specifically to solve
this class of wall -- it handles the anti-bot challenge on its own
infrastructure (real non-datacenter egress) and hands back the resolved
page. Needs a SCRAPFLY_KEY env var (T Dawg's own Scrapfly account/API key,
set as a GitHub Actions secret -- see nightly.yml). A single `session` name
is reused across the list-page fetch and every sale-date POST so the cookie
Scrapfly's side obtains on the first request carries through to the rest,
same as the old browser-context approach did. Playwright/stealth are no
longer imported or needed by this file; the other ingest scripts (e.g.
tax_sale_ingest.py) still use Playwright for the county's own separate,
much milder Imperva check, which that approach does solve.

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

HOA/COA FORECLOSURE (added 2026-09-22): SC law (§27-30-150 for HOAs,
§27-31-210 for condos) forecloses an unpaid-assessment lien "in like manner
as a mortgage" -- there is no separate case type/nature-of-action code for
it, it runs through this exact same Master-in-Equity sale list under
Foreclosure (420). So this is NOT a new data source, just a plaintiff-name
classifier bolted onto the feed already being pulled: any row whose
Plaintiff matches an HOA/COA-style name gets an EXTRA 'hoa_foreclosure' tag
alongside 'foreclosure_mie'. Expect some false negatives from management
companies filing on an HOA's behalf under their own name -- acceptable,
this is a bonus signal, not the primary one.
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
REQUEST_DELAY_SECONDS = 1.5
SOURCE_NAME = "foreclosure_mie"

SCRAPFLY_ENDPOINT = "https://api.scrapfly.io/scrape"
# Fixed session name so every call in a single run shares Scrapfly's cookie
# jar/egress IP -- the site's challenge cookie obtained on the list-page
# fetch has to still be valid when we POST for each sale date.
SCRAPFLY_SESSION = "greenville-mie"


def scrapfly_request(method, url, api_key, data=None):
    """
    Runs one request through Scrapfly's Unblocker instead of a local
    browser. Raises RuntimeError with the diagnostic detail on any failure
    so a broken run fails loud in the Actions log instead of silently
    returning empty data (see this file's docstring for why that matters --
    a silent "0 dates" cost real debugging time last round).
    """
    params = {
        "key": api_key,
        "url": url,
        "unblocker": "true",
        "session": SCRAPFLY_SESSION,
        "country": "us",
    }
    resp = requests.request(method, SCRAPFLY_ENDPOINT, params=params, data=data, timeout=60)
    try:
        payload = resp.json()
    except ValueError:
        raise RuntimeError(
            f"Scrapfly API returned non-JSON (http {resp.status_code}): {resp.text[:300]!r}"
        )
    if resp.status_code != 200:
        reject = resp.headers.get("X-Scrapfly-Reject-Code", "")
        error_detail = payload.get("error") or {}
        result = payload.get("result", {})
        # Print the whole thing to the log (not just a short exception
        # message) so a new/unexpected failure shape is debuggable from the
        # Actions log alone -- same "no silent black box" rule as the rest
        # of this file's diagnostics.
        print(f"  Scrapfly full error payload: {json.dumps(payload)}", file=sys.stderr)
        raise RuntimeError(
            f"Scrapfly API call failed (http {resp.status_code}, reject={reject!r}) "
            f"for {url}: message={error_detail.get('message')!r} "
            f"target_status={result.get('status_code')!r} "
            f"target_reason={result.get('reason')!r}"
        )
    result = payload.get("result", {})
    target_status = result.get("status_code")
    content = result.get("content", "")
    if target_status and target_status >= 400:
        raise RuntimeError(
            f"Target site returned http {target_status} through Scrapfly for {url}: "
            f"{content[:300]!r}"
        )
    return content


# Plaintiff-name patterns that mean this foreclosure is an HOA/COA
# assessment-lien foreclosure rather than a mortgage foreclosure. Matched
# case-insensitively as a substring. Kept broad -- a missed HOA case just
# stays tagged as a plain foreclosure_mie lead (still surfaces), a false
# positive costs nothing since it's an additive bonus tag.
HOA_PLAINTIFF_PATTERN = re.compile(
    r"(HOMEOWNERS?\s*ASSOC|PROPERTY\s*OWNERS?\s*ASSOC|OWNERS?\s*ASSOC|"
    r"CONDOMINIUM\s*ASSOC|COMMUNITY\s*ASSOC|\bHOA\b|\bPOA\b|\bCOA\b)",
    re.IGNORECASE,
)


def fetch_future_sale_dates(api_key):
    # Scrapfly's Unblocker runs the bot-challenge gauntlet on its own
    # (non-datacenter) egress and hands back the resolved page.
    html = scrapfly_request("GET", LIST_PAGE_URL, api_key)
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", attrs={"name": "closedate"})
    if not select:
        # Diagnostic: surface what we actually got back instead of another
        # silent "0 dates" if the challenge page changes shape again.
        print(
            f"  WARNING: no <select name='closedate'> found in Scrapfly's response. "
            f"body length: {len(html)}, first 300 chars: {html[:300]!r}",
            file=sys.stderr,
        )
        return []
    today = date.today()
    dates = []
    all_parsed = []
    skipped_unparsed = 0
    for opt in select.find_all("option"):
        value = (opt.get("value") or opt.get_text(strip=True) or "").strip()
        try:
            d = datetime.strptime(value, "%m/%d/%Y").date()
        except ValueError:
            skipped_unparsed += 1
            continue
        all_parsed.append(value)
        if d >= today:
            dates.append(value)
    if skipped_unparsed:
        print(
            f"  WARNING: {skipped_unparsed} <option> value(s) in the "
            f"closedate select didn't parse as MM/DD/YYYY.",
            file=sys.stderr,
        )
    # DIAGNOSTIC (added 2026-09-22 after a run returned 0 future dates while
    # a real logged-in browser session showed 5 -- print exactly what this
    # run saw so a future "0 dates" result is debuggable from the log alone
    # instead of requiring a fresh manual re-check every time.
    print(
        f"  diag: today={today.isoformat()}, {len(select.find_all('option'))} <option> "
        f"tag(s) found, {len(all_parsed)} parsed as dates, {len(dates)} >= today. "
        f"All parsed dates: {all_parsed}",
        file=sys.stderr,
    )
    return dates


def fetch_sale_list(api_key, sale_date):
    # Same Scrapfly session as fetch_future_sale_dates, so this POST carries
    # whatever cookie the challenge issued on the list-page fetch.
    return scrapfly_request(
        "POST",
        GENERATE_URL,
        api_key,
        data={"closedate": sale_date, "target": "Generate List"},
    )


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

    is_hoa = bool(HOA_PLAINTIFF_PATTERN.search(row["plaintiff"] or ""))
    tags = [SOURCE_NAME] + (["hoa_foreclosure"] if is_hoa else [])

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "sale_date": sale_date,
            "case_number": row["case_number"],
            "plaintiff": row["plaintiff"],
            "law_firm": row["law_firm"],
            "hoa_foreclosure": is_hoa,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, zip, county, owner_name, source_tags, raw)
            values (%s, %s, %s, %s, 'Greenville', %s, %s::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                city = coalesce(excluded.city, leads.city),
                zip = coalesce(excluded.zip, leads.zip),
                owner_name = coalesce(excluded.owner_name, leads.owner_name),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, row["city"], row["state"], row["zip"], row["defendant"], tags, raw_payload),
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
      +35 if the property is in an active tax-sale redemption period (owner
          is about to permanently lose the property if they don't act)
      +30 if a permit shows storm/fire/water/damage repair language
          (insurance_damage -- T Dawg's "utmost importance" category)
      +20 if the MIE foreclosure plaintiff is an HOA/COA (hoa_foreclosure)
      +20 if the property shows 15+ years of ownership or a last sale price
          well below current fair market value (high_equity proxy)

    FIXED 2026-09-21: was missing the tired_landlord bonus AND the
    `where is_sold = false` guard, so this script (runs before
    absentee_owner_ingest.py in nightly.yml) was un-zeroing already-sold
    leads' scores each night. Fixed so this script is independently
    correct regardless of run order.

    UPDATED 2026-09-21: added the redemption_period bonus alongside the new
    redemption_period_ingest.py script.

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
    scrapfly_key = os.environ.get("SCRAPFLY_KEY")
    if not scrapfly_key:
        print("SCRAPFLY_KEY is not set", file=sys.stderr)
        sys.exit(1)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Finding upcoming foreclosure sale dates...")

    conn = psycopg2.connect(db_url)

    try:
        sale_dates = fetch_future_sale_dates(scrapfly_key)
    except RuntimeError as e:
        print(f"  ERROR fetching sale-date list: {e}", file=sys.stderr)
        log_run(conn, 0, 0, f"error fetching sale-date list: {e}")
        conn.commit()
        conn.close()
        sys.exit(1)

    print(f"Found {len(sale_dates)} upcoming sale dates: {sale_dates}")

    if not sale_dates:
        log_run(conn, 0, 0, "no upcoming sale dates found — bot-challenge page or list page structure may have changed")
        conn.commit()
        conn.close()
        return

    total_found = 0
    new_count = 0

    for sale_date in sale_dates:
        try:
            html = fetch_sale_list(scrapfly_key, sale_date)
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
