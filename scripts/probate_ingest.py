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
from lead_common import ensure_schema, rescore_all  # noqa: E402

BASE_URL = "https://www.greenvillecounty.org/appsas400/Probate/"
DETAILS_URL = "https://www.greenvillecounty.org/appsas400/Probate/SearchDetails.aspx?CaseNumber={case_number}"
COUNTY_CODE = "23"
SEQ_WIDTH = 5
CONSECUTIVE_MISS_LIMIT = 25
REQUEST_DELAY_SECONDS = 1.5   # was 0.4: the county started answering 403 after 15 cases (run #32, 2026-09-27)
BLOCK_BACKOFF_SECONDS = (30, 90, 240)
NIGHTLY_LOOKUP_BUDGET = 900    # ~25 min at 1.5s; the 2024-2026 backfill finishes over ~2 weeks of nights


class Blocked(Exception):
    """The county site is refusing us (403/429). Stop for the night --
    never count these as 'case doesn't exist' misses."""
SOURCE_NAME = "probate"
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}

LABELS_IN_ORDER = [
    "Name", "Party Type", "Address", "Date of Death", "Date of Birth", "Sex",
    "Status", "Case Type", "Case SubType", "File Date",
    "Expiration of Estate Creditor Claims Period", "PR Appointed Date",
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
    if resp.status_code in (403, 429):
        for wait in BLOCK_BACKOFF_SECONDS:
            print(f"  {cn}: county site answered {resp.status_code}; waiting {wait}s and retrying", file=sys.stderr)
            time.sleep(wait)
            try:
                session.get(BASE_URL, timeout=30)  # fresh cookie
                resp = session.get(url, timeout=30)
            except requests.RequestException:
                continue
            if resp.status_code not in (403, 429):
                break
        else:
            raise Blocked(f"{cn}: still {resp.status_code} after backoff")
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


# Postal cities used for Greenville County addresses. Longest first so
# "Travelers Rest" wins over any shorter overlap.
KNOWN_CITIES = sorted([
    "Greenville", "Greer", "Simpsonville", "Mauldin", "Fountain Inn", "Travelers Rest",
    "Taylors", "Piedmont", "Pelzer", "Marietta", "Landrum", "Gray Court", "Easley",
    "Cleveland", "Slater", "Tigerville", "Duncan", "Woodruff", "Williamston", "Liberty",
], key=len, reverse=True)
_CITY_RE = re.compile(
    r"^(?P<street>.+?)[,\s]+(?P<city>" + "|".join(re.escape(c) for c in KNOWN_CITIES) +
    r")[,\s]+SC[,\s]+(?P<zip>\d{5})", re.I)


def parse_address_parts(full_addr):
    """
    Split 'STREET CITY SC ZIP' -> (street, city, zip).
    FIX 2026-09-25: the old regex was lazy on the street, so
    "12 MAIN ST GREER SC 29651" became street "12", city "Main St Greer",
    and unparseable addresses were hard-coded to city "Greenville". Now the
    city must be a known county postal city; otherwise the street keeps
    everything before "SC", city is left blank, and the zip is still kept.
    """
    m = _CITY_RE.match(full_addr)
    if m:
        return m.group("street").strip(" ,"), m.group("city").title(), m.group("zip")
    m = re.match(r"^(.*?)[,\s]+SC[,\s]+(\d{5})", full_addr, re.I)
    if m:
        return m.group(1).strip(" ,"), None, m.group(2)
    return full_addr.strip(" ,"), None, None


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
        "creditor_deadline": fields.get("Expiration of Estate Creditor Claims Period") or None,
        **parse_pr(fields.get("PARTIES INVOLVED") or ""),
    }


def parse_pr(parties_text):
    """Personal Representative name/address from the PARTIES INVOLVED block
    ("Name: DOE , JANE Q Party Type: Personal Representative
    Address: 123 MAIN ST ...")."""
    m = re.search(r"Name:?\s*(.*?)\s*Party Type:?\s*Personal Representative\s*Address:?\s*(.*?)"
                  r"(?=\s*Name:|$)", parties_text or "", re.I)
    if not m:
        return {"pr_name": None, "pr_address": None}
    return {"pr_name": re.sub(r"\s*,\s*", ", ", m.group(1)).strip(" ,"), "pr_address": m.group(2).strip()}


def split_decedent_name(name):
    """'DOE , JANE QUINN' -> ('DOE', 'JANE'). The court writes LAST , FIRST MIDDLE."""
    if not name or "," not in name:
        return None, None
    last, rest = name.split(",", 1)
    last = re.sub(r"[^A-Z' -]", "", last.upper()).strip()
    first = (re.findall(r"[A-Z']+", rest.upper()) or [None])[0]
    return (last or None), first


def decedent_middle_initial(name):
    """'DOE , JANE QUINN' -> 'Q'; 'DOE , JOHN' -> None. JR/SR/II/III/IV are suffixes, not middles."""
    if not name or "," not in name:
        return None
    toks = [t for t in re.findall(r"[A-Z']+", name.split(",", 1)[1].upper())
            if t not in ("JR", "SR", "II", "III", "IV")]
    return toks[1][0] if len(toks) >= 2 else None


def street_only(addr):
    if not addr:
        return None
    street = parse_address_parts(addr)[0]
    return street or None


def upsert_lead(conn, row):
    street, city, zip_code = parse_address_parts(row["address"])
    if not street:
        return False
    # Greenville County only (standing rule): a decedent who lived in
    # Atlanta or Anderson County isn't a Greenville property lead. All
    # Greenville County zips start 296.
    if not (zip_code and zip_code.startswith("296")):
        return False
    # A PO box is a mailbox, not a property (one real case listed
    # a PO box -- those houses are found by the owner-name match instead).
    if re.search(r"\bP\.?\s*O\.?\s*BOX\b|\bPO BOX\b", street, re.I):
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
                raw = coalesce(leads.raw, '{}'::jsonb) || excluded.raw,
                updated_at = now()
            """,
            (street, city, zip_code, row["decedent_name"], SOURCE_NAME, raw_payload),
        )
    return True


# rescore_all now lives in lead_common.py (one shared formula for every script).

def get_high_water_seq(conn, year):
    with conn.cursor() as cur:
        cur.execute(
            """
            select notes from source_runs
            where source_name = %s and notes like %s
            order by run_at desc limit 1
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


YEARS_BACK = 2  # estates stay open 1-3 years; one real 2024 estate was still open after 22 months


def ensure_cases_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists probate_cases (
                case_number text primary key,
                decedent_name text, decedent_last text, decedent_first text,
                date_of_death text, file_date text, closed_date text,
                pr_name text, pr_address text, decedent_address text,
                creditor_deadline text, fetched_at timestamptz not null default now()
            )
            """
        )
        # added 2026-09-26: middle initial + street-only addresses for matching
        cur.execute("alter table probate_cases add column if not exists decedent_middle text")
        cur.execute("alter table probate_cases add column if not exists decedent_street text")
        cur.execute("alter table probate_cases add column if not exists pr_street text")


def save_case(conn, row):
    last, first = split_decedent_name(row["decedent_name"])
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into probate_cases
                (case_number, decedent_name, decedent_last, decedent_first, date_of_death,
                 file_date, closed_date, pr_name, pr_address, decedent_address,
                 creditor_deadline, decedent_middle, decedent_street, pr_street, fetched_at)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            on conflict (case_number) do update set
                closed_date = excluded.closed_date, pr_name = excluded.pr_name,
                pr_address = excluded.pr_address, creditor_deadline = excluded.creditor_deadline,
                decedent_middle = excluded.decedent_middle, decedent_street = excluded.decedent_street,
                pr_street = excluded.pr_street, fetched_at = now()
            """,
            (row["case_number"], row["decedent_name"], last, first, row["date_of_death"],
             row["file_date"], row["closed_date"], row.get("pr_name"), row.get("pr_address"),
             row["address"], row.get("creditor_deadline"), decedent_middle_initial(row["decedent_name"]),
             street_only(row["address"]), street_only(row.get("pr_address"))),
        )


def match_owners_to_estates(conn):
    """
    ADDED 2026-09-26. Tags an existing distress lead 'probate' when the
    county owner of record is a decedent with an open estate -- the county
    writes owners LAST FIRST ("Doe Jane Q", "Doe John Jr Doe
    Jane Q") and the court writes LAST , FIRST, so the match is the
    LAST+FIRST pair appearing together in the owner name. This catches
    estates whose court file lists only a PO box, not the house.
    Only open estates (no closed date). Name matches get the
    'probate_name_match' review flag so a common name is double-checked.
    Never creates a lead -- only tags properties already on a distress list.
    """
    with conn.cursor() as cur:
        cur.execute(
            r"""
            update leads l set
                source_tags = array(select distinct unnest(l.source_tags || array['probate'])),
                raw = coalesce(l.raw, '{}'::jsonb) || jsonb_build_object('probate', jsonb_build_object(
                    'case_number', c.case_number, 'decedent_name', c.decedent_name,
                    'date_of_death', c.date_of_death, 'file_date', c.file_date,
                    'pr_name', c.pr_name, 'pr_address', c.pr_address,
                    'creditor_deadline', c.creditor_deadline,
                    -- owner_name_and_address = the probate file's address (decedent's
                    -- or PR's) is the property itself or the owner's mailing address.
                    -- owner_name alone = verify by hand (common names).
                    'match', case when exists (
                        select 1 from unnest(array[c.decedent_street, c.pr_street]) s(st)
                        where address_core(s.st) is not null and (
                            address_core(s.st) = address_core(l.address)
                            or address_core(l.mailing_address) = address_core(s.st)
                            or address_core(l.mailing_address) like address_core(s.st) || ' %'))
                        then 'owner_name_and_address' else 'owner_name' end)),
                updated_at = now()
            from probate_cases c
            where l.list_count > 0 and l.is_sold = false and l.is_duplicate = false
              and coalesce(c.closed_date, '') = ''
              and c.decedent_last is not null and c.decedent_first is not null
              and length(c.decedent_first) >= 2
              and upper(coalesce(l.owner_name, '')) ~ ('\m' || c.decedent_last || '\s+' || c.decedent_first || '\M')
              -- middle-initial rule: if the county lists a single-letter middle
              -- initial right after LAST FIRST, it must be the decedent's
              -- (Doe John E is not DOE, JOHN WAYNE).
              and (c.decedent_middle is null
                   or upper(l.owner_name) !~ ('\m' || c.decedent_last || '\s+' || c.decedent_first || '\s+[A-Z]\M')
                   or upper(l.owner_name) ~ ('\m' || c.decedent_last || '\s+' || c.decedent_first || '\s+' || c.decedent_middle || '\M'))
              and not ('probate' = any(l.source_tags) and l.raw->'probate'->>'case_number' = c.case_number)
            """
        )
        return cur.rowcount


def crawl_year(conn, session, year, budget):
    start_seq = get_high_water_seq(conn, year) + 1
    seq, misses, lookups, found, new_count = start_seq, 0, 0, 0, 0
    highest = start_seq - 1
    while misses < CONSECUTIVE_MISS_LIMIT and lookups < budget:
        cn = case_number(year, seq)
        try:
            html = fetch_case(session, cn)
        except Blocked as e:
            print(f"  STOPPED for tonight -- {e}. Resumes from {cn} next run.", file=sys.stderr)
            log_run(conn, found, new_count,
                    f"blocked: year:{year} high_water_seq:{highest} lookups:{lookups} found_cases:{found}")
            conn.commit()
            raise
        lookups += 1
        if html is None:
            misses += 1
            seq += 1
            time.sleep(REQUEST_DELAY_SECONDS)
            continue
        misses = 0
        highest = seq
        found += 1
        try:
            parsed = parse_case_detail(html, cn)
        except Exception as e:
            parsed = None
            print(f"  {cn}: parse error: {e}", file=sys.stderr)
        if parsed:
            save_case(conn, parsed)
            if upsert_lead(conn, parsed):
                new_count += 1
        if found % 100 == 0:
            conn.commit()
        seq += 1
        time.sleep(REQUEST_DELAY_SECONDS)
    log_run(conn, found, new_count,
            f"ok: year:{year} high_water_seq:{highest} lookups:{lookups} found_cases:{found} "
            f"estate_leads:{new_count} stopped_after_misses:{misses}")
    conn.commit()
    print(f"  {year}: {lookups} lookups, {found} cases, {new_count} address leads, high-water {highest}")
    return lookups


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    budget = int(os.environ.get("MAX_ROWS") or NIGHTLY_LOOKUP_BUDGET)
    conn = psycopg2.connect(db_url)
    ensure_schema(conn)
    ensure_cases_table(conn)
    conn.commit()
    session = new_session()
    this_year = date.today().year
    # Older years first: they're finite and get finished; the current year
    # keeps growing and is picked up every night after the backfill.
    for year in range(this_year - YEARS_BACK, this_year + 1):
        if budget <= 0:
            break
        try:
            budget -= crawl_year(conn, session, year, budget)
        except Blocked:
            break  # still match owners against the cases already saved
    n = match_owners_to_estates(conn)
    print(f"  owner-name matches to open estates: {n} lead(s) tagged probate")
    rescore_all(conn)
    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
