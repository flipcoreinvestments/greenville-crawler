#!/usr/bin/env python3
"""
Greenville County Code Enforcement — "Unfit Structures" (condemned / unfit-
for-habitation) ingest. Covers category #16 (code violations / condemned
property) from T Dawg's 17/19-category list.

Source: https://app.greenvillecounty.org/unfit_structures.htm ("Browse
Unfit Structures"), a public county-run legacy LANSA web app (same
CGI-bin/web-4GL family IBM shops have run since the 90s — no login, no
CAPTCHA, disclaimer-gated like every other county portal already in this
pipeline). Confirmed 2026-09-22 by hand in a real browser:

  - The search page (CE_00/CE0001) is a stateful form: it renders a set of
    hidden fields plus a POST action URL containing a per-session
    transaction token (e.g. ".../LANSAWEB?WEBEVENT+L0F5101925CE0550187CF091
    +GP1+ENG"). That token is only valid for the session that generated it,
    so this script GETs the search page fresh every run, scrapes the
    current hidden fields + action URL out of the HTML, and POSTs them
    back with LDROP_SCR (page size) forced to "ALL", the free-text filter
    left blank -- exactly what "leave blank for ALL" on the visible form
    does -- and ASTDRENTST forced to "SEARCH" (the visible Search link's
    onclick sets this hidden field before submitting; a plain form replay
    without it gets silently re-served the blank search form instead of
    running the query -- see the bug-fix note in fetch_all_cases() for how
    this was caught and confirmed). Small dataset (6 open cases county-wide
    as of 2026-09-22), so no pagination logic is needed.
  - The results table is plain HTML: Case Number | Case Location | Case
    Map Number (the map number is the county TMS/parcel ID). This alone is
    enough to make a lead -- Case Location is already a normal mailable
    street address.
  - The per-case detail page IS stateless and GET-addressable with no
    session/cookie dependency at all (verified from a brand-new tab with
    no prior visit to the search page):
      https://app.greenvillecounty.org/cgi-bin/lansaweb?procfun+pop_00+pop0001+GP1+FUNCPARMS+CECYER(S0020):{YY}+CECNBR(S0080):{NNNNNNNN}
    where YY/NNNNNNNN come straight from splitting "23-90000876" on its
    dash. It adds the Property Owner name and the condemnation hearing
    date/location -- a live contact and a hard deadline, both good outreach
    hooks. Best-effort: if a detail fetch fails, the lead is still created
    from the browse-list row alone (address + map number), just without an
    owner name.

SCORING: "unfit for human habitation" with a condemnation hearing already
scheduled is a severe distress signal -- arguably the single clearest sign
of a property its owner has stopped maintaining. Tagged 'code_violation',
weighted +25 in rescore_all() (between permit_expired/demolition's +20 and
foreclosure_mie's +30) -- T Dawg's call to adjust if she wants it weighted
differently.

NOTE: this list is NOT a general vacancy list (that was researched
exhaustively already and confirmed to have no public source for Greenville
County/City) -- it only covers structures far enough into disrepair to be
formally condemned. Real, but a small, specific slice, not a vacancy proxy.
"""

import os
import re
import sys
import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
import psycopg2

SEARCH_URL = "https://app.greenvillecounty.org/cgi-bin/lansaweb?procfun+CE_00+CE0001+GP1+eng"
DETAIL_URL_TMPL = (
    "https://app.greenvillecounty.org/cgi-bin/lansaweb?procfun+pop_00+pop0001+GP1+"
    "FUNCPARMS+CECYER(S0020):{year}+CECNBR(S0080):{case_num}"
)
SOURCE_NAME = "code_violation"
REQUEST_TIMEOUT = 30
HEADERS = {
    "User-Agent": "RestartHomesResearch/1.0 (+info@restarthomes.net; one-off public-record lookup, low volume)"
}


def get_search_form(session):
    resp = session.get(SEARCH_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form")
    if form is None:
        raise RuntimeError("could not find search form on unfit-structures page")

    action = form.get("action")
    if action and action.startswith("/"):
        action = "https://app.greenvillecounty.org" + action

    fields = {}
    for el in form.find_all(["input", "select"]):
        name = el.get("name")
        if not name:
            continue
        if el.name == "select":
            selected = el.find("option", selected=True)
            fields[name] = selected.get("value", selected.text) if selected else ""
        else:
            fields[name] = el.get("value", "")

    return action, fields


def fetch_all_cases(session):
    action, fields = get_search_form(session)
    fields["LDROP_SCR"] = "ALL"
    fields["ASC_POSTO"] = ""  # blank = ALL, per the visible form's own hint
    # BUG FIX 2026-09-22 (found via production validation run returning 0
    # results against a page with 6 real open cases): the visible "Search"
    # link isn't a plain form submit -- its onclick is
    # `document.LANSA.ASTDRENTST.value='SEARCH'; HandleEvent(...)`, i.e. it
    # mutates this hidden field to 'SEARCH' before posting. Without it, the
    # LANSA backend just re-serves the blank search form (200 OK, same
    # byte length as the GET) instead of running the query -- no exception,
    # so it silently looked like "zero cases" instead of a failed request.
    # Confirmed fix by replaying the exact POST by hand: 0 matches without
    # this field, 6/6 correct matches with it.
    fields["ASTDRENTST"] = "SEARCH"

    resp = session.post(action, data=fields, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n")

    rows = []
    # Rows look like: "23-90000876 17 HARRIS AVE 0230-00.05.059-00"
    pattern = re.compile(
        r"(\d{2}-\d{6,8})\s+(.+?)\s+(\d{4}-\d{2}\.\d{2}\.\d{3}-\d{2})"
    )
    for m in pattern.finditer(re.sub(r"[ \t]+", " ", text)):
        case_number, location, map_number = m.groups()
        rows.append({
            "case_number": case_number.strip(),
            "location": re.sub(r"\s+", " ", location).strip(),
            "map_number": map_number.strip(),
        })
    return rows


def fetch_owner(session, case_number):
    try:
        year, num = case_number.split("-", 1)
    except ValueError:
        return None, None
    url = DETAIL_URL_TMPL.format(year=year, case_num=num)
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  {case_number}: detail fetch failed: {e}", file=sys.stderr)
        return None, None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text(" "))

    owner = None
    m = re.search(r"Property Owner:\s*(.+?)(?:CLOSE THIS WINDOW|$)", text)
    if m:
        owner = m.group(1).strip(" :\n\t\r") or None

    hearing = None
    m = re.search(r"(The hearing set for this case.*?University Ridge)", text)
    if m:
        hearing = m.group(1).strip()

    return owner, hearing


def upsert_lead(conn, row, owner, hearing):
    address = row["location"]
    if not address:
        return False

    raw_payload = json.dumps({
        SOURCE_NAME: {
            "case_number": row["case_number"],
            "map_number": row["map_number"],
            "hearing_info": hearing,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into leads (address, city, state, county, owner_name, source_tags, raw)
            values (%s, 'Greenville', 'SC', 'Greenville', %s, ARRAY[%s]::text[], %s::jsonb)
            on conflict (lower(address)) do update set
                owner_name = coalesce(leads.owner_name, excluded.owner_name),
                source_tags = array(select distinct unnest(leads.source_tags || excluded.source_tags)),
                raw = leads.raw || excluded.raw,
                updated_at = now()
            """,
            (address, owner, SOURCE_NAME, raw_payload),
        )
    return True


def rescore_all(conn):
    """
    Shared score formula -- kept IDENTICAL in every ingest script. Adds
    'code_violation' at +25 alongside the existing bonuses (see
    absentee_owner_ingest.py for the full running commentary on the rest).
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

    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching unfit-structures list...")
    try:
        rows = fetch_all_cases(session)
    except Exception as e:
        print(f"  fetch failed: {e}", file=sys.stderr)
        rows = []
    print(f"  {len(rows)} open unfit-structure cases found.")

    conn = psycopg2.connect(db_url)
    new_count = 0
    for row in rows:
        owner, hearing = fetch_owner(session, row["case_number"])
        if upsert_lead(conn, row, owner, hearing):
            new_count += 1

    rescore_all(conn)
    log_run(conn, len(rows), new_count, "ok")
    conn.commit()
    conn.close()
    print(f"Done. {len(rows)} cases checked, {new_count} unfit-structure leads upserted.")


if __name__ == "__main__":
    main()
