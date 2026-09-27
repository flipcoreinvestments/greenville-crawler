#!/usr/bin/env python3
"""
Inspection-history proof for City of Greenville permits (added 2026-09-25).

WHY: the city's permit map layer only says a permit is open (IS) or closed
(CL). "Open" does NOT mean stalled -- T Dawg's top-10 check found 805
Crescent Av tagged as an open damage permit while the city's own records
showed a slab inspection APPROVED four days earlier (active job).

SOURCE: the city's public permit center (Click2Gov),
https://grvl-egov.aspgov.com/grvlc2gbp/ -- "Required Inspections" lists
every inspection on a permit application with its status and result date.
Flow (same as a person clicking through): index -> Schedule/Cancel
Inspections -> search by application number (year + number) -> Required
Inspections. Read-only; nothing is scheduled or submitted beyond the search.

RULE (building code, IRC/IBC section 105.5): a permit becomes invalid if
work is suspended or abandoned for 180 days. Inspections are the proof of
work. So a permit is STALLED only when it is still open, has no approved
final, and its most recent inspection (or its application date, if it has
never had one) is 180+ days old.

Results are cached in permit_inspection_checks so each permit is looked up
at most once a week, capped per run, at a polite pace.
"""

import re
import time
from datetime import date, datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

BASE = "https://grvl-egov.aspgov.com/grvlc2gbp/"
STALLED_DAYS = 180
RECHECK_DAYS = 7
MAX_LOOKUPS_PER_RUN = 250
REQUEST_DELAY_SECONDS = 1.0
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; RestartHomesResearch/1.0; +info@restarthomes.net)"}
_DATE_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists permit_inspection_checks (
                permit_num text primary key,
                checked_at timestamptz not null default now(),
                found boolean not null,
                total_inspections integer,
                done_inspections integer,
                last_inspection date,
                final_approved boolean,
                error text
            )
            """
        )


def split_permit_num(permit_num):
    """City permit numbers look like 2500003626 -> ('25', '00003626')."""
    digits = re.sub(r"\D", "", str(permit_num or ""))
    if len(digits) != 10:
        return None
    return digits[:2], digits[2:]


def parse_required_inspections(html):
    """Columns: Permit | Inspection Type | Status | Resulted | Min | Max."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) >= 4:
            rows.append(cells)
    done, last, final_ok = 0, None, False
    for r in rows:
        m = _DATE_RE.match(r[3].strip())
        if m:
            done += 1
            d = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            last = d if last is None or d > last else last
        if "FINAL" in r[1].upper() and r[2].strip().upper() == "APPROVED":
            final_ok = True
    return {"total": len(rows), "done": done, "last": last, "final_approved": final_ok}


class Click2Gov:
    def __init__(self, session=None):
        self.s = session or requests.Session()
        self.s.headers.update(HEADERS)

    def _get(self, url):
        r = self.s.get(url if url.startswith("http") else BASE + url, timeout=30)
        r.raise_for_status()
        return r.text

    def lookup(self, permit_num):
        parts = split_permit_num(permit_num)
        if not parts:
            return {"found": False, "error": "unparseable permit number"}
        year, number = parts
        idx = BeautifulSoup(self._get("index.html"), "html.parser")
        link = next((a for a in idx.find_all("a") if "schedule" in a.get_text().lower()), None)
        if not link:
            raise RuntimeError("Click2Gov layout changed: no Schedule/Cancel Inspections link")
        page = BeautifulSoup(self._get(link["href"]), "html.parser")
        form = next((f for f in page.find_all("form") if f.find("input", {"name": "permit.appYear"})), None)
        if not form:
            raise RuntimeError("Click2Gov layout changed: no application-number search form")
        data = {i.get("name"): i.get("value", "") for i in form.find_all("input")
                if i.get("name") and i.get("type") != "submit"}
        data["permit.appYear"], data["permit.appNumber"] = year, number
        data["finish"] = "Continue »"
        r = self.s.post(BASE + form.get("action", "selectpermit.html"), data=data, timeout=30)
        r.raise_for_status()
        res = BeautifulSoup(r.text, "html.parser")
        req = next((a for a in res.find_all("a") if "required inspections" in a.get_text().lower()), None)
        if not req:
            return {"found": False, "error": None}
        parsed = parse_required_inspections(self._get(req["href"]))
        return {"found": True, "error": None, **parsed}


def is_stalled(check, applied, today=None):
    """True / False, or None when there is no usable check yet (never guess)."""
    today = today or date.today()
    if not check or not check.get("found"):
        return None
    if check.get("final_approved"):
        return False
    ref = check.get("last_inspection") or applied
    if ref is None:
        return None
    return (today - ref).days >= STALLED_DAYS


def load_checks(conn):
    with conn.cursor() as cur:
        cur.execute("select permit_num, found, last_inspection, final_approved, checked_at "
                    "from permit_inspection_checks")
        return {r[0]: {"found": r[1], "last_inspection": r[2], "final_approved": r[3], "checked_at": r[4]}
                for r in cur.fetchall()}


def run_checks(conn, permit_nums, client=None, max_lookups=MAX_LOOKUPS_PER_RUN, delay=REQUEST_DELAY_SECONDS):
    """Look up permits never checked or not checked in RECHECK_DAYS.
    Stops early if the site is unreachable (e.g. blocks the runner) -- in
    that case nothing is guessed; permits just stay unverified."""
    ensure_table(conn)
    existing = load_checks(conn)
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECHECK_DAYS)
    todo = [p for p in dict.fromkeys(permit_nums)
            if p not in existing or existing[p]["checked_at"] < cutoff]
    client = client or Click2Gov()
    done = failures = 0
    for p in todo[:max_lookups]:
        try:
            res = client.lookup(p)
        except Exception as e:  # network block / layout change
            failures += 1
            print(f"  inspection lookup failed for {p}: {e}")
            if failures >= 5 and done == 0:
                print("  WARNING: city permit center unreachable -- inspection checks skipped this run")
                break
            continue
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into permit_inspection_checks
                    (permit_num, checked_at, found, total_inspections, done_inspections,
                     last_inspection, final_approved, error)
                values (%s, now(), %s, %s, %s, %s, %s, %s)
                on conflict (permit_num) do update set
                    checked_at = now(), found = excluded.found,
                    total_inspections = excluded.total_inspections,
                    done_inspections = excluded.done_inspections,
                    last_inspection = excluded.last_inspection,
                    final_approved = excluded.final_approved, error = excluded.error
                """,
                (p, res["found"], res.get("total"), res.get("done"), res.get("last"),
                 res.get("final_approved"), res.get("error")),
            )
        conn.commit()
        done += 1
        time.sleep(delay)
    print(f"  inspection checks: {done} looked up this run, {max(0, len(todo) - done)} still waiting")
    return done
