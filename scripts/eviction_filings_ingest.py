#!/usr/bin/env python3
"""
Greenville County magistrate EVICTION filings -> signal stacked on existing leads.

Built 2026-11-24 on Tala's orders (Nov 21 standing order + Nov 24 "approve"):
  * the LANDLORD (plaintiff) is the lead, never the tenant; residential only, no mobile homes
  * a filing is STACKED (tag 'eviction_filing') only onto a property that is ALREADY a lead
  * everything else goes ONLY on the hidden list (table eviction_hidden_list): never the
    master list, GHL, stories, skip-trace queues, the Team Board or reports
  * closed / dismissed cases remove only this signal

SOURCE (verified by probe runs 2026-11-24): Greenville County Public Index,
https://www2.greenvillecounty.org/scjd/publicindex/  (Accept/Decline disclaimer, no login,
no CAPTCHA). The result table has NO property address, only: Name, Party Type, Case
Number, Filed Date, Case Status, Disposition Date, Type, Subtype, Judgment #, Court
Agency. The court/case-type/sub-type dropdowns are ignored by the search, so we search
ONE filed-date day at a time and filter the Subtype column ourselves ("Rule to Vacate",
"Notice to Quit").

NAME-MATCH SAFEGUARD (Tala, Nov 24): because there is no address, a filing is stacked only when
  1. the plaintiff name equals the deed owner's name exactly (same words, any order), AND
  2. that lead is already flagged absentee or tired landlord, AND
  3. that name belongs to exactly ONE lead (an owner with several properties can't be tied
     to the house in the case), AND
  4. the lead is residential and not a mobile home.
Anything else goes to the hidden list with the reason.

MODES (EVICTION_MODE): 'test' (default) changes nothing on leads or the hidden list; it writes
at most 10 matches into the private table eviction_test_preview for Tala to review and prints
COUNTS ONLY (this repo is public). 'live' stacks and expires.
"""
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

import psycopg2
from lead_common import ensure_schema, rescore_all, IS_RESIDENTIAL_SQL  # noqa: E402

START_URL = "https://www2.greenvillecounty.org/scjd/publicindex/"
SOURCE_NAME = "eviction_filing"
P = "ctl00$ContentPlaceHolder1$"
EVICTION_SUBTYPE_RE = re.compile(r"rule to vacate|notice to quit", re.I)
CLOSED_STATUS_RE = re.compile(r"dispos|dismiss|settled|withdraw|satisf|closed", re.I)
EXPIRE_AFTER_DAYS = 90
ROW_CAP_WARNING = 500   # days run ~260-290 rows; a hard cap has not been seen
NAME_NOISE = {"JR", "SR", "II", "III", "IV", "ETAL", "ET", "AL", "AKA", "THE"}
HEADERS = "UnstuckHomesResearch/1.0 (+tala@unstuckhomes.com; low-volume public-record lookup)"


# ------------------------------------------------------------------ pure helpers (unit tested)
def norm_name(name):
    """Words of a name, uppercase, no punctuation, no Jr/Sr/et al. Order does not matter."""
    words = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper().replace("&", " AND ")).split()
    return tuple(sorted(w for w in words if w not in NAME_NOISE))


def names_match(a, b):
    na, nb = norm_name(a), norm_name(b)
    return len(na) >= 2 and na == nb


def is_eviction_row(row):
    return (row.get("Type") or "").strip().lower() == "civil" and bool(EVICTION_SUBTYPE_RE.search(row.get("Subtype") or ""))


def is_open(status, filed, today=None):
    today = today or today_et()
    if CLOSED_STATUS_RE.search(status or ""):
        return False
    return filed is None or (today - filed).days <= EXPIRE_AFTER_DAYS


def parse_us_date(s):
    try:
        return datetime.strptime((s or "").strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def classify(plaintiff, leads_by_name):
    """Returns ('stack', lead) or ('hidden', reason). leads_by_name: {norm_name tuple: [lead dict]}"""
    key = norm_name(plaintiff)
    cands = leads_by_name.get(key, []) if len(key) >= 2 else []
    if not cands:
        return "hidden", "no lead owned by this exact name"
    if len(cands) > 1:
        return "hidden", "name owns several lead properties, cannot tell which house"
    lead = cands[0]
    if not (lead.get("is_absentee") or lead.get("is_tired_landlord")):
        return "hidden", "lead is not flagged absentee or tired landlord"
    return "stack", lead


# ------------------------------------------------------------------ fetching
def fetch_day(page, day):
    """All result rows for one Case Filed day, as dicts keyed by the table headers."""
    ds = day.strftime("%m/%d/%Y")
    page.select_option(f"select[name='{P}DropDownListDateFilter']", label="Case Filed")
    page.wait_for_load_state("domcontentloaded")
    page.fill(f"input[name='{P}TextBoxDateFrom']", ds)
    page.fill(f"input[name='{P}TextBoxDateTo']", ds)
    page.click(f"input[name='{P}ButtonSearch']")
    page.wait_for_load_state("domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)
    best = None
    for t in page.query_selector_all("table"):
        n = len(t.query_selector_all("tr"))
        if n > 1 and (best is None or n > len(best.query_selector_all("tr"))):
            best = t
    if best is None:
        return []
    rows = best.query_selector_all("tr")
    hdr = [re.sub(r"\s+", " ", c.inner_text()).strip() for c in rows[0].query_selector_all("th,td")]
    if "Case Number" not in hdr:
        return []
    out = []
    for r in rows[1:]:
        cells = [re.sub(r"\s+", " ", c.inner_text()).strip() for c in r.query_selector_all("td")]
        if len(cells) == len(hdr):
            out.append(dict(zip(hdr, cells)))
    return out


def open_form(ctx):
    """A fresh search form behind the disclaimer (the form cannot be reused after a result page)."""
    page = ctx.new_page()
    page.goto(START_URL, timeout=45000, wait_until="domcontentloaded")
    page.click("input[value='Accept']")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(1500)
    return page


def today_et():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).date()


def fetch_window(days):
    from playwright.sync_api import sync_playwright
    cases, day_counts, failed_days = {}, {}, []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent=HEADERS)
        for i in range(days):
            day = today_et() - timedelta(days=i)
            if day.weekday() >= 5:   # courts file on weekdays
                continue
            page = None
            try:
                page = open_form(ctx)
                rows = fetch_day(page, day)
            except Exception as e:
                failed_days.append(day.isoformat())
                print(f"  {day}: fetch failed ({type(e).__name__})", file=sys.stderr)
                continue
            finally:
                if page:
                    page.close()
            day_counts[day.isoformat()] = len(rows)
            if len(rows) >= ROW_CAP_WARNING:
                print(f"  WARNING: {day} returned {len(rows)} rows (may be capped)")
            for r in rows:
                if is_eviction_row(r):
                    c = cases.setdefault(r["Case Number"], {"rows": []})
                    c["rows"].append(r)
        browser.close()
    return cases, day_counts, failed_days


def plaintiffs_of(case):
    r0 = case["rows"][0]
    plaintiffs = [r["Name"] for r in case["rows"] if (r.get("Party Type") or "").lower() == "plaintiff" and r.get("Name")]
    return sorted(set(plaintiffs)), r0


# ------------------------------------------------------------------ database
def load_leads_by_name(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select id, owner_name, is_absentee, is_tired_landlord, raw
            from leads
            where is_sold = false and is_duplicate = false and list_count > 0
              and owner_name is not null
              and {IS_RESIDENTIAL_SQL}
              and coalesce(land_use, '') !~* 'mobile|manufactur'
            """
        )
        by = {}
        for id_, owner, absentee, tired, raw in cur.fetchall():
            by.setdefault(norm_name(owner), []).append(
                {"id": id_, "owner_name": owner, "is_absentee": absentee, "is_tired_landlord": tired, "raw": raw or {}})
    return by


def ensure_tables(conn, test):
    with conn.cursor() as cur:
        cur.execute("""create table if not exists eviction_hidden_list (
            case_number text primary key, plaintiff text, court text, filed_date date, status text,
            subtype text, reason text, first_seen timestamptz not null default now(), last_seen timestamptz not null default now())""")
        if test:
            cur.execute("""create table if not exists eviction_test_preview (
                id serial primary key, run_at timestamptz not null default now(), case_number text, plaintiff text,
                filed_date date, status text, court text, subtype text, lead_id bigint, lead_address text,
                lead_owner text, is_absentee boolean, is_tired_landlord boolean, decision text, reason text)""")


def stack_on_lead(conn, lead, cases_for_lead):
    existing = (lead["raw"].get(SOURCE_NAME) or {}).get("cases", {})
    merged = {**existing, **{cn: v for cn, v in cases_for_lead.items()}}
    payload = {SOURCE_NAME: {"cases": merged, "match": "owner_name_exact + absentee/tired-landlord, unique owner",
                             "fetched_at": datetime.now(timezone.utc).isoformat()}}
    with conn.cursor() as cur:
        cur.execute(
            """update leads set source_tags = array(select distinct unnest(source_tags || array[%s]::text[])),
               raw = coalesce(raw, '{}'::jsonb) || %s::jsonb, updated_at = now() where id = %s""",
            (SOURCE_NAME, json.dumps(payload), lead["id"]))


def expire_signals(conn, today=None):
    """Remove the tag from leads whose every stored case is closed or older than 90 days."""
    removed = 0
    with conn.cursor() as cur:
        cur.execute("select id, raw->'eviction_filing'->'cases' from leads where %s = any(source_tags)", (SOURCE_NAME,))
        for id_, cases in cur.fetchall():
            cases = cases or {}
            if any(is_open(c.get("status"), parse_us_date(c.get("filed"))) for c in cases.values()):
                continue
            cur.execute("update leads set source_tags = array_remove(source_tags, %s), updated_at = now() where id = %s",
                        (SOURCE_NAME, id_))
            removed += 1
    return removed


# ------------------------------------------------------------------ main
def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    mode = (os.environ.get("EVICTION_MODE") or "test").strip().lower()
    test = mode != "live"
    days = int(os.environ.get("EVICTION_DAYS") or (21 if test else 45))
    print(f"[{datetime.now(timezone.utc).isoformat()}] eviction filings, mode={'TEST (no changes to leads)' if test else 'LIVE'}, window={days} days")

    cases, day_counts, failed_days = fetch_window(days)
    print(f"  weekday searches ok={len(day_counts)} failed={len(failed_days)}; rows seen={sum(day_counts.values())}; "
          f"eviction cases (Rule to Vacate / Notice to Quit)={len(cases)}")
    if not day_counts:
        print("  nothing fetched; stopping without changes", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    ensure_schema(conn)
    ensure_tables(conn, test)
    by_name = load_leads_by_name(conn)

    stack, hidden_n, reasons, preview = {}, 0, {}, []
    for cn, case in sorted(cases.items()):
        plaintiffs, r0 = plaintiffs_of(case)
        filed = parse_us_date(r0.get("Filed Date"))
        info = {"filed": r0.get("Filed Date"), "status": r0.get("Case Status"), "court": r0.get("Court Agency"),
                "subtype": r0.get("Subtype")}
        if not plaintiffs:
            decision, detail = "hidden", "no plaintiff row"
            plaintiffs = [""]
        for pl in plaintiffs:
            if pl:
                decision, detail = classify(pl, by_name)
            if decision == "stack" and not is_open(info["status"], filed):
                decision, detail = "hidden", "case already closed"
            if decision == "stack":
                lead = detail
                stack.setdefault(lead["id"], (lead, {}))[1][cn] = info
                if len(preview) < 10:
                    preview.append((cn, pl, filed, info, lead, "stack", "exact owner name, flagged landlord, unique"))
            else:
                hidden_n += 1
                reasons[detail] = reasons.get(detail, 0) + 1
                if not test:
                    with conn.cursor() as cur:
                        cur.execute(
                            """insert into eviction_hidden_list (case_number, plaintiff, court, filed_date, status, subtype, reason)
                               values (%s,%s,%s,%s,%s,%s,%s)
                               on conflict (case_number) do update set status = excluded.status, reason = excluded.reason,
                                 last_seen = now()""",
                            (cn, pl, info["court"], filed, info["status"], info["subtype"], detail))
    print(f"  would stack on {len(stack)} existing lead(s); hidden-list cases={hidden_n}; reasons={reasons}")

    if test:
        with conn.cursor() as cur:
            cur.execute("delete from eviction_test_preview")
            for cn, pl, filed, info, lead, decision, reason in preview:
                cur.execute(
                    """insert into eviction_test_preview (case_number, plaintiff, filed_date, status, court, subtype, lead_id,
                       lead_address, lead_owner, is_absentee, is_tired_landlord, decision, reason)
                       select %s,%s,%s,%s,%s,%s,l.id,l.address,l.owner_name,l.is_absentee,l.is_tired_landlord,%s,%s from leads l where l.id=%s""",
                    (cn, pl, filed, info["status"], info["court"], info["subtype"], decision, reason, lead["id"]))
        conn.commit()
        conn.close()
        print(f"  TEST ONLY: {len(preview)} match(es) saved to the private table eviction_test_preview; nothing else changed.")
        return

    for lead, cs in stack.values():
        stack_on_lead(conn, lead, cs)
    if not failed_days:
        removed = expire_signals(conn)
    else:
        removed = 0
        print(f"  expiry skipped: {len(failed_days)} day(s) failed to fetch")
    rescore_all(conn)
    with conn.cursor() as cur:
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) values (%s,%s,%s,%s)",
                    (SOURCE_NAME, len(cases), len(stack), f"ok; hidden={hidden_n}; expired={removed}"))
    conn.commit()
    conn.close()
    print(f"Done. Stacked on {len(stack)} lead(s); {hidden_n} case(s) on the hidden list; {removed} signal(s) expired.")


if __name__ == "__main__":
    main()
