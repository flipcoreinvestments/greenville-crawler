#!/usr/bin/env python3
"""
Week-over-week simulation of the GHL flow (2026-10-16). Synthetic leads only
(no real names, phones or addresses -- the repo is public).

Needs a throwaway Postgres: TEST_DATABASE_URL=postgresql://... python tests/test_ghl_flow.py
It creates the tables it needs and drops them at the start of every run.

Scenarios checked, night by night:
  N0  legacy leads hand-imported before the cutoff produce NO rows (handled on the 10.15 sheet)
  N1  a GHL export shows a dead lead's contact is still there -> DELETE (once)
      a lead whose old contact is gone and still needs a retrace -> nothing (stays on the pull list)
      a newly traced lead -> NEW;   two properties, one owner, same phone -> ONE row
      a lead already in GHL with a new error -> UPDATE, Phone 1 = the phone GHL has
      a Personal Representative appointed on a lead in GHL -> RETRACE (yellow, right person in Skiptrace columns)
      a dead property whose owner contact still stands for a live property -> no DELETE
  N1b the on-demand export the same day rebuilds the same daily file
  N2  nothing re-sent; the 7-day file still carries N1's rows
  N3  GHL export: retrace contact deleted, "wrong number" + "wrong person" tags from calls;
      retrace result for the new PR read -> NEW for the new person, no retrace tag;
      wrong number never exported again; wrong person with the same search -> ORANGE, kept off the pull list
"""
import csv
import io
import json
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import export_call_lists as ex  # noqa: E402
import ghl_sync as gs  # noqa: E402
import skiptrace_intake as si  # noqa: E402
from skiptrace_prep import compute_skiptrace_identity  # noqa: E402

UTC = timezone.utc
BEFORE = datetime(2026, 10, 10, 12, tzinfo=UTC)
D1 = datetime(2026, 10, 16, 12, tzinfo=UTC)
D3 = datetime(2026, 10, 18, 12, tzinfo=UTC)


def setup(conn):
    with conn.cursor() as cur:
        for t in ("ghl_sent", "ghl_sync_files", "lead_error_log", "skiptrace_files", "source_runs",
                  "pipeline_migrations"):
            cur.execute(f"drop table if exists {t} cascade")
        cur.execute("drop table if exists leads cascade")
        cur.execute("""create table leads (
            id uuid primary key default gen_random_uuid(), address text, city text, state text, zip text,
            owner_name text, mailing_address text, raw jsonb default '{}'::jsonb, source_tags text[],
            list_count int default 1, is_sold boolean default false, is_duplicate boolean default false,
            land_use text, score int default 50, is_vacant boolean, is_absentee boolean, is_tired_landlord boolean,
            is_out_of_state_land boolean, has_assumable_loan boolean, owner_parcel_count int, review_reasons text[])""")
        cur.execute("""create table source_runs (id bigserial, source_name text, records_found int,
                       records_new int, notes text, run_at timestamptz default now())""")
    ex.ensure_schema(conn)
    si.ensure_columns(conn)
    gs.ensure_columns(conn)
    with conn.cursor() as cur:
        for col, typ in [("lead_status", "text"), ("lead_status_reason", "text"), ("lead_status_at", "timestamptz")]:
            cur.execute(f"alter table leads add column if not exists {col} {typ}")
    ex.ensure_schema(conn)  # rebuild the views with every column
    conn.commit()


def add(conn, key, **kw):
    lid = str(uuid.uuid4())
    base = {"id": lid, "address": f"{100 + len(KEYS)} Test St", "city": "Greenville", "state": "SC",
            "zip": "29601", "owner_name": "SMITH " + "".join(c for c in key.upper() if c.isalpha()),
            "mailing_address": "", "source_tags": ["tax_sale"]}
    base.update(kw)
    if base.get("skiptrace_status") == "traced" and "skiptrace_input" not in kw:
        # what the pull list sent BatchData for this lead
        first, last, a, _ = compute_skiptrace_identity(base, base["source_tags"])
        base["skiptrace_input"] = {"first": first, "last": last, "address": base["address"],
                                   "searched_address": a, "system_first": first, "system_last": last}
    cols = list(base)
    vals = [json.dumps(v) if isinstance(v, dict) else v for v in base.values()]
    with conn.cursor() as cur:
        cur.execute(f"insert into leads ({', '.join(cols)}) values ({', '.join(['%s'] * len(cols))})", vals)
    KEYS[key] = lid
    return lid


def upd(conn, key, **kw):
    with conn.cursor() as cur:
        sets = ", ".join(f"{k} = %s" for k in kw)
        vals = [json.dumps(v) if isinstance(v, dict) else v for v in kw.values()]
        cur.execute(f"update leads set {sets} where id = %s", vals + [KEYS[key]])


def err(conn, key, reason, at):
    with conn.cursor() as cur:
        cur.execute("insert into lead_error_log (lead_id, reason, found_at) values (%s, %s, %s)",
                    (KEYS[key], reason, at))


def night(conn, today, record=True, hour=12):
    files, _ = ex.build(conn, today)
    if record:
        ex.log_ghl_sent(conn, ex.GHL_KEYS, today, datetime(today.year, today.month, today.day, hour, tzinfo=UTC))
        conn.commit()
    f = {n: b for n, b, _ in files}
    daily = list(csv.DictReader(io.StringIO(f["ghl_upload/ghl_upload.csv"])))
    week = list(csv.DictReader(io.StringIO(f["ghl_upload/ghl_upload_last_7_days.csv"])))
    house = (list(csv.DictReader(io.StringIO(f["house_leads.csv"])))
             + list(csv.DictReader(io.StringIO(f["probate_pr_leads.csv"]))))
    nots = list(csv.DictReader(io.StringIO(f["not_skiptraced/not_skiptraced_leads.csv"])))
    return daily, week, house, nots, f


def by_addr(rows):
    return {r["Property Address"]: r for r in rows}


def addr(conn, key):
    with conn.cursor() as cur:
        cur.execute("select address from leads where id = %s", (KEYS[key],))
        return cur.fetchone()[0]


KEYS = {}
FAILS = []


def check(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAILS.append(msg)


def main():
    conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    setup(conn)
    traced_before = {"skiptrace_status": "traced", "skiptrace_date": BEFORE}
    # ---- legacy (hand-imported 10.1 / 10.7) leads
    add(conn, "legacy_ok", skiptrace_phones=["8640000001"],
        **traced_before)
    add(conn, "legacy_dead", skiptrace_phones=["8640000002"], lead_status="dead", lead_status_at=BEFORE,
        **traced_before)
    err(conn, "legacy_dead", "now DEAD (taxes paid) -- remove or close it in GHL", BEFORE)
    add(conn, "legacy_dead2", skiptrace_phones=["8640000003"], lead_status="dead", lead_status_at=BEFORE,
        **traced_before)
    add(conn, "legacy_retrace", owner_name="DOE JOHN", source_tags=["probate"],
        raw={"probate": {"pr_name": "DOE , JANE", "pr_address": "5 OTHER RD GREER SC 29650"}},
        skiptrace_phones=["8640000004"], skiptrace_input={"first": "John", "last": "Doe"}, **traced_before)
    err(conn, "legacy_retrace", "Personal Representative changed: blank -> DOE, JANE", BEFORE)
    conn.commit()

    print("N0 (10/16 first night, before any GHL export):")
    daily, week, house, nots, _ = night(conn, date(2026, 10, 16))
    check(len(daily) == 0, f"no GHL rows for leads handled on the 10.15 sheet (got {len(daily)})")
    check(addr(conn, "legacy_retrace") in by_addr(house), "the retrace lead is on the pull list to trace")
    hr = by_addr(house).get(addr(conn, "legacy_retrace"), {})
    check(hr.get("Phone 1", "") == "", "...with no phones (they're the wrong person's)")
    check((hr.get("Skiptrace First Name") or hr.get("Contact First Name")) == "Jane", "...and the new PR in the Skiptrace columns")

    # ---- GHL export #1: dead2 contact still there, retrace contact already deleted by hand
    contacts = [{"Contact Id": "c1", "First Name": "Legacy", "Last Name": "Ok", "Phone": "+18640000001",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c3", "First Name": "Dead", "Last Name": "Two", "Phone": "+18640000003",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c9", "First Name": "Old", "Last Name": "Owner", "Phone": "+18640000099",
                 "Email": "", "Tags": "list-tax-sale"}]
    with conn.cursor() as cur:
        cur.execute("update leads set ghl_phone = null")
    stats = gs.apply_sync(conn, gs.parse_contacts(contacts), D1 - timedelta(hours=6), D1 - timedelta(hours=5))
    conn.commit()
    check(stats["matched"] == 2 and stats["gone"] == 2 and stats["unmatched"] == 1,
          f"sync: 2 leads matched by phone, 2 already gone, 1 GHL list contact matches no lead ({stats})")
    # ---- new activity on 10/16
    add(conn, "new1", skiptrace_phones=["8640000010", "8640000011"], skiptrace_status="traced",
        skiptrace_date=D1)
    add(conn, "multiA", owner_name="BROWN AMY", skiptrace_phones=["8640000020"], skiptrace_status="traced",
        skiptrace_date=D1)
    add(conn, "multiB", owner_name="BROWN AMY", skiptrace_phones=["8640000020"], skiptrace_status="traced",
        skiptrace_date=D1)
    add(conn, "upd1", skiptrace_phones=["8640000031", "8640000030"], skiptrace_status="traced",
        skiptrace_date=BEFORE, ghl_phone="8640000030", ghl_in_crm=True, ghl_seen_at=D1 - timedelta(hours=6))
    err(conn, "upd1", "mailing address changed: 1 A St -> 9 B St; dropped off: Tax Sale", D1)
    add(conn, "pr1", owner_name="KING EDNA", source_tags=["probate"],
        raw={"probate": {"pr_name": "KING , RAY", "pr_address": "8 PINE RD GREER SC 29651"}},
        skiptrace_phones=["8640000040"], skiptrace_status="traced", skiptrace_date=BEFORE,
        skiptrace_input={"first": "Edna", "last": "King"}, ghl_phone="8640000040", ghl_in_crm=True,
        ghl_seen_at=D1 - timedelta(hours=6))
    err(conn, "pr1", "Personal Representative changed: blank -> KING, RAY", D1)
    add(conn, "sibDead", owner_name="GRAY TOM", skiptrace_phones=["8640000050"], skiptrace_status="traced",
        skiptrace_date=BEFORE, ghl_phone="8640000050", ghl_in_crm=True, lead_status="dead", lead_status_at=D1)
    add(conn, "sibLive", owner_name="GRAY TOM", skiptrace_phones=["8640000050"], skiptrace_status="traced",
        skiptrace_date=BEFORE, ghl_phone="8640000050", ghl_in_crm=True)
    conn.commit()

    print("N1 (10/16 after the first GHL export):")
    daily, week, house, nots, f = night(conn, date(2026, 10, 16))
    d = by_addr(daily)
    check(d.get(addr(conn, "legacy_dead2"), {}).get("Action") == "DELETE",
          "dead lead whose contact is still in GHL -> DELETE")
    check(addr(conn, "legacy_dead") not in d, "dead lead already deleted in GHL -> nothing")
    check(addr(conn, "legacy_retrace") not in d, "retrace lead whose old contact is gone -> nothing to tag")
    check(d.get(addr(conn, "new1"), {}).get("Action") == "NEW" and d[addr(conn, "new1")]["Phone 1"] == "8640000010",
          "newly traced lead -> NEW with its phones")
    multi = [r for r in daily if r["Phone 1"] == "8640000020"]
    check(len(multi) == 1 and multi[0]["Other Properties (same contact)"],
          "one owner, two properties, same phone -> ONE row with the other property listed")
    u = d.get(addr(conn, "upd1"), {})
    check(u.get("Action") == "UPDATE" and u.get("Phone 1") == "8640000030" and u.get("Phone 2") == "8640000031",
          "UPDATE matches on the phone GHL has (Phone 1), other numbers after it")
    check("dropped: list-tax-sale" in u.get("Tags", ""), "a dropped list comes through as a 'dropped:' tag")
    p = d.get(addr(conn, "pr1"), {})
    check(p.get("Action") == "RETRACE" and p.get("Color") == "YELLOW" and p.get("Phone 1") == "8640000040"
          and "retrace" in p.get("Tags", ""), "PR appointed on a lead in GHL -> RETRACE yellow, tags the old contact")
    check(p.get("Skiptrace First Name") == "Ray", "...with the new PR ready in the Skiptrace columns")
    check(addr(conn, "sibDead") not in d, "dead property of an owner who still has a live property -> no DELETE")
    first_daily = f["ghl_upload/ghl_upload.csv"]

    print("N1b (the on-demand export later the same day):")
    daily, week, house, nots, f = night(conn, date(2026, 10, 16), hour=18)
    check(f["ghl_upload/ghl_upload.csv"] == first_daily, "same-day re-export rebuilds the same daily file")

    print("N2 (10/17, nothing new):")
    daily, week, house, nots, f = night(conn, date(2026, 10, 17))
    check(len(daily) == 0, f"nothing is sent twice (got {len(daily)} rows)")
    check(len(week) >= 5, f"the last-7-days file still has the week's rows ({len(week)})")

    print("N3 (10/18: she deleted the retrace + dead contacts, exported GHL; callers tagged numbers):")
    contacts = [{"Contact Id": "c1", "First Name": "Legacy", "Last Name": "Ok", "Phone": "+18640000001",
                 "Email": "", "Tags": "list-tax-sale, wrong person"},
                {"Contact Id": "c10", "First Name": "New", "Last Name": "One", "Phone": "+18640000010",
                 "Email": "", "Tags": "list-tax-sale, wrong number"},
                {"Contact Id": "c20", "First Name": "Amy", "Last Name": "Brown", "Phone": "+18640000020",
                 "Email": "", "Tags": "list-tax-sale, wrong number"},
                {"Contact Id": "c30", "First Name": "Upd", "Last Name": "One", "Phone": "+18640000030",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c50", "First Name": "Tom", "Last Name": "Gray", "Phone": "+18640000050",
                 "Email": "", "Tags": "list-tax-sale"}]
    stats = gs.apply_sync(conn, gs.parse_contacts(contacts), D3 - timedelta(hours=6), D3 - timedelta(hours=5))
    conn.commit()
    check(stats["wrong_number"] == 3 and stats["wrong_person"] == 3, f"call feedback read ({stats})")
    # the retrace results for the new PRs come back
    with conn.cursor() as cur:
        si.mark_row(cur, {"Property Address": addr(conn, "pr1"), "Property Zip": "29601", "First Name": "Ray",
                          "Last Name": "King", "Phone": "8640000041"}, "traced", "test-file")
        si.mark_row(cur, {"Property Address": addr(conn, "legacy_retrace"), "Property Zip": "29601",
                          "First Name": "Jane", "Last Name": "Doe", "Phone": "8640000042"}, "traced", "test-file")
        cur.execute("update leads set skiptrace_date = %s, ghl_reset_at = %s where id in (%s, %s)",
                    (D3, D3, KEYS["pr1"], KEYS["legacy_retrace"]))
    conn.commit()
    daily, week, house, nots, f = night(conn, date(2026, 10, 18))
    d = by_addr(daily)
    n = d.get(addr(conn, "pr1"), {})
    check(n.get("Action") == "NEW" and n.get("Phone 1") == "8640000041" and "retrace" not in n.get("Tags", ""),
          "retraced lead -> NEW contact for the new person, no retrace tag")
    check(d.get(addr(conn, "legacy_retrace"), {}).get("Action") == "NEW", "legacy retrace lead -> NEW once traced")
    check(addr(conn, "new1") not in d,
          "wrong number, other numbers still good -> no row (GHL already has them; the tag is on the contact)")
    m = [r for r in daily if r["Phone 1"] == "8640000020"]
    check(len(m) == 1 and m[0]["Action"] == "RETRACE" and m[0]["Color"] == "ORANGE",
          "every number wrong (one owner, 2 properties) -> ONE orange RETRACE row, needs a different search")
    check(addr(conn, "multiA") not in by_addr(house) and addr(conn, "multiB") not in by_addr(house),
          "...and not back on the pull list for the same search")
    lo = d.get(addr(conn, "legacy_ok"), {})
    check(lo.get("Action") == "RETRACE" and lo.get("Color") == "ORANGE" and "Same person" in lo.get("Edit Before Trace", ""),
          "wrong person reported, same search -> ORANGE with the edit note")
    check(addr(conn, "legacy_ok") not in by_addr(house), "...and kept OFF the pull list (no paying twice)")
    check(addr(conn, "legacy_ok") in by_addr(nots), "...waiting in not_skiptraced with a note")
    tr = by_addr(list(csv.DictReader(io.StringIO(f["text_ready/text_ready.csv"]))))
    check("8640000010" not in json.dumps(tr), "the wrong number is never on the texting list")

    print("N4 (10/19, nothing new):")
    daily, *_ = night(conn, date(2026, 10, 19))
    check(len(daily) == 0, f"nothing re-sent (got {len(daily)})")

    print("N5 (10/20: a NEW lead dies before any export; ORANGE fixed and traced; an old error re-worded):")
    D5 = datetime(2026, 10, 20, 9, tzinfo=UTC)
    upd(conn, "pr1", lead_status="dead", lead_status_at=D5)
    with conn.cursor() as cur:
        # she changed the name on the orange row, traced it: a different person
        si.mark_row(cur, {"Property Address": addr(conn, "legacy_ok"), "Property Zip": "29601",
                          "First Name": "Bob", "Last Name": "Jones", "Phone": "8640000060"}, "traced", "t5")
        # the all-numbers-wrong owner: same name, second provider
        si.mark_row(cur, {"Property Address": addr(conn, "multiA"), "Property Zip": "29601",
                          "First Name": "Amy", "Last Name": "Brown", "Phone": "8640000070"}, "traced", "t5")
        cur.execute("update leads set skiptrace_date = %s, ghl_reset_at = %s where id in (%s, %s)",
                    (D5, D5, KEYS["legacy_ok"], KEYS["multiA"]))
    err(conn, "upd1", "owner name spelling changed", D1 - timedelta(hours=1))  # older than the 10/16 row
    conn.commit()
    daily, week, house, nots, f = night(conn, date(2026, 10, 20))
    d = by_addr(daily)
    x = d.get(addr(conn, "pr1"), {})
    check(x.get("Action") == "DELETE" and x.get("Phone 1") == "8640000041",
          "lead sent NEW, then died (no GHL export in between) -> DELETE on the NEW phone")
    x = d.get(addr(conn, "legacy_ok"), {})
    check(x.get("Action") == "NEW" and x.get("Phone 1") == "8640000060" and "retrace" not in x.get("Tags", ""),
          "orange fixed + traced -> NEW for the right person")
    x = [r for r in daily if r["Phone 1"] == "8640000070"]
    check(len(x) == 1 and x[0]["Action"] == "NEW", "all-numbers-wrong traced with a 2nd provider -> NEW")
    check(addr(conn, "multiB") in by_addr(nots), "...its sister property still waits for its own trace")
    check(addr(conn, "upd1") not in d, "an error older than the last row sent doesn't resend an UPDATE")

    print("N6 (10/21: GHL export -- she skipped importing the 10/20 NEW for Bob; the dead lead came back):")
    D6 = datetime(2026, 10, 21, 6, tzinfo=UTC)
    contacts = [{"Contact Id": "c10", "First Name": "New", "Last Name": "One", "Phone": "+18640000010",
                 "Email": "", "Tags": "list-tax-sale, wrong number"},
                {"Contact Id": "c30", "First Name": "Upd", "Last Name": "One", "Phone": "+18640000030",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c50", "First Name": "Tom", "Last Name": "Gray", "Phone": "+18640000050",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c70", "First Name": "Amy", "Last Name": "Brown", "Phone": "+18640000070",
                 "Email": "", "Tags": "list-tax-sale"},
                {"Contact Id": "c42", "First Name": "Jane", "Last Name": "Doe", "Phone": "+18640000042",
                 "Email": "", "Tags": "list-probate"}]
    gs.apply_sync(conn, gs.parse_contacts(contacts), D6, D6)
    upd(conn, "pr1", lead_status=None, lead_status_at=D6)
    err(conn, "upd1", "added to: Code Violation", D6 + timedelta(hours=1))
    conn.commit()
    daily, week, house, nots, f = night(conn, date(2026, 10, 21))
    d = by_addr(daily)
    x = d.get(addr(conn, "legacy_ok"), {})
    check(x.get("Action") == "NEW" and x.get("Phone 1") == "8640000060",
          "NEW that a later GHL export shows never got in -> sent again")
    x = d.get(addr(conn, "pr1"), {})
    check(x.get("Action") == "NEW" and x.get("Phone 1") == "8640000041",
          "deleted lead that came back alive -> NEW again")
    x = d.get(addr(conn, "upd1"), {})
    check(x.get("Action") == "UPDATE" and x.get("Phone 1") == "8640000030", "a genuinely new error -> UPDATE")
    check(addr(conn, "new1") not in d, "the same wrong-number tag read again -> nothing")
    check(set(by_addr(daily)) <= set(by_addr(week)), f"the 7-day file has everything still to do ({len(week)} rows)")

    print("N6b (10/21 later: she deleted New One's contact herself -- owner said stop):")
    contacts = [c for c in contacts if c["Contact Id"] != "c10"]
    gs.apply_sync(conn, gs.parse_contacts(contacts), D6 + timedelta(hours=8), D6 + timedelta(hours=8))
    upd(conn, "new1", bad_phones=["8640000010", "8640000099"])  # another number reported wrong
    conn.commit()
    daily, *_ = night(conn, date(2026, 10, 21), hour=20)
    check(addr(conn, "new1") not in by_addr(daily), "a contact she deleted herself is never re-created")

    print("N6c (the same caller tags read again in the next export):")
    c2 = contacts + [{"Contact Id": "c60", "First Name": "Bob", "Last Name": "Jones", "Phone": "+18640000060",
                      "Email": "", "Tags": "list-tax-sale, wrong person"}]
    st1 = gs.apply_sync(conn, gs.parse_contacts(c2), D6 + timedelta(hours=9), D6 + timedelta(hours=9))
    st2 = gs.apply_sync(conn, gs.parse_contacts(c2), D6 + timedelta(hours=10), D6 + timedelta(hours=10))
    conn.commit()
    check(st1["wrong_person"] == 1 and st2["wrong_person"] == 0 and st2["wrong_number"] == 0,
          f"a caller tag is acted on once per contact ({st1['wrong_person']}, {st2['wrong_person']})")
    upd(conn, "legacy_ok", retrace_reason=None)  # (undo, so the next nights stay simple)
    upd(conn, "legacy_ok", ghl_feedback=["c60|person"])
    conn.commit()

    print("N7 (10/22: the revived lead dies again, no export since):")
    upd(conn, "pr1", lead_status="dead", lead_status_at=datetime(2026, 10, 22, 3, tzinfo=UTC))
    conn.commit()
    daily, *_ = night(conn, date(2026, 10, 22))
    d = by_addr(daily)
    check(d.get(addr(conn, "pr1"), {}).get("Action") == "DELETE", "died again -> DELETE again")
    check(len(daily) == 1, f"and nothing else ({len(daily)} rows)")

    print("N8 (10/23: PR changes between the pull and the trace upload; new PR has no address):")
    add(conn, "gap1", owner_name="LEE ROSE", source_tags=["probate"],
        raw={"probate": {"pr_name": "LEE , RAY", "pr_address": "9 ELM ST GREER SC 29651"}})
    conn.commit()
    night(conn, date(2026, 10, 23))  # on the pull list with Ray -> offer recorded
    upd(conn, "gap1", raw={"probate": {"pr_name": "LEE , SUE"}})
    with conn.cursor() as cur:
        si.mark_row(cur, {"Property Address": addr(conn, "gap1"), "Property Zip": "29601", "First Name": "Ray",
                          "Last Name": "Lee", "Phone": "8640000080"}, "traced", "t8")
    conn.commit()
    daily, week, house, nots, f = night(conn, date(2026, 10, 24))
    g = by_addr(house).get(addr(conn, "gap1"), {})
    check(bool(g), "traced the old PR, a new PR was appointed before the upload -> back on the pull list as a retrace")
    check("no address" in g.get("Edit Before Trace", ""), "...ORANGE note: the new PR has no address on file")
    conn.close()
    print("\nALL PASSED" if not FAILS else f"\n{len(FAILS)} FAILED")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
