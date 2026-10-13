#!/usr/bin/env python3
"""
SKIP-TRACE FOLLOW-UP (added 2026-10-11, T Dawg's ask). Runs every night
after the QA check (qa_check.py) and before the call lists are written.

1. SECOND QA ON NOT-TRACED LEADS. For every lead whose trace came back empty,
   compare what was searched (skiptrace_input, recorded by
   skiptrace_intake.py) with what the system would search TODAY:
     - fixed        something that was missing or wrong is now right (owner
                    first name added, name cleaned up, Personal
                    Representative found, mailing address used for a lot
                    with no house number, zip added...). Goes to
                    lead-exports/fixed_unskiptraced/ to be traced again.
     - needs_human  nothing left the system can fix. Stays in
                    lead-exports/not_skiptraced/ with a "#" in front of the
                    owner name and a plain note on what a person has to check.
2. ERRORS ON PULLED LEADS. Every lead already pulled (traced or not) is
   compared with how it looked when it was last checked. A sale, a dropped
   or new list, a changed owner / mailing address / city / zip, a new
   Personal Representative, or a dead / wrong-information mark is logged in
   lead_error_log and listed in lead-exports/errors/errors.csv so the GHL
   contact can be updated. The run prints a GitHub warning with the COUNT
   only (no names -- the repo is public); the daily watchdog reads it and
   notifies her.
3. REPEAT OFFENDERS. Each night a traced lead is still a solid lead (on a
   distress list, not dead/wrong), the month is recorded. Still showing up
   in a later month than it was traced = repeat offender: "!" in front of
   the owner name and a copy in lead-exports/repeat_offenders/.
"""
import json
import os
import re
import sys

import psycopg2
import psycopg2.extras

from lead_common import ensure_schema
from skiptrace_prep import compute_skiptrace_identity, owner_trace_names, is_untraceable_address
from skiptrace_intake import ensure_columns

MAIL_TAIL = re.compile(r",?\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$", re.I)


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def current_trace(r):
    tags = list(r.get("source_tags") or [])
    first, last, addr, note = compute_skiptrace_identity(r, tags)
    m = MAIL_TAIL.search(addr or "")
    z = m.group(2) if m else ""
    if addr == r["address"]:
        z = r.get("zip") or ""
    return {"first": first, "last": last, "address": addr, "zip": z, "note": note}


def diagnose(r):
    """('fixed' | 'needs_human', plain-English note)."""
    inp = r.get("skiptrace_input") or {}
    cur = current_trace(r)
    tags = set(r.get("source_tags") or [])
    fixes = []
    if inp:
        if cur["first"] and not inp.get("first"):
            fixes.append(f"owner's first name added ({cur['first']} {cur['last']})")
        elif (cur["first"] or cur["last"]) and (inp.get("first") or inp.get("last")) and \
                _norm(cur["first"] + cur["last"]) != _norm((inp.get("first") or "") + (inp.get("last") or "")):
            who = "Personal Representative" if "probate" in tags else "name"
            fixes.append(f"{who} corrected to {cur['first']} {cur['last']}".strip())
        if cur["address"] and _norm(cur["address"])[:12] != _norm(inp.get("address"))[:12]:
            fixes.append(f"now searching {cur['address']}")
        if cur["zip"] and not inp.get("zip"):
            fixes.append(f"zip added ({cur['zip']})")
    if fixes and not cur["note"]:
        return "fixed", "; ".join(fixes)

    # nothing to fix automatically -- say what a person has to check
    n = owner_trace_names(r.get("owner_name"))
    if cur["note"]:
        why = cur["note"]
    elif n["kind"] == "Company":
        why = ("company-owned: look the company up on the SC Secretary of State site "
               "(businessfilings.sc.gov), then trace the owner / registered agent by name")
    elif n["kind"] == "Trust" and not n["first"]:
        why = "family trust with no person's name on the deed: find the trustee (deed or probate file) and trace them"
    elif is_untraceable_address(r.get("address")) and not r.get("mailing_address"):
        why = "no house number and no mailing address: nothing a skip-trace can search"
    elif not r.get("zip"):
        why = "property zip still missing: confirm the address on the county site"
    elif (r.get("skiptrace_attempts") or 0) >= 2:
        why = "traced twice with a clean name and address and still nothing: try a second skip-trace provider or a manual people search"
    else:
        why = "clean name and address, trace still came back empty: try a second skip-trace provider or a manual people search"
    return "needs_human", why


def second_qa(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""select l.* from leads l where l.skiptrace_status = 'failed'
                       and coalesce(l.lead_status, '') not in ('dead')""")
        rows = cur.fetchall()
    counts = {"fixed": 0, "needs_human": 0}
    with conn.cursor() as cur:
        for r in rows:
            status, note = diagnose(r)
            counts[status] += 1
            cur.execute("""update leads set skiptrace_fix_status = %s, skiptrace_fix_note = %s
                           where id = %s and (skiptrace_fix_status is distinct from %s
                                              or skiptrace_fix_note is distinct from %s)""",
                        (status, note, r["id"], status, note))
    conn.commit()
    return counts


SNAP_SQL = """jsonb_build_object(
    'owner', coalesce(l.owner_name, ''), 'mailing', coalesce(l.mailing_address, ''),
    'city', coalesce(l.city, ''), 'zip', coalesce(l.zip, ''),
    'pr', coalesce(l.raw->'probate'->>'pr_name', ''),
    'lists', coalesce((select jsonb_agg(t order by t) from unnest(l.source_tags) t
                       where t not in ('absentee_owner')), '[]'::jsonb),
    'sold', l.is_sold, 'status', coalesce(l.lead_status, ''))"""

LIST_NAMES = {
    "tax_sale": "Tax Sale", "repeat_tax_delinquent": "Tax Sale 2+ Years", "foreclosure_mie": "Foreclosure Auction",
    "hoa_foreclosure": "HOA Foreclosure", "pre_foreclosure": "Pre-Foreclosure", "probate": "Probate",
    "pre_probate": "Pre-Probate", "permit_expired": "Expired Permit", "permit_demolition": "Demolition Permit",
    "insurance_damage": "Damage Permit", "code_violation": "Code Violation", "involuntary_lien": "Involuntary Lien",
    "failed_listing": "Failed Listing",
}


def describe_changes(old, new, reason):
    out = []
    if new.get("status") == "dead" and old.get("status") != "dead":
        out.append(f"now DEAD ({reason or 'see dead list'}) -- remove or close it in GHL")
    elif new.get("status") == "wrong_info" and old.get("status") != "wrong_info":
        out.append(f"now WRONG INFORMATION ({reason or 'see wrong information list'})")
    if new.get("sold") and not old.get("sold") and not out:
        out.append("sold (new deed recorded)")
    for key, label in [("owner", "owner"), ("mailing", "mailing address"), ("city", "city"),
                       ("zip", "zip"), ("pr", "Personal Representative")]:
        # 2026-10-13: only a real change counts -- the county re-sending the
        # same name in different capitals / commas ("MAHAFFEY , WINFRED J" ->
        # "Mahaffey Winfred J") is not an error on the lead.
        if _norm(old.get(key)) != _norm(new.get(key)) and (new.get(key) or ""):
            was = old.get(key) or "blank"
            out.append(f"{label} changed: {was} -> {new.get(key)}")
    o, n = set(old.get("lists") or []), set(new.get("lists") or [])
    if n - o:
        out.append("now also on: " + ", ".join(LIST_NAMES.get(x, x) for x in sorted(n - o)))
    if o - n and new.get("status") != "dead":
        out.append("dropped off: " + ", ".join(LIST_NAMES.get(x, x) for x in sorted(o - n)))
    return "; ".join(out)


_CHANGE = re.compile(r"^\s*(owner|mailing address|city|zip|Personal Representative) changed: (.*) -> (.*)$")


def dismiss_false_alarms(conn):
    """2026-10-13: errors logged before the capitals/commas fix whose only
    'change' was formatting are marked dismissed (kept, never deleted), so
    they don't tag a lead "update lead info"."""
    with conn.cursor() as cur:
        cur.execute("alter table lead_error_log add column if not exists dismissed boolean not null default false")
        cur.execute("select id, reason from lead_error_log where not dismissed")
        ids = []
        for eid, reason in cur.fetchall():
            parts = [p for p in (reason or "").split("; ") if p.strip()]
            ms = [_CHANGE.match(p) for p in parts]
            if parts and all(m and _norm(m.group(2)) == _norm(m.group(3)) for m in ms):
                ids.append(eid)
        if ids:
            cur.execute("update lead_error_log set dismissed = true where id = any(%s)", (ids,))
    conn.commit()
    return len(ids)


def detect_errors(conn):
    with conn.cursor() as cur:
        # first time a pulled lead is seen: remember how it looks, no error
        cur.execute(f"update leads l set skiptrace_snapshot = {SNAP_SQL} "
                    f"where l.skiptrace_status is not null and l.skiptrace_snapshot is null")
        cur.execute(f"""select l.id, l.skiptrace_snapshot, {SNAP_SQL}, l.lead_status_reason
                        from leads l where l.skiptrace_status is not null
                          and l.skiptrace_snapshot is distinct from {SNAP_SQL}""")
        rows = cur.fetchall()
        new_errors = 0
        for lead_id, old, new, reason in rows:
            what = describe_changes(old or {}, new or {}, reason)
            if what:
                cur.execute("insert into lead_error_log (lead_id, reason) values (%s, %s)", (lead_id, what))
                new_errors += 1
            cur.execute("update leads set skiptrace_snapshot = %s::jsonb where id = %s", (json.dumps(new), lead_id))
    conn.commit()
    return new_errors


def track_months(conn):
    with conn.cursor() as cur:
        cur.execute("""update leads l set active_months = array_append(coalesce(l.active_months, '{}'),
                                                                       to_char(now(), 'YYYY-MM'))
                       from active_leads a
                       where a.id = l.id and l.skiptrace_status = 'traced' and l.lead_status is null
                         and not (to_char(now(), 'YYYY-MM') = any(coalesce(l.active_months, '{}')))""")
        cur.execute("""select count(*) from leads where skiptrace_status = 'traced' and lead_status is null
                         and exists (select 1 from unnest(coalesce(active_months, '{}')) m
                                     where m > to_char(skiptrace_date, 'YYYY-MM'))""")
        repeat = cur.fetchone()[0]
    conn.commit()
    return repeat


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("set statement_timeout = '10min'")
    ensure_columns(conn)
    ensure_schema(conn)
    conn.commit()
    qa = second_qa(conn)
    dismissed = dismiss_false_alarms(conn)
    if dismissed:
        print(f"Dismissed {dismissed} old error(s) that were only capital-letter/comma differences.")
    errors = detect_errors(conn)
    repeat = track_months(conn)
    with conn.cursor() as cur:
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) "
                    "values ('skiptrace_followup', %s, %s, %s)",
                    (qa["fixed"] + qa["needs_human"], errors,
                     f"ok: fixed:{qa['fixed']} needs_human:{qa['needs_human']} new_errors:{errors} "
                     f"repeat_offenders:{repeat}"))
    conn.commit()
    conn.close()
    print(f"Not-traced leads: {qa['fixed']} fixed and ready to trace again, "
          f"{qa['needs_human']} need a person to check (#).")
    print(f"Repeat offenders: {repeat}.")
    print(f"New errors on already-pulled leads: {errors}.")
    if errors:
        # count only -- the repo and its logs are public
        print(f"::warning title=LEAD_ERRORS::{errors} already-pulled lead(s) changed -- "
              f"see lead-exports/errors/errors.csv and update them in GHL")


if __name__ == "__main__":
    main()
