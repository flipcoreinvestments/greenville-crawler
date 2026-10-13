#!/usr/bin/env python3
"""
HEALTH GATE -- the last step of every nightly run (added 2026-10-10).

Why: from Oct 2 to Oct 10, 2026 the absentee-owner step crashed every night
and the probate crawl was stuck on empty pages, and nobody found out for
8 days. This step makes the run FAIL (red X + GitHub's failure email/push)
whenever something is broken or quietly stuck, with a plain-English reason
at the top of the log.

Checks (each one is a real failure seen before, or a source going silent):
  1. Every nightly source logged a finished run in the last 30 hours.
  2. The weekly foreclosure source logged a run in the last 8 days.
  3. Probate is not stuck: the "found cases but saved no estates" pattern
     (empty county pages counted as cases) three nights running.
  4. The call-list export files were written in the last 30 hours.
Read-only: it never changes a lead.
"""
import os
import re
import sys

import psycopg2

NIGHTLY_SOURCES = {
    "tax_sale": "Tax sale",
    "redemption_period": "Repeat tax delinquent",
    "building_permits": "City permits",
    "absentee_owner": "Owner details (absentee, mailing address, sold)",
    "probate": "Probate",
    "code_violation": "Code violations",
    "qa_check": "Nightly QA check (dead / wrong information)",
    "skiptrace_intake": "Skip-trace results import",
    "skiptrace_followup": "Skip-trace follow-up (fixed / needs human / errors)",
    "dnc_scrub": "Do Not Call scrub (texting list)",
}
WEEKLY_SOURCES = {"foreclosure_mie": "Foreclosure auctions"}


def check(conn):
    problems = []
    with conn.cursor() as cur:
        for src, label in NIGHTLY_SOURCES.items():
            cur.execute(
                "select max(run_at) from source_runs where source_name = %s "
                "and run_at > now() - interval '30 hours' and coalesce(notes, '') not like 'blocked%%'",
                (src,))
            if cur.fetchone()[0] is None:
                problems.append(f"{label} ({src}) has not finished a run in the last 30 hours.")
        for src, label in WEEKLY_SOURCES.items():
            cur.execute("select max(run_at) from source_runs where source_name = %s "
                        "and run_at > now() - interval '8 days'", (src,))
            if cur.fetchone()[0] is None:
                problems.append(f"{label} ({src}) has not run in the last 8 days.")

        # Probate stuck pattern: many "found" cases, zero estates saved, 3 runs in a row
        cur.execute("select notes from source_runs where source_name = 'probate' "
                    "and notes like 'ok:%%' order by run_at desc limit 3")
        notes = [r[0] for r in cur.fetchall()]
        def stuck(n):
            f = re.search(r"found_cases:(\d+)", n or "")
            e = re.search(r"estate_leads:(\d+)", n or "")
            return bool(f and e and int(f.group(1)) >= 100 and int(e.group(1)) == 0)
        if len(notes) == 3 and all(stuck(n) for n in notes):
            problems.append("Probate looks stuck: 3 runs in a row found 100+ cases but saved no estates.")

        cur.execute("select max(updated_at) from storage.objects where bucket_id = 'lead-exports' "
                    "and name = 'house_leads.csv'")
        row = cur.fetchone()
        if not row or row[0] is None:
            problems.append("The call list house_leads.csv was not found in lead-exports.")
        else:
            cur.execute("select %s < now() - interval '30 hours'", (row[0],))
            if cur.fetchone()[0]:
                problems.append("The call lists were not rewritten in the last 30 hours.")
    return problems


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    try:
        problems = check(conn)
    finally:
        conn.close()
    if problems:
        print("SYSTEM HEALTH: PROBLEMS FOUND")
        for p in problems:
            print(f"::error::{p}")
            print(f"  - {p}")
        sys.exit(1)
    print("SYSTEM HEALTH: all sources ran and the call lists are fresh.")


if __name__ == "__main__":
    main()
