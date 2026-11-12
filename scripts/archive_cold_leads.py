#!/usr/bin/env python3
"""
Move "cold" rows out of the working leads table into leads_archive.
Added 2026-11-12 (Tala's call after the November crawler failures).

WHY
---
On Nov 12 the leads table held 107,515 rows, but only 3,265 were on any
distress list. About 94,000 of the rest are leftovers from before the
2026-09-25 rewrite of absentee_owner_ingest.py (it used to create a row for
every out-of-town owner in the county, see lead_common.py). They are not
leads, nothing reads them, but every nightly step that scans the table had
to wade through them, and several steps ran past the database's time limit.

WHAT COUNTS AS COLD (all must be true)
--------------------------------------
- on no distress list (list_count 0, no source tags)
- never skip traced, no phones, no lead status (dead / wrong info stay put),
  no Morbius story
- not a duplicate / merge record, and no other lead points at it
- never sent to GHL, not in Morbius's contacts or skip-trace queue, not in
  the error log, not matched to a bankruptcy filing
- nothing in raw except county owner facts (absentee_owner / geocode)
- first added more than 45 days ago

Nothing is deleted for good: rows move to leads_archive with archived_at.
If a cold property lands on a list again, the list ingest simply creates a
fresh lead and the nightly owner-facts step fills it in, same as any new
lead. lead_common.restore_archived_lead() moves one back when needed
(the bankruptcy match uses it).

Safe to run every night: moves rows in batches of 5,000 and only touches
rows that meet every rule above.
"""

import os
import sys

import psycopg2

import lead_common  # noqa: F401  database safety: login retry, 20-min timeout, no-op schema checks never lock
from lead_common import ensure_archive_table

BATCH = 5000
MIN_AGE_DAYS = 45

COLD_SQL = f"""
    coalesce(l.list_count, 0) = 0
    and coalesce(cardinality(l.source_tags), 0) = 0
    and l.skiptrace_status is null
    and l.skiptrace_phones is null
    and l.lead_status is null
    and l.story is null
    and coalesce(l.is_duplicate, false) = false
    and l.merged_into is null
    and l.created_at < now() - interval '{MIN_AGE_DAYS} days'
    and coalesce(l.raw, '{{}}'::jsonb) - 'absentee_owner' - 'geocode' = '{{}}'::jsonb
    and not exists (select 1 from leads m where m.merged_into = l.id)
    and not exists (select 1 from morbius_contacts c where c.lead_id = l.id)
    and not exists (select 1 from morbius_skiptrace_queue q where q.lead_id = l.id::text)
    and not exists (select 1 from ghl_sent g where g.lead_id = l.id)
    and not exists (select 1 from lead_error_log e where e.lead_id = l.id)
    and not exists (select 1 from bankruptcy_filings b where b.matched_lead_id = l.id::text)
"""


def archive(conn):
    cols = ensure_archive_table(conn)
    col_list = ", ".join(f'"{c}"' for c in cols)
    moved_total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                with pick as (
                    select l.id from leads l where {COLD_SQL} limit {BATCH}
                ), moved as (
                    delete from leads l using pick where l.id = pick.id returning l.*
                )
                insert into leads_archive ({col_list}, archived_at)
                select {col_list}, now() from moved
                """
            )
            n = cur.rowcount
        conn.commit()
        moved_total += n
        if n:
            print(f"  moved {n} cold row(s) to leads_archive ({moved_total} so far)", flush=True)
        if n < BATCH:
            break
    return moved_total


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    moved = archive(conn)
    with conn.cursor() as cur:
        cur.execute("select count(*) from leads")
        left = cur.fetchone()[0]
        cur.execute("select count(*) from leads_archive")
        archived = cur.fetchone()[0]
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) "
                    "values ('archive_cold_leads', %s, %s, %s)",
                    (left, moved, f"leads left: {left}; archive holds: {archived}"))
    conn.commit()
    conn.close()
    print(f"Archive: moved {moved} cold row(s) tonight. Working leads table: {left} rows; "
          f"archive: {archived} rows.")


if __name__ == "__main__":
    main()
