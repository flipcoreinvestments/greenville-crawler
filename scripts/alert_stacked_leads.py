#!/usr/bin/env python3
"""
Alerts, straight into the GitHub Actions log, whenever a lead newly crosses
onto a 2nd (or further) source list -- the same property/owner now shows
up on multiple of T Dawg's 17/19 distress-signal lists at once, which is
the single highest-priority pattern in this whole pipeline (stacking =
compounding motivation to sell).

T Dawg's request: "add an alert when you do your pulls to flag anyone that
shows up on multiple lists." This is that alert. The "delete duplicates,
one name per list" half of the same request is
merge_duplicate_addresses.py, which runs immediately before this step in
nightly.yml so a lead's list_count is already deduped/correct by the time
this script evaluates it.

FIRES ONCE PER INCREASE, not every night: a new `last_alerted_list_count`
column remembers the list_count this lead was last alerted at. A lead
that's been sitting on 3 lists for months doesn't re-alert every single
night forever -- only a genuine NEW list stacking onto it since the last
alert does. First run ever will alert on every existing 2+-list lead (that
is the correct, expected one-time catch-up).
"""
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

ALERT_THRESHOLD = 2  # "shows up on multiple lists" = 2 or more


def ensure_columns(conn):
    """
    Adds last_alerted_list_count if missing. Returns True if the column was
    just created for the first time (a "bootstrap" run), False if it
    already existed.

    On a genuine first run, every existing 2+-list lead would otherwise
    look "newly stacked" (since the new column defaults to 0 for every
    row), flooding the log with an alert for the entire historical
    backlog -- tens of thousands of leads -- instead of just what's
    actually new. main() uses the return value to seed the column to each
    lead's current list_count silently on that first run, so alerts start
    firing only for genuine increases from that point forward.
    """
    with conn.cursor() as cur:
        cur.execute(
            "select 1 from information_schema.columns "
            "where table_name = 'leads' and column_name = 'last_alerted_list_count'"
        )
        already_existed = cur.fetchone() is not None
        cur.execute(
            "alter table leads add column if not exists last_alerted_list_count int not null default 0"
        )
    return not already_existed


def bootstrap_baseline(conn):
    """First-run only: silently set last_alerted_list_count = list_count for
    every existing lead, so this run alerts on nothing and future runs only
    alert on genuine new stacking from today's baseline forward."""
    with conn.cursor() as cur:
        cur.execute("update leads set last_alerted_list_count = list_count")
        print(f"  bootstrap: seeded last_alerted_list_count for {cur.rowcount} existing lead(s) "
              f"(no alerts fired for this historical backlog).")


def find_newly_stacked(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select id, address, city, zip, list_count, source_tags, score, owner_name
            from leads
            where is_sold = false
              and is_duplicate = false
              and list_count >= %s
              and list_count > last_alerted_list_count
            order by list_count desc, score desc
            """,
            (ALERT_THRESHOLD,),
        )
        return cur.fetchall()


def mark_alerted(conn, leads):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            "update leads set last_alerted_list_count = %s where id = %s",
            [(lead["list_count"], lead["id"]) for lead in leads],
        )


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    is_first_run = ensure_columns(conn)
    conn.commit()

    if is_first_run:
        print(f"[{datetime.now(timezone.utc).isoformat()}] First run -- bootstrapping baseline "
              f"instead of alerting on the entire historical backlog.")
        bootstrap_baseline(conn)
        conn.commit()
        print("Done. Baseline set; future runs will alert only on genuinely new stacking.")
        conn.close()
        return

    leads = find_newly_stacked(conn)
    print(f"[{datetime.now(timezone.utc).isoformat()}] {len(leads)} lead(s) newly stacked on "
          f"{ALERT_THRESHOLD}+ lists since the last alert run.")

    for lead in leads:
        print("=" * 60)
        location = f"{lead['address']}, {lead['city'] or ''} {lead['zip'] or ''}".strip()
        print(f"ALERT: {location}")
        print(f"  owner: {lead['owner_name'] or 'unknown'}")
        print(f"  now on {lead['list_count']} list(s): {', '.join(lead['source_tags'] or [])}")
        print(f"  score: {lead['score']}")

    if leads:
        mark_alerted(conn, leads)
        conn.commit()
    conn.close()
    print(f"Done. {len(leads)} alert(s) logged and marked so they won't repeat tomorrow.")


if __name__ == "__main__":
    main()
