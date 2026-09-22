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
    with conn.cursor() as cur:
        cur.execute(
            "alter table leads add column if not exists last_alerted_list_count int not null default 0"
        )


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
    ensure_columns(conn)
    conn.commit()

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
