#!/usr/bin/env python3
"""
NIGHTLY QA CHECK (added 2026-10-11, T Dawg's ask).

Every night, after all the sources have run and before the call lists are
written, re-check:
  - the 1,000 most recently added leads,
  - every lead that has been skip-traced (those are already in GHL), and
  - every lead already marked DEAD or WRONG INFORMATION (so a mark clears
    itself when the problem goes away -- e.g. a house that comes back onto
    the tax sale list).

Each one is marked:
  dead        sold (new deed recorded), merged as a duplicate, on no distress
              list anymore, or not a residential property
  wrong_info  something on the lead is wrong or missing and can't be fixed
              automatically: no owner name, two owners' names run together,
              no city/zip, no usable address to skip trace, mailing address
              that isn't an address, a Personal Representative name that
              still has page text in it, or a probate address that may not be
              the estate's
  (blank)     checked and fine

export_call_lists.py then writes the marked leads to their own files --
lead-exports/dead/dead_leads.csv and
lead-exports/wrong_information/wrong_information_leads.csv -- and keeps
them OUT of house_leads.csv, land_leads.csv and probate_pr_leads.csv, so a
dead or wrong lead never ends up in a skip-trace upload.

Nothing is ever deleted: a dead lead keeps its skip-trace history, so if
the same house comes back on a list it is NOT traced (or paid for) twice.
"""
import os
import sys

import psycopg2

from lead_common import ensure_schema, IS_RESIDENTIAL_SQL

RECENT_LEADS = 1000

# a capital letter glued to the end of a lowercase word ("Frisbee Betty DFrisbee
# Robert F") or to a closing parenthesis ("(Jtwros)Lake") = two county owner
# lines run together with no separator.
MASHED_NAME_SQL = r"(l.owner_name ~ '[a-z][A-Z][a-z]' or l.owner_name ~ '\)[A-Za-z]')"


def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists lead_status text")
        cur.execute("alter table leads add column if not exists lead_status_reason text")
        cur.execute("alter table leads add column if not exists lead_status_at timestamptz")
        cur.execute("alter table leads add column if not exists skiptrace_status text")
        cur.execute("create index if not exists idx_leads_lead_status on leads(lead_status) "
                    "where lead_status is not null")


def run_qa(conn):
    res_sql = IS_RESIDENTIAL_SQL.replace("land_use", "l.land_use")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            with scope as (
                select id from (select id from leads order by created_at desc nulls last limit {RECENT_LEADS}) r
                union select id from leads where skiptrace_status is not null
                union select id from leads where lead_status is not null
            ),
            checked as (
                select l.id,
                    array_remove(array[
                        case when l.is_sold then 'sold (new deed recorded'
                             || coalesce(' ' || to_char(l.sold_at, 'MM/DD/YYYY'), '') || ')' end,
                        case when l.is_duplicate then 'duplicate of another lead' end,
                        case when coalesce(l.list_count, 0) = 0 and not l.is_sold and not l.is_duplicate
                             then 'no longer on any distress list (taxes paid, case closed or list expired)' end,
                        case when not {res_sql} then 'not a residential property' end
                    ], null) as dead_why,
                    array_remove(array[
                        case when coalesce(trim(l.owner_name), '') = '' then 'no owner name' end,
                        case when {MASHED_NAME_SQL} then 'owner name garbled (two names run together)' end,
                        case when coalesce(l.city, '') = '' or coalesce(l.zip, '') = ''
                             then 'property city/zip missing' end,
                        case when l.address !~ '^[0-9]' and coalesce(l.mailing_address, '') = ''
                             then 'no house number and no mailing address -- nothing to skip trace' end,
                        case when 'invalid_mailing_address' = any(coalesce(l.review_reasons, '{{}}'))
                             then 'mailing address is not an address' end,
                        case when 'corrupted_address' = any(coalesce(l.review_reasons, '{{}}'))
                             then 'property address is garbled' end,
                        case when 'probate_owner_mismatch' = any(coalesce(l.review_reasons, '{{}}'))
                             then 'probate address may not be the estate''s' end,
                        case when coalesce(l.raw->'probate'->>'pr_name', '') ~ '(Party Type|Name:)'
                             then 'Personal Representative name has page text in it' end
                    ], null) as wrong_why
                from leads l join scope s on s.id = l.id
            ),
            decided as (
                select id,
                    case when cardinality(dead_why) > 0 then 'dead'
                         when cardinality(wrong_why) > 0 then 'wrong_info' end as status,
                    case when cardinality(dead_why) > 0 then array_to_string(dead_why, '; ')
                         when cardinality(wrong_why) > 0 then array_to_string(wrong_why, '; ') end as why
                from checked
            )
            update leads l set
                lead_status = d.status,
                lead_status_reason = d.why,
                lead_status_at = case when l.lead_status is distinct from d.status then now() else l.lead_status_at end
            from decided d
            where d.id = l.id
              and (l.lead_status is distinct from d.status or l.lead_status_reason is distinct from d.why)
            """
        )
        changed = cur.rowcount
        cur.execute("select coalesce(lead_status, 'ok'), count(*) from leads "
                    "where lead_status is not null group by 1 order by 1")
        totals = dict(cur.fetchall())
    return changed, totals


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
    changed, totals = run_qa(conn)
    with conn.cursor() as cur:
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) "
                    "values ('qa_check', %s, %s, %s)",
                    (sum(totals.values()), changed,
                     f"ok: dead:{totals.get('dead', 0)} wrong_info:{totals.get('wrong_info', 0)} changed:{changed}"))
    conn.commit()
    conn.close()
    print(f"QA check: {changed} lead(s) changed status. Dead: {totals.get('dead', 0)}, "
          f"wrong information: {totals.get('wrong_info', 0)}")


if __name__ == "__main__":
    main()
