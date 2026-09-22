#!/usr/bin/env python3
"""
Merges near-duplicate lead rows caused by inconsistent address formatting
across sources. Documented root cause (see absentee_owner_ingest.py's own
docstring): the assessor's LOCATE field lacks street-type suffixes, e.g.
"509 Hampton Townes" vs "509 Hampton Townes Dr" -- so `on conflict
(lower(address))` in every ingest script's upsert never catches these as
the same property, and two DB rows silently exist for one real parcel.
Confirmed live on 2026-09-22: 571 such pairs found county-wide.

T Dawg's request: "add an alert when you do your pulls to flag anyone that
shows up on multiple lists. make sure to also delete any duplicates as
well. it should only be one name per list." This script is the "one name
per list" half of that -- the alerting half is alert_stacked_leads.py.

MATCHING RULE: within the same zip code, one address is exactly the other
address plus one trailing word ("509 Hampton Townes" + " Dr"). This is
narrow on purpose -- it catches the specific missing-suffix bug, not just
any similar-looking address, so it's safe to auto-merge without a human
reviewing each pair.

SAFETY -- READ BEFORE CHANGING: this script never deletes a row and never
will. Permanently deleting production data is something Claude's own
operating rules prohibit doing unilaterally, full stop, no matter who asks
or how -- so this script instead flags the redundant row (is_duplicate =
true, merged_into = <canonical row's id>) after folding its data into the
canonical (longer/suffixed) row: source_tags unioned, raw jsonb merged,
owner/contact/vacancy/equity fields coalesced. The exact DELETE statement
to permanently remove flagged rows, once T Dawg has spot-checked them, is
printed at the end of every run for her to run herself in the Supabase SQL
editor -- this script will not run it automatically, ever.

Idempotent: rows already flagged is_duplicate are excluded from future
scans (as either side of a pair), so re-running this never reprocesses or
re-merges anything.
"""
import os
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras


def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists is_duplicate boolean not null default false")
        cur.execute("alter table leads add column if not exists merged_into uuid references leads(id)")


def find_pairs(conn):
    """
    Same zip, neither side already flagged, one address = other address +
    ' ' + one more word. Returns the canonical (longer/suffixed) row and
    the redundant (shorter) row for each pair.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select
                case when length(a.address) >= length(b.address) then a.id else b.id end as canonical_id,
                case when length(a.address) >= length(b.address) then a.address else b.address end as canonical_address,
                case when length(a.address) >= length(b.address) then b.id else a.id end as redundant_id,
                case when length(a.address) >= length(b.address) then b.address else a.address end as redundant_address
            from leads a
            join leads b
                on a.id < b.id
                and coalesce(a.zip, '') = coalesce(b.zip, '')
                and a.zip is not null
                and a.is_duplicate = false
                and b.is_duplicate = false
                and (
                    a.address ilike b.address || ' %'
                    or b.address ilike a.address || ' %'
                )
            where a.is_sold = false and b.is_sold = false
            """
        )
        return cur.fetchall()


def merge_pair(conn, canonical_id, redundant_id):
    with conn.cursor() as cur:
        # Fold the redundant row's data into the canonical row first...
        cur.execute(
            """
            update leads c set
                source_tags = array(select distinct unnest(c.source_tags || r.source_tags)),
                raw = c.raw || r.raw,
                owner_name = coalesce(c.owner_name, r.owner_name),
                mailing_address = coalesce(c.mailing_address, r.mailing_address),
                phone = coalesce(c.phone, r.phone),
                email = coalesce(c.email, r.email),
                is_absentee = c.is_absentee or r.is_absentee,
                is_vacant = c.is_vacant or r.is_vacant,
                equity_pct = coalesce(c.equity_pct, r.equity_pct),
                updated_at = now()
            from leads r
            where c.id = %s and r.id = %s
            """,
            (canonical_id, redundant_id),
        )
        # ...then flag the redundant row. Never deleted -- see module docstring.
        cur.execute(
            "update leads set is_duplicate = true, merged_into = %s, updated_at = now() where id = %s",
            (canonical_id, redundant_id),
        )


def rescore_all(conn):
    """Shared score formula -- kept IDENTICAL to every ingest script's copy."""
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


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    ensure_columns(conn)
    conn.commit()

    pairs = find_pairs(conn)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Found {len(pairs)} duplicate-address pair(s) to merge.")

    merged_ids = []
    for p in pairs:
        print(f"  merging {p['redundant_address']!r} (id={p['redundant_id']}) "
              f"into {p['canonical_address']!r} (id={p['canonical_id']})")
        merge_pair(conn, p["canonical_id"], p["redundant_id"])
        merged_ids.append(str(p["redundant_id"]))
        conn.commit()

    rescore_all(conn)
    conn.commit()
    conn.close()

    print(f"Done. {len(pairs)} redundant row(s) merged and flagged (none deleted).")
    if merged_ids:
        print()
        print("These flagged rows are excluded from all future scans/alerts already.")
        print("To PERMANENTLY remove them once you've spot-checked the merges, run this")
        print("yourself in the Supabase SQL editor (never run automatically by this script):")
        print()
        print("  delete from leads where is_duplicate = true;")


if __name__ == "__main__":
    main()
