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
import re
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from lead_common import SUFFIXES, ensure_schema, rescore_all  # noqa: E402


def ensure_columns(conn):
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists is_duplicate boolean not null default false")
        cur.execute("alter table leads add column if not exists merged_into uuid references leads(id)")


def find_pairs(conn):
    """
    Same zip, neither side already flagged, one address = other address +
    ' ' + one more word. Returns the canonical (longer/suffixed) row and
    the redundant (shorter) row for each pair.

    PERFORMANCE NOTE (2026-09-22): the first version of this query joined on
    `a.address ilike b.address || ' %'` directly -- an unanchored ILIKE
    pattern that can't use an index and forces a full nested-loop scan
    across every same-zip pair (effectively O(n^2) string comparisons
    across ~98k rows). That hit Supabase's statement timeout in production
    (confirmed both here and independently via the SQL editor on the same
    query shape). Fixed by normalizing each address down to a "core" (strip
    the trailing street-type word) in a CTE first, then joining on an exact
    match of (zip, core) -- a plain equality join Postgres can hash, which
    is what a manual re-check in the SQL editor confirmed runs instantly
    (571/571 duplicate pairs found) versus the old query's timeout.
    """
    # FIX 2026-09-25: the old version required BOTH rows to have the same
    # zip. Tax-sale rows had no zip at all, and absentee rows had the OWNER'S
    # MAILING zip, so real duplicates (e.g. "1124 Wembley" / "1124 Wembley
    # Rd") never matched. Now: same address core, zips don't conflict,
    # parcel numbers don't conflict, and the core appears exactly twice
    # (a core shared by 3+ rows could be the same street number in
    # different towns -- skipped rather than guessed).
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            with normalized as (
                select id, address, zip, pin, address_core(address) as core
                from leads
                where is_sold = false and is_duplicate = false
            ),
            unambiguous as (
                select core from normalized where core is not null
                group by core having count(*) = 2
            )
            select
                case when length(a.address) >= length(b.address) then a.id else b.id end as canonical_id,
                case when length(a.address) >= length(b.address) then a.address else b.address end as canonical_address,
                case when length(a.address) >= length(b.address) then b.id else a.id end as redundant_id,
                case when length(a.address) >= length(b.address) then b.address else a.address end as redundant_address
            from normalized a
            join normalized b on a.id < b.id and a.core = b.core and a.address <> b.address
            join unambiguous u on u.core = a.core
            where (a.zip is null or b.zip is null or a.zip = b.zip)
              and (a.pin is null or b.pin is null or a.pin = b.pin)
            """
        )
        return cur.fetchall()


def find_pin_pairs(conn):
    """
    ADDED 2026-09-30: two rows with the SAME parcel number whose addresses
    differ only by a direction/street type the core match can't see --
    "218 S Moore Rd" / "218 Moore", "209 W Park Ave" / "209 Park" (76 such
    parcels live on 2026-09-30). Different house numbers on one parcel
    ("192 Lightning Ln" / "186 Lightening Ln") are NOT merged -- see
    same_street().
    Canonical = the row updated most recently (the address the source uses
    today). Only parcels with exactly two active rows; 3+ is left alone.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            with a as (
                select id, address, pin, updated_at, list_count,
                       count(*) over (partition by pin) as n,
                       row_number() over (partition by pin order by updated_at desc nulls last,
                                          length(address) desc, id) as rk
                from leads
                where is_sold = false and is_duplicate = false and pin is not null and pin <> ''
            )
            select c.id as canonical_id, c.address as canonical_address,
                   r.id as redundant_id, r.address as redundant_address
            from a c join a r on r.pin = c.pin and c.rk = 1 and r.rk = 2
            where c.n = 2 and (c.list_count > 0 or r.list_count > 0)
            """
        )
        return [p for p in cur.fetchall() if same_street(p["canonical_address"], p["redundant_address"])]


_DIRS = {"n", "s", "e", "w", "north", "south", "east", "west"}
_SUFFIX_WORDS = set(SUFFIXES.split("|"))


def same_street(a, b):
    """Same house number (00003 == 3) and the shorter address's street words
    are all in the longer one, ignoring N/S/E/W and Rd/St/Ave...
    '209 Park' ~ '209 W Park Ave' yes; '9 Alex' ~ '517 Hampton Townes' no;
    '192 Lightning Ln' ~ '186 Lightening Ln' no (left for a human)."""
    def parts(x, drop_dirs):
        toks = re.findall(r"[a-z0-9]+", (x or "").lower())
        if not toks or not toks[0].isdigit():
            return None, set()
        drop = _SUFFIX_WORDS | (_DIRS if drop_dirs else set())
        return int(toks[0]), {t for t in toks[1:] if t not in drop}
    for drop_dirs in (True, False):   # "1820 North" -- the street IS a direction word
        na, ta = parts(a, drop_dirs)
        nb, tb = parts(b, drop_dirs)
        if na is None or na != nb:
            return False
        if ta and tb:
            return ta <= tb or tb <= ta
    return False


def merge_pair(conn, canonical_id, redundant_id):
    with conn.cursor() as cur:
        # Fold the redundant row's data into the canonical row first...
        cur.execute(
            """
            update leads c set
                source_tags = array(select distinct unnest(c.source_tags || r.source_tags)),
                raw = coalesce(c.raw, '{}'::jsonb) || coalesce(r.raw, '{}'::jsonb),
                owner_name = coalesce(c.owner_name, r.owner_name),
                mailing_address = coalesce(c.mailing_address, r.mailing_address),
                phone = coalesce(c.phone, r.phone),
                email = coalesce(c.email, r.email),
                is_absentee = coalesce(c.is_absentee, r.is_absentee),
                is_vacant = c.is_vacant or r.is_vacant,
                is_tired_landlord = coalesce(c.is_tired_landlord, r.is_tired_landlord),
                is_long_term_owner = coalesce(c.is_long_term_owner, r.is_long_term_owner),
                pin = coalesce(c.pin, r.pin),
                zip = coalesce(c.zip, r.zip),
                city = coalesce(c.city, r.city),
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


# rescore_all now lives in lead_common.py (one shared formula for every script).


def refold_duplicates(conn):
    """
    FIX 2026-09-25: every ingest upserts on lower(address), so a source that
    uses the SHORT form of an address keeps writing new tags onto the row
    that was already flagged is_duplicate -- and those tags never reached
    the canonical row. Fold them over every run.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update leads c set
                source_tags = array(select distinct unnest(c.source_tags || d.source_tags)),
                raw = coalesce(c.raw, '{}'::jsonb) || coalesce(d.raw, '{}'::jsonb),
                updated_at = now()
            from leads d
            where d.is_duplicate and d.merged_into = c.id
              and not (d.source_tags <@ c.source_tags)
            """
        )
        return cur.rowcount

def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    ensure_columns(conn)
    ensure_schema(conn)
    conn.commit()

    refolded = refold_duplicates(conn)
    print(f"Folded late-arriving tags from duplicate rows into {refolded} canonical lead(s).")
    conn.commit()

    pairs = find_pairs(conn)
    seen = {p["redundant_id"] for p in pairs} | {p["canonical_id"] for p in pairs}
    pairs += [p for p in find_pin_pairs(conn) if p["redundant_id"] not in seen and p["canonical_id"] not in seen]
    print(f"[{datetime.now(timezone.utc).isoformat()}] Found {len(pairs)} duplicate-address pair(s) to merge.")

    merged_ids = []
    for i, p in enumerate(pairs, start=1):
        print(f"  merging {p['redundant_address']!r} (id={p['redundant_id']}) "
              f"into {p['canonical_address']!r} (id={p['canonical_id']})")
        merge_pair(conn, p["canonical_id"], p["redundant_id"])
        merged_ids.append(str(p["redundant_id"]))
        # Commit in batches rather than after every pair -- cuts round trips
        # to the DB roughly 50x. Still safe to interrupt: an uncommitted
        # pair simply gets picked up again by find_pairs() next run (this
        # script is idempotent), never left half-merged.
        if i % 50 == 0:
            conn.commit()
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
