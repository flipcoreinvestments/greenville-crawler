#!/usr/bin/env python3
"""
Shared pieces every ingest script uses, so they can never drift apart again.

WHY THIS FILE EXISTS (2026-09-25 audit, T Dawg's call)
------------------------------------------------------
The audit found the lead table was being inflated by OWNER-PROFILE tags that
are not distress lists at all:
  - high_equity     91,266 leads  (a proxy that couldn't see mortgages)
  - absentee_owner  38,886 leads  (mailing address != property address)
  - tired_landlord  16,552 leads  (owner holds 3+ parcels)
list_count is a generated column = number of source_tags, so each of those
tags counted as a "list" worth +25. Result: 37,650 leads scored 50+ with no
distress at all, and ~95k of 98k "leads" were only on the list because of
them. T Dawg: "there's no reason that tag should be on 95k leads, that is
completely contradictory to this list stack."

NEW RULE: source_tags holds DISTRESS LISTS ONLY (DISTRESS_TAGS below).
Owner-profile facts live in their own columns (is_absentee,
is_tired_landlord, is_long_term_owner) and add a small score bonus, but
never create a lead and never count as a list. A row with zero distress
tags is not a lead (see the active_leads view).

Also here: the ONE shared score formula, the mailing-address sanity check,
the address "core" used for matching, and tag expiry helpers.
"""

import re

# Every tag that means "this property is on a real distress list".
DISTRESS_TAGS = [
    "tax_sale",
    "repeat_tax_delinquent",  # replaced redemption_period 2026-09-25 (redemption can't be proven)
    "foreclosure_mie",
    "hoa_foreclosure",
    "permit_expired",
    "permit_demolition",
    "insurance_damage",
    "code_violation",
    "probate",
    "out_of_state_land",
    # PropStream exports (propstream_import.py, added 2026-09-27)
    "pre_probate",
    "pre_foreclosure",
    "involuntary_lien",
    "failed_listing",
]

# Retired as tags. The first three move to boolean columns; high_equity is
# dropped entirely (no public source can show a mortgage payoff).
RETIRED_TAGS = ["high_equity", "absentee_owner", "tired_landlord", "long_term_owner"]

# Same street-type list merge_duplicate_addresses.py has always used.
# "av" added 2026-09-25: the city permit feed writes "805 CRESCENT AV".
SUFFIXES = ("dr|drive|st|street|av|ave|avenue|rd|road|ln|lane|ct|court|cir|circle|way|blvd|"
            "boulevard|pl|place|trl|trail|pkwy|parkway|hwy|highway|ter|terrace|cv|cove")
_SUFFIX_RE = re.compile(r"\s+(" + SUFFIXES + r")\.?$")


def address_core(addr):
    """Python twin of the SQL address_core() function created below."""
    if not addr:
        return None
    a = re.sub(r"\s+", " ", addr.strip().lower())
    a = _SUFFIX_RE.sub("", a)
    return re.sub(r"\s+", " ", a).strip() or None


# Business owners (added 2026-09-26). T Dawg checked #1 on the top-10 list,
# 1820 E North St, at the Register of Deeds: owner Made New Renovations LLC
# has 61 recorded documents, is still buying in 2026, and pays its private
# mortgages off in months -- an active flipper carrying back taxes on a
# project, not a motivated homeowner. 6 of that top 10 were business-owned.
# "TRUST" and "ESTATE" are deliberately NOT here: family trusts and estates
# are usually individual/heir situations.
ENTITY_WORDS = (
    r"LLC|L\.?L\.?C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|LP|LLP|LTD|PLLC|PC|"
    r"HOMES|PROPERTIES|PROPERTY|INVEST[A-Z]*|HOLDINGS?|REALTY|BUILDERS?|"
    # prefix forms: the county cuts owner names off at ~30 characters
    # ("Upstate Movement And Develop" -- 345 Ligon St, 2026-09-26)
    r"CONSTRUCT[A-Z]*|RENOVAT[A-Z]*|DEVELOP[A-Z]*|PARTNERS|PARTNERSHIP|GROUP|VENTURES|"
    r"ENTERPRIS[A-Z]*|CAPITAL|MANAGEMENT|CHURCH|MINISTRIES|BANK|ASSOCIATION|ASSOC|HOA|"
    r"AUTHORITY|COUNTY|CITY OF|STATE OF|UNITED STATES|SCHOOL|UNIVERSITY"
)
ENTITY_SQL = r"\m(" + ENTITY_WORDS.replace("\\m", "") + r")\M"
_ENTITY_RE = re.compile(r"\b(" + ENTITY_WORDS + r")\b", re.I)


# Residential-only (T Dawg, 2026-09-26: "remove all commercial leads" --
# 10 S Academy St, #3 on the list, is an office building for lease). Codes and
# labels are the county's own, verified in backfill_land_use.py:
#   1100 Single Family, 1101 SF w/ auxiliary use, 110 Duplex, 112 Mplex,
#   1170 MH w/ land, 1171 MH on MH file, 1180 Residential Vacant,
#   9170 Ag Vacant, 9171 Ag Improved
# Everything else with a known code (office, retail, warehouse, assisted
# living, 6800 Commercial Vacant, HOA common areas, codes with no county
# label) is NOT residential. No code on file yet = unknown, flagged '*'.
RESIDENTIAL_LAND_USE = ("1100", "1101", "110", "112", "1170", "1171", "1180", "9170", "9171")
LAND_CODE_SQL = "nullif(substring(coalesce(land_use, '') from '^\\s*(\\d+)'), '')"
IS_RESIDENTIAL_SQL = f"({LAND_CODE_SQL} is null or {LAND_CODE_SQL} in ({', '.join(repr(c) for c in RESIDENTIAL_LAND_USE)}))"


def land_code(land_use):
    m = re.match(r"\s*(\d+)", land_use or "")
    return m.group(1) if m else None


def is_residential(land_use):
    """True/False, or None when no land use is on file yet."""
    code = land_code(land_use)
    return None if code is None else code in RESIDENTIAL_LAND_USE


def owner_is_entity(owner_name):
    return bool(owner_name and _ENTITY_RE.search(owner_name))


def mailing_is_address(mailing):
    """
    The county's mailing-address field sometimes holds a person's NAME
    ("SMITH JOHN R") instead of an address. A real mailing street starts with
    a house/box number or is a PO Box. Anything else can't be compared to the
    property address, so absentee must be "unknown" (None), never True.
    """
    if not mailing:
        return False
    s = mailing.strip().upper()
    return bool(re.match(r"^\d", s) or re.match(r"^P\.?\s*O\.?\s*BOX\b", s))


def ensure_schema(conn):
    """Idempotent. Safe to call at the start of every script."""
    with conn.cursor() as cur:
        cur.execute("alter table leads add column if not exists is_tired_landlord boolean")
        cur.execute("alter table leads add column if not exists is_long_term_owner boolean")
        cur.execute("alter table leads add column if not exists owner_parcel_count integer")
        cur.execute("alter table leads add column if not exists pin text")
        cur.execute("create index if not exists idx_leads_pin on leads(pin)")
        body = (
            "select nullif(trim(regexp_replace(regexp_replace(lower(trim(coalesce(a, ''))), "
            "'\\s+(" + SUFFIXES + ")\\.?$', '', 'g'), '\\s+', ' ', 'g')), '')"
        )
        cur.execute("select prosrc from pg_proc where proname = 'address_core'")
        existing = cur.fetchone()
        if not existing or existing[0].strip() != body:
            cur.execute(f"create or replace function address_core(a text) returns text "
                        f"language sql immutable as $fn$ {body} $fn$")
            # the index stores computed values -- rebuild it whenever the
            # function's definition changes, or lookups silently go stale
            cur.execute("drop index if exists idx_leads_address_core")
        cur.execute("create index if not exists idx_leads_address_core on leads(address_core(address))")
        cur.execute(
            """
            create table if not exists pipeline_migrations (
                name text primary key,
                applied_at timestamptz not null default now()
            )
            """
        )
        # Owner type lives in the views, not a stored column: adding a stored
        # generated column rewrites every row once, and the project is on the
        # free plan (500 MB). Person-owned leads are the main list;
        # business-owned leads get their own view.
        # A person-named owner who mails to the SAME address as a business
        # owner is part of that business (345 Ligon St and Made New
        # Renovations LLC both mail to 701 Easley Bridge Rd).
        mail_key = "lower(regexp_replace(coalesce({t}.mailing_address, ''), '[^a-zA-Z0-9]', '', 'g'))"
        entity = (
            f"(coalesce(l.owner_name, '') ~* '{ENTITY_SQL}' or exists ("
            f"select 1 from leads b where b.mailing_address is not null and b.id <> l.id "
            f"and {mail_key.format(t='b')} = {mail_key.format(t='l')} "
            f"and coalesce(b.owner_name, '') ~* '{ENTITY_SQL}'))"
        )
        cur.execute("drop view if exists active_leads")
        cur.execute("drop view if exists business_owned_leads")
        cur.execute(
            f"""
            create view active_leads as
            select l.*, false as owner_is_entity from leads l
            where l.is_sold = false and l.is_duplicate = false and l.list_count > 0
              and not {entity} and {IS_RESIDENTIAL_SQL.replace('land_use', 'l.land_use')}
            """
        )
        cur.execute(
            f"""
            create view business_owned_leads as
            select l.*, true as owner_is_entity from leads l
            where l.is_sold = false and l.is_duplicate = false and l.list_count > 0
              and {entity} and {IS_RESIDENTIAL_SQL.replace('land_use', 'l.land_use')}
            """
        )


def _batched_update(cur, set_sql, where_sql, batch_size, label):
    """UPDATE in chunks with a VACUUM between, so the free-plan disk never
    has to hold two full copies of the table at once."""
    total = 0
    while True:
        cur.execute(
            f"update leads set {set_sql} where id in "
            f"(select id from leads where {where_sql} limit %s)",
            (batch_size,),
        )
        if cur.rowcount == 0:
            break
        total += cur.rowcount
        cur.execute("vacuum leads")
    print(f"  migration: {label}: {total} rows")
    return total


def _migration_done(cur, name):
    cur.execute("select 1 from pipeline_migrations where name = %s", (name,))
    return cur.fetchone() is not None


def run_migrations(conn, batch_size=5000):
    """
    One-time data fixes from the 2026-09-25 audit. Each is recorded in
    pipeline_migrations so it only ever runs once.

    Runs in batches with a VACUUM after each one: the project is on
    Supabase's free plan (500 MB, 352 MB used at audit time), and rewriting
    ~95k rows in one go would briefly double the table on disk.
    """
    old_autocommit = conn.autocommit
    conn.commit()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            if not _migration_done(cur, "2026_09_25_retire_profile_tags"):
                # 1. copy tag facts into their new columns (small, one pass)
                cur.execute("update leads set is_tired_landlord = true "
                            "where 'tired_landlord' = any(source_tags) and is_tired_landlord is not true")
                cur.execute("update leads set is_long_term_owner = true "
                            "where 'long_term_owner' = any(source_tags) and is_long_term_owner is not true")
                cur.execute("update leads set source_tags = array_replace(source_tags, 'hoa_coa_foreclosure', 'hoa_foreclosure') "
                            "where 'hoa_coa_foreclosure' = any(source_tags)")
                # 2. strip retired tags, batch by batch
                retired_sql = "array[" + ",".join("'%s'" % t for t in RETIRED_TAGS) + "]::text[]"
                total = _batched_update(
                    cur,
                    f"source_tags = array(select distinct t from unnest(source_tags) t "
                    f"where t <> all({retired_sql})), equity_pct = null",
                    f"source_tags && {retired_sql}",
                    batch_size, "retired tags stripped")
                cur.execute("update leads set equity_pct = null where equity_pct is not null")
                # 3. stacking alerts compare against the last alerted list_count,
                #    which was inflated by the retired tags -- reset it down.
                cur.execute("update leads set last_alerted_list_count = list_count "
                            "where last_alerted_list_count > list_count")
                cur.execute("insert into pipeline_migrations (name) values ('2026_09_25_retire_profile_tags')")

            if not _migration_done(cur, "2026_09_25_fix_zip_city"):
                # The absentee script wrote the OWNER'S MAILING zip into `zip`
                # (e.g. "300 South" had 33154 = Miami) and building permits
                # wrote OWNER_ZIP the same way. Only probate and the MIE
                # foreclosure list supply a real property zip/city. Every
                # other script hard-coded city = 'Greenville' for the whole
                # county (Greer, Simpsonville, Taylors...). Blank beats wrong.
                _batched_update(
                    cur, "zip = null, city = case when city = 'Greenville' then null else city end",
                    "(zip is not null or city = 'Greenville') "
                    "and not (coalesce(raw, '{}'::jsonb) ? 'probate' or coalesce(raw, '{}'::jsonb) ? 'foreclosure_mie')",
                    batch_size, "cleared mailing zip / hard-coded city")
                cur.execute("insert into pipeline_migrations (name) values ('2026_09_25_fix_zip_city')")

            if not _migration_done(cur, "2026_09_25_backfill_pin"):
                _batched_update(
                    cur,
                    # digits only: the code-violation source writes
                    # "0230-00.05.059-00", the assessor "0230000505900"
                    "pin = nullif(regexp_replace(coalesce(raw->'tax_sale'->>'map_number', "
                    "raw->'redemption_period'->>'map_number', raw->'code_violation'->>'map_number', "
                    "case when jsonb_typeof(raw->'absentee_owner'->'pin') = 'string' "
                    "then raw->'absentee_owner'->>'pin' end), '\\D', '', 'g'), '')",
                    "pin is null and (raw->'tax_sale'->>'map_number' is not null "
                    "or raw->'redemption_period'->>'map_number' is not null "
                    "or raw->'code_violation'->>'map_number' is not null "
                    "or jsonb_typeof(raw->'absentee_owner'->'pin') = 'string')",
                    batch_size, "parcel number filled")
                cur.execute("insert into pipeline_migrations (name) values ('2026_09_25_backfill_pin')")
    finally:
        conn.autocommit = old_autocommit


def rescore_all(conn):
    """
    THE score formula. Every script imports this one -- never copy it again.
      +25 per DISTRESS list the property is on (list_count; tags are
          distress-only now, see module docstring)
      +15 absentee owner (column, not a list)
      +15 tired landlord, owner holds 3+ parcels (column, not a list)
      +up to 25 scaled from tax-sale amount owed -- only while the property
          is actually still on the tax sale list
      +30 foreclosure auction scheduled
      +20 stalled/expired building permit
      +20 demolition permit
      +20 on the tax sale list two years running (repeat_tax_delinquent)
      +30 storm/fire/water damage repair permit
      +20 HOA/COA is the foreclosing plaintiff
      +25 code violation (condemned / unfit structure)
      +30 pre-foreclosure (PropStream: default recorded, last 6 months)
      +20 deceased owner (probate court estate OR PropStream pre-probate)
      +20 USPS vacant (PropStream) -- only counts on a lead already on a list
      +10 involuntary lien (HOA / mechanic's / utility / child support)
    Leads with no distress list score 0, and so do COMMERCIAL properties
    (known land use outside RESIDENTIAL_LAND_USE). high_equity is gone.
    Only rows whose score actually changes are written (the old formula
    rewrote all ~98k rows eight times a night, bloating the free-plan DB).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            with s as (
                select id,
                    case when list_count = 0 or not {IS_RESIDENTIAL_SQL} then 0 else
                        (list_count * 25)
                        + (case when is_absentee then 15 else 0 end)
                        + (case when is_tired_landlord then 15 else 0 end)
                        + (case when 'tax_sale' = any(source_tags)
                                then least(coalesce(case when raw->'tax_sale'->>'amount_due' ~ '^[0-9]+(\\.[0-9]+)?$'
                                                         then (raw->'tax_sale'->>'amount_due')::numeric end, 0) / 50, 25)
                                else 0 end)
                        + (case when 'foreclosure_mie' = any(source_tags) then 30 else 0 end)
                        + (case when 'permit_expired' = any(source_tags) then 20 else 0 end)
                        + (case when 'permit_demolition' = any(source_tags) then 20 else 0 end)
                        + (case when 'repeat_tax_delinquent' = any(source_tags) then 20 else 0 end)
                        + (case when 'insurance_damage' = any(source_tags) then 30 else 0 end)
                        + (case when 'hoa_foreclosure' = any(source_tags) then 20 else 0 end)
                        + (case when 'code_violation' = any(source_tags) then 25 else 0 end)
                        + (case when 'pre_foreclosure' = any(source_tags) then 30 else 0 end)
                        + (case when source_tags && array['probate', 'pre_probate'] then 20 else 0 end)
                        + (case when is_vacant is true then 20 else 0 end)
                        + (case when 'involuntary_lien' = any(source_tags) then 10 else 0 end)
                    end as new_score
                from leads
                where is_sold = false
            )
            update leads l set score = s.new_score
            from s
            where l.id = s.id and l.score is distinct from s.new_score
            """
        )


def remove_tag(conn, tag, where_sql, params=None):
    """Remove `tag` from every lead matching where_sql. Returns rows changed."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update leads set source_tags = array_remove(source_tags, %(tag)s), updated_at = now()
            where %(tag)s = any(source_tags) and ({where_sql})
            """,
            {"tag": tag, **(params or {})},
        )
        return cur.rowcount


def _currently_tagged(conn, tag):
    with conn.cursor() as cur:
        cur.execute("select count(*) from leads where %s = any(source_tags) and is_sold = false "
                    "and is_duplicate = false", (tag,))
        return cur.fetchone()[0]


def _shrink_guard(conn, tag, n_current, min_ratio):
    """True if it's safe to expire. A source list that suddenly shrinks to
    under min_ratio of what's tagged today looks like a broken page/parse,
    not a real mass payoff -- skip expiry and say so loudly."""
    tagged = _currently_tagged(conn, tag)
    if tagged >= 20 and n_current < tagged * min_ratio:
        print(f"  WARNING: expiry skipped for {tag}: source list has {n_current} entries but "
              f"{tagged} leads are tagged (< {int(min_ratio * 100)}%). Check the source page.")
        return False
    return True


def expire_by_raw_key(conn, tag, raw_key, field, current_values, min_ratio=0.5):
    """
    Drop `tag` from leads whose raw->raw_key->>field (e.g. the tax map
    number or court case number) is NOT on the list the source published
    today. Caller must only call this after a COMPLETE, successful fetch of
    the source list -- never after a partial/failed one.
    """
    current = sorted({str(v) for v in current_values if v})
    if not current:
        print(f"  expiry skipped for {tag}: current list is empty (treating as a failed fetch)")
        return 0
    if not _shrink_guard(conn, tag, len(current), min_ratio):
        return 0
    n = remove_tag(
        conn, tag,
        "coalesce(raw->%(rk)s->>%(f)s, '') <> all(%(cur)s::text[])",
        {"rk": raw_key, "f": field, "cur": current},
    )
    print(f"  expired {tag} from {n} lead(s) no longer on the source list")
    return n


def expire_by_address(conn, tag, current_addresses, min_ratio=0.5):
    """Same as expire_by_raw_key but matched on the address core (for
    sources with no parcel/case number stored per tag, e.g. permits)."""
    cores = sorted({c for c in (address_core(a) for a in current_addresses) if c})
    if not cores:
        print(f"  expiry skipped for {tag}: current list is empty (treating as a failed fetch)")
        return 0
    if not _shrink_guard(conn, tag, len(cores), min_ratio):
        return 0
    n = remove_tag(conn, tag, "coalesce(address_core(address), '') <> all(%(cores)s::text[])",
                   {"cores": cores})
    print(f"  expired {tag} from {n} lead(s) no longer on the source list")
    return n
