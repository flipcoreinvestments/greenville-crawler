#!/usr/bin/env python3
"""
Full-coverage land_use backfill — Greenville County leads.

WHY THIS EXISTS
----------------
tax_sale_ingest.py and redemption_period_ingest.py already capture the
county's own human-readable "Land Use" field (scraped straight off the
Real Property Search detail page) for the leads they touch. That's fully
authoritative -- but it only covered ~2,400 of ~98,000 leads (the ones
that happened to also appear on a tax-sale/redemption list).

absentee_owner_ingest.py's nightly full-county ArcGIS sweep touches
essentially every parcel in the county (~96,000 of ~98,000 leads as of
2026-09-22) and already captures a raw NUMERIC land-use code from that
feed into raw->'absentee_owner'->'landuse' -- it just never decodes it or
promotes it to the top-level `land_use` column. That raw code was sitting
there unused.

There is no authoritative published Greenville County / SC DOR table
mapping these numeric codes to descriptions (confirmed via web research
2026-09-22: checked the county's own Real Property Services page and its
"Advanced Internet Mapping System" PDF, plus SC DOR docs -- none publish
the full code list). So codes are NOT guessed. Each code in VERIFIED_CODES
below was confirmed by fetching a real sample parcel's authoritative
Real Property Search "Land Use" field for that exact code
(https://www.greenvillecounty.org/appsas400/RealProperty/Details.aspx)
on 2026-09-22 and copying the county's own text verbatim. This covers the
21 highest-volume codes, together ~92,250 of ~96,246 raw-coded leads
(~96%). See CODE_VERIFICATION_LOG at the bottom of this file for exactly
which PIN backed each code.

WHAT THIS SCRIPT DOES
----------------------
For every lead where land_use IS NULL (never overwrites an existing,
already-verified value from tax_sale/redemption_period):

  1. Raw code is in VERIFIED_CODES  -> land_use = "CODE (Description)",
     using the county's own confirmed text.
  2. Raw code exists but is NOT in VERIFIED_CODES (long-tail code, or the
     literal code "0" -- confirmed 2026-09-22 to NOT reliably mean any one
     thing: a sampled "0"-coded parcel turned out to actually be
     "1100 (Single Family)" on the live county record, so "0" looks like
     a stale/placeholder value in the ArcGIS feed rather than a real
     category, and is deliberately NOT decoded to avoid guessing)
     -> land_use = "UNVERIFIED (code {code})". This keeps the property
     visible to any `land_use is not null` filter while being honest that
     it isn't decoded yet.
  3. No raw code at all (lead came only from a source that doesn't touch
     the ArcGIS sweep -- foreclosure_mie, probate, code_enforcement,
     building_permits, hoa_foreclosure -- and was never matched/merged
     into an absentee_owner row either)
     -> land_use = "UNKNOWN (no source data)".

Net effect: after this runs, NO lead has land_use IS NULL. Nothing
disappears from a property-type filter for lack of a value, and nothing
is ever labeled with a fabricated description.
"""

import os
import sys

import psycopg2
from psycopg2.extras import execute_values

# Verified 2026-09-22 against the county's own Real Property Search detail
# page (https://www.greenvillecounty.org/appsas400/RealProperty/Details.aspx),
# one sample PIN per code. Text is copied verbatim from that page (including
# the county's own spelling/typos, e.g. "Multi-purpse") so it matches the
# format tax_sale_ingest.py / redemption_period_ingest.py already write.
VERIFIED_CODES = {
    "110": "110 (Duplex)",
    "122": "122 (Apartment Subsidized (E))",
    "130": "130 (Mobile Home park)",
    "409": "409 (Office-dental)",
    "421": "421 (Office-general)",
    "431": "431 (Branch)",
    "520": "520 (General)",
    "521": "521 (Strip Center)",
    "610": "610 (Fast food)",
    "620": "620 (Full Service)",
    "810": "810 (Religious/Church)",
    "821": "821 (Government-post office)",
    "850": "850 (Schools)",
    "940": "940 (Warehouse General)",
    "960": "960 (Multi-purpse)",
    "1100": "1100 (Single Family)",
    "1170": "1170 (MH w/ land)",
    "1171": "1171 (MH on MH file)",
    "1180": "1180 (Residential Vacant)",
    "1181": "1181 (Homeowners assoc. prop)",
    "1182": "1182 (Common Areas)",
    "6800": "6800 (Commercial Vacant)",
    "9170": "9170 (Ag Vacant)",
}

# "0" is deliberately excluded from VERIFIED_CODES -- see module docstring.
# It falls through to the UNVERIFIED bucket like any other unmapped code.


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("alter table leads add column if not exists land_use text")

        # 1. Verified codes -> decoded description. One UPDATE per code
        #    (23 total) keeps each statement simple and cheap to reason
        #    about, and lets us log a per-code row count.
        verified_total = 0
        for code, description in VERIFIED_CODES.items():
            cur.execute(
                """
                update leads
                   set land_use = %s
                 where land_use is null
                   and raw->'absentee_owner'->>'landuse' = %s
                """,
                (description, code),
            )
            if cur.rowcount:
                print(f"  verified  {code:>6}  ->  {description:<40}  {cur.rowcount:>6} rows")
            verified_total += cur.rowcount

        # 2. Any other raw code present but not in VERIFIED_CODES (long-tail
        #    codes, plus the literal "0") -> honest "not decoded yet" label,
        #    never a guessed description.
        cur.execute(
            """
            update leads
               set land_use = 'UNVERIFIED (code ' || (raw->'absentee_owner'->>'landuse') || ')'
             where land_use is null
               and raw->'absentee_owner'->>'landuse' is not null
            """
        )
        unverified_total = cur.rowcount

        # 3. No raw code at all (source never ran this parcel through the
        #    ArcGIS sweep) -> explicit "no data" label rather than NULL, so
        #    it still shows up in a `land_use is not null` filter.
        cur.execute(
            """
            update leads
               set land_use = 'UNKNOWN (no source data)'
             where land_use is null
            """
        )
        unknown_total = cur.rowcount

        conn.commit()

        cur.execute("select count(*) from leads")
        total = cur.fetchone()[0]
        cur.execute("select count(*) from leads where land_use is null")
        remaining_null = cur.fetchone()[0]

        print()
        print(f"Backfilled {verified_total} rows with a verified decoded land_use")
        print(f"Backfilled {unverified_total} rows with an UNVERIFIED (code N) placeholder")
        print(f"Backfilled {unknown_total} rows with UNKNOWN (no source data)")
        print(f"Total leads: {total}   land_use still NULL: {remaining_null}")
        if remaining_null:
            print("WARNING: some leads still have land_use IS NULL -- investigate.", file=sys.stderr)
            sys.exit(1)

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()


# CODE_VERIFICATION_LOG (2026-09-22)
# -----------------------------------
# code   sample PIN        sample address              confirmed Land Use text
# 0      0560190124900     206 Gauley St               1100 (Single Family) -- see docstring, NOT trusted as "0 means SFR"
# 110    0373000502300     30 Gilbert Ct               110 (Duplex)
# 122    0148000200520     5305 Parker Cone Way        122 (Apartment Subsidized (E))
# 130    0401000203600     1410 Donaldson Rd           130 (Mobile Home park)
# 409    P015040101300     3001 Wade Hampton Blvd      409 (Office-dental)
# 421    0081000200500     423 Vardry St               421 (Office-general)
# 431    0543030100735     14 W Orchard Dr             431 (Branch)
# 520    0151001401300     116 Poinsett Hwy            520 (General)
# 521    0279000100101     15 Pelham Rd                521 (Strip Center)
# 610    0330000100215     12 Berryblue Ct             610 (Fast food)
# 620    0543010102602     625 Congaree Rd             620 (Full Service)
# 810    M010020100400     30 Fairforest Way           810 (Religious/Church)
# 821    P015100105200     2400 Wade Hampton Blvd      821 (Government-post office)
# 850    0540020102407     1440 Pelham Rd              850 (Schools)
# 940    0176000100206     202 Arcadia Dr              940 (Warehouse General)
# 960    0223000100802     220 Old Piedmont Hwy        960 (Multi-purpse)
# 1100   0539010104300     113 Colonial Ln             1100 (Single Family)
# 1170   0238010108800     12 Sorrell Dr               1170 (MH w/ land)
# 1171   0231000101507     34 Lewis St                 1171 (MH on MH file)
# 1180   0084010501500     14 Ethel St                 1180 (Residential Vacant)
# 1182   0324000400100     (common-area parcel)        1182 (Common Areas)
# 6800   0224000201403     2217 Anderson Rd            6800 (Commercial Vacant)
#
# 1181 (Homeowners assoc. prop) and 9170 (Ag Vacant) were already confirmed
# live in production via tax_sale_ingest.py / redemption_period_ingest.py
# (verified 2026-09-22 by a direct query against leads.land_use), not
# resampled here.
