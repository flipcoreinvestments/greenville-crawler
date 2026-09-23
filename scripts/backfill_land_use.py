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
(https://www.greenvillecounty.org/appsas400/RealProperty/Details.aspx).
As of 2026-09-23 this covers every raw code seen in production except the
literal code "0" (see below) -- 100% of the ~96,246 raw-coded leads that
have a decodable code. See CODE_VERIFICATION_LOG at the bottom of this
file for exactly which PIN(s) backed each code.

2026-09-23 update: verified the remaining 81 long-tail codes (3,479
leads) the same way. Three of them -- 112, 420, 513 -- returned a
mismatched description on their FIRST sampled parcel (e.g. code 420's
first sample showed "620 (Full Service)" on the live county record,
not a 420-prefixed description), the same stale/placeholder-data pattern
already seen with code "0". Per the never-guess rule, each of those three
was re-checked against 2-3 additional independent sample parcels before
being trusted; all additional samples agreed with each other (e.g. 420
came back "420 (Office high rise)" 3/3 times on other parcels), so the
first sample was the fluke and the code itself is decoded normally.
Two codes -- 105 and 1183 -- have no parenthetical description on the
county's own page for ANY sampled parcel (confirmed on 1 and 4 samples
respectively); those are recorded as the bare numeric code, exactly as
the county's own site shows it, rather than inventing a label.

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
    # -- 81 long-tail codes verified 2026-09-23, same methodology --
    "105": "105",  # no description on county's own record (confirmed, 1 sample)
    "112": "112 (Mplex)",
    "113": "113 (Group hse converted)",
    "140": "140 (Nursing Home)",
    "141": "141 (Assisted living)",
    "142": "142 (Converted Res)",
    "143": "143 (Hise-rise retirement w/dining)",
    "205": "205 (Commercila common)",
    "230": "230 (Apt-rooming/B&B)",
    "240": "240 (Luxury)",
    "250": "250 (Extended stay)",
    "270": "270 (Mid-Service)",
    "271": "271 (Motel economy)",
    "272": "272 (Motel budget)",
    "273": "273 (Motel low cost)",
    "300": "300 (Car wash full service)",
    "301": "301 (Car wash-self service)",
    "310": "310 (Serv Station-gas)",
    "320": "320 (Cashier Booth-gas)",
    "330": "330 (Serv garg-Body shop)",
    "331": "331 (Mini lube)",
    "332": "332 (Service Center)",
    "350": "350 (Dealship/maint/service)",
    "360": "360 (Dealship/Showroom)",
    "370": "370 (Parking Garage)",
    "371": "371 (Parking Lot)",
    "410": "410 (Office-medical)",
    "411": "411 (Vet clinic)",
    "413": "413 (Rehab center)",
    "414": "414 (Vet clinic converted/res)",
    "420": "420 (Office high rise)",
    "423": "423 (Office-convert/res)",
    "424": "424 (Office inter/whse)",
    "425": "425 (Office retail strip)",
    "430": "430 (Full-service)",
    "510": "510 (Conv. Store--super)",
    "511": "511 (Conv. Store)",
    "512": "512 (Mom/Pop grocery)",
    "513": "513 (Super Market)",
    "522": "522 (Show Room)",
    "523": "523 (Drug Store)",
    "530": "530 (Discount)",
    "531": "531 (Discount Warehouse)",
    "532": "532 (Lumber-showroom/retail)",
    "550": "550 (Shop Ctr/Neighborhood)",
    "570": "570 (Department Store)",
    "580": "580 (Barber/Beauty-convert)",
    "581": "581 (Barber/Beauty-convent)",
    "590": "590 (Laundry/cleaner full service)",
    "591": "591 (Laundrymat (self))",
    "630": "630 (Neighborhood)",
    "631": "631 (Night Club)",
    "632": "632 (Rest/lounge/sports)",
    "710": "710 (Bowling alley)",
    "720": "720 (Gym/athletic club)",
    "721": "721 (Health Club)",
    "740": "740 (Movie Theatre)",
    "741": "741 (Theatre--play/dining)",
    "750": "750 (Golf-A)",
    "751": "751 (Club house/golf)",
    "753": "753 (Golf-par 3)",
    "770": "770 (Community Recreation)",
    "780": "780 (Theme park)",
    "790": "790 (Tennis/Racquet)",
    "805": "805",  # no description on county's own record (confirmed, 3 samples)
    "851": "851 (day care conventional)",
    "852": "852 (Day care-converted res)",
    "860": "860 (Fraternal Organizations)",
    "872": "872 (Funeral home conventional)",
    "873": "873 (Funeral home converted)",
    "890": "890 (Broadcasting facility)",
    "891": "891 (Utility facility)",
    "910": "910 (Mini-Warehouses)",
    "930": "930 (Truck Terminal)",
    "950": "950 (Warehouse Distribution)",
    "970": "970 (Industrial light)",
    "980": "980 (Hangars)",
    "990": "990 (Cold Storage)",
    "1101": "1101 (SF- w/ auxiliary use)",
    "1183": "1183",  # no description on county's own record (confirmed, 4 samples)
    "9171": "9171 (Ag Improved)",
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
#
# CODE_VERIFICATION_LOG continued (2026-09-23) -- 81 long-tail codes.
# One sample PIN per code unless noted; 112/420/513 needed extra samples
# after their first sample mismatched (see docstring); 105/1183 confirmed
# to have no county-side description on every sample checked.
# 105  0559040101900 (1 sample, no description)
# 112  0399000100300+2 more (Mplex; 1st sample 0001000400204 was a stale-data fluke)
# 113  0095000100200 (Group hse converted)                   140  0082000300200 (Nursing Home)
# 141  0016000100200 (Assisted living)                       142  0039020100704 (Converted Res)
# 143  0056000700200 (Hise-rise retirement w/dining)          205  0014000401200 (Commercila common)
# 230  0013000101100 (Apt-rooming/B&B)                        240  0032000100105 (Luxury)
# 250  0072000201000 (Extended stay)                          270  0003000201900 (Mid-Service)
# 271  0096000500100 (Motel economy)                          272  0172000100500 (Motel budget)
# 273  0172000100400 (Motel low cost)                         300  0056000200201 (Car wash full service)
# 301  0149000600200 (Car wash-self service)                  310  0005000401700 (Serv Station-gas)
# 320  0151001301700 (Cashier Booth-gas)                      330  0011000200300 (Serv garg-Body shop)
# 331  0173020501205 (Mini lube)                               332  0002000100100 (Service Center)
# 350  0143000100112 (Dealship/maint/service)                 360  0158000105403 (Dealship/Showroom)
# 370  0001000100204 (Parking Garage)                         371  0001000600502 (Parking Lot)
# 410  0005000301300 (Office-medical)                         411  0039030300100 (Vet clinic)
# 413  0008000201100 (Rehab center)                           414  0541030102300 (Vet clinic converted/res)
# 420  0050000200100+1 more (Office high rise; 1st sample 0001000400203 was a stale-data fluke)
# 423  0004000100101 (Office-convert/res)                     424  M008040100349 (Office inter/whse)
# 425  0001000300700 (Office retail strip)                    430  0014000100500 (Full-service)
# 510  0048000801200 (Conv. Store--super)                      511  0004000102900 (Conv. Store)
# 512  0126000600800 (Mom/Pop grocery)
# 513  M015050100615+1 more (Super Market; 1st sample 0002000601900 was a stale-data fluke)
# 522  0198000300100 (Show Room)                               523  0001000400401 (Drug Store)
# 530  0104000200308 (Discount)                                531  0174040100401 (Discount Warehouse)
# 532  P009030104500 (Lumber-showroom/retail)                  550  0102000100101 (Shop Ctr/Neighborhood)
# 570  0273000100101 (Department Store)                        580  0005000301400 (Barber/Beauty-convert)
# 581  0001000300603 (Barber/Beauty-convent)                   590  0031000501400 (Laundry/cleaner full service)
# 591  0143000100107 (Laundrymat (self))                       630  0039030302000 (Neighborhood)
# 631  0017000200301 (Night Club)                              632  0001000600900 (Rest/lounge/sports)
# 710  0269000101101 (Bowling alley)                           720  0056000600300 (Gym/athletic club)
# 721  0262000101410 (Health Club)                             740  0173010600102 (Movie Theatre)
# 741  0089000102100 (Theatre--play/dining)                    750  0209000301400 (Golf-A)
# 751  0525060121601 (Club house/golf)                         753  P015130100100 (Golf-par 3)
# 770  0041000100200 (Community Recreation)                    780  0547030103723 (Theme park)
# 790  0056000200100 (Tennis/Racquet)
# 805  0039020101700+2 more (no description, 3 samples all bare "805")
# 851  0033000100100 (day care conventional)                  852  0005000102200 (Day care-converted res)
# 860  0033000101101 (Fraternal Organizations)                 872  0004000102700 (Funeral home conventional)
# 873  0016000200700 (Funeral home converted)                  890  0012000102500 (Broadcasting facility)
# 891  0048000101500 (Utility facility)                        910  0062000200100 (Mini-Warehouses)
# 930  0252000100907 (Truck Terminal)                          950  0168000800200 (Warehouse Distribution)
# 970  0054000500500 (Industrial light)                        980  0282000200401 (Hangars)
# 990  0350000100109 (Cold Storage)                            1101 0030000100500 (SF- w/ auxiliary use)
# 1183 0034000100101+3 more (no description, 4 samples all bare "1183")
# 9171 0132000100400 (Ag Improved)
#
# "0" remains deliberately excluded -- the 2026-09-22 sample (PIN
# 0560190124900, resolved to "1100 (Single Family)" on the live county
# record, not "0" anything) still stands as the reason; not resampled
# again on 2026-09-23.
