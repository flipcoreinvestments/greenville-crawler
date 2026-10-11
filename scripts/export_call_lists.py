#!/usr/bin/env python3
"""
CALL LISTS + SYSTEM HEALTH, written every night (added 2026-09-30).

T Dawg doesn't write SQL, so the lists come to her instead of her querying
for them. After every nightly run this script writes three files into the
PRIVATE Supabase Storage bucket "lead-exports" (never the public repo --
these hold owner names and addresses):

  house_leads.csv   houses, duplex/multiplex, mobile homes, ag improved,
                    unknown land use -- person-owned, residential
  land_leads.csv    residential vacant lots + ag vacant land
  probate_pr_leads.csv     deceased owner + a Personal Representative/family
                           contact to skip trace (2026-10-10, see below)
  deceased_owner_hold.csv  deceased owner, nobody to call yet -- don't trace
  system_health.txt one-page status: when it last ran, list sizes, database
                    space left, Scrapfly credits left, PropStream list ages

Each CSV is sorted the way she calls: '***' (vacant + behind on taxes)
first, then highest score. Same rows as the house_leads / land_leads views.
Files are overwritten every night, so the bucket never grows.

Download: Supabase -> Storage -> lead-exports -> click the file -> Download.
The CSV opens in Excel/Google Sheets and uploads straight into PropStream,
BatchData or BatchDialer (property address, city, state, zip are separate
columns, which is what their skip-trace importers ask for).
"""
import csv
import io
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

from lead_common import ensure_schema
from skiptrace_prep import compute_skiptrace_identity, probate_contact, probate_heirs, split_name

PROBATE_CASE_URL = "https://www.greenvillecounty.org/appsas400/Probate/SearchDetails.aspx?CaseNumber={case_number}"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://izxqrskybwtqzbbbzlux.supabase.co")
BUCKET = "lead-exports"
DB_LIMIT_MB = 500          # Supabase free plan
DB_WARN_PCT = 80
PROPSTREAM_STALE_DAYS = 60  # propstream_import.py drops a list after this
PROPSTREAM_WARN_DAYS = 45

LIST_LABELS = {
    "tax_sale": "Tax sale list",
    "repeat_tax_delinquent": "Tax sale 2 years running",
    "foreclosure_mie": "Foreclosure auction scheduled",
    "hoa_foreclosure": "HOA foreclosure",
    "permit_expired": "Expired/stalled permit",
    "permit_demolition": "Demolition permit",
    "insurance_damage": "Storm/fire/water damage permit",
    "code_violation": "Code violation (unfit structure)",
    "probate": "Probate (owner deceased)",
    "pre_probate": "Pre-probate (owner deceased)",
    "pre_foreclosure": "Pre-foreclosure",
    "involuntary_lien": "Involuntary lien",
    "failed_listing": "Failed MLS listing",
}

REVIEW_LABELS = {
    "missing_owner": "no owner name",
    "absentee_flag_but_same_address": "absentee flag conflicts",
    "corrupted_address": "bad address",
    "invalid_mailing_address": "mailing address is a name",
    "probate_owner_mismatch": "probate address may not be the estate's",
    "land_use_unknown": "property type unknown",
    "probate_name_match": "probate matched by owner name -- confirm it's the same person",
}

# GHL (added 2026-10-01). Column names match T Dawg's GHL custom fields in
# the "Lead Intel" folder EXACTLY, and the dropdown values match the options
# she created, so the GHL import maps them with no manual work.
GHL_LIST_OPTIONS = {
    "tax_sale": "Tax Sale",
    "repeat_tax_delinquent": "Tax Sale 2+ Years",
    "foreclosure_mie": "Foreclosure Auction",
    "hoa_foreclosure": "HOA Foreclosure",
    "pre_foreclosure": "Pre-Foreclosure",
    "probate": "Probate",
    "pre_probate": "Pre-Probate",
    "permit_expired": "Expired Permit",
    "permit_demolition": "Demolition Permit",
    "insurance_damage": "Damage Permit",
    "code_violation": "Code Violation",
    "involuntary_lien": "Involuntary Lien",
    "failed_listing": "Failed Listing",
}
# order the Call Brief mentions lists in: most urgent first
BRIEF_ORDER = ["foreclosure_mie", "hoa_foreclosure", "pre_foreclosure", "probate", "pre_probate",
               "tax_sale", "repeat_tax_delinquent", "code_violation", "permit_demolition",
               "insurance_damage", "permit_expired", "involuntary_lien", "failed_listing"]

COLUMNS = [
    "Call Brief", "Distress List", "Owner Situation", "Decision Maker", "Tags",
    "Rank", "Flag", "Score", "Property Address", "Property City", "Property State", "Property Zip",
    "Owner Name", "Mailing Address", "Mailing State", "Mailing Zip",
    # Added 2026-10-09 (T Dawg's ask): the address/name to upload into the
    # skip-trace/dialer platform -- swapped to the Personal Representative
    # (probate) or the owner's own mailing address (land with no house
    # number) so skip trace doesn't search the dead owner or an unmailable
    # address. See skiptrace_prep.py. Property Address/Owner Name above are
    # never touched -- those stay accurate for calling.
    "Skiptrace First Name", "Skiptrace Last Name", "Skiptrace Address", "Skiptrace State", "Skiptrace Zip",
    "List Count", "Tax Owed", "Years on Tax Sale List", "Tax Sale Status", "Foreclosure Auction Date",
    "Personal Representative", "PR Address", "Date of Death",
    # 2026-10-10 (T Dawg's ask): case # + the county's case page, so she can
    # look the estate up herself and reverse-search the family.
    "Probate Case #", "Probate Case Link",
    "Vacant (USPS)", "Absentee Owner", "Tired Landlord", "Out-of-State Owner", "Assumable Loan",
    "Land Use", "Parcel #", "Check Before Calling", "Exported",
    # Added 2026-10-09 (T Dawg's ask): show whether a lead was already
    # skip-traced so the same address is never paid for twice. Read from
    # leads.skiptrace_status / skiptrace_date (set when a trace batch is
    # marked, e.g. her 10.7.26 batch: 199 traced, 201 failed). NOT stamped
    # by this export -- the nightly run exports every lead every night, so
    # an export stamp would mark everything "pulled" after one night.
    "Skip-Traced", "Last Skip-Traced",
    "Last Sale Amount", "Last Recorded Sale Date",
    # 2026-10-11: the owner's mailing address split into the separate fields
    # BatchData/PropStream's skip-trace upload has for it (street / city /
    # state / zip). Upload BOTH the property address and these -- BatchData
    # says the mailing address improves the match because owners often live
    # somewhere else. Added at the end so saved GHL import mappings still line up.
    "Mailing Street", "Mailing City",
]

MAIL_TAIL = re.compile(r",?\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$", re.I)


def split_mailing(mailing):
    """'412 Simsbury Way Greer, SC 29650' -> ('SC', '29650'). The county
    field has no comma between street and city, so the city is left inside
    the full address rather than guessed."""
    m = MAIL_TAIL.search(mailing or "")
    return (m.group(1).upper(), m.group(2)) if m else ("", "")


def yn(v):
    return "Yes" if v is True else ("No" if v is False else "")


def tax_sale_status(raw, tags):
    if "tax_sale" not in tags:
        return ""
    fetched = ((raw or {}).get("tax_sale") or {}).get("fetched_at") or ""
    try:
        seen = datetime.fromisoformat(fetched.replace("Z", "+00:00")).date()
    except ValueError:
        return "On county tax sale list"
    if (date.today() - seen).days <= 3:
        return "On county tax sale list"
    return (f"Sale held -- last on county list {seen:%m/%d/%Y}; owner has 12 months "
            f"from the sale to redeem")


def tax_sale_years(raw, tags):
    """'3 (2024, 2025, 2026)'. Published notices only go back to 2024, so
    3 means 'at least 3'."""
    if "tax_sale" not in tags:
        return ""
    years = sorted({int(y) for y in (raw or {}).get("tax_sale_years") or []})
    if not years:
        return ""
    return f"{len(years)} ({', '.join(str(y) for y in years)})"


def last_sale(raw):
    """(amount, date) of the last recorded deed, from the county parcel
    record (raw.absentee_owner). Under $1,000 is a family/quitclaim transfer,
    not a purchase price, and is labeled so. 1970 dates are the old parse bug
    (fixed 2026-10-01) and are left blank until the next nightly refresh."""
    a = (raw or {}).get("absentee_owner") or {}
    price, d = a.get("sale_price"), str(a.get("deed_date") or "")[:10]
    if d.startswith("1970"):
        d = ""
    if d:
        try:
            d = datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y")
        except ValueError:
            d = ""
    try:
        p = float(price)
    except (TypeError, ValueError):
        return "", d
    if p <= 0:
        return "$0 (no sale price recorded)", d
    if p < 1000:
        return f"${p:,.0f} (family/quitclaim transfer, not a sale)", d
    return f"${p:,.0f}", d


def money(v):
    return f"${float(v):,.0f}" if re.match(r"^\d+(\.\d+)?$", str(v or "")) else ""


def owner_situation(r, mail_state):
    out = []
    if r.get("is_vacant") is True:
        out.append("Vacant")
    if r.get("is_absentee") is True:
        out.append("Absentee")
    if r.get("is_tired_landlord") is True:
        out.append("Tired Landlord")
    if r.get("is_out_of_state_land") is True or (mail_state and mail_state != "SC"):
        out.append("Out-of-State")
    if r.get("has_assumable_loan") is True:
        out.append("Assumable Loan")
    return out


def call_brief(r, tags, raw, mail_state, reasons):
    """2-4 plain sentences, most urgent first, built only from what the
    lists say. Never guesses."""
    t = set(tags)
    parts = []
    for tag in BRIEF_ORDER:
        if tag not in t:
            continue
        if tag == "foreclosure_mie":
            d = (raw.get("foreclosure_mie") or {}).get("sale_date")
            parts.append(f"FORECLOSURE AUCTION {d}." if d else "FORECLOSURE AUCTION scheduled.")
        elif tag == "hoa_foreclosure":
            parts.append("The HOA is the one foreclosing.")
        elif tag == "pre_foreclosure":
            parts.append("PRE-FORECLOSURE: lender filed a default.")
        elif tag == "probate":
            pb = raw.get("probate") or {}
            s = "PROBATE: owner died" + (f" {pb['date_of_death']}." if pb.get("date_of_death") else ".")
            c_name, c_role, c_addr = probate_contact(pb)
            if c_name:
                s += f" Call the {c_role.split(' (')[0].lower()}, {c_name}" + (f", {c_addr}." if c_addr else ".")
            else:
                s += " Estate is open; personal representative not on file yet -- don't skip trace yet."
            parts.append(s)
        elif tag == "pre_probate" and "probate" not in t:
            parts.append("Owner on title is deceased (no estate opened yet).")
        elif tag == "tax_sale":
            amt = money((raw.get("tax_sale") or {}).get("amount_due"))
            yrs = sorted({int(y) for y in raw.get("tax_sale_years") or []})
            s = "TAX SALE" + (f": owes {amt} in back taxes" if amt else "")
            if len(yrs) > 1:
                s += f", on the county tax sale list {len(yrs)} years ({yrs[0]}-{yrs[-1]})"
            s += "."
            status = tax_sale_status(raw, tags)
            if status.startswith("Sale held"):
                s += " " + status.replace(" -- ", ": ") + "."
            parts.append(s)
        elif tag == "repeat_tax_delinquent" and not raw.get("tax_sale_years"):
            parts.append("Also on last year's tax sale list.")
        elif tag == "code_violation":
            parts.append("Code violation: county/city found the house unfit to live in.")
        elif tag == "permit_demolition":
            parts.append("Demolition permit filed.")
        elif tag == "insurance_damage":
            parts.append("Repair permit for storm, fire or water damage.")
        elif tag == "permit_expired":
            parts.append("Building permit expired or stalled (unfinished work).")
        elif tag == "involuntary_lien":
            parts.append("Involuntary lien recorded (HOA, contractor, utility or child support).")
        elif tag == "failed_listing":
            parts.append("Listed on the MLS and didn't sell.")
    extra = []
    if r.get("is_vacant") is True:
        extra.append("House shows vacant (USPS)")
    if mail_state and mail_state != "SC":
        extra.append(f"owner lives out of state ({mail_state})")
    elif r.get("is_absentee") is True:
        extra.append("owner doesn't live there")
    if r.get("is_tired_landlord") is True:
        n = r.get("owner_parcel_count")
        extra.append(f"owner holds {n} properties" if n else "owner holds 3+ properties")
    if r.get("has_assumable_loan") is True:
        extra.append("assumable FHA/VA/USDA loan")
    if extra:
        e = "; ".join(extra)
        parts.append(e[0].upper() + e[1:] + ".")
    if reasons:
        # CONVENTION CHANGE (2026-10-09, T Dawg's explicit ask): any lead we
        # couldn't verify accurately gets the literal phrase "NEED TO VERIFY
        # LEAD" worked into the Call Brief text, instead of a leading
        # asterisk on the address -- so whoever is calling sees it right in
        # the brief with no separate legend to remember. The triple-asterisk
        # Flag ('***' = confirmed vacant + tax-delinquent) is unchanged --
        # that's a separate positive-confirmation signal, not an uncertainty
        # warning.
        parts.append("NEED TO VERIFY LEAD -- " + "; ".join(reasons) + ".")
    return " ".join(parts)


def lead_rows(conn, view):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            select v.*, (v.is_vacant is true and v.source_tags && array['tax_sale', 'repeat_tax_delinquent'])
                        as triple
            from {view} v
            order by (v.is_vacant is true and v.source_tags && array['tax_sale', 'repeat_tax_delinquent']) desc,
                     v.score desc, v.address
            """
        )
        return cur.fetchall()


def to_csv(rows, today):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(COLUMNS)
    for i, r in enumerate(rows, start=1):
        raw = r.get("raw") or {}
        tags = list(r.get("source_tags") or [])
        probate = raw.get("probate") or {}
        mst, mzip = split_mailing(r.get("mailing_address"))
        amount = (raw.get("tax_sale") or {}).get("amount_due") or ""
        reasons = [REVIEW_LABELS.get(x, x.replace("_", " ")) for x in (r.get("review_reasons") or [])
                   if x not in ("single_source", "incomplete_address")]
        # Skip-trace identity swap (2026-10-09): trace the Personal
        # Representative (probate) or the owner's own mailing address (land
        # with no house number) instead of the deceased owner / an
        # unmailable address. See skiptrace_prep.py. A note comes back when
        # a lead can't get the normal treatment -- folds into the same
        # "NEED TO VERIFY LEAD" text every other review reason uses.
        st_first, st_last, st_address, st_note = compute_skiptrace_identity(r, tags)
        st_state, st_zip = split_mailing(st_address)
        if st_address == r["address"]:
            # Tracing the property itself: its street carries no state/zip,
            # so use the property's own (filled by fill_city_zip.py).
            st_state, st_zip = r.get("state") or "SC", r.get("zip") or ""
        if st_note:
            reasons = reasons + [st_note]
        # FLAG: '***' (confirmed vacant + tax-delinquent) is the only flag
        # left here -- the single-'*' "needs review" signal moved into the
        # Call Brief text itself (see call_brief()'s "NEED TO VERIFY LEAD").
        flag = "***" if r.get("triple") else ""
        ordered = [x for x in BRIEF_ORDER if x in tags]
        decision = ""
        if "probate" in tags:
            c_name, c_role, c_addr = probate_contact(probate)
            if c_name:
                decision = c_name + (f", {c_addr}" if c_addr else "")
        # Skip-trace status (2026-10-09): Traced / Failed / No.
        st_status = (r.get("skiptrace_status") or "").strip().lower()
        st_label = {"traced": "Traced", "failed": "Failed"}.get(st_status, "No")
        st_when = r.get("skiptrace_date")
        if isinstance(st_when, str):
            try:
                st_when = datetime.fromisoformat(st_when[:10])
            except ValueError:
                pass
        w.writerow([
            call_brief(r, tags, raw, mst, reasons),
            ", ".join(GHL_LIST_OPTIONS[x] for x in ordered),
            ", ".join(owner_situation(r, mst)),
            decision,
            ", ".join("list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[x].lower()).strip("-") for x in ordered),
            i, flag, int(r.get("score") or 0), r["address"], (r.get("city") or "").title(),
            r.get("state") or "SC", r.get("zip") or "",
            r.get("owner_name") or "", r.get("mailing_address") or "", mst, mzip,
            st_first, st_last, st_address, st_state, st_zip,
            r.get("list_count") or 0,  # "Lists" column removed 2026-10-01: duplicated Distress List
            f"${float(amount):,.2f}" if re.match(r"^\d+(\.\d+)?$", str(amount)) else "",
            tax_sale_years(raw, tags),
            tax_sale_status(raw, tags),
            ((raw.get("foreclosure_mie") or {}).get("sale_date") or "") if "foreclosure_mie" in tags else "",
            (probate.get("pr_name") or "") if "probate" in tags else "",
            (probate.get("pr_address") or "") if "probate" in tags else "",
            (probate.get("date_of_death") or "") if "probate" in tags else "",
            probate.get("case_number") or "",
            (PROBATE_CASE_URL.format(case_number=probate["case_number"]) if probate.get("case_number") else ""),
            yn(r.get("is_vacant")), yn(r.get("is_absentee")), yn(r.get("is_tired_landlord")),
            yn(r.get("is_out_of_state_land")), yn(r.get("has_assumable_loan")),
            r.get("land_use") or "", r.get("pin") or "", "; ".join(reasons), today.isoformat(),
            st_label,
            st_when.strftime("%m/%d/%Y") if hasattr(st_when, "strftime") else (str(st_when) if st_when else ""),
            *last_sale(raw),
            *split_contact_address(r.get("mailing_address"))[:2],
        ])
    return buf.getvalue()


# ------------------------------------------------------------ probate lists
# 2026-10-10 (T Dawg's ask): segment the deceased-owner leads so skip-trace
# money only goes to a living, reachable person.
#   probate_pr_leads.csv     open estate with a Personal Representative (or a
#                            successor PR / special administrator / affiant /
#                            applicant on the case file). Contact = that
#                            person; the deceased owner and the family named
#                            on the case file get their own columns.
#   deceased_owner_hold.csv  owner died but nobody to call yet (estate open
#                            with no PR named, or pre-probate with no estate
#                            filed). DON'T skip trace -- it only finds the
#                            dead person's old number. Mail/door-knock only;
#                            the nightly probate backfill moves a lead to the
#                            PR list the night a PR shows up on the case.
# house_leads.csv / land_leads.csv no longer include either group, so the
# same lead is never in two upload files.
PROBATE_COLUMNS = [
    "Contact First Name", "Contact Last Name", "Contact Full Name", "Contact Role",
    "Contact Street", "Contact City", "Contact State", "Contact Zip",
    "Deceased Person's Name", "Heirs", "Date of Death", "Probate Case #", "Probate Case Link",
    "Property Address", "Property City", "Property State", "Property Zip", "Owner on Title",
    "Distress List", "Tags", "Call Brief", "Flag", "Score", "Vacant (USPS)", "Tax Owed",
    "Land Use", "Parcel #", "Check Before Calling", "Skip-Traced", "Last Skip-Traced", "Exported",
]

_KNOWN_TOWNS = sorted([
    "Greenville", "Greer", "Simpsonville", "Mauldin", "Fountain Inn", "Travelers Rest", "Taylors",
    "Piedmont", "Pelzer", "Marietta", "Landrum", "Gray Court", "Easley", "Cleveland", "Slater",
    "Tigerville", "Duncan", "Woodruff", "Williamston", "Liberty", "Anderson", "Spartanburg",
    "Pickens", "Central", "Clemson", "Seneca", "Laurens", "Columbia", "Charlotte", "Belton",
    "Honea Path", "Boiling Springs", "Rock Hill", "Fort Mill", "Summerville", "Myrtle Beach", "Inman", "Lyman", "Wellford", "Moore", "Roebuck",
], key=len, reverse=True)
_ADDR_TAIL = re.compile(r"^(?P<rest>.*?)[,\s]+(?P<st>[A-Z]{2})[,\s]+(?P<zip>\d{5})(?:-\d{4})?\s*$", re.I)


def split_contact_address(addr):
    """'123 MAIN ST GREER SC 29651' -> ('123 MAIN ST', 'Greer', 'SC', '29651').
    The court writes no comma before the city, so the city is only split off
    when it's a known town; otherwise it stays on the street (skip-trace
    tools still match on name + state + zip)."""
    a = re.sub(r"\s+", " ", addr or "").strip()
    m = _ADDR_TAIL.match(a)
    if not m:
        return a, "", "", ""
    rest, st, z = m.group("rest").strip(" ,"), m.group("st").upper(), m.group("zip")
    for town in _KNOWN_TOWNS:
        if rest.upper().endswith(" " + town.upper()):
            return rest[: -len(town)].strip(" ,"), town, st, z
    # Not a known town: the city is whatever follows the last street-type
    # word (or apartment/unit number) -- "4353 DON TOMASO DR # 9 LOS ANGELES".
    toks = rest.split()
    cut = None
    pm = re.match(r"^(P\.?\s*O\.?\s*BOX|BOX)\s+\S+\s+(.+)$", rest, re.I)
    if pm:
        return rest[: len(rest) - len(pm.group(2))].strip(" ,"), pm.group(2).strip(" ,").title(), st, z
    for i, t in enumerate(toks):
        u = t.upper().strip(".,")
        if i > 0 and u in _STREET_WORDS:
            cut = i + 1
            if cut < len(toks) and toks[cut].upper().strip(".,") in _UNIT_WORDS:
                cut += 2  # "APT 1H", "# 9", "STE 200"
        elif u in _UNIT_WORDS and cut is not None and i == cut:
            cut = i + 2
    if cut is not None and cut < len(toks):
        return " ".join(toks[:cut]).strip(" ,"), " ".join(toks[cut:]).strip(" ,").title(), st, z
    return rest, "", st, z


_STREET_WORDS = {
    "ST", "STREET", "RD", "ROAD", "DR", "DRIVE", "AVE", "AVENUE", "AV", "LN", "LANE", "CT", "COURT",
    "WAY", "BLVD", "CIR", "CIRCLE", "HWY", "HIGHWAY", "TRL", "TRAIL", "PL", "PLACE", "PKWY", "TER",
    "TERRACE", "LOOP", "RUN", "PT", "POINTE", "XING", "PIKE", "ROW", "SQ", "EXT", "N", "S", "E", "W",
}
_UNIT_WORDS = {"APT", "UNIT", "STE", "SUITE", "#", "LOT", "BLDG"}


def segment(row):
    """'dead' | 'wrong_info' | 'probate_pr' | 'deceased_hold' | 'main' for one
    house/land lead. dead / wrong_info come from the nightly QA check
    (qa_check.py) and win over everything, so those leads never reach a
    skip-trace upload."""
    status = (row.get("lead_status") or "").strip()
    if status in ("dead", "wrong_info"):
        return status
    tags = set(row.get("source_tags") or [])
    if "probate" in tags:
        name, _, _ = probate_contact((row.get("raw") or {}).get("probate"))
        return "probate_pr" if name else "deceased_hold"
    if "pre_probate" in tags:
        return "deceased_hold"
    return "main"


def probate_csv(rows, today):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(PROBATE_COLUMNS)
    for r in rows:
        raw = r.get("raw") or {}
        tags = list(r.get("source_tags") or [])
        pb = raw.get("probate") or {}
        name, role, addr = probate_contact(pb)
        first, last = split_name(name)
        street, city, st, z = split_contact_address(addr)
        if not street:
            # PR with no address on the case: trace them at the property
            street, city, st, z = r["address"], (r.get("city") or "").title(), r.get("state") or "SC", r.get("zip") or ""
        mst, _ = split_mailing(r.get("mailing_address"))
        reasons = [REVIEW_LABELS.get(x, x.replace("_", " ")) for x in (r.get("review_reasons") or [])
                   if x not in ("single_source", "incomplete_address")]
        if not addr:
            reasons.append("no address on the case file for the contact -- traced at the property address")
        ordered = [x for x in BRIEF_ORDER if x in tags]
        amount = (raw.get("tax_sale") or {}).get("amount_due") or ""
        st_status = (r.get("skiptrace_status") or "").strip().lower()
        st_when = r.get("skiptrace_date")
        w.writerow([
            first, last, name, role, street, city, st, z,
            re.sub(r"\s*,\s*", ", ", pb.get("decedent_name") or "").strip(" ,") or r.get("owner_name") or "",
            probate_heirs(pb, exclude_name=name, decedent_name=pb.get("decedent_name")),
            pb.get("date_of_death") or "", pb.get("case_number") or "",
            PROBATE_CASE_URL.format(case_number=pb["case_number"]) if pb.get("case_number") else "",
            r["address"], (r.get("city") or "").title(), r.get("state") or "SC", r.get("zip") or "",
            r.get("owner_name") or "",
            ", ".join(GHL_LIST_OPTIONS[x] for x in ordered),
            ", ".join("list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[x].lower()).strip("-") for x in ordered),
            call_brief(r, tags, raw, mst, reasons),
            "***" if r.get("triple") else "", int(r.get("score") or 0), yn(r.get("is_vacant")),
            f"${float(amount):,.2f}" if re.match(r"^\d+(\.\d+)?$", str(amount)) else "",
            r.get("land_use") or "", r.get("pin") or "", "; ".join(reasons),
            {"traced": "Traced", "failed": "Failed"}.get(st_status, "No"),
            st_when.strftime("%m/%d/%Y") if hasattr(st_when, "strftime") else (str(st_when)[:10] if st_when else ""),
            today.isoformat(),
        ])
    return buf.getvalue()


# --------------------------------------------------------- dead / wrong info
STATUS_COLUMNS = ["Status", "Why", "Marked On"]


def dead_rows(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("select l.*, false as triple from leads l where l.lead_status = 'dead' "
                    "order by l.lead_status_at desc nulls last, l.address")
        return cur.fetchall()


def status_csv(rows, today):
    """Same columns as the call lists, with Status / Why / Marked On in front,
    so a dead or wrong lead can be found in GHL by its address and fixed or
    removed there."""
    body = list(csv.reader(io.StringIO(to_csv(rows, today))))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(STATUS_COLUMNS + body[0])
    for r, line in zip(rows, body[1:]):
        when = r.get("lead_status_at")
        w.writerow([{"dead": "Dead", "wrong_info": "Wrong Information"}.get(r.get("lead_status"), ""),
                    r.get("lead_status_reason") or "",
                    when.strftime("%m/%d/%Y") if hasattr(when, "strftime") else ""] + line)
    return buf.getvalue()


# ------------------------------------------------------------------ health
def health_report(conn, counts, today):
    lines = [f"Restart Homes lead system -- status as of {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC", ""]
    warn = []
    with conn.cursor() as cur:
        lines.append("LEAD LISTS")
        for name, n in counts.items():
            lines.append(f"  {name:<22}{n:>7,}")
        cur.execute("select count(*) from business_owned_leads")
        lines.append(f"  {'business_owned_leads':<22}{cur.fetchone()[0]:>7,}  (not exported)")
        lines.append("")

        cur.execute("select pg_database_size(current_database())")
        mb = cur.fetchone()[0] / 1024 / 1024
        pct = mb / DB_LIMIT_MB * 100
        lines.append("DATABASE SPACE (free plan limit 500 MB)")
        lines.append(f"  {mb:,.0f} MB used = {pct:.0f}%")
        if pct >= DB_WARN_PCT:
            warn.append(f"Database is {pct:.0f}% full ({mb:,.0f} of {DB_LIMIT_MB} MB). "
                        f"Run 'vacuum full analyze leads;' in the Supabase SQL editor, or upgrade to Pro.")
        lines.append("")

        cur.execute("select source_name, max(run_at) from source_runs where source_name <> 'scrapfly_credits' "
                    "group by source_name having max(run_at) > now() - interval '60 days' order by 1")
        lines.append("LAST RUN PER SOURCE")
        for src, at in cur.fetchall():
            age = (datetime.now(timezone.utc) - at).days if at else None
            lines.append(f"  {src:<28}{at:%Y-%m-%d}" + (f"  ({age} days ago)" if age else ""))
            if age is not None and age > (8 if src == "foreclosure_mie" else 2):
                warn.append(f"{src} has not run in {age} days.")
        lines.append("")

        cur.execute("select notes, run_at from source_runs where source_name = 'scrapfly_credits' "
                    "order by run_at desc limit 1")
        r = cur.fetchone()
        lines.append("SCRAPFLY (weekly foreclosure check, ~150 credits per run)")
        if r:
            lines.append(f"  {r[0]}  (checked {r[1]:%Y-%m-%d})")
            m = re.search(r"remaining=(\d+)", r[0] or "")
            if m and int(m.group(1)) < 450:
                warn.append(f"Scrapfly has {m.group(1)} credits left -- about {int(m.group(1)) // 150} "
                            f"more weekly foreclosure runs. Free credits never refill.")
        else:
            lines.append("  not checked yet (next Monday's run records it)")
        lines.append("")

        lines.append("PROPSTREAM EXPORTS (dropped from leads at 60 days old)")
        for key, label in [("propstream_pre_probate", "Pre-Probate"),
                           ("propstream_pre_foreclosure", "Pre-Foreclosures"),
                           ("propstream_vacant", "Vacant"),
                           ("propstream_involuntary_lien", "Liens"),
                           ("propstream_failed_listing", "Failed Listings"),
                           ("propstream_assumable", "Assumable")]:
            cur.execute("select max(raw->%s->>'file_date') from leads where raw ? %s", (key, key))
            d = cur.fetchone()[0]
            if not d:
                lines.append(f"  {label:<18}not uploaded")
                continue
            age = (today - date.fromisoformat(d[:10])).days
            drops = date.fromisoformat(d[:10]) + timedelta(days=PROPSTREAM_STALE_DAYS)
            lines.append(f"  {label:<18}exported {d[:10]}  ({age} days old, drops off {drops:%m/%d/%Y})")
            if age >= PROPSTREAM_WARN_DAYS:
                warn.append(f"PropStream {label} export drops off every lead on {drops:%m/%d/%Y} "
                            f"({PROPSTREAM_STALE_DAYS - age} days) -- re-export it from PropStream and upload it.")
    head = ["ACTION NEEDED:"] + [f"  - {w}" for w in warn] if warn else ["ACTION NEEDED: nothing"]
    return "\n".join(lines[:2] + head + [""] + lines[2:]) + "\n", warn


# ------------------------------------------------------------------ upload
def upload(files, key, url=SUPABASE_URL, session=None):
    import requests
    s = session or requests.Session()
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    # create the private bucket the first time (409/400 = already exists)
    r = s.post(f"{url}/storage/v1/bucket", headers=h,
               json={"id": BUCKET, "name": BUCKET, "public": False}, timeout=60)
    if r.status_code not in (200, 201, 400, 409):
        r.raise_for_status()
    for name, body, ctype in files:
        r = s.post(f"{url}/storage/v1/object/{BUCKET}/{name}",
                   headers={**h, "x-upsert": "true", "Content-Type": ctype},
                   data=body.encode("utf-8"), timeout=120)
        r.raise_for_status()
        print(f"  uploaded {BUCKET}/{name} ({len(body):,} bytes)")


def build(conn, today=None):
    today = today or date.today()
    out, counts = [], {}
    probate_pr, hold, wrong = [], [], []
    for view in ("house_leads", "land_leads"):
        rows = lead_rows(conn, view)
        main_rows = []
        for r in rows:
            seg = segment(r)
            {"probate_pr": probate_pr, "deceased_hold": hold, "wrong_info": wrong,
             "dead": []}.get(seg, main_rows).append(r)
        counts[view] = len(main_rows)
        out.append((f"{view}.csv", to_csv(main_rows, today), "text/csv"))
    counts["probate_pr_leads"] = len(probate_pr)
    counts["deceased_owner_hold"] = len(hold)
    out.append(("probate_pr_leads.csv", probate_csv(probate_pr, today), "text/csv"))
    out.append(("deceased_owner_hold.csv", to_csv(hold, today), "text/csv"))
    # 2026-10-11 (T Dawg's ask): the nightly QA check's results, each in its
    # own folder. Wrong-information leads come from the call-list views; dead
    # leads (sold / off every list) are no longer in those views, so they're
    # read straight from the leads table.
    dead = dead_rows(conn)
    counts["dead_leads"] = len(dead)
    counts["wrong_information_leads"] = len(wrong)
    out.append(("dead/dead_leads.csv", status_csv(dead, today), "text/csv"))
    out.append(("wrong_information/wrong_information_leads.csv", status_csv(wrong, today), "text/csv"))
    report, warn = health_report(conn, counts, today)
    out.append(("system_health.txt", report, "text/plain"))
    return out, warn


def main():
    db_url = os.environ.get("DATABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not db_url or not key:
        print("DATABASE_URL and SUPABASE_SERVICE_KEY must be set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    with conn.cursor() as cur:
        # Skip-trace status columns (already added live 2026-10-07 via
        # skiptrace_status_migration.sql; no-op if present). Added BEFORE
        # ensure_schema() so the rebuilt views include them.
        cur.execute("alter table leads add column if not exists skiptrace_status text")
        cur.execute("alter table leads add column if not exists skiptrace_date timestamptz")
        # nightly QA marks (qa_check.py, 2026-10-11) -- added before the views
        # are rebuilt so they carry the columns.
        cur.execute("alter table leads add column if not exists lead_status text")
        cur.execute("alter table leads add column if not exists lead_status_reason text")
        cur.execute("alter table leads add column if not exists lead_status_at timestamptz")
    ensure_schema(conn)
    conn.commit()
    files, warn = build(conn)
    conn.close()
    upload(files, key)
    print(files[-1][1])
    for w in warn:
        print(f"::warning::{w}")


if __name__ == "__main__":
    main()
