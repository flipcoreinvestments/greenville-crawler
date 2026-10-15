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
from skiptrace_prep import compute_skiptrace_identity, probate_contact, probate_heirs, split_name, owner_trace_names, _clean_addr
from ghl_sync import ensure_columns as ensure_ghl_columns, plan_ghl_rows, good_phones

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
    # 2026-10-11: second co-owner (spouse etc.) as a backup person to trace,
    # and what kind of owner it is (Person / Trust / Company).
    "Skiptrace Owner 2 First Name", "Skiptrace Owner 2 Last Name", "Owner Type",
    # 2026-10-13 (T Dawg's ask): a lead already pulled whose information
    # changed (errors/errors.csv) stays on whatever list it is on, tagged
    # "update lead info" so a GHL import with "update existing contacts"
    # updates the contact already there. This column says what changed.
    "What Changed (update GHL)",
    # 2026-10-14: the phones the skip trace returned (newest first). GHL
    # matches an existing contact by phone or email, so these are what make
    # "update existing contacts" find the contact instead of creating a new one.
    "Phone 1", "Phone 2", "Phone 3", "Phone 4", "Phone 5",
    # 2026-10-16 (T Dawg's ask): what she has to fix by hand before this lead
    # can be traced (blank = nothing, trace it as is). Filter on it so only
    # the leads that need an edit get touched.
    "Edit Before Trace",
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


# 2026-10-14: filled by build() from the latest tax_sale run -- whether the
# county list is still up and the sale date printed on it.
TAX_SALE = {"list_up": None, "sale_date": None}


def load_tax_sale_info(conn):
    with conn.cursor() as cur:
        cur.execute("""select records_found, notes, run_at from source_runs where source_name = 'tax_sale'
                       order by run_at desc limit 1""")
        r = cur.fetchone()
        cur.execute("""select substring(notes from 'sale_date:([0-9-]+)') from source_runs
                       where source_name = 'tax_sale' and notes like '%%sale_date:%%'
                       order by run_at desc limit 1""")
        d = cur.fetchone()
    TAX_SALE["list_up"] = bool(r and (r[0] or 0) > 0
                               and (datetime.now(timezone.utc) - r[2]) < timedelta(days=3))
    TAX_SALE["sale_date"] = date.fromisoformat(d[0]) if d and d[0] else None


def tax_sale_status(raw, tags):
    """A lead keeps its tax_sale tag while its parcel is on the county list
    (a parcel whose owner pays drops off the list and loses the tag). After
    the sale the county takes the list down; sold parcels keep the tag for
    the 12-month redemption period."""
    if "tax_sale" not in tags:
        return ""
    sale = TAX_SALE.get("sale_date")
    today = date.today()
    if sale and today <= sale:
        return f"On county tax sale list -- sale {sale:%m/%d/%Y}"
    if sale and today > sale:
        return (f"Sale held {sale:%m/%d/%Y} -- owner has 12 months from the sale "
                f"(to {sale.replace(year=sale.year + 1):%m/%d/%Y}) to redeem")
    if TAX_SALE.get("list_up") is not False:
        return "On county tax sale list"
    fetched = ((raw or {}).get("tax_sale") or {}).get("fetched_at") or ""
    try:
        seen = datetime.fromisoformat(fetched.replace("Z", "+00:00")).date()
    except ValueError:
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


def _owner2(r, tags):
    """(owner-2 first, owner-2 last, owner type). Probate leads trace the
    Personal Representative, so no co-owner is offered there."""
    n = owner_trace_names(r.get("owner_name"))
    if "probate" in tags:
        return "", "", n["kind"]
    return n["first2"], n["last2"], n["kind"]


# 2026-10-13: open errors (leads already pulled whose information changed in
# the last ERROR_DAYS days), filled by build() before any file is written.
ERROR_DAYS = 30
OPEN_ERRORS = {}
OPEN_ERRORS_AT = {}  # 2026-10-16: lead id -> newest found_at (the GHL file sends each change once)


def open_errors(conn, days=ERROR_DAYS):
    """{lead id: 'what changed; ...'} for every lead on the error list."""
    with conn.cursor() as cur:
        cur.execute("alter table lead_error_log add column if not exists dismissed boolean not null default false")
        # error time for the GHL file leaves out caller feedback read from a
        # GHL export (ghl_sync.py) -- that's already on the contact in GHL
        cur.execute("""select lead_id, string_agg(reason, '; ' order by found_at desc),
                              max(found_at) filter (where reason not like 'caller reported%%')
                       from lead_error_log where found_at > now() - (%s || ' days')::interval
                         and not coalesce(dismissed, false)
                       group by lead_id""", (str(days),))
        rows = cur.fetchall()
    OPEN_ERRORS_AT.clear()
    OPEN_ERRORS_AT.update({str(k): at for k, _, at in rows})
    return {str(k): v for k, v, _ in rows}


_INCOMPLETE_STREET = re.compile(r"^\s*\d+\s+[A-Za-z]+\s*$")


def same_search(r, first, last, addr):
    """True when tracing this lead now would send BatchData the same name and
    the same address it already searched -- it would just return the same
    numbers again (the money T Dawg lost on the 10.11 retrace list)."""
    inp = r.get("skiptrace_input") or {}
    if not (inp.get("first") or inp.get("last")):
        return False
    if not _same_person(inp.get("first"), inp.get("last"), first, last):
        return False
    # the address searched (recorded since 2026-10-16); older traces didn't
    # record it, so the same person counts as the same search -- she checks
    # the address by hand (ORANGE) rather than the system paying twice
    then = inp.get("searched_address")
    if not then:
        return True
    n = lambda x: re.sub(r"[^a-z0-9]", "", re.split(r",", x or "")[0].lower())[:14]
    return not n(addr) or n(addr) == n(then)


def trace_edit_note(r, first, last, addr):
    """What has to be fixed by hand before this lead can be traced ('' = trace as is)."""
    tags = set(r.get("source_tags") or [])
    kind = owner_trace_names(r.get("owner_name")).get("kind")
    if not (first or last):
        if "probate" in tags or "pre_probate" in tags:
            return ("Wrong name -- no personal representative or heir on file yet: find one (probate case link) "
                    "and type the name into Skiptrace First/Last Name")
        return "No person to trace -- type the owner's name into Skiptrace First/Last Name"
    if not first and kind in ("Company", "Trust"):
        return ("Company/trust owner -- replace the Skiptrace name with a real person "
                "(officer, registered agent or trustee)")
    if not (addr or "").strip():
        return "No address to trace -- add the person's mailing address in Skiptrace Address"
    if addr == r.get("address") and _INCOMPLETE_STREET.match(addr or ""):
        return "Incomplete address -- add the street type (Rd, St, Dr...) to Skiptrace Address"
    if r.get("retrace_reason") and same_search(r, first, last, addr):
        why = re.sub(r"^caller reported ", "caller said ", r["retrace_reason"])
        return (f"Same person as the last trace ({why}) -- tracing the same name and "
                "address again returns the same numbers: change the Skiptrace name/address to the right "
                "person, or use a second skip-trace provider or a manual search")
    return ""


def person_changed(r, first, last):
    """The person to trace now (first, last) is someone else than the one
    traced: differs from the name searched AND from who the system itself
    would have traced at the time (skiptrace_input.system_*, recorded since
    2026-10-16) -- so a name she corrected by hand before tracing isn't
    mistaken for a change, while a Personal Representative appointed later is."""
    inp = r.get("skiptrace_input") or {}
    if not (inp.get("first") or inp.get("last")) or not (first or last):
        return False
    if _same_person(inp.get("first"), inp.get("last"), first, last):
        return False
    if "system_first" in inp or "system_last" in inp:
        if (inp.get("system_first") or inp.get("system_last")) and _same_person(
                inp.get("system_first"), inp.get("system_last"), first, last):
            return False
    return True


def _same_person(a_first, a_last, b_first, b_last):
    """Same last name and same first 3 letters of the first name (Loyd /
    Lloyd, Jr / Sr suffixes and middle names don't count as a change)."""
    n = lambda x: re.sub(r"[^a-z]", "", (x or "").lower())
    if n(a_last) != n(b_last):
        return False
    fa, fb = n(a_first), n(b_first)
    return not fa or not fb or fa[:3] == fb[:3]


def ghl_update_tags(r, st_first, st_last):
    """(extra GHL tags, what changed) for one lead.
      update lead info    the lead is on the error list -- update its GHL contact
      delete - dead lead  ... and it is now dead (sold / off every list)
      retrace             it was traced, but the person to trace has changed
                          since (a Personal Representative was appointed, the
                          owner changed) -- the old phones are the wrong person's"""
    tags, changed = [], OPEN_ERRORS.get(str(r.get("id"))) or ""
    if changed:
        tags.append("update lead info")
        if r.get("lead_status") == "dead":
            tags.append("delete - dead lead")
    inp = r.get("skiptrace_input") or {}
    if r.get("skiptrace_status") == "traced" and person_changed(r, st_first, st_last):
        tags.append("retrace")
        note = f"person to trace is now {st_first} {st_last} (traced {inp.get('first', '')} {inp.get('last', '')})"
        changed = f"{changed}; {note}" if changed else note
    elif r.get("retrace_reason") and r.get("skiptrace_status") == "traced":
        # 2026-10-16: a caller tagged the GHL contact "wrong person" (ghl_sync.py)
        tags.append("retrace")
        if r["retrace_reason"] not in changed:
            changed = f"{changed}; {r['retrace_reason']}" if changed else r["retrace_reason"]
    return tags, changed


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
        extra_tags, changed = ghl_update_tags(r, st_first, st_last)
        # 2026-10-16: a lead to retrace carries no phones on the lists (they
        # are the wrong person's), and numbers a caller reported wrong are
        # never exported again.
        retrace = "retrace" in extra_tags
        phones = [] if retrace else good_phones(r)
        to_trace = retrace or st_status != "traced"
        edit = trace_edit_note(r, st_first, st_last, st_address) if to_trace else ""
        w.writerow([
            call_brief(r, tags, raw, mst, reasons),
            ", ".join(GHL_LIST_OPTIONS[x] for x in ordered),
            ", ".join(owner_situation(r, mst)),
            decision,
            ", ".join(["list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[x].lower()).strip("-")
                       for x in ordered] + extra_tags),
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
            _clean_addr(probate.get("pr_address")) if "probate" in tags else "",
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
            *_owner2(r, tags),
            changed,
            *((phones + [""] * 5)[:5]),
            edit,
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
    "What Changed (update GHL)", "Edit Before Trace",
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


def needs_retrace(row):
    """Traced, but the person to call is wrong: the person to trace changed
    (Personal Representative appointed, owner changed) or a caller tagged the
    GHL contact "wrong person" (2026-10-16)."""
    if row.get("skiptrace_status") != "traced":
        return False
    if row.get("retrace_reason"):
        return True
    first, last, _, _ = compute_skiptrace_identity(row, list(row.get("source_tags") or []))
    return person_changed(row, first, last)


def retrace_is_same_search(row):
    """A retrace that would search the same name and address as last time
    (wrong person reported, but the system has no different person yet)."""
    if not needs_retrace(row):
        return False
    first, last, addr, _ = compute_skiptrace_identity(row, list(row.get("source_tags") or []))
    return same_search(row, first, last, addr)


def segment(row):
    """'dead' | 'wrong_info' | 'probate_pr' | 'deceased_hold' | 'main' for one
    house/land lead. dead / wrong_info come from the nightly QA check
    (qa_check.py) and win over everything, so those leads never reach a
    skip-trace upload."""
    status = (row.get("lead_status") or "").strip()
    if status in ("dead", "wrong_info"):
        return status
    # 2026-10-11: anything already pulled for skip trace never goes back on
    # the morning lists (skiptrace_intake.py marks it from her result files)
    st = (row.get("skiptrace_status") or "").strip()
    # 2026-10-14: ...unless the person to trace has changed since (a Personal
    # Representative was appointed, the owner changed). Those go back on the
    # pull list tagged "retrace" so they're traced with the day's new leads.
    if st == "traced" and not needs_retrace(row):
        return "skiptraced"
    # 2026-10-16 retrace guard: never put a lead back on a pull list when the
    # trace would search the same name and address again (same numbers back,
    # money burnt). It waits in not_skiptraced/ with a note instead.
    if st == "traced" and retrace_is_same_search(row):
        row["skiptrace_fix_note"] = ("caller reported the wrong person, but the system would search the same "
                                     "name and address again -- find the right person (or use a second "
                                     "provider) before paying for another trace")
        return "not_skiptraced"
    if st == "failed":
        return "fixed_unskiptraced" if row.get("skiptrace_fix_status") == "fixed" else "not_skiptraced"
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
        extra_tags, changed = ghl_update_tags(r, first, last)
        to_trace = "retrace" in extra_tags or st_status != "traced"
        edit = trace_edit_note(r, first, last, addr or r["address"]) if to_trace else ""
        w.writerow([
            first, last, name, role, street, city, st, z,
            re.sub(r"\s*,\s*", ", ", pb.get("decedent_name") or "").strip(" ,") or r.get("owner_name") or "",
            probate_heirs(pb, exclude_name=name, decedent_name=pb.get("decedent_name")),
            pb.get("date_of_death") or "", pb.get("case_number") or "",
            PROBATE_CASE_URL.format(case_number=pb["case_number"]) if pb.get("case_number") else "",
            r["address"], (r.get("city") or "").title(), r.get("state") or "SC", r.get("zip") or "",
            r.get("owner_name") or "",
            ", ".join(GHL_LIST_OPTIONS[x] for x in ordered),
            ", ".join(["list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[x].lower()).strip("-")
                       for x in ordered] + extra_tags),
            call_brief(r, tags, raw, mst, reasons),
            "***" if r.get("triple") else "", int(r.get("score") or 0), yn(r.get("is_vacant")),
            f"${float(amount):,.2f}" if re.match(r"^\d+(\.\d+)?$", str(amount)) else "",
            r.get("land_use") or "", r.get("pin") or "", "; ".join(reasons),
            {"traced": "Traced", "failed": "Failed"}.get(st_status, "No"),
            st_when.strftime("%m/%d/%Y") if hasattr(st_when, "strftime") else (str(st_when)[:10] if st_when else ""),
            today.isoformat(),
            changed,
            edit,
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


# ----------------------------------------------------- skip-trace folders
def _d(v):
    if hasattr(v, "strftime"):
        return v.strftime("%m/%d/%Y")
    return str(v)[:10] if v else ""


def months_since_trace(r):
    when = r.get("skiptrace_date")
    traced_month = when.strftime("%Y-%m") if hasattr(when, "strftime") else str(when or "")[:7]
    return len([m for m in (r.get("active_months") or []) if m > traced_month])


def is_repeat_offender(r):
    """Traced, and still a solid lead in a later month than it was traced."""
    return (r.get("skiptrace_status") == "traced") and months_since_trace(r) >= 1


def prefixed_csv(rows, today, headers, values, mark=None):
    """The normal call-list columns with extra columns in front. `mark`
    puts a "!" (repeat offender) or "#" (needs a person) in front of the
    owner name so it shows up in GHL too."""
    shown = []
    for r in rows:
        r2 = dict(r)
        m = mark(r) if mark else ""
        if m and r2.get("owner_name"):
            r2["owner_name"] = m + r2["owner_name"]
        shown.append(r2)
    body = list(csv.reader(io.StringIO(to_csv(shown, today))))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers + body[0])
    for r, line in zip(rows, body[1:]):
        w.writerow(values(r) + line)
    return buf.getvalue()


def error_rows(conn, days=ERROR_DAYS):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """select * from (
                 select distinct on (l.id) e.found_at, e.reason, l.*, false as triple
                 from lead_error_log e join leads l on l.id = e.lead_id
                 where e.found_at > now() - (%s || ' days')::interval
                   and not coalesce(e.dismissed, false)
                 order by l.id, e.found_at desc) x
               order by found_at desc, address""", (str(days),))
        return cur.fetchall()


def errors_csv(rows, today):
    """Already-pulled leads whose information changed -- update them in GHL."""
    return prefixed_csv(rows, today, ["Found On", "What Changed (update GHL)", "Skip-Traced?"],
                        lambda r: [_d(r.get("found_at")), r.get("reason") or "",
                                   {"traced": "Traced", "failed": "Not traced"}.get(r.get("skiptrace_status"), "")])


# ------------------------------------------------------- GHL upload file
# 2026-10-15: ONE file to import into GoHighLevel each day.
# 2026-10-16 (rebuilt, T Dawg's ask -- "no backtracking week over week"):
# what goes in it is decided by ghl_sync.plan_ghl_rows(), against what GHL
# actually holds (the GHL contact export she drops in the ghl-sync bucket):
#   RED     DELETE   dead -- import tags it "delete - dead lead"; bulk delete that tag
#   ORANGE  RETRACE  wrong/changed person and she must edit it first ("Edit
#                    Before Trace" says what) -- import tags the old contact
#                    "retrace"; bulk delete that tag; fix; trace
#   YELLOW  RETRACE  same, the right person is already in the Skiptrace columns
#   YELLOW  UPDATE   same person, something changed -- import updates it
#   GREEN   NEW      traced, alive, not in GHL -- import creates it
# Phone 1 is always the phone GHL has for the contact (the only thing its
# import matches on). Each change is in exactly ONE daily file (ghl_sent log),
# so importing every file once never re-creates a contact she deleted.
#   ghl_upload/ghl_upload.csv (+ .xlsx colored)   today's changes only
#   ghl_upload/ghl_upload_last_7_days.csv (+ .xlsx) everything from the last 7
#                                                 daily files, latest state --
#                                                 for a once-a-week import
#   ghl_upload/history/YYYY-MM-DD.csv            every day's file, kept
GHL_FILL = {"RED": "EA9999", "ORANGE": "F9CB9C", "YELLOW": "FFFF00", "GREEN": "B6D7A8"}
GHL_HEAD = ["Action", "Color", "Edit Before Trace", "Other Properties (same contact)"]
_DROPPED = re.compile(r"dropped off: ([^;]+)")
_NAME_TO_KEY = {v: k for k, v in GHL_LIST_OPTIONS.items()}
_NAME_TO_KEY.update({"Damage Permit": "insurance_damage"})


def _list_slug(key):
    return "list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[key].lower()).strip("-")


def ghl_row_tags(x, today):
    r = x["lead"]
    tags = [_list_slug(k) for k in BRIEF_ORDER if k in set(r.get("source_tags") or [])]
    if x["action"] == "DELETE":
        tags += ["update lead info", "delete - dead lead"]
    elif x["action"] == "RETRACE":
        tags += ["update lead info", "retrace"]
    elif x["action"] == "UPDATE":
        tags.append("update lead info")
        for m in _DROPPED.finditer(r.get("error") or ""):
            for name in m.group(1).split(","):
                k = _NAME_TO_KEY.get(name.strip())
                if k:
                    tags.append("dropped: " + _list_slug(k))
    else:
        tags.append("skiptraced")
    tags.append(f"ghl file {today:%Y-%m-%d}")
    return ", ".join(dict.fromkeys(tags))


def ghl_leads(rows):
    """The lead dicts plan_ghl_rows() needs: + error / error_at / retrace."""
    out, seen = [], set()
    for r in rows:
        if str(r.get("id")) in seen:
            continue
        seen.add(str(r.get("id")))
        tags = list(r.get("source_tags") or [])
        first, last, addr, _ = compute_skiptrace_identity(r, tags)
        d = dict(r)
        d["error"] = OPEN_ERRORS.get(str(r.get("id"))) or ""
        d["error_at"] = OPEN_ERRORS_AT.get(str(r.get("id")))
        rr = r.get("retrace_reason") or ""
        d["retrace_at"] = r.get("ghl_seen_at") if rr else None
        d["retrace"] = ({"who": " -- ".join(x for x in (f"{first} {last}".strip(), rr) if x),
                         "edit": trace_edit_note(r, first, last, addr)}
                        if r.get("lead_status") != "dead" and needs_retrace(r) else None)
        out.append(d)
    return out


def _ghl_lines(plan, today):
    body = list(csv.reader(io.StringIO(to_csv([x["lead"] for x in plan], today))))
    head = GHL_HEAD + body[0]
    i_tags, i_p1, i_edit = (COLUMNS.index("Tags"), COLUMNS.index("Phone 1"),
                            COLUMNS.index("Edit Before Trace"))
    lines = []
    for x, line in zip(plan, body[1:]):
        line[i_tags] = ghl_row_tags(x, today)
        line[i_p1:i_p1 + 5] = (list(x["phones"]) + [""] * 5)[:5]
        line[i_edit] = x["edit"]
        others = "; ".join(f"{o['lead'].get('address')} [{o['action']}]" for o in x.get("others") or [])
        lines.append([x["action"], x["color"], x["edit"], others] + line)
    return head, lines


def _xlsx(head, lines):
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
    except ImportError:
        return None
    wb = Workbook()
    ws = wb.active
    ws.title = "GHL upload"
    ws.append(head)
    for c in ws[1]:
        c.font = Font(bold=True)
    for line in lines:
        ws.append(line)
        fill = GHL_FILL.get(line[1])
        if fill:
            for c in ws[ws.max_row]:
                c.fill = PatternFill("solid", fgColor=fill)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


def _csv(head, lines):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(head)
    w.writerows(lines)
    return buf.getvalue()


def load_ghl_sent(conn):
    with conn.cursor() as cur:
        cur.execute("select lead_id, action, detail, sent_at, file_date from ghl_sent")
        return {(str(a), b, c): (d, f) for a, b, c, d, f in cur.fetchall()}


def ghl_upload_files(rows, sent_log, today):
    """-> (files, counts, keys to log as sent today)."""
    leads = ghl_leads(rows)
    sent = {k: v[0] for k, v in sent_log.items()}
    # a second export on the same day (the on-demand button) rebuilds the
    # whole day's file, not just what changed since the morning
    today_keys = {k for k, v in sent_log.items() if v[1] == today}
    daily = plan_ghl_rows(leads, {k: v for k, v in sent.items() if k not in today_keys})
    week_ago = today - timedelta(days=6)
    recent = {k for k, v in sent_log.items() if v[1] and v[1] >= week_ago}
    weekly = plan_ghl_rows(leads, {k: v for k, v in sent.items() if k not in recent})
    files = []
    for name, plan in (("ghl_upload/ghl_upload", daily), ("ghl_upload/ghl_upload_last_7_days", weekly)):
        head, lines = _ghl_lines(plan, today)
        files.append((name + ".csv", _csv(head, lines), "text/csv"))
        x = _xlsx(head, lines)
        if x:
            files.append((name + ".xlsx", x, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
        if plan is daily:
            files.append((f"ghl_upload/history/{today:%Y-%m-%d}.csv", _csv(head, lines), "text/csv"))
    keys = []
    for x in daily:
        for y in [x] + list(x.get("others") or []):
            keys.append((str(y["lead"]["id"]), y["action"], y["detail"]))
    counts = {c: sum(1 for x in daily if x["color"] == c) for c in ("RED", "ORANGE", "YELLOW", "GREEN")}
    counts["week"] = len(weekly)
    return files, counts, keys


def log_ghl_sent(conn, keys, today, now=None):
    """Record what went out in today's file. A NEW row makes the lead count
    as in GHL from then on (ghl_sync.ghl_state) until a GHL export says
    otherwise -- nothing on the lead itself is changed here, so the
    on-demand export later the same day rebuilds the identical file."""
    with conn.cursor() as cur:
        for lead_id, action, detail in keys:
            cur.execute("""insert into ghl_sent (lead_id, action, detail, file_date, sent_at)
                           values (%s, %s, %s, %s, coalesce(%s, now()))
                           on conflict (lead_id, action, detail)
                           do update set sent_at = excluded.sent_at, file_date = excluded.file_date""",
                        (lead_id, action, detail, today, now))


# ------------------------------------------------------- texting list
# 2026-10-13 (T Dawg's ask): the leads ready to text. Built from the
# skip-traced leads, keeping only phones dnc_scrub.py checked against a
# Do Not Call registry copy under 31 days old and found clean (not on the
# registry, never opted out). Leads whose person to trace has changed
# (retrace) and dead / wrong-information leads are left out.
TEXT_COLUMNS = ["First Name", "Last Name", "Phone 1", "Phone 2", "Phone 3", "Phone 4", "Phone 5",
                "Property Address", "Property City", "Property State", "Property Zip",
                "Mailing Street", "Mailing City", "Mailing State", "Mailing Zip",
                "Distress List", "Tags", "Owner Type", "Last Skip-Traced", "Phones Removed (DNC / opt-out / unchecked)"]
DNC_FRESH_DAYS = 31


def clean_phones(conn):
    """{phone: True if textable}. Missing table (scrub never ran) = nothing textable."""
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.phone_dnc')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute(f"""select phone, (on_dnc is false and not suppressed
                                         and registry_as_of > now() - interval '{DNC_FRESH_DAYS} days')
                        from phone_dnc""")
        return dict(cur.fetchall())


def text_ready_csv(conn, rows, today):
    ok = clean_phones(conn)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(TEXT_COLUMNS)
    n = 0
    for r in rows:
        if r.get("lead_status") or r.get("skiptrace_status") != "traced":
            continue
        tags = list(r.get("source_tags") or [])
        first, last, _, _ = compute_skiptrace_identity(r, tags)
        extra, _ = ghl_update_tags(r, first, last)
        if "retrace" in extra:
            continue
        phones = good_phones(r)  # 2026-10-16: numbers a caller reported wrong are dropped
        good = [p for p in phones if ok.get(p)]
        if not good:
            continue
        ordered = [x for x in BRIEF_ORDER if x in tags]
        street, mcity, mst, mzip = split_contact_address(r.get("mailing_address"))
        w.writerow([first, last] + (good + [""] * 5)[:5] + [
            r["address"], (r.get("city") or "").title(), r.get("state") or "SC", r.get("zip") or "",
            street, mcity, mst, mzip,
            ", ".join(GHL_LIST_OPTIONS[x] for x in ordered),
            ", ".join(["list-" + re.sub(r"[^a-z0-9]+", "-", GHL_LIST_OPTIONS[x].lower()).strip("-")
                       for x in ordered] + extra),
            owner_trace_names(r.get("owner_name"))["kind"], _d(r.get("skiptrace_date")),
            len(phones) - len(good)])
        n += 1
    return buf.getvalue(), n


def dnc_health(conn, today):
    """Lines + warnings for system_health.txt."""
    lines, warn = ["DO NOT CALL REGISTRY (texting list uses only phones checked against a copy under 31 days old)"], []
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.dnc_registry_files')")
        if cur.fetchone()[0] is None:
            return lines + ["  no registry files uploaded yet -- text_ready.csv stays empty", ""], \
                ["Upload this month's Do Not Call registry download to the dnc-registry bucket -- "
                 "until then the texting list (text_ready.csv) is empty."]
        cur.execute("""select code, max(uploaded_at) from dnc_registry_files, unnest(area_codes) code
                       group by code order by code""")
        rows = cur.fetchall()
    if not rows:
        warn.append("Upload this month's Do Not Call registry download to the dnc-registry bucket -- "
                    "until then the texting list (text_ready.csv) is empty.")
    for code, up in rows:
        age = (datetime.now(timezone.utc) - up).days
        lines.append(f"  area code {code}: uploaded {up:%m/%d/%Y} ({age} days old)")
        if age >= DNC_FRESH_DAYS:
            warn.append(f"Do Not Call file for area code {code} is {age} days old -- numbers in {code} are off "
                        f"the texting list until a new download is uploaded.")
        elif age >= DNC_FRESH_DAYS - 5:
            warn.append(f"Do Not Call file for area code {code} expires in {DNC_FRESH_DAYS - age} days -- "
                        f"download a fresh copy and upload it to dnc-registry.")
    return lines + [""], warn


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
    # 2026-10-16: how fresh the system's picture of GHL is (ghl_sync.py)
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.ghl_sync_files')")
        has = cur.fetchone()[0] is not None
        last = None
        if has:
            cur.execute("""select file_time, contacts, matched_leads, gone, wrong_number, wrong_person,
                                  unmatched_contacts from ghl_sync_files where file_time is not null
                           order by file_time desc limit 1""")
            last = cur.fetchone()
    lines.append("GHL CONTACT EXPORT (ghl-sync bucket -- how the system knows what GHL holds)")
    if last:
        age = (datetime.now(timezone.utc) - last[0]).days
        lines.append(f"  last export {last[0]:%m/%d/%Y} ({age} days old): {last[1]:,} contacts, {last[2]:,} leads "
                     f"matched, {last[3]:,} no longer in GHL, {last[4]} wrong-number tags, {last[5]} wrong-person "
                     f"tags, {last[6]} list contacts matching no lead")
        if age > 8:
            warn.append("GHL contact export is over a week old -- export all contacts from GHL and drop the file "
                        "in the ghl-sync bucket (the weekly routine), so the GHL file matches what's in GHL.")
    else:
        lines.append("  none yet -- export all contacts from GHL and drop the file in the ghl-sync bucket")
        warn.append("No GHL contact export yet -- export all contacts from GHL and drop the CSV in the ghl-sync "
                    "bucket once, so every GHL row matches the phone GHL actually has.")
    lines.append("")
    dl, dw = dnc_health(conn, today)
    lines += dl
    warn += dw
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
                   data=body if isinstance(body, bytes) else body.encode("utf-8"), timeout=120)
        r.raise_for_status()
        print(f"  uploaded {BUCKET}/{name} ({len(body):,} bytes)")


GHL_KEYS = []  # rows in today's GHL file -- logged as sent once the files are uploaded


def build(conn, today=None):
    today = today or date.today()
    OPEN_ERRORS.clear()
    OPEN_ERRORS.update(open_errors(conn))
    load_tax_sale_info(conn)
    out, counts = [], {}
    probate_pr, hold, wrong = [], [], []
    traced, not_traced, fixed = [], [], []
    all_rows = []
    for view in ("house_leads", "land_leads"):
        rows = lead_rows(conn, view)
        all_rows += rows
        main_rows = []
        for r in rows:
            seg = segment(r)
            {"probate_pr": probate_pr, "deceased_hold": hold, "wrong_info": wrong, "dead": [],
             "skiptraced": traced, "not_skiptraced": not_traced,
             "fixed_unskiptraced": fixed}.get(seg, main_rows).append(r)
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
    # 2026-10-11 skip-trace folders (see skiptrace_intake.py / skiptrace_followup.py)
    repeat = [r for r in traced if is_repeat_offender(r)]
    counts.update({"skiptraced_leads": len(traced), "repeat_offenders": len(repeat),
                   "not_skiptraced_leads": len(not_traced), "fixed_unskiptraced_leads": len(fixed)})
    out.append(("skiptraced/skiptraced_leads.csv", prefixed_csv(
        traced, today, ["Repeat Offender", "Months On Lists Since Traced", "Last Skip-Traced"],
        lambda r: ["!" if is_repeat_offender(r) else "", months_since_trace(r), _d(r.get("skiptrace_date"))],
        mark=lambda r: "! " if is_repeat_offender(r) else ""), "text/csv"))
    out.append(("repeat_offenders/repeat_offenders.csv", prefixed_csv(
        repeat, today, ["Months On Lists Since Traced", "Last Skip-Traced"],
        lambda r: [months_since_trace(r), _d(r.get("skiptrace_date"))], mark=lambda r: "! "), "text/csv"))
    out.append(("not_skiptraced/not_skiptraced_leads.csv", prefixed_csv(
        not_traced, today, ["Needs Human (#)", "Why It Can't Be Traced Yet / What To Check", "Times Traced"],
        lambda r: ["#", r.get("skiptrace_fix_note") or "", r.get("skiptrace_attempts") or 1],
        mark=lambda r: "# "), "text/csv"))
    out.append(("fixed_unskiptraced/fixed_unskiptraced_leads.csv", prefixed_csv(
        fixed, today, ["What Was Fixed", "Times Traced"],
        lambda r: [r.get("skiptrace_fix_note") or "", r.get("skiptrace_attempts") or 1]), "text/csv"))
    text_csv, n_text = text_ready_csv(conn, traced, today)
    counts["text_ready_leads"] = n_text
    out.append(("text_ready/text_ready.csv", text_csv, "text/csv"))
    err = error_rows(conn)
    counts["errors_last_30_days"] = len(err)
    out.append(("errors/errors.csv", errors_csv(err, today), "text/csv"))
    # 2026-10-14: the same error list split the two ways it goes into GHL --
    # update_in_ghl.csv (still a lead: import with "update existing contacts")
    # and delete_in_ghl.csv (now dead: import to tag, then bulk delete).
    upd = [r for r in err if r.get("lead_status") != "dead"]
    dele = [r for r in err if r.get("lead_status") == "dead"]
    counts["errors_update_in_ghl"] = len(upd)
    counts["errors_delete_in_ghl"] = len(dele)
    out.append(("errors/update_in_ghl.csv", errors_csv(upd, today), "text/csv"))
    out.append(("errors/delete_in_ghl.csv", errors_csv(dele, today), "text/csv"))
    # 2026-10-16: the GHL file is planned from every lead (live and dead)
    # against what GHL holds -- see ghl_sync.py.
    g_files, g_counts, g_keys = ghl_upload_files(all_rows + list(dead), load_ghl_sent(conn), today)
    GHL_KEYS[:] = g_keys
    counts.update({"ghl_upload_red_delete": g_counts["RED"], "ghl_upload_orange_retrace_edit": g_counts["ORANGE"],
                   "ghl_upload_yellow": g_counts["YELLOW"], "ghl_upload_green_new": g_counts["GREEN"],
                   "ghl_upload_last_7_days": g_counts["week"]})
    out += g_files
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
    # skip-trace pipeline columns + error log (skiptrace_intake.py, 2026-10-11)
    from skiptrace_intake import ensure_columns as ensure_skiptrace_columns
    ensure_skiptrace_columns(conn)
    ensure_ghl_columns(conn)
    ensure_schema(conn)
    conn.commit()
    files, warn = build(conn)
    upload(files, key)
    # only after the files are safely uploaded: today's GHL rows count as sent
    log_ghl_sent(conn, GHL_KEYS, date.today())
    conn.commit()
    conn.close()
    print(files[-1][1])
    for w in warn:
        print(f"::warning::{w}")


if __name__ == "__main__":
    main()
