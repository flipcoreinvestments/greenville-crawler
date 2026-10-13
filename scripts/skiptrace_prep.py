"""
skiptrace_prep.py

THE "WHY DID SKIP TRACE RETURN THE DEAD PERSON'S NUMBER" FIX, plus the same
fix for land leads that have no mailable property address at all.

export_call_lists.py's house_leads.csv / land_leads.csv is what T Dawg
uploads straight into PropStream / BatchData / BatchDialer -- whatever is in
"Owner Name" + "Property Address" there is exactly what that platform
searches for a phone number. For two lead types that's the wrong search:

  - PROBATE: the owner of record is kept as the DECEASED on purpose (see
    probate_ingest.py / the standing "keep the dead person's name" rule).
    Searching the decedent's name only ever finds their old, disconnected
    number. The person who can actually answer is the Personal
    Representative -- probate_ingest.py's parse_pr() already captures this
    at raw->probate->pr_name/pr_address once match_owners_to_estates() or
    backfill_pr() has run.
  - LAND WITH NO HOUSE NUMBER: a property address like "Watson Dr" (no
    house number -- see is_untraceable_address() below) isn't a mailable
    address at all, so no skip-trace platform can search it. The owner's
    OWN mailing address (where their tax bill goes) is what should be
    searched instead.

This computes that swap one row at a time, for a SEPARATE set of
"Skiptrace *" columns export_call_lists.py adds to the CSV -- it never
touches "Property Address" / "Owner Name", which stay accurate for calling
and for T Dawg's own reference. Every other lead type (tax sale,
foreclosure, code violation, etc.) is untouched: Skiptrace columns just
mirror Owner Name / Property Address.

Note: is_untraceable_address() is a DIFFERENT test from the land_leads /
house_leads split in lead_common.py (that split is by the county's own
land_use code). A land_leads row can still have a perfectly good numbered
address; this only swaps the trace identity when there's genuinely no
house number to search on, whichever list the row is in.
"""

import re

from lead_common import owner_is_entity
from name_order import decide_order

NO_PR_NOTE = ("no personal representative on file yet -- any phone/email found belongs to the "
              "deceased owner, not a reachable person")
PR_NO_ADDRESS_NOTE = "no address on file for the personal representative -- trace may come back empty"
PRE_PROBATE_NOTE = ("owner on title reported deceased and no estate filed yet -- any phone found may be the "
                     "deceased's; look for the heirs/family before calling")
LAND_NO_MAILING_NOTE = "no mailing address on file -- this lead cannot be skip-traced at all until one is found"


def is_untraceable_address(address):
    """
    No house number (e.g. "Watson Dr", "Beechwood Dr") -- there's no
    mailbox a skip-trace/dialer platform can search by. Confirmed 2026-10-09
    against live data: a real slice of tax-sale-tagged vacant land looks
    exactly like this.
    """
    a = (address or "").strip()
    if not a:
        return True
    return not bool(re.match(r"^[0-9]", a))


def split_name(name):
    """
    County records write 'LASTNAME FIRSTNAME MIDDLE' (space-separated); the
    probate court's Personal Representative field comes back 'LAST ,
    FIRST MIDDLE' (comma-separated, per probate_ingest.py's parse_pr()).
    Normalizes both to (first, last). house_leads/land_leads are built from
    active_leads, which already excludes business-owned parcels (see
    lead_common.py), but this still backs off safely if an entity name ever
    reaches here -- the whole name goes in `last`, blank `first`.
    """
    if not name:
        return "", ""
    if owner_is_entity(name):
        return "", name.strip()
    n = re.sub(r"\s*,\s*", " ", name.strip())
    n = re.sub(r"\s+", " ", n)
    toks = n.split()
    if not toks:
        return "", ""
    if len(toks) == 1:
        return "", toks[0].title()
    # 2026-10-13: most sources write LAST FIRST, but some names are typed
    # FIRST LAST ("LINDA HOOD") -- name_order.py checks and flips them.
    first, last = decide_order(toks)
    return first.title(), last.title()


# --------------------------------------------------------------------------
# PROBATE CONTACT + HEIRS (added 2026-10-10, T Dawg's ask: "make the PR the
# contact name so I skip trace the right person, keep the deceased's name in
# its own column, and list the heirs").
#
# The county case page's "Parties Involved" block is the only public list of
# people on an estate. Party types seen across all 181 live probate leads:
# Personal Representative, Creditor of the estate, Attorney, Affiant,
# Applicant, Respondent, Petitioner, Also Known As, Special Administrator,
# Payor of court fees, Agent, Bondsmen, Successor Personal Representative,
# Other, Demandant. The county site has NO "Heir" party type -- the full
# heir list is inside the filed papers (application / small-estate
# affidavit), not online. So "Heirs" here = the family-side people the case
# file names (co-PRs, affiants, applicants, petitioners, respondents...),
# each with their role. Attorneys, creditors, bond companies, agents and
# fee payors are never listed -- they aren't family and calling them wastes
# a trace.
# --------------------------------------------------------------------------
_PARTY_RE = re.compile(
    r"Name:?\s*((?:(?!Name:|Party Type).)*?)\s*Party Type:?\s*((?:(?!Address|Name:).)*?)\s*"
    r"(?:Address:?\s*((?:(?!Name:).)*?))?\s*(?=Name:|$)", re.I)

# who to call, best first. Successor PR replaced the original PR.
CONTACT_ROLES = ["successor personal representative", "personal representative",
                 "special administrator", "affiant", "applicant", "petitioner"]
# family-side roles worth listing as heirs (everything in CONTACT_ROLES too)
FAMILY_ROLES = set(CONTACT_ROLES) | {"respondent", "other", "demandant"}
ROLE_LABELS = {
    "successor personal representative": "Successor Personal Representative",
    "personal representative": "Personal Representative",
    "special administrator": "Special Administrator",
    "affiant": "Affiant (small-estate filer)",
    "applicant": "Applicant",
    "petitioner": "Petitioner",
    "respondent": "Respondent",
    "other": "Other party",
    "demandant": "Demandant",
}


def _clean_person(name):
    return re.sub(r"\s+", " ", re.sub(r"\s*,\s*", ", ", name or "")).strip(" ,")


# 2026-10-13: the last party on a case page runs into the court site's footer
# ("... GREENVILLE SC 29605 County Home \u2022 Browser Information \u2022 Legal
# Notice ..."). Cut the address off where the footer starts.
_FOOTER_RE = re.compile(r"\s*(County Home|Browser Information|Legal Notice|Privacy Statement|Contact Us)\b.*$", re.I | re.S)


def _clean_addr(addr):
    return _FOOTER_RE.sub("", re.sub(r"\s+", " ", addr or "")).strip(" ,\u2022")


def parse_parties(parties_text):
    """'( 3 ) Name: DOE , JANE Party Type: Personal Representative Address: 1 MAIN ST ...'
    -> [{'name': 'DOE, JANE', 'role': 'personal representative', 'address': '1 MAIN ST ...'}, ...]"""
    out = []
    for m in _PARTY_RE.finditer(parties_text or ""):
        name = _clean_person(m.group(1))
        role = re.sub(r"\s+", " ", (m.group(2) or "")).strip().lower()
        if not name or not role:
            continue
        out.append({"name": name, "role": role, "address": _clean_addr(m.group(3))})
    return out


def probate_contact(probate):
    """The person to skip trace on a probate lead: (name, role_label, address)
    or (None, None, None). Uses the parsed PR first (raw.probate.pr_name), then
    the parties block for a successor PR / special administrator / affiant /
    applicant / petitioner."""
    probate = probate or {}
    parties = parse_parties(probate.get("parties"))
    for role in CONTACT_ROLES:
        for p in parties:
            if p["role"] == role and not owner_is_entity(p["name"]):
                if role == "personal representative" and probate.get("pr_name"):
                    # the parsed PR (same person; keeps one spelling everywhere)
                    return (_clean_person(probate["pr_name"]), ROLE_LABELS[role],
                            _clean_addr(probate.get("pr_address")) or p["address"])
                return p["name"], ROLE_LABELS[role], p["address"]
    if probate.get("pr_name"):
        return _clean_person(probate["pr_name"]), ROLE_LABELS["personal representative"], _clean_addr(probate.get("pr_address"))
    return None, None, None


def probate_heirs(probate, exclude_name=None, decedent_name=None):
    """'DOE, JOHN (Applicant); DOE, MARY (Respondent)' -- every family-side
    person on the case file except the contact already in its own column and
    the decedent. Blank when the file names nobody else."""
    skip = {_clean_person(exclude_name).upper(), _clean_person(decedent_name).upper()} - {""}
    seen, out = set(), []
    for p in parse_parties((probate or {}).get("parties")):
        if p["role"] not in FAMILY_ROLES or owner_is_entity(p["name"]):
            continue
        key = p["name"].upper()
        if key in skip or key in seen:
            continue
        seen.add(key)
        out.append(f"{p['name']} ({ROLE_LABELS.get(p['role'], p['role'].title())})")
    return "; ".join(out)


# --------------------------------------------------------------------------
# OWNER NAME -> SKIP-TRACE NAME (added 2026-10-11, T Dawg's ask: "some are
# LLCs, some have 7 names, some are trusts -- how do I search that as a name?")
#
# The county writes owners LAST FIRST MIDDLE, glues co-owners on with "&" or
# ",", tags ownership types in parentheses ((JTWROS), (L-EST), (SURV)...), and
# cuts the whole field off at ~30 characters. A skip-trace platform needs ONE
# person's first + last name. Rules:
#   person        first owner listed = the person to trace; the second owner
#                 goes in the Owner 2 columns (a spouse/co-owner is a second
#                 shot at a working number)
#   trust         a trust named for a person ("Jenkins Lonnie Revocable
#                 Trust") is traced as that person -- usually the trustee
#                 and owner. A family-name trust ("Anderson Family Living
#                 Trust") has only a last name to search; the address does
#                 the rest.
#   company       LLC/Inc/etc. has no person to search by name: first name
#                 blank, the full company name in last name (BatchData's
#                 guidance: include the full business name). These leads are
#                 already kept off the call lists (business_owned_leads).
# --------------------------------------------------------------------------
_TAG_RE = re.compile(r"\([^)&]*\)?|\bETAL\b|\bET AL\b|\bL-?EST\b|\bJTWROS\b|\bA/K/A\b.*$|\bAKA\b.*$", re.I)
_TRUST_WORDS = re.compile(
    r"\b(REVOCABLE|IRREVOCABLE|REVOC|REV|LIVING|FAMILY|TRUSTEES?|TRUST|TR|TRS|U/W|ESTATE OF|ESTATE|"
    r"THE|DECLARATION|DATED|DTD|AGREEMENT|B|LIFE|MARITAL|BYPASS|CREDIT|SHELTER|SURVIVORS?|QTIP)\b\.?", re.I)
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}
_PARTICLES = {"DE", "DEL", "DELA", "DA", "DI", "DOS", "VAN", "VON", "LA", "LE", "ST", "MAC", "O"}
# organizations the county lists that aren't in the business-owner word list
# (kept local so the call-list split in lead_common.py doesn't change)
_ORG_RE = re.compile(r"\b(HOUSING|CONNECTIONS|FOUNDATION|SERVICES|ASSOCIATES|FUND|VETERANS|AFFAIRS|"
                     r"HOMEOWNERS|ADMIN|SOCIETY|CLUB|LODGE|CENTER|ACADEMY|CHILDREN|GRANDCHILDREN|HEIRS)\b", re.I)


def _person_from_county(chunk, fallback_last=""):
    """'Ables Milton Edward Jr' -> ('Milton', 'Ables'). 'David M' with a
    fallback last name (co-owner listed without a surname, 'Butler William M
    & David M') -> ('David', 'Butler')."""
    toks = [t for t in re.findall(r"[A-Za-z][A-Za-z'\-]*", chunk or "") if t.upper().strip(".") not in _SUFFIXES]
    if not toks:
        return "", ""
    # "De Jesus Claudia" -> last name "De Jesus"
    while len(toks) > 2 and toks[0].upper() in _PARTICLES:
        toks = [toks[0] + " " + toks[1]] + toks[2:]
    if fallback_last and (len(toks) == 1 or (len(toks) == 2 and len(toks[1]) == 1)):
        return toks[0].title(), fallback_last.title()
    if len(toks) == 1:
        return "", toks[0].title()
    # 2026-10-13: most sources write LAST FIRST, but some names are typed
    # FIRST LAST ("LINDA HOOD") -- name_order.py checks and flips them.
    first, last = decide_order(toks)
    return first.title(), last.title()


def owner_trace_names(owner_name):
    """-> dict(first, last, first2, last2, kind) for the skip-trace upload."""
    raw = (owner_name or "").strip()
    out = {"first": "", "last": "", "first2": "", "last2": "", "kind": ""}
    if not raw:
        return out
    if owner_is_entity(raw) or _ORG_RE.search(raw):
        out.update(last=re.sub(r"\s+", " ", raw),
                   kind="Trust" if re.search(r"\bTRUST", raw, re.I) else "Company")
        return out
    # probate-court style "OLIVER , STEPHEN LEE" (LAST , FIRST): one person
    m = re.match(r"^\s*([A-Za-z'\-]+)\s*,\s*([A-Za-z].*)$", raw)
    if m and "&" not in raw and "," not in m.group(2):
        # comma format is explicit: the first word after the comma is the first name
        given = [t for t in re.findall(r"[A-Za-z][A-Za-z'\-]*", m.group(2)) if t.upper().strip(".") not in _SUFFIXES]
        f = given[0].title() if given else ""
        out.update(first=f, last=m.group(1).title(), kind="Person")
        return out
    name = _TAG_RE.sub(" ", raw)
    # leftovers of a hand-deleted "(JTWROS)" tag: "os)", "OS"
    name = re.sub(r"\b(?:os|OS)\)|\s(?:os|OS)\b|\)", " ", name)
    # county mashed two owner lines together ("Frisbee Betty DFrisbee Robert F")
    name = re.sub(r"([a-z])([A-Z][a-z])", r"\1 & \2", name)
    chunks = [c.strip() for c in re.split(r"\s*[&,]\s*", name) if c.strip()]
    is_trust = bool(re.search(r"\b(TRUST|TRUSTEE|U/W|REVOC)", raw, re.I))
    first_chunk = chunks[0] if chunks else ""
    if is_trust:
        first_chunk = _TRUST_WORDS.sub(" ", first_chunk)
    f1, l1 = _person_from_county(first_chunk)
    out.update(first=f1, last=l1, kind="Trust" if is_trust else "Person")
    if len(chunks) > 1:
        second = _TRUST_WORDS.sub(" ", chunks[1]) if is_trust else chunks[1]
        toks = re.findall(r"[A-Za-z][A-Za-z'\-]*", second)
        # skip a co-owner the county cut off mid-name ("Ledfo", "Jenn", "B")
        if toks and not (len(toks) == 1 and len(toks[0]) <= 4 and toks[0].upper() != l1.upper()):
            f2, l2 = _person_from_county(second, fallback_last=l1)
            if f2 and (f2, l2) != (f1, l1):
                out.update(first2=f2, last2=l2)
    return out


def compute_skiptrace_identity(r, tags):
    """
    r: one row dict from house_leads/land_leads (needs owner_name, address,
    mailing_address, raw). tags: the row's source_tags.

    Returns (first, last, address, note) -- `address` is the full trace
    address (street + whatever city/state/zip came with it, same shape as
    the existing "Mailing Address" field so split_mailing() can still pull
    state/zip off the end). `note` is None or a short phrase to fold into
    the Call Brief's "NEED TO VERIFY LEAD" text when this lead can't get
    the normal treatment.
    """
    tags = set(tags or [])
    owner_name = r.get("owner_name") or ""
    property_address = r.get("address") or ""
    mailing_address = r.get("mailing_address") or ""
    raw = r.get("raw") or {}
    probate = raw.get("probate") or {}

    if "probate" in tags:
        # 2026-10-10: PR, or a successor PR / special administrator / affiant /
        # applicant from the case file when no PR is named (probate_contact).
        pr_name, _role, pr_address = probate_contact(probate)
        if pr_name:
            first, last = split_name(pr_name)
            pr_address = pr_address or ""
            if pr_address:
                return first, last, pr_address, None
            return first, last, property_address, PR_NO_ADDRESS_NOTE
        n = owner_trace_names(owner_name)
        return n["first"], n["last"], property_address, NO_PR_NOTE

    if "pre_probate" in tags:
        # 2026-10-10: PropStream says the owner died but no Greenville probate
        # case exists yet, so there's no Personal Representative to trace.
        # Trace the owner of record as before, but warn in the Call Brief.
        n = owner_trace_names(owner_name)
        first, last = n["first"], n["last"]
        addr = property_address if not is_untraceable_address(property_address) else mailing_address
        return first, last, addr, PRE_PROBATE_NOTE

    if is_untraceable_address(property_address):
        n = owner_trace_names(owner_name)
        first, last = n["first"], n["last"]
        if mailing_address:
            return first, last, mailing_address, None
        return first, last, "", LAND_NO_MAILING_NOTE

    n = owner_trace_names(owner_name)
    return n["first"], n["last"], property_address, None
