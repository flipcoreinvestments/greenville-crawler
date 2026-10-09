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

NO_PR_NOTE = ("no personal representative on file yet -- any phone/email found belongs to the "
              "deceased owner, not a reachable person")
PR_NO_ADDRESS_NOTE = "no address on file for the personal representative -- trace may come back empty"
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
    return toks[1].title(), toks[0].title()


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
        pr_name = probate.get("pr_name")
        if pr_name:
            first, last = split_name(pr_name)
            pr_address = probate.get("pr_address") or ""
            if pr_address:
                return first, last, pr_address, None
            return first, last, property_address, PR_NO_ADDRESS_NOTE
        first, last = split_name(owner_name)
        return first, last, property_address, NO_PR_NOTE

    if is_untraceable_address(property_address):
        first, last = split_name(owner_name)
        if mailing_address:
            return first, last, mailing_address, None
        return first, last, "", LAND_NO_MAILING_NOTE

    first, last = split_name(owner_name)
    return first, last, property_address, None
