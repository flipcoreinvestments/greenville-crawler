#!/usr/bin/env python3
"""
Never-call numbers (Tala, 2026-11-14).

A number on the never-call list (Supabase table phone_suppression: STOP replies,
"wrong number" texts, anything Tala says must never be called again) must not stay
on any GHL card. The database already marks such a number bad on every lead
(trigger trg_block_suppressed_phones), so the sync never writes it again. This
module cleans up cards that still carry one:

  - a blocked number in Phone 2-5 is cleared;
  - a blocked Phone 1 is replaced by the best good number left on the lead;
  - when no good number is left, the contact gets Do Not Disturb (all channels),
    so the dialer and texting can't reach the blocked number.

Contact-level DND is only used when nothing good is left, because GHL's DND covers
the whole contact: turning it on for a card that still has a good number would
also stop calls to the real owner.

Pure functions only (no network, no database); morbius_sync.py does the writes.
"""
import phone_grade as pg

NEVER_CALL_TAG = "never call number"


def plan(contact, cf, blocked, good):
    """What to change on one card.

    contact: GHL contact dict; cf: {custom field name lower: value};
    blocked: set of 10-digit never-call numbers; good: the lead's good numbers,
    best first (already excluding bad and Do Not Call numbers).
    -> None when nothing to do, else dict(phone=<new Phone 1 or None>,
       clear=[custom field names to empty], dnd=bool, removed=[numbers]).
    """
    nums = pg.card_numbers(contact, cf)
    hit = {label: n for label, n in nums.items() if n in blocked}
    if not hit:
        return None
    good = [p for p in good if p and p not in blocked]
    clear = sorted(label.lower() for label in hit if label != "Phone 1")
    new_phone = None
    dnd = False
    if "Phone 1" in hit:
        if good:
            new_phone = good[0]
        else:
            dnd = True
    # nothing good left anywhere on the card -> DND as well
    left = [n for label, n in nums.items() if label not in hit]
    if not good and not left:
        dnd = True
    if dnd and (contact or {}).get("dnd") and not clear and not new_phone:
        return None  # already blocked in GHL, nothing left to clean
    return {"phone": new_phone, "clear": clear, "dnd": dnd, "removed": sorted(set(hit.values()))}


def update_body(p, fields):
    """GHL PUT /contacts body for a plan. fields: {custom field name lower: id}."""
    body = {}
    if p.get("phone"):
        body["phone"] = p["phone"]
    cfs = [{"id": fields[name], "field_value": ""} for name in p.get("clear") or [] if name in fields]
    if cfs:
        body["customFields"] = cfs
    if p.get("dnd"):
        body["dnd"] = True
    return body


def note_text(p):
    nums = ", ".join(f"{n[:3]}-{n[3:6]}-{n[6:]}" for n in p["removed"])
    if p.get("dnd"):
        return (f"Never-call number on this card ({nums}). No good number is left, so Do Not Disturb "
                f"is on. Do not call or text this contact unless a new, checked number is added.")
    return f"Never-call number removed from this card ({nums}). Do not call or text it."
