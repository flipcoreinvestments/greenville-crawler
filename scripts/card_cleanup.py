#!/usr/bin/env python3
"""
Card cleanup + the callable list (Tala, 2026-11-15).

Tala's orders:
  - "No bad numbers, no dead leads, no DNC numbers on my cards." Every number on a GHL card
    that is marked bad (her call results, numbers she deleted, earlier feedback), on the
    National Do Not Call list, or on the never-call list comes OFF the card, even when she
    typed it in herself. Phone 1 becomes the best good number left.
  - No dead or weak leads added: a NEW card goes into GHL only when its lead is verified
    (Confidence >= CALLABLE_MIN_CONF, not dead) and its best good number is graded A, B or C.
    Rule 19: when she asks for an amount, it is that many NEW leads, best call rating and
    Confidence first (morbius_sync ranks them and counts the amount down).
  - No "bad number" tags on cards (her Nov 14 order).
  - Rule 23: every number on the master "Phone Numbers - Bad Numbers" list counts as bad.
  - DNC rule: a contact stays in GHL as Do Not Contact ONLY if they told us themselves not
    to call (DND or a dnc/opt-out tag on the card). A lead whose only numbers are on the
    National Do Not Call list, and who never asked us anything, is removed from GHL.

Pure functions only (no network, no database); morbius_sync.py does the writes.
"""
import phone_grade as pg

CALLABLE_MIN_CONF = 45      # county / court confirmed; PropStream-only leads top out at 40
CALLABLE_GRADES = ("A", "B", "C")
NATIONAL_DNC_REASON = "national DNC only (never asked us not to call): removed per Tala's Nov 15 DNC rule"


def remove_set(card, bad, on_dnc, blocked):
    """Numbers on the card that must come off. card: {'Phone 1': digits, ...}.
    -> {digits: reason} with reason 'bad', 'dnc' or 'never call'."""
    out = {}
    for n in card.values():
        if n in blocked:
            out[n] = "never call"
        elif n in bad:
            out[n] = "bad"
        elif n in on_dnc:
            out[n] = "dnc"
    return out


def plan(contact, cf, remove, good):
    """What to change on one card.
    remove: {digits: reason} from remove_set; good: the lead's good numbers, best first
    (already without bad / DNC / never-call numbers).
    -> None when nothing to do, else dict(phone=<new Phone 1, '' to clear, or None to keep>,
       clear=[custom field names to empty], removed={digits: reason}, fill={field: number})."""
    nums = pg.card_numbers(contact, cf)
    hit = {label: n for label, n in nums.items() if n in remove}
    good = [p for p in good if p and p not in remove]
    kept = [n for label, n in nums.items() if label not in hit]
    if not hit:
        # nothing bad on the card; only fix Phone 1 when it's empty and a good number exists
        if "Phone 1" not in nums and good:
            return {"phone": good[0], "clear": [], "removed": {}, "fill": {}}
        return None
    new_phone = None
    if "Phone 1" in hit:
        # best good number not already sitting in another slot, else any good number, else clear
        spare = [p for p in good if p not in kept]
        new_phone = spare[0] if spare else (good[0] if good else "")
    clear = sorted(label.lower() for label in hit if label != "Phone 1")
    return {"phone": new_phone, "clear": clear, "removed": {n: remove[n] for n in hit.values()}, "fill": {}}


def update_body(p, fields):
    """GHL PUT /contacts body for a plan (Phone 1 clearing is sent separately, see phone_clear)."""
    body = {}
    if p.get("phone"):
        body["phone"] = p["phone"]
    cfs = [{"id": fields[name], "field_value": ""} for name in p.get("clear") or [] if name in fields]
    if cfs:
        body["customFields"] = cfs
    return body


def phone_clear(p):
    """True when Phone 1 must be emptied (it was bad and no good number is left)."""
    return p is not None and p.get("phone") == ""


def callable_status(good_graded, conf, dead=False):
    """good_graded: [(phone, grade)] best first, only good numbers. -> (callable?, reason)."""
    if dead:
        return False, "dead lead"
    if not good_graded:
        return False, "no good number left"
    best = good_graded[0][1]
    if best not in CALLABLE_GRADES:
        return False, f"best number is grade {best}"
    if conf < CALLABLE_MIN_CONF:
        return False, f"Confidence {conf} is under {CALLABLE_MIN_CONF} (not verified)"
    return True, f"Confidence {conf}, best number grade {best}"


def national_dnc_only(lead_numbers, on_dnc, personal_opt_out, talked):
    """lead_numbers: the lead's traced numbers that aren't marked bad.
    True when every one of them is on the National DNC list and the person never told us
    anything themselves (no DND / opt-out on the card, never talked) -> remove from GHL."""
    nums = [n for n in lead_numbers if n]
    if personal_opt_out or talked or not nums:
        return False
    return all(n in on_dnc for n in nums)


def note_text(p):
    labels = {"bad": "marked bad", "dnc": "on the Do Not Call list", "never call": "never-call list"}
    parts = [f"{n[:3]}-{n[3:6]}-{n[6:]} ({labels.get(r, r)})" for n, r in sorted(p["removed"].items())]
    tail = " Phone 1 is now the best good number." if p.get("phone") else ""
    if p.get("phone") == "":
        tail = " No good number is left on this card."
    return "Removed from this card: " + ", ".join(parts) + "." + tail
