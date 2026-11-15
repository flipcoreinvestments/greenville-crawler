#!/usr/bin/env python3
"""
Checks for the card cleanup + callable list (Tala, 2026-11-15).
Synthetic numbers only (the repo is public). No network, no database.

    python tests/test_card_cleanup.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import card_cleanup as cc  # noqa: E402

fails = []


def check(label, cond):
    if not cond:
        fails.append(label)


A, B, C, BAD, DNC, X = "5550000001", "5550000002", "5550000003", "5550000004", "5550000005", "5550000099"
FIELDS = {"phone 2": "f2", "phone 3": "f3", "phone 4": "f4", "phone 5": "f5"}

# remove_set: bad, DNC and never-call numbers are all flagged; good ones are not
card = {"Phone 1": BAD, "Phone 2": DNC, "Phone 3": X, "Phone 4": A}
rs = cc.remove_set(card, {BAD}, {DNC}, {X})
check("remove_set reasons", rs == {BAD: "bad", DNC: "dnc", X: "never call"})

# 1. clean card -> nothing to do
check("clean card", cc.plan({"phone": "+1" + A}, {"phone 2": B}, {}, [A, B]) is None)

# 2. bad Phone 1 -> replaced by the best good number not already on the card
p = cc.plan({"phone": "+1" + BAD}, {"phone 2": A}, {BAD: "bad"}, [A, B])
check("bad phone 1 replaced by spare good", p and p["phone"] == B and p["clear"] == [])

# 3. bad Phone 1, only good number already in Phone 2 -> still promoted to Phone 1
p = cc.plan({"phone": "+1" + BAD}, {"phone 2": A}, {BAD: "bad"}, [A])
check("bad phone 1 replaced by phone 2 number", p and p["phone"] == A)

# 4. DNC in Phone 3 (her own edit) -> cleared; Phone 1 untouched
p = cc.plan({"phone": "+1" + A}, {"phone 3": DNC}, {DNC: "dnc"}, [A])
check("dnc phone 3 cleared", p and p["phone"] is None and p["clear"] == ["phone 3"] and p["removed"] == {DNC: "dnc"})

# 5. every number bad, nothing good left -> Phone 1 emptied, slots cleared
p = cc.plan({"phone": "+1" + BAD}, {"phone 2": DNC}, {BAD: "bad", DNC: "dnc"}, [])
check("all bad -> phone 1 cleared", cc.phone_clear(p) and p["clear"] == ["phone 2"])
check("note says no good number", "No good number" in cc.note_text(p))

# 6. empty Phone 1 with a good number available -> filled
p = cc.plan({}, {}, {}, [A])
check("empty phone 1 filled", p and p["phone"] == A)

# 7. update body never sends an empty phone (that goes separately)
body = cc.update_body({"phone": "", "clear": ["phone 2"], "removed": {}}, FIELDS)
check("body has no empty phone", "phone" not in body and body["customFields"] == [{"id": "f2", "field_value": ""}])

# callable_status
check("A + verified = callable", cc.callable_status([(A, "A")], 60)[0])
check("C + verified = callable", cc.callable_status([(A, "C")], 45)[0])
check("D best = not callable", not cc.callable_status([(A, "D")], 80)[0])
check("ungraded = not callable", not cc.callable_status([(A, "?")], 80)[0])
check("low confidence = not callable", not cc.callable_status([(A, "A")], 20)[0])
check("no number = not callable", not cc.callable_status([], 90)[0])
check("dead = not callable", not cc.callable_status([(A, "A")], 90, dead=True)[0])

# national_dnc_only
check("all on national DNC -> remove", cc.national_dnc_only([DNC], {DNC}, False, False))
check("one clean number -> keep", not cc.national_dnc_only([DNC, A], {DNC}, False, False))
check("personal opt-out -> keep as DNC", not cc.national_dnc_only([DNC], {DNC}, True, False))
check("talked to them -> keep", not cc.national_dnc_only([DNC], {DNC}, False, True))
check("no numbers -> not a DNC case", not cc.national_dnc_only([], {DNC}, False, False))

if fails:
    print("FAIL: " + "; ".join(fails))
    sys.exit(1)
print(f"card cleanup: all checks passed")
