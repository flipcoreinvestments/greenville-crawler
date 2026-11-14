#!/usr/bin/env python3
"""
Checks for the never-call cleanup (Tala, 2026-11-14).
Synthetic numbers only (the repo is public). No network, no database.

    python tests/test_never_call.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import never_call as nc  # noqa: E402

fails = []


def check(label, cond):
    if not cond:
        fails.append(label)


A, B, C, X = "5550000001", "5550000002", "5550000003", "5550000099"
blocked = {X}
FIELDS = {"phone 2": "f2", "phone 3": "f3", "phone 4": "f4", "phone 5": "f5"}

# 1. nothing blocked on the card -> nothing to do
check("clean card", nc.plan({"phone": "+1" + A}, {"phone 2": B}, blocked, [A, B]) is None)

# 2. blocked number in Phone 3, good Phone 1 -> clear Phone 3 only, no DND (would stop the real owner)
p = nc.plan({"phone": "+1" + A}, {"phone 2": B, "phone 3": X}, blocked, [A, B])
check("phone 3 cleared", p and p["clear"] == ["phone 3"] and not p["dnd"] and p["phone"] is None)
body = nc.update_body(p, FIELDS)
check("phone 3 body", body == {"customFields": [{"id": "f3", "field_value": ""}]})

# 3. blocked Phone 1, a good number left -> Phone 1 replaced, no DND
p = nc.plan({"phone": "+1" + X}, {"phone 2": B}, blocked, [B])
check("phone 1 replaced", p and p["phone"] == B and not p["dnd"] and p["removed"] == [X])

# 4. blocked Phone 1 and no good number left -> DND
p = nc.plan({"phone": "+1" + X}, {}, blocked, [])
check("dnd when nothing good", p and p["dnd"] and nc.update_body(p, FIELDS) == {"dnd": True})

# 5. already DND and nothing else to clean -> no repeat writes every night
check("no repeat", nc.plan({"phone": "+1" + X, "dnd": True}, {}, blocked, []) is None)

# 6. the good list itself never hands back a blocked number
p = nc.plan({"phone": "+1" + X}, {"phone 2": C}, blocked, [X, C])
check("never promote blocked", p and p["phone"] == C)

# 7. note names the number, never as a raw digit string only
check("note", "555-000-0099" in nc.note_text(nc.plan({"phone": "+1" + X}, {}, blocked, [])))

if fails:
    print("FAILED:", *fails, sep="\n  ")
    sys.exit(1)
print("never_call: all checks passed")
