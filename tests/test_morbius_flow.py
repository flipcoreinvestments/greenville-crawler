#!/usr/bin/env python3
"""
Checks for Morbius's lead-flow safeguards and deed names (2026-11-10).
Synthetic names and data only (the repo is public). No network, no database.

    python tests/test_morbius_flow.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import morbius_flow as f  # noqa: E402

NOW = datetime(2026, 11, 20, 15, 0, tzinfo=timezone.utc)
FIRST, OFFER = "stage-new", "stage-offer"
fails = []


def check(label, got, want):
    if got != want:
        fails.append(f"{label}: got {got!r}, want {want!r}")


def ago(days):
    return (NOW - timedelta(days=days)).isoformat()


def card(stage=FIRST, status="open"):
    return [{"pipelineStageId": stage, "status": status, "lastStageChangeAt": ago(1)}]


def s(contact=None, opps=None, notes=(), calls=()):
    return f.summarize(contact or {"tags": []}, card() if opps is None else opps, list(notes), list(calls),
                       {OFFER}, FIRST)


# ---------------------------------------------------------------- names
for raw, want in [
    ("SAMPLE JOHN A JR", ("John A", "Sample Jr")),
    ("SAMPLE , JOHN ALLEN", ("John Allen", "Sample")),
    ("SAMPLE FAMILY LIVING TRUST", ("Sample Family Living Trust", "")),
    ("EXAMPLE HOLDINGS LLC", ("Example Holdings LLC", "")),
    ("ESTATE OF SAMPLE JOHN", ("Estate of Sample John", "")),
    ("DE TESTA CLARA", ("Clara", "De Testa")),
    ("JOHN A SAMPLE", ("John A", "Sample")),
    ("SAMPLE JOHN (JTWROS)", ("John", "Sample")),
    ("", ("", "")),
]:
    check(f"deed_name {raw!r}", f.deed_name(raw), want)
check("second owner", f.second_owner("SAMPLE JANE M & SAMPLE RICHARD T", "Sample"), "Richard T Sample")
check("second owner, no surname", f.second_owner("SAMPLE WILLIAM M & DAVID M", "Sample"), "David M Sample")
check("second owner, cut off", f.second_owner("SAMPLE WILLIAM M & JENN", "Sample"), "")
check("second owner, trust", f.second_owner("SAMPLE JANE & JOHN REVOCABLE TRUST", ""), "")

# ---------------------------------------------------------------- behind on follow-up
fresh = s()
check("never called, added 2 days ago: not behind", f.is_behind(dict(fresh, last_touch=None), ago(2), NOW), False)
check("never called, added 4 days ago: behind", f.is_behind(dict(fresh, last_touch=None), ago(4), NOW), True)
three = s(notes=[{"body": "no answer", "dateAdded": ago(4)}], opps=[{"pipelineStageId": FIRST, "status": "open"}])
check("1 attempt, last touch 4 days ago: behind", f.is_behind(three, ago(10), NOW), True)
six = s(notes=[{"body": "NA", "dateAdded": ago(5)}] * 6, opps=[{"pipelineStageId": FIRST, "status": "open"}])
check("6 attempts, last 5 days ago: on the 10-day rhythm, not behind", f.is_behind(six, ago(30), NOW), False)
check("closed card never behind", f.is_behind(s(opps=card(status="lost")), ago(30), NOW), False)
check("offer stage never behind", f.is_behind(s(opps=card(OFFER)), ago(30), NOW), False)
check("Do Not Call never behind", f.is_behind(s({"tags": ["DNC"]}), ago(30), NOW), False)

# ---------------------------------------------------------------- dead weight
na_calls = [{"direction": "outbound", "status": "no-answer", "duration": 0, "date": ago(d)} for d in range(6)]
check("6 dialer no-answers: tag", f.dead_weight(s(calls=na_calls)), True)
check("5 no-answers: no tag", f.dead_weight(s(calls=na_calls[:5])), False)
check("6 'no answer' notes from her cell: tag",
      f.dead_weight(s(notes=[{"body": "No answer", "dateAdded": ago(1)}] * 6)), True)
talked = na_calls + [{"direction": "outbound", "status": "completed", "duration": 95, "date": ago(0)}]
check("talked once: never tagged", f.dead_weight(s(calls=talked)), False)
check("'talked' tag: never tagged", f.dead_weight(s({"tags": ["talked"]}, calls=na_calls)), False)
check("Do Not Call: never tagged", f.dead_weight(s({"tags": ["dnc"]}, calls=na_calls)), False)
check("GHL DND on: never tagged", f.dead_weight(s({"tags": [], "dnd": True}, calls=na_calls)), False)
check("offer stage: never tagged", f.dead_weight(s(opps=card(OFFER), calls=na_calls)), False)
check("inbound calls don't count",
      f.dead_weight(s(calls=[dict(x, direction="inbound") for x in na_calls])), False)

# ---------------------------------------------------------------- worked
check("note = worked", s(notes=[{"body": "talked, call back Tue", "dateAdded": ago(1)}])["worked"], True)
check("moved off New = worked", s(opps=card("stage-contacted"))["worked"], True)
check("untouched card in New = not worked", s(opps=[{"pipelineStageId": FIRST, "status": "open"}])["worked"], False)

# ---------------------------------------------------------------- urgent + release
check("urgent: 3 sources, 100", f.is_urgent(100, 3, None), True)
check("not urgent: 2 sources", f.is_urgent(100, 2, None), False)
check("not urgent: 95", f.is_urgent(95, 3, None), False)
check("not urgent: deadline passed", f.is_urgent(100, 3, "sale date passed"), False)
for worked, want in [(0, 0), (49, 0), (50, 50), (99, 50), (100, 100), (230, 200)]:
    check(f"release at {worked} worked", f.earned_release(worked), want)

if fails:
    print("FAILED:\n  " + "\n  ".join(fails))
    sys.exit(1)
print("All Morbius lead-flow checks passed.")
