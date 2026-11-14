#!/usr/bin/env python3
"""
Checks for phone grades, litigators and the bad-number loop (Tala, 2026-11-12).
Synthetic names and numbers only (the repo is public). No network, no database.

    python tests/test_phone_grade.py
"""
import os
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

import phone_grade as pg  # noqa: E402

fails = []
TODAY = date(2026, 11, 12)


def check(label, cond):
    if not cond:
        fails.append(label)


row = {
    "Skiptrace:litigator": "false", "Skiptrace:death.deceased": "true",
    "Skiptrace:phoneNumbers.0.number": "5550000001", "Skiptrace:phoneNumbers.0.type": "Mobile",
    "Skiptrace:phoneNumbers.0.reachable": "true", "Skiptrace:phoneNumbers.0.lastReportedDate": "2026-05-01",
    "Skiptrace:phoneNumbers.0.dnc": "false",
    "Skiptrace:phoneNumbers.1.number": "(555) 000-0002", "Skiptrace:phoneNumbers.1.type": "Land Line",
    "Skiptrace:phoneNumbers.1.reachable": "true", "Skiptrace:phoneNumbers.1.lastReportedDate": "2025-01-01",
    "Skiptrace:phoneNumbers.2.number": "15550000003", "Skiptrace:phoneNumbers.2.type": "Land Line",
    "Skiptrace:phoneNumbers.2.reachable": "false", "Skiptrace:phoneNumbers.2.lastReportedDate": "2012-03-01",
    "Skiptrace:phoneNumbers.3.number": "5550000004", "Skiptrace:phoneNumbers.3.type": "Mobile",
    "Skiptrace:phoneNumbers.3.reachable": "false", "Skiptrace:phoneNumbers.3.lastReportedDate": "2026-01-01",
}
meta = pg.row_phone_meta(row)
check("all four numbers read", set(meta) == {"5550000001", "5550000002", "5550000003", "5550000004"})
check("rank kept", meta["5550000001"]["rank"] == 1 and meta["5550000004"]["rank"] == 4)
check("person flags", pg.row_person_flags(row) == {"litigator": False, "deceased": True})
check("no batchdata columns -> no meta", pg.row_phone_meta({"phone": "5550000001"}) == {})

g = {p: pg.grade(meta[p], TODAY) for p in meta}
check("seen this year, reachable cell = A", g["5550000001"] == "A")
check("landline seen last year = B", g["5550000002"] == "B")
check("2012 number = D", g["5550000003"] == "D")
check("cell seen this year but not reachable = B (one lower)", g["5550000004"] == "B")
check("no details = ?", pg.grade({}, TODAY) == "?")
check("no date = ?", pg.grade({"type": "Mobile", "reachable": True}, TODAY) == "?")
check("person traced died = D", pg.grade(meta["5550000001"], TODAY, traced_deceased=True) == "D")
check("landline seen this year can't be A", pg.grade({"type": "Land Line", "reachable": True,
                                                      "last_reported": "2026-09-22"}, TODAY) == "B")
check("2024 = C", pg.grade({"type": "Mobile", "reachable": True, "last_reported": "2024-12-31"}, TODAY) == "C")
check("2023 = D", pg.grade({"type": "Mobile", "reachable": True, "last_reported": "2023-12-31"}, TODAY) == "D")
check("2025 not reachable = C", pg.grade({"type": "Mobile", "reachable": False,
                                         "last_reported": "2025-06-01"}, TODAY) == "C")

order = [p for p, _ in pg.graded(["5550000003", "5550000004", "5550000002", "5550000001"], meta, TODAY)]
check("best grade first, ties by BatchData rank", order == ["5550000001", "5550000002", "5550000004", "5550000003"])

check("deleted numbers found", pg.deleted_numbers({"phone": "+15550000001", "cf:phone 2": "(555) 000-0002"},
                                                  {"5550000002"}) == {"5550000001"})
check("nothing deleted", pg.deleted_numbers({"phone": "5550000001"}, {"5550000001"}) == set())
check("phone in a name field", pg.has_phone_digits("864-555-0101") and not pg.has_phone_digits("Mary Jo"))

pen, why = pg.confidence_penalty(["5550000001"], meta, TODAY)
check("A costs nothing", pen == 0)
pen, why = pg.confidence_penalty(["5550000003"], meta, TODAY)
check("D costs 35", pen == 35 and "D" in why)
pen, _ = pg.confidence_penalty([], meta, TODAY)
check("no callable number costs the most", pen == pg.NO_PHONE_PENALTY)

clean = {"5550000002": True, "5550000004": True}
check("best number on DNC flagged",
      pg.best_is_dnc(list(meta), meta, clean, {"5550000001"}, TODAY))
check("best number clean -> not flagged",
      not pg.best_is_dnc(list(meta), meta, {"5550000001": True}, set(), TODAY))
check("best number unchecked (not on registry) -> not flagged",
      not pg.best_is_dnc(list(meta), meta, clean, set(), TODAY))

contact = {"phone": "+1 555-000-0001"}
cf = {"phone 2": "5550000002", "phone 3": "5550000004"}
fb = pg.bad_numbers(contact, dict(cf, **{"call result": "All numbers bad"}), [])
check("all numbers bad", fb["all_bad"] and fb["bad"] == {"5550000001", "5550000002", "5550000004"})
fb = pg.bad_numbers(contact, dict(cf, **{"call result": "Wrong person", "working numbers": ["Phone 2"]}), [])
check("wrong person: unticked numbers bad, ticked kept",
      fb["wrong_person"] and fb["bad"] == {"5550000001", "5550000004"})
fb = pg.bad_numbers(contact, dict(cf, **{"call outcome": "Wrong number"}), [])
check("wrong number outcome, nothing ticked -> Phone 1", fb["bad"] == {"5550000001"})
fb = pg.bad_numbers(contact, cf, [{"body": "disconnected (555) 000-0004"}])
check("note naming a number marks that number", fb["bad"] == {"5550000004"})
fb = pg.bad_numbers(contact, cf, [{"body": "wrong number"}])
check("plain wrong-number note -> Phone 1", fb["bad"] == {"5550000001"})
fb = pg.bad_numbers(contact, cf, [{"body": "all numbers disconnected"}])
check("'all' in a note -> every number", fb["all_bad"])
fb = pg.bad_numbers(contact, cf, [{"body": "no answer, left vm"}])
check("no answer is not a bad number", not fb["bad"])
fb = pg.bad_numbers(contact, dict(cf, **{"working numbers": "Phone 1"}), [{"body": "dropped call on the others"}])
check("ticked working number never marked bad", "5550000001" not in fb["bad"] and fb["bad"])

cands = pg.trace_candidates(("Pat", "Sample"), "SAMPLE, ROBIN (Applicant); SAMPLE, PAT (Respondent)",
                            ("Lee", "Sample"), ("Dale", "Sample"), owner_deceased=True)
check("candidates in order, no dupes, no dead owner",
      [(f, l) for f, l, _ in cands] == [("Pat", "Sample"), ("Robin", "Sample"), ("Lee", "Sample")])
nxt = pg.next_trace_target(cands, [("Pat", "Sample")])
check("next name after the one tried", nxt[:2] == ("Robin", "Sample"))
check("everyone tried -> None", pg.next_trace_target(cands, [(f, l) for f, l, _ in cands]) is None)

# Tala 2026-11-14: never re-trace the same information; a person comes back only with a new address
c2 = pg.trace_candidates(("Pat", "Sample"), "", None, None, owner_deceased=True, system_address="12 Court Rd, Greer SC")
check("same person, same address -> None",
      pg.next_trace_target(c2, [("Pat", "Sample", "12 COURT RD, GREER SC")]) is None)
check("same person, new address -> traced again",
      (pg.next_trace_target(c2, [("Pat", "Sample", "5 Old House Ln, Greenville SC")]) or ("",))[0] == "Pat")
check("same person, earlier address unknown -> None (can't prove it changed)",
      pg.next_trace_target(c2, [("Pat", "Sample")]) is None)
check("corrected last name -> traced",
      (pg.next_trace_target(pg.trace_candidates(("Pat", "Sampson"), "", None, None, True), [("Pat", "Sample", "x")]) or ("",))[1] == "Sampson")

# Tala 2026-11-14: no PR named -> the heir (family, same last name as the person who died) comes first
from skiptrace_prep import probate_contact
_pc = probate_contact({"decedent_name": "SAMPLE , PAT", "parties":
    "( 2 ) Name: OTHERS , KIM Party Type: Applicant Address: 1 A ST GREER SC 29651 "
    "Name: SAMPLE , ROBIN Party Type: Respondent Address: 2 B ST TAYLORS SC 29687"})
check("heir before non-family applicant", _pc[0] == "SAMPLE, ROBIN" and "2 B ST" in _pc[2])
_pc = probate_contact({"decedent_name": "SAMPLE , PAT", "pr_name": "OTHERS, KIM", "pr_address": "1 A ST GREER SC 29651",
    "parties": "( 2 ) Name: OTHERS , KIM Party Type: Personal Representative Address: 1 A ST GREER SC 29651 "
    "Name: SAMPLE , ROBIN Party Type: Respondent Address: 2 B ST TAYLORS SC 29687"})
check("named PR still first", _pc[0] == "OTHERS, KIM")
_pc = probate_contact({"decedent_name": "SAMPLE , PAT", "parties":
    "( 1 ) Name: OTHERS , KIM Party Type: Applicant Address: 1 A ST GREER SC 29651"})
check("no heir on file -> applicant", _pc[0] == "OTHERS, KIM")

if fails:
    print("FAILED:\n  " + "\n  ".join(fails))
    sys.exit(1)
print("all phone-grade checks passed")
