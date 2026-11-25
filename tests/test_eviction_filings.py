import os, sys
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from eviction_filings_ingest import norm_name, names_match, is_eviction_row, is_open, classify, parse_us_date  # noqa: E402


def test_name_order_and_noise():
    assert names_match("SMITH JOHN A", "John A. Smith Jr")
    assert not names_match("SMITH JOHN", "SMITH JOHN A")      # middle initial differs -> not exact
    assert not names_match("SMITH", "SMITH")                   # one word is never a match
    assert names_match("Acme Rentals, LLC", "ACME RENTALS LLC")


def test_eviction_row_filter():
    assert is_eviction_row({"Type": "Civil", "Subtype": "Rule to Vacate"})
    assert is_eviction_row({"Type": "Civil", "Subtype": "Notice to Quit"})
    assert not is_eviction_row({"Type": "Traffic", "Subtype": "Rule to Vacate"})
    assert not is_eviction_row({"Type": "Civil", "Subtype": "Summons & Complaint"})


def test_open_closed():
    today = date(2026, 11, 24)
    assert is_open("Pending", date(2026, 11, 1), today)
    assert not is_open("Disposed", date(2026, 11, 1), today)
    assert not is_open("Settled", date(2026, 11, 1), today)
    assert not is_open("Pending", date(2026, 6, 1), today)    # older than 90 days


def lead(i, owner, absentee=False, tired=False):
    return {"id": i, "owner_name": owner, "is_absentee": absentee, "is_tired_landlord": tired, "raw": {}}


def test_classify_safeguards():
    by = {}
    for l in [lead(1, "DOE JANE", absentee=True), lead(2, "ROE RICK"), lead(3, "TWO PROPS", tired=True), lead(4, "PROPS TWO", tired=True)]:
        by.setdefault(norm_name(l["owner_name"]), []).append(l)
    assert classify("Jane Doe", by)[0] == "stack"
    assert classify("Rick Roe", by) == ("hidden", "lead is not flagged absentee or tired landlord")
    assert classify("Two Props", by)[0] == "hidden"           # two leads share the name
    assert classify("Nobody Here", by)[0] == "hidden"


def test_parse_date():
    assert parse_us_date("11/24/2026") == date(2026, 11, 24)
    assert parse_us_date("") is None
