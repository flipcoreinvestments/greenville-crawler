"""
PHONE GRADES, LITIGATORS AND THE BAD-NUMBER LOOP (added 2026-11-12, Tala's rules).

Why: on Nov 12 Tala called 26 crawler leads off her cell and reached nobody.
BatchData's result file says, for every number it returns, whether it is a cell
or a landline, whether it was reachable in the last 3 months and when it was
last tied to that person -- and flags known TCPA litigators and owners who have
died. The intake used to keep only the digits, so Morbius could not tell a
2010 landline from a live cell. Her rules:

  1. Every number gets a grade for how likely it is to reach the person
     (re-weighted 2026-11-13, Tala: "if they answered it this year it's an A and every
     year after that it goes down"):
       by the year BatchData last saw the number with that person
         this year = A, last year = B, two years ago = C, three or more = D
       then: not reachable lately -> one grade lower; a landline can't be an A;
       the person traced has died -> D; no date at all -> ? (unknown).
     A grade only says the line is likely live. A number that reached the WRONG person
     is marked bad from her call results and never used again, whatever its grade.
     The best grade is Phone 1. The grade feeds Confidence so the freshest,
     most reachable leads sit at the top.
  2. Litigators (people who sue over calls) never go into GHL, never on a list.
  3. If the BEST number is on the Do Not Call registry, Morbius never quietly
     swaps in a worse number: the lead is flagged "best number on DNC" (mail /
     door knock) until Larry says otherwise.
  4. A wrong / disconnected / bad number never deletes a lead. The number is
     marked dead, the lead is flagged, and the next person tied to the property
     is traced (Personal Representative, heirs, co-owner on the deed...). Only
     when every name has been traced and every number failed is the lead
     "unreachable" -- hot ones get the "idicore search" tag for Tala.

Pure functions only (no network, no database) so the tests can check them.
"""
import re
from datetime import date, datetime

GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "?": 2, "D": 3}
GRADE_SCORE = {"A": 100, "B": 70, "C": 40, "?": 40, "D": 10}
# what the best callable number's grade takes off Confidence
CONF_PENALTY = {"A": 0, "B": 10, "C": 20, "?": 20, "D": 35}
NO_PHONE_PENALTY = 35

IDICORE_TAG = "idicore search"
UNREACHABLE_TAG = "unreachable"
BAD_NUMBER_TAG = "bad number"
BEST_DNC_TAG = "best number dnc"
LITIGATOR_TAG = "litigator"
EMAIL_TAG = "email campaign"
IDICORE_MIN_CONF = 60   # "hot enough" to pay a few cents for IDI Core
EMAIL_MIN_CONF = 60

_PN = re.compile(r"^skiptrace:phonenumbers\.(\d+)\.(\w+)$", re.I)


def _digits(v):
    d = re.sub(r"\D", "", str(v or ""))
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return d if len(d) == 10 else ""


def _true(v):
    return str(v or "").strip().lower() in ("true", "1", "yes", "y")


def _date(v):
    s = str(v or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


# ------------------------------------------------------------------ BatchData rows
def row_phone_meta(row):
    """BatchData result row -> {phone: {rank, type, reachable, last_reported, dnc, carrier}}.
    rank 1 = BatchData's most likely number. Rows without Skiptrace:phoneNumbers.* -> {}."""
    slots = {}
    for k, v in row.items():
        m = _PN.match((k or "").strip())
        if m:
            slots.setdefault(int(m.group(1)), {})[m.group(2).lower()] = v
    out = {}
    for i in sorted(slots):
        s = slots[i]
        p = _digits(s.get("number"))
        if not p or p in out:
            continue
        d = _date(s.get("lastreporteddate"))
        out[p] = {"rank": i + 1,
                  "type": (s.get("type") or "").strip(),
                  "reachable": _true(s.get("reachable")) if s.get("reachable") not in (None, "") else None,
                  "last_reported": d.isoformat() if d else "",
                  "dnc": _true(s.get("dnc")),
                  "carrier": (s.get("carrier") or "").strip()}
    return out


def row_person_flags(row):
    """-> {litigator, deceased} for the person BatchData traced (None when the file doesn't say)."""
    low = {(k or "").strip().lower(): v for k, v in row.items()}
    out = {}
    for key, col in (("litigator", "skiptrace:litigator"), ("deceased", "skiptrace:death.deceased")):
        if low.get(col) not in (None, ""):
            out[key] = _true(low.get(col))
    return out


# ------------------------------------------------------------------ grades
def grade(meta, today=None, traced_deceased=False):
    """One number's grade (see the module header). meta None/{} = no details -> '?'."""
    if traced_deceased:
        return "D"
    if not meta:
        return "?"
    today = today or date.today()
    d = _date(meta.get("last_reported"))
    if d is None:
        return "?"
    years = max(0, today.year - d.year)
    g = min(years, 3)                       # 0=A 1=B 2=C 3=D
    if meta.get("reachable") is False:
        g = min(g + 1, 3)
    t = (meta.get("type") or "").lower()
    landline = bool(t) and not (t.startswith("mobile") or t in ("wireless", "cell"))
    if landline and g == 0:
        g = 1
    return "ABCD"[g]


def deleted_numbers(written, card):
    """Numbers Morbius put on the card that are no longer anywhere on it = Tala deleted them
    (2026-11-13: she deletes bad numbers by hand). written: what Morbius last wrote
    ({"phone": ..., "cf:phone 2": ...}); card: set of digits on the card now."""
    had = {_norm_phone((written or {}).get("phone"))}
    for k, v in (written or {}).items():
        if k.lower().startswith("cf:phone "):
            had.add(_norm_phone(v))
    return {p for p in had if p} - set(card or ())


def has_phone_digits(name):
    """A name field holding a phone number (7+ digits) -- a bad import, never a real name."""
    return len(re.sub(r"\D", "", str(name or ""))) >= 7


def graded(phones, meta, today=None, traced_deceased=False):
    """[(phone, grade)] best first; ties keep BatchData's own order (rank), then list order."""
    meta = meta or {}
    rows = []
    for i, p in enumerate(phones):
        m = meta.get(p) or {}
        rows.append((GRADE_ORDER[grade(m, today, traced_deceased)], m.get("rank") or 99, i, p,
                     grade(m, today, traced_deceased)))
    rows.sort()
    return [(r[3], r[4]) for r in rows]


def best_grade(phones, meta, today=None, traced_deceased=False):
    g = graded(phones, meta, today, traced_deceased)
    return g[0][1] if g else None


def confidence_penalty(callable_phones, meta, today=None, traced_deceased=False):
    """-> (points to take off Confidence, plain reason). Only for traced leads."""
    b = best_grade(callable_phones, meta, today, traced_deceased)
    if b is None:
        return NO_PHONE_PENALTY, "no number we can call"
    label = {"A": "a fresh cell", "B": "reachable but older", "C": "not reachable lately",
             "?": "no phone details from the trace yet", "D": "old number / person traced has died"}[b]
    return CONF_PENALTY[b], f"best number grade {b} ({label})"


def describe(p, meta, g, dnc_ok=True):
    """'(864) 555-0101 A · cell, reachable, seen 2026-03' for the Skip Trace Info field."""
    m = meta or {}
    bits = []
    if m.get("type"):
        bits.append("cell" if m["type"].lower().startswith("mobile") else m["type"].lower())
    if m.get("reachable") is True:
        bits.append("reachable")
    elif m.get("reachable") is False:
        bits.append("not reachable")
    if m.get("last_reported"):
        bits.append("seen " + m["last_reported"][:7])
    if not dnc_ok:
        bits.append("DO NOT CALL registry")
    num = f"({p[:3]}) {p[3:6]}-{p[6:]}" if len(p) == 10 else p
    return f"{num} {g}" + (" · " + ", ".join(bits) if bits else "")


def best_is_dnc(phones, meta, clean, on_dnc, today=None, traced_deceased=False):
    """True when the best-graded number is ON the Do Not Call registry and every number we
    may call is graded worse (Tala: 'the first one's probably their phone').
    clean: {phone: True if callable}; on_dnc: set of phones found on the registry."""
    g = graded(phones, meta, today, traced_deceased)
    if not g:
        return False
    top_phone, top_grade = g[0]
    if clean.get(top_phone) or top_phone not in on_dnc:
        return False
    callable_ = [x for x in g if clean.get(x[0])]
    if not callable_:
        return True
    return GRADE_ORDER[callable_[0][1]] > GRADE_ORDER[top_grade]


# ------------------------------------------------------------------ her call results
BAD_NOTE = re.compile(r"wrong\s*(number|#|num|person)|disconnected|not\s*in\s*service|no\s*longer\s*in\s*service"
                      r"|bad\s*(number|#|num)|dropped|does\s*n[o']?t\s*live|doesn'?t\s*own|not\s*(the\s*)?owner"
                      r"|never\s*heard\s*of|all\s*numbers?\s*(are\s*)?bad|number\s*(is\s*)?(bad|dead|invalid)"
                      r"|invalid\s*number|fax", re.I)
ALL_NOTE = re.compile(r"\ball\b", re.I)
PHONE_IN_TEXT = re.compile(r"\(?\b(\d{3})\)?[\s.\-]*(\d{3})[\s.\-]*(\d{4})\b")


def _norm_phone(v):
    return _digits(v)


def card_numbers(contact, cf):
    """{'Phone 1': digits, 'Phone 2': ...} from a GHL contact; cf = {field name lower: value}."""
    out = {"Phone 1": _norm_phone((contact or {}).get("phone"))}
    for i in range(2, 6):
        out[f"Phone {i}"] = _norm_phone(cf.get(f"phone {i}"))
    return {k: v for k, v in out.items() if v}


def bad_numbers(contact, cf, notes):
    """What Tala's call results say about the numbers on this card.
    cf: {field name lower: value} for the card's custom fields; notes: GHL notes.
    -> dict(bad=set(phones), all_bad=bool, wrong_person=bool, reasons=[...]).
    Rules: 'All numbers bad' = every number; 'Wrong person' or a Call Outcome of
    'Wrong number' = every number she didn't tick under Working Numbers (Phone 1 if
    she ticked none on a 'Wrong number'); a note saying wrong number / disconnected /
    not in service / bad number / dropped marks the numbers it names, else the
    unticked ones, else Phone 1. Ticked Working Numbers are never marked bad."""
    nums = card_numbers(contact, cf)
    working_raw = cf.get("working numbers") or []
    if isinstance(working_raw, str):
        working_raw = [w for w in re.split(r"[,;]", working_raw) if w.strip()]
    working = {nums.get(w.strip().title()) for w in working_raw} - {None, ""}
    unticked = {p for k, p in nums.items() if p not in working}
    result = (str(cf.get("call result") or "")).strip().lower()
    outcome = (str(cf.get("call outcome") or "")).strip().lower()
    bad, reasons, wrong_person = set(), [], False
    if result == "all numbers bad":
        bad |= set(nums.values())
        reasons.append("Call Result: All numbers bad")
    elif result == "wrong person":
        bad |= unticked
        wrong_person = True
        reasons.append("Call Result: Wrong person")
    if outcome == "wrong number":
        bad |= (unticked if working else {nums.get("Phone 1")} - {None})
        reasons.append("Call Outcome: Wrong number")
    for n in notes or []:
        body = n.get("body") or ""
        if not BAD_NOTE.search(body):
            continue
        named = {"".join(m.groups()) for m in PHONE_IN_TEXT.finditer(body)}
        if named:
            bad |= {p for p in named if p in set(nums.values())}
        elif ALL_NOTE.search(body):
            bad |= set(nums.values())
        elif working:
            bad |= unticked
        elif nums.get("Phone 1"):
            bad.add(nums["Phone 1"])
        if re.search(r"wrong\s*person|does\s*n[o']?t\s*live|doesn'?t\s*own|not\s*(the\s*)?owner|never\s*heard", body, re.I):
            wrong_person = True
        reasons.append("note: " + " ".join(body.split())[:60])
    bad -= working
    all_bad = bool(nums) and set(nums.values()) <= bad
    return dict(bad=bad, all_bad=all_bad, wrong_person=wrong_person, reasons=reasons)


# ------------------------------------------------------------------ who to trace next
def _key(first, last):
    n = lambda x: re.sub(r"[^a-z]", "", (x or "").lower())
    return (n(first)[:3], n(last))


def split_person(name):
    """'DOE, MARY ANN (Respondent)' / 'Mary Ann Doe' -> ('Mary', 'Doe')."""
    s = re.sub(r"\(.*?\)", "", name or "").strip()
    if not s:
        return "", ""
    if "," in s:
        last, rest = [x.strip() for x in s.split(",", 1)]
        first = (rest.split() or [""])[0]
        return first.title(), last.title()
    parts = s.split()
    if len(parts) == 1:
        return "", parts[0].title()
    return parts[0].title(), parts[-1].title()


def trace_candidates(system_person, heirs_text, co_owner, owner, owner_deceased):
    """Everyone tied to the property, in the order Tala wants them traced:
    the system's person (Personal Representative on a probate), each heir on the
    court file, a co-owner on the deed, then the owner (only if not deceased).
    Each item: (first, last, why). Duplicates removed."""
    out, seen = [], set()

    def add(first, last, why):
        if not (first or last):
            return
        k = _key(first, last)
        if k in seen:
            return
        seen.add(k)
        out.append((first, last, why))

    if system_person:
        add(system_person[0], system_person[1], "the person to call")
    for chunk in (heirs_text or "").split(";"):
        f, l = split_person(chunk)
        add(f, l, "heir on the probate file")
    if co_owner:
        add(co_owner[0], co_owner[1], "co-owner on the deed")
    if owner and not owner_deceased:
        add(owner[0], owner[1], "owner on the deed")
    return out


def next_trace_target(candidates, tried):
    """First candidate not traced yet. tried: [(first, last)]. None = everyone tried."""
    done = {_key(f, l) for f, l in tried}
    for f, l, why in candidates:
        if _key(f, l) not in done:
            return f, l, why
    return None
