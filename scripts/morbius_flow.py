"""
MORBIUS LEAD-FLOW SAFEGUARDS (added 2026-11-10, Tala's rules from the morning meeting).

Pure helpers (no network, no database) so they can be tested on their own.
morbius_sync.py calls them.

  RELEASE      every 50 crawler leads Tala calls/updates releases the next 50
               hottest leads (was 100).
  BACKLOG      a lead is "behind" when its next call is overdue on the Power
               Dialer's own rhythm: the dialer re-queues a lead 2 days after the
               last call (Confidence 100) or 3 days (everyone else), so a lead
               untouched longer than that is behind. Leads the dialer finished
               with (tag "nurture") are not behind. If 100+ leads are behind, no
               new non-urgent leads are released or added to GHL.
  URGENT       only when 3+ independent sources confirm the situation AND
               Confidence is 100. Urgent leads always get through, all at once,
               even during a backlog pause.
  DEAD WEIGHT  6+ no-answer attempts and she never talked to them (the Power
               Dialer marks this with the tag "nurture" after 6 calls never
               reached; Morbius also counts GHL call logs, the "Call attempts"
               field and her "no answer" notes) -> GHL tag
               "delete in 5 days". If she removes the tag, the lead is kept for
               good. If the tag is still there after 5 days, the contact is
               backed up and removed from GHL.
  NEVER        a Do Not Call / opted-out contact is never tagged or deleted (it
               stays in GHL as Do Not Contact so it's never contacted again);
               a lead she talked to, or moved to an offer stage or beyond, is
               never tagged or deleted.

Names (Tala's standing rule, 2026-11-10): First/Last Name = the owner exactly as
on the deed, in normal First Last order, keeping middle names/initials and
Jr/Sr/III. A trust, estate, LLC or other entity owner -> First Name = the full
entity name, Last Name blank.
"""
import re
from datetime import datetime, timezone

from lead_common import owner_is_entity
from name_order import decide_order
from skiptrace_prep import _ORG_RE, _PARTICLES, _SUFFIXES, _TAG_RE

RELEASE_PER = 50            # leads called/updated per release
RELEASE_SIZE = 50           # leads released each time
BACKLOG_LIMIT = 100         # behind leads that pause new leads
HOT_DAYS = 2                # Power Dialer re-queue wait for Confidence 100
REQUEUE_DAYS = 3            # Power Dialer re-queue wait for everyone else
NURTURE_TAG = "nurture"     # the dialer stops calling and tags this
DEAD_NO_ANSWERS = 6
DELETE_TAG = "delete in 5 days"
DELETE_AFTER_DAYS = 5
URGENT_MIN_SOURCES = 3
URGENT_MIN_CONFIDENCE = 100
TALKED_SECONDS = 30         # a connected call this long counts as talking

DNC_TAGS = {"dnc", "dnd", "do not contact", "do not call", "opted out", "opt out", "opt-out", "stop"}
TALKED_TAGS = {"talked"}
NO_ANSWER_STATUSES = {"no-answer", "no_answer", "noanswer", "busy", "failed", "voicemail", "canceled",
                      "cancelled", "missed"}
# a note Tala logs after a cell-phone call nobody picked up: "no answer", "NA", "VM", "left voicemail"
NO_ANSWER_NOTE = re.compile(r"^\s*(no\s*answer|no\s*ans\b|na\b|n/a\b|vm\b|voice\s*mail|left\s*(a\s*)?(vm|voice\s*mail|message)"
                            r"|didn'?t\s*answer|did\s*not\s*answer|no\s*pick\s*up)", re.I)
ABBREV = {"LLC", "INC", "LP", "LLP", "PC", "PLLC", "LTD", "II", "III", "IV", "USA", "U/W", "DTD", "FBO"}
SMALL_WORDS = {"OF", "AND", "THE", "FOR"}
TRUSTISH = re.compile(r"\b(TRUST|TRUSTEE|TRUSTEES|U/W|REVOC|ESTATE)\b", re.I)


# ------------------------------------------------------------------ names
def _tokens(chunk):
    return re.findall(r"[A-Za-z][A-Za-z'\-]*\.?", chunk or "")


def _cap(w):
    w = w.strip()
    if len(w.strip(".")) == 1:
        return w.strip(".").upper()
    return "-".join("'".join(p.capitalize() for p in part.split("'")) for part in w.split("-"))


def _person(chunk):
    toks = [t.strip(".") if len(t.strip(".")) > 1 else t for t in _tokens(chunk)]
    toks = [t for t in toks if t]
    suf = [t for t in toks if t.upper().strip(".") in _SUFFIXES]
    toks = [t for t in toks if t.upper().strip(".") not in _SUFFIXES]
    if not toks:
        return "", ""
    while len(toks) > 2 and toks[0].upper() in _PARTICLES:
        toks = [toks[0] + " " + toks[1]] + toks[2:]
    sfx = " ".join(s.upper().strip(".").title() if s.upper().strip(".") in ("JR", "SR") else s.upper().strip(".")
                   for s in suf)
    if len(toks) == 1:
        return "", " ".join(x for x in (_cap(toks[0]), sfx) if x)
    first, last = decide_order(toks)
    rest = list(toks)
    for w in (first, last):
        if w in rest:
            rest.remove(w)
    # keep the middle names in the order the deed lists them
    given = " ".join(_cap(w) for w in [first] + rest)
    surname = " ".join(_cap(p) for p in last.split())
    return given, " ".join(x for x in (surname, sfx) if x)


def deed_name(owner_name):
    """Primary owner as on the deed -> (first, last).
    'SMITH JOHN A JR' -> ('John A', 'Smith Jr'); 'SMITH , JOHN ALLEN' -> ('John Allen', 'Smith');
    'SMITH FAMILY LIVING TRUST' -> ('Smith Family Living Trust', '')."""
    raw = " ".join((owner_name or "").split())
    if not raw:
        return "", ""
    if owner_is_entity(raw) or _ORG_RE.search(raw) or TRUSTISH.search(raw):
        words = []
        for i, w in enumerate(raw.split()):
            bare = w.upper().strip(".,")
            if bare in ABBREV:
                words.append(w.upper())
            elif i and bare in SMALL_WORDS:
                words.append(w.lower())
            else:
                words.append(_cap(w))
        return " ".join(words), ""
    # probate-court style "LAST , FIRST MIDDLE": the order is explicit
    m = re.match(r"^\s*([A-Za-z'\-]+(?:\s+[A-Za-z'\-]+)?)\s*,\s*([A-Za-z].*)$", raw)
    if m and "&" not in raw and "," not in m.group(2):
        toks = _tokens(m.group(2))
        suf = [t for t in toks if t.upper().strip(".") in _SUFFIXES]
        given = [t for t in toks if t.upper().strip(".") not in _SUFFIXES]
        sfx = " ".join(s.upper().strip(".").title() if s.upper().strip(".") in ("JR", "SR") else s.upper().strip(".")
                       for s in suf)
        last = " ".join(_cap(p) for p in m.group(1).split())
        return " ".join(_cap(w) for w in given), " ".join(x for x in (last, sfx) if x)
    name = _TAG_RE.sub(" ", raw)
    name = re.sub(r"\b(?:os|OS)\)|\s(?:os|OS)\b|\)", " ", name)
    name = re.sub(r"([a-z])([A-Z][a-z])", r"\1 & \2", name)
    chunks = [c.strip() for c in re.split(r"\s*[&,]\s*", name) if c.strip()]
    return _person(chunks[0] if chunks else "")


def second_owner(owner_name, first_last):
    """Second owner on the deed for the 'Name 2' field, same First Last format, or "".
    first_last: the primary owner's last name, used when the deed lists 'JANE M' with no surname."""
    raw = " ".join((owner_name or "").split())
    if not raw or owner_is_entity(raw) or _ORG_RE.search(raw) or TRUSTISH.search(raw):
        return ""
    name = _TAG_RE.sub(" ", raw)
    name = re.sub(r"\b(?:os|OS)\)|\s(?:os|OS)\b|\)", " ", name)
    name = re.sub(r"([a-z])([A-Z][a-z])", r"\1 & \2", name)
    chunks = [c.strip() for c in re.split(r"\s*&\s*", name) if c.strip()]
    if len(chunks) < 2:
        return ""
    toks = [t for t in _tokens(chunks[1]) if t.upper().strip(".") not in _SUFFIXES]
    # a co-owner the county cut off mid-name ("Jenn", "B") isn't worth showing
    if not toks or (len(toks) == 1 and len(toks[0]) <= 4):
        return ""
    if first_last and (len(toks) == 1 or (len(toks) == 2 and len(toks[1].strip(".")) == 1)):
        return " ".join([_cap(t) for t in toks] + [re.sub(r"\s+(Jr|Sr|II|III|IV|V)$", "", first_last)])
    f, l = _person(chunks[1])
    full = " ".join(x for x in (f, l) if x)
    return "" if full == " ".join(x for x in deed_name(owner_name) if x) else full


# ------------------------------------------------------------------ activity
def parse_dt(v):
    if v in (None, ""):
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc)
    s = str(v).strip()
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def summarize(contact, opps, notes, calls, offer_stage_ids, first_stage_id, confidence=0, call_attempts=0):
    """One GHL contact -> what Morbius needs to know about Tala's follow-up.
    calls: list of dicts {direction, status, duration, date} (outbound call log).
    confidence / call_attempts: the contact's Confidence and Power Dialer "Call attempts" fields.
    Returns dict(worked, open, past_offer, dnc, talked, nurture, hot, attempts, no_answers, last_touch)."""
    tags = {t.strip().lower() for t in (contact or {}).get("tags") or []}
    dnc = bool((contact or {}).get("dnd")) or bool(tags & DNC_TAGS)
    open_card = any((o.get("status") or "open") == "open" for o in opps) if opps else False
    past_offer = any(o.get("pipelineStageId") in offer_stage_ids for o in opps)
    moved = any((o.get("status") or "open") != "open"
                or (o.get("pipelineStageId") and first_stage_id and o.get("pipelineStageId") != first_stage_id)
                for o in opps)
    touches, no_ans, talked = [], 0, bool(tags & TALKED_TAGS)
    out_calls = [c for c in calls if (c.get("direction") or "outbound") == "outbound"]
    for c in out_calls:
        d = parse_dt(c.get("date"))
        if d:
            touches.append(d)
        st = (c.get("status") or "").strip().lower()
        dur = c.get("duration") or 0
        if st in NO_ANSWER_STATUSES:
            no_ans += 1
        elif st in ("completed", "answered", "in-progress") and dur >= TALKED_SECONDS:
            talked = True
    for n in notes:
        d = parse_dt(n.get("dateAdded") or n.get("createdAt"))
        if d:
            touches.append(d)
        if NO_ANSWER_NOTE.match(n.get("body") or ""):
            no_ans += 1
    for o in opps:
        d = parse_dt(o.get("lastStageChangeAt") or o.get("lastStatusChangeAt"))
        if d and moved:
            touches.append(d)
    attempts = max(len(out_calls) + len(notes), int(call_attempts or 0))
    if not talked:
        # never reached: every dialer attempt so far was a no-answer
        no_ans = max(no_ans, int(call_attempts or 0))
    return dict(worked=moved or bool(notes) or attempts > 0, open=open_card, past_offer=past_offer, dnc=dnc,
                talked=talked, nurture=NURTURE_TAG in tags, hot=int(confidence or 0) >= 100,
                attempts=attempts, no_answers=no_ans, last_touch=max(touches) if touches else None)


def is_behind(s, added_at, now):
    """Overdue for the next call on the Power Dialer rhythm: 2 days (Confidence 100) or 3 days since
    the last call / note / stage move, or since it was added if never touched. Done with = not behind."""
    if not s["open"] or s["dnc"] or s["past_offer"] or s.get("nurture"):
        return False
    since = s["last_touch"] or parse_dt(added_at)
    if since is None:
        return False
    gap = HOT_DAYS if s.get("hot") else REQUEUE_DAYS
    return (now - since).total_seconds() > gap * 86400


def dead_weight(s):
    """6+ no-answers (or the dialer's "nurture" tag) and never talked, not Do Not Call, not at an offer stage."""
    never_reached = s["no_answers"] >= DEAD_NO_ANSWERS or s.get("nurture")
    return bool(never_reached and not s["talked"] and not s["dnc"] and not s["past_offer"])


def is_urgent(conf, n_sources, past_deadline_reason):
    return (conf >= URGENT_MIN_CONFIDENCE and n_sources >= URGENT_MIN_SOURCES and not past_deadline_reason)


def earned_release(worked):
    return (worked // RELEASE_PER) * RELEASE_SIZE
