"""
MORBIUS NIGHTLY GHL SYNC (added 2026-11-07, Tala's ask).

Morbius keeps Tala's Unstuck Homes GoHighLevel in step with the crawler's one
master lead list, straight through the GHL API (no more CSV imports):

  GREEN   a traced house lead with a callable phone that isn't in GHL yet
          -> create the contact (+ a Sellers pipeline card in "New")
  YELLOW  same person, something changed -> update the contact in place
  ORANGE  the person to call changed (owner died, PR appointed, wrong person)
          -> tag "retrace" + say why in Skip Trace Info (no new trace is paid
          for here; skip tracing always waits for Tala's approval)
  RED     dead lead (situation resolved / sold / no longer on any list), or any
          contact Tala tagged "delete" -> backed up, then deleted from GHL

Rules (Tala, 2026-11-07):
  - every lead is ADDRESS-LED: the contact's street address is the property in
    question; first/last name is the OWNER on the deed (even if deceased); on a
    probate lead the phone is the Personal Representative's.
  - every crawler contact carries the tag "crawler"; heat tags warm (30+ days on
    the list), hot (60+), very hot (90+); "re-added" when a deleted lead returns.
  - only phones checked clean against the Do Not Call registry (< 31 days old)
    and never reported wrong are used; the first clean phone is the primary.
  - her edits win: a field she changed in GHL is never overwritten.
  - a contact she has worked (pipeline card past "New", or any note) is never
    deleted for being dead -- only her own "delete" tag removes it.
  - safety brake: if a run would delete more than 25 contacts, it deletes none
    and says so.
  - a backup of every deleted contact is kept in morbius_contacts.backup.

Lead-flow safeguards (Tala, 2026-11-10; details in morbius_flow.py):
  - every 50 leads called/updated releases the next 50 hottest leads for skip tracing.
  - 100+ leads behind on follow-up -> no new non-urgent leads are released or added to GHL.
  - urgent = 3+ independent sources AND Confidence 100; urgent leads always get through.
  - 6+ no-answers and never reached -> tag "delete in 5 days"; removed 5 days later unless
    she takes the tag off. Do Not Call contacts are never tagged or removed.
  - First/Last Name = the deed owner exactly (middle names, Jr/Sr kept; trusts keep their name).

Logs print COUNTS ONLY (the repo and its logs are public). No names, phones or
addresses are ever printed.

Usage:  python morbius_sync.py [--live] [--limit N]
        Without --live nothing is written to GHL (practice run).
"""
import argparse
import csv
import io
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import lead_common  # noqa: F401  database safety: login retry, 20-min timeout, no-op schema checks never lock
import requests

from export_call_lists import OPEN_ERRORS, clean_phones, lead_rows, load_tax_sale_info, needs_retrace, open_errors, to_csv
from ghl_sync import good_phones
from skiptrace_prep import compute_skiptrace_identity, owner_trace_names, probate_heirs
import morbius_flow as flow
import phone_grade as pg
import never_call
from morbius_flow import deed_name

BASE = "https://services.leadconnectorhq.com"
TOKEN = os.environ.get("GHL_MORBIUS_TOKEN", "").strip()
LOC = os.environ.get("GHL_LOCATION_ID", "").strip()
HEAD = {"Authorization": f"Bearer {TOKEN}", "Version": "2021-07-28", "Accept": "application/json"}
PIPELINE_NAME = "Sellers"
FIRST_STAGE = "New"
DELETE_BRAKE = 25
PROBATE_TAGS = {"probate", "pre_probate"}
# Tala doesn't work mobile/manufactured homes (2026-11-07). They stay in the
# master list (sellable inventory) but never go into her calling GHL.
import re as _re
NOT_WORKED = _re.compile(r"\bMH\b|mobile|manufactur", _re.I)


def not_worked(lead):
    return bool(NOT_WORKED.search(lead.get("land_use") or ""))
# fields Morbius writes (her edits to these always win)
TRACKED = ["firstName", "lastName", "phone", "address1", "city", "state", "postalCode"]


# ------------------------------------------------------------------ GHL API
class GHL:
    def __init__(self, live):
        self.live = live
        self.s = requests.Session()
        self.s.headers.update(HEAD)
        self.calls = 0

    def _req(self, method, path, write=False, **kw):
        if write and not self.live:
            return None
        for attempt in range(4):
            self.calls += 1
            r = self.s.request(method, BASE + path, timeout=45, **kw)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            time.sleep(0.12)
            return r
        return r

    def custom_fields(self):
        r = self._req("GET", f"/locations/{LOC}/customFields", params={"model": "contact"})
        r.raise_for_status()
        return {f["name"].strip().lower(): f["id"] for f in r.json().get("customFields", [])}

    def sellers_stage(self):
        """-> (pipeline id, first stage id, ids of the 'offer' stage and every stage after it)."""
        r = self._req("GET", "/opportunities/pipelines", params={"locationId": LOC})
        r.raise_for_status()
        for p in r.json().get("pipelines", []):
            if p.get("name", "").strip().lower() == PIPELINE_NAME.lower():
                stages = sorted(p.get("stages") or [], key=lambda s: s.get("position") or 0)
                first = next((s for s in stages if s.get("name", "").strip().lower() == FIRST_STAGE.lower()),
                             stages[0] if stages else None)
                at = next((i for i, s in enumerate(stages) if "offer" in (s.get("name") or "").lower()), None)
                offer_ids = {s.get("id") for s in stages[at:]} if at is not None else set()
                return p["id"], (first or {}).get("id"), offer_ids
        return None, None, set()

    def get_contact(self, cid):
        r = self._req("GET", f"/contacts/{cid}")
        # 400/404/422: the card is gone (already deleted, or merged away) -- treat as missing, never crash.
        if r is None or r.status_code in (400, 404, 422):
            return None
        r.raise_for_status()
        return r.json().get("contact")

    def create_contact(self, body):
        r = self._req("POST", "/contacts/", write=True, json=dict(body, locationId=LOC))
        if r is None:
            return "dry-run", False
        if r.status_code in (200, 201):
            return r.json()["contact"]["id"], False
        # GHL refuses a duplicate phone/email and names the existing contact
        try:
            meta = r.json().get("meta") or {}
        except ValueError:
            meta = {}
        if r.status_code == 400 and meta.get("contactId"):
            return meta["contactId"], True
        raise RuntimeError(f"create contact HTTP {r.status_code}")

    def update_contact(self, cid, body):
        r = self._req("PUT", f"/contacts/{cid}", write=True, json=body)
        if r is not None and r.status_code not in (200, 201):
            raise RuntimeError(f"update contact HTTP {r.status_code}")

    def add_tags(self, cid, tags):
        if tags:
            self._req("POST", f"/contacts/{cid}/tags", write=True, json={"tags": sorted(set(tags))})

    def remove_tags(self, cid, tags):
        if tags:
            self._req("DELETE", f"/contacts/{cid}/tags", write=True, json={"tags": sorted(set(tags))})

    def add_note(self, cid, body):
        self._req("POST", f"/contacts/{cid}/notes", write=True, json={"body": body})

    def has_notes(self, cid):
        return bool(self.notes(cid))

    def notes(self, cid):
        r = self._req("GET", f"/contacts/{cid}/notes")
        if r is None or r.status_code != 200:
            return []
        return r.json().get("notes") or []

    call_log_ok = None  # True once a call log was read, False if the token can't read conversations

    def call_log(self, cid):
        """Outbound/inbound calls GHL logged for this contact (dialer or GHL app calls).
        Calls Tala makes from her own cell aren't here -- her "no answer" notes cover those.
        -> list of {direction, status, duration, date}; [] if none or not readable."""
        if self.call_log_ok is False:
            return []
        r = self._req("GET", "/conversations/search", params={"locationId": LOC, "contactId": cid})
        if r is None or r.status_code in (401, 403):
            self.call_log_ok = False
            return []
        if r.status_code != 200:
            return []
        out = []
        for conv in (r.json().get("conversations") or [])[:3]:
            m = self._req("GET", f"/conversations/{conv.get('id')}/messages",
                          params={"type": "TYPE_CALL", "limit": 100})
            if m is None or m.status_code in (401, 403):
                self.call_log_ok = False
                return []
            if m.status_code != 200:
                continue
            body = m.json().get("messages") or {}
            msgs = body.get("messages") if isinstance(body, dict) else body
            for x in msgs or []:
                mt = str(x.get("messageType") or "").upper()
                if mt and "CALL" not in mt:
                    continue  # asked for calls only; skip anything else GHL sends back
                call = ((x.get("meta") or {}).get("call") or {})
                out.append({"direction": x.get("direction") or "outbound",
                            "status": call.get("status") or x.get("status") or "",
                            "duration": call.get("duration") or 0,
                            "date": x.get("dateAdded")})
        self.call_log_ok = True
        return out

    def opportunities(self, cid):
        r = self._req("GET", "/opportunities/search", params={"location_id": LOC, "contact_id": cid})
        if r is None or r.status_code != 200:
            return []
        return r.json().get("opportunities") or []

    def create_opportunity(self, cid, name, pipeline_id, stage_id):
        if not (pipeline_id and stage_id):
            return None
        r = self._req("POST", "/opportunities/", write=True,
                      json={"locationId": LOC, "pipelineId": pipeline_id, "pipelineStageId": stage_id,
                            "contactId": cid, "name": name, "status": "open"})
        if r is not None and r.status_code in (200, 201):
            return (r.json().get("opportunity") or {}).get("id")
        return None

    def delete_contact(self, cid):
        r = self._req("DELETE", f"/contacts/{cid}", write=True)
        return r is None or r.status_code in (200, 201, 404)

    def tagged_delete(self):
        """Contact ids Tala tagged 'delete' (any contact, crawler or not)."""
        ids, page = [], 1
        while page <= 20:
            r = self._req("POST", "/contacts/search",
                          json={"locationId": LOC, "page": page, "pageLimit": 100,
                                "filters": [{"field": "tags", "operator": "contains", "value": "delete"}]})
            if r is None or r.status_code != 200:
                return ids, (None if r is None else r.status_code)
            batch = r.json().get("contacts") or []
            ids += [c["id"] for c in batch
                    if any(t.strip().lower() == "delete" for t in (c.get("tags") or []))]
            if len(batch) < 100:
                break
            page += 1
        return ids, 200


# ------------------------------------------------------------------ database
def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""create table if not exists morbius_contacts (
                         lead_id uuid primary key, ghl_contact_id text, opportunity_id text,
                         written jsonb not null default '{}'::jsonb,
                         created_at timestamptz not null default now(), updated_at timestamptz,
                         deleted_at timestamptz, delete_reason text, backup jsonb,
                         readded_count int not null default 0)""")
        cur.execute("""create table if not exists morbius_runs (
                         id bigserial primary key, run_at timestamptz not null default now(),
                         mode text not null, counts jsonb not null, notes text)""")
        cur.execute("alter table morbius_contacts enable row level security")
        cur.execute("alter table morbius_runs enable row level security")
        cur.execute("alter table leads add column if not exists story text")
        cur.execute("alter table morbius_contacts add column if not exists deleted_tags text[]")
        # 2026-11-12 (phone_grade.py): phone details, litigator / deceased flags, every name traced,
        # and where a lead stands after every number failed ('idicore search' / 'unreachable')
        for col, typ in (("phone_meta", "jsonb"), ("litigator", "boolean"), ("trace_deceased", "boolean"),
                         ("trace_history", "jsonb"), ("bad_phones", "text[]"), ("ghl_feedback", "text[]"),
                         ("retrace_reason", "text"), ("reach_status", "text")):
            cur.execute(f"alter table leads add column if not exists {col} {typ}")
        cur.execute("""create table if not exists morbius_skiptrace_queue (
                         lead_id text primary key, status text not null, reason text, confidence int,
                         list_count int, days_on_list int, updated_at timestamptz default now(),
                         alerted_at timestamptz, approved_at timestamptz)""")
        cur.execute("alter table morbius_skiptrace_queue add column if not exists retrace_first text")
        cur.execute("alter table morbius_skiptrace_queue add column if not exists retrace_last text")
        cur.execute("alter table morbius_skiptrace_queue enable row level security")
        cur.execute("revoke all on morbius_skiptrace_queue from anon, authenticated")
        cur.execute("""create table if not exists morbius_worked (ghl_contact_id text primary key,
                         first_seen timestamptz default now())""")
        cur.execute("""create table if not exists morbius_state (key text primary key, value bigint not null default 0)""")
        cur.execute("alter table morbius_worked enable row level security")
        cur.execute("alter table morbius_state enable row level security")
        cur.execute("revoke all on morbius_worked, morbius_state from anon, authenticated")
        # 2026-11-10: "delete in 5 days" tags -- when each was added, and whether Tala kept the lead
        cur.execute("""create table if not exists morbius_delete_watch (ghl_contact_id text primary key,
                         tagged_at timestamptz not null default now(), kept_at timestamptz,
                         deleted_at timestamptz)""")
        cur.execute("alter table morbius_delete_watch enable row level security")
        cur.execute("revoke all on morbius_delete_watch from anon, authenticated")
    conn.commit()


def load_map(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("select * from morbius_contacts")
        return {str(r["lead_id"]): r for r in cur.fetchall()}



# ------------------------------------------------------------------ confidence
# Which public office / data source each distress tag comes from. Two tags from
# the same office (tax sale + redemption) count as ONE source.
SOURCE_OF = {
    "tax_sale": "county tax collector", "repeat_tax_delinquent": "county tax collector",
    "probate": "probate court", "foreclosure_mie": "Master-in-Equity court",
    "hoa_foreclosure": "Master-in-Equity court", "code_violation": "county code enforcement",
    "permit_expired": "city permits", "permit_demolition": "city permits", "insurance_damage": "city permits",
    "pre_foreclosure": "PropStream", "pre_probate": "PropStream", "involuntary_lien": "PropStream",
    "failed_listing": "PropStream",
}
RAW_KEY = {"county tax collector": ["tax_sale", "redemption_period"], "probate court": ["probate"],
           "Master-in-Equity court": ["foreclosure_mie"], "county code enforcement": ["code_violation"],
           "city permits": ["building_permits", "permit"]}
ID_DOUBT = {"probate_name_match", "missing_owner", "probate_owner_mismatch", "corrupted_address"}


def _fresh(raw, source, days=7):
    for k in RAW_KEY.get(source, []):
        f = (raw.get(k) or {}).get("fetched_at") if isinstance(raw.get(k), dict) else None
        if f:
            try:
                t = datetime.fromisoformat(f.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - t).days <= days:
                    return True
            except ValueError:
                pass
    return False


def past_deadline(raw, today=None):
    """2026-11-08 (Tala's rule): a lead whose Master-in-Equity foreclosure sale
    date or tax-sale redemption deadline has already PASSED may already be sold.
    Until a source shows it again (the weekly MIE ingest re-tags it with a new
    future sale date, or the redemption record is refreshed), we can't be sure
    the owner still owns it -- so it can never be 'full confidence' or urgent.
    Returns a plain-English reason, or None."""
    today = today or datetime.now(timezone.utc).date()
    checks = (("Master-in-Equity sale date", (raw.get("foreclosure_mie") or {}).get("sale_date")),
              ("tax-sale redemption deadline", (raw.get("redemption_period") or {}).get("redemption_deadline")))
    for label, val in checks:
        if not val:
            continue
        v = str(val).strip()
        dd = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                dd = datetime.strptime(v[:10], fmt).date()
                break
            except ValueError:
                pass
        if dd and dd < today:
            return f"{label} {dd:%m/%d/%Y} already passed -- may be sold, verify before calling or tracing"
    return None


def n_sources(lead):
    """How many independent offices / data sources show this lead's situation."""
    return len({SOURCE_OF[t] for t in (lead.get("source_tags") or []) if t in SOURCE_OF})


def urgent_lead(lead, raw, today=None):
    """Tala's rule (2026-11-10): urgent = 3+ independent sources AND Confidence 100
    (fresh data, clean identity) AND no passed sale/redemption deadline."""
    conf, _ = confidence(lead, raw)
    return flow.is_urgent(conf, n_sources(lead), past_deadline(raw, today))


AUTO_MAX = 90  # 2026-11-11 (Tala): 100 means "verified by hand, nothing left to check"


def _owner_tokens(name):
    return set(_re.findall(r"[A-Z]+", (name or "").upper()))


def _probate_is_owner(lead, raw):
    """2026-11-11 (training): probate only counts when the person who died is ON
    THE DEED. The court file's address can be a relative's house (the PR lives
    there), which made one of the top-10 leads a false lead."""
    p = raw.get("probate") or {}
    dec = p.get("decedent_name") or ""
    if not dec or "," not in dec:
        return True  # can't tell; keep the old behaviour
    last, rest = [x.strip() for x in dec.split(",", 1)]
    first = (rest.split() or [""])[0]
    own = _owner_tokens(lead.get("owner_name"))
    return bool(last) and last.upper() in own and (not first or first.upper() in own)


def _tax_paid(raw):
    """Delinquent tax year marked paid (tax_paid_check.py). Returns (record, before_sale).
    Paid BEFORE the tax sale = the owner paid, the tax signal is resolved.
    Paid AFTER the sale is ambiguous: the county also marks the year paid when the
    tax-sale BUYER's money posts, so it may NOT mean the owner redeemed."""
    tp = raw.get("tax_paid") or {}
    if not tp.get("paid_on"):
        return None, None
    try:
        paid = datetime.strptime(tp["paid_on"], "%m/%d/%Y").date()
    except ValueError:
        return None, None
    sale = (raw.get("redemption_period") or {}).get("sale_date")
    try:
        sale_d = datetime.strptime(str(sale)[:10], "%Y-%m-%d").date() if sale else None
    except ValueError:
        sale_d = None
    return tp, (sale_d is None or paid < sale_d)


def _recent_full_price(raw, today=None):
    ao = raw.get("absentee_owner") or {}
    try:
        price = float(ao.get("sale_price") or 0)
        d = datetime.strptime(str(ao.get("deed_date") or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    today = today or datetime.now(timezone.utc).date()
    if price >= 50000 and (today - d).days <= 3 * 365:
        return f"bought {d:%m/%d/%Y} for ${price:,.0f} (recent full-price purchase, little margin)"
    return None


def confidence(lead, raw):
    """0-100: how sure we are this owner is really in this situation right now.
    2026-11-11 rules from Tala's hand-check of the top 10:
      - 100 only when verified by hand (raw.morbius_override); automatic max is 90
      - taxes paid after the sale -> the tax signal no longer counts
      - probate counts only if the person who died is on the deed
      - PropStream alone (pre-foreclosure / deceased / liens) is unconfirmed: max 40
      - an expired permit / insurance flag alone is not distress: max 20
      - a full-price purchase in the last 3 years: max 30
    A passed foreclosure-sale / redemption deadline caps it at 50 (see past_deadline)."""
    ov = raw.get("morbius_override") or {}
    if ov.get("confidence") is not None:
        return int(ov["confidence"]), "hand-checked by Tala + Morbius: " + (ov.get("note") or "verified")
    score, why = _confidence_auto(lead, raw)
    notes = []
    tags = set(lead.get("source_tags") or [])
    paid, before_sale = _tax_paid(raw)
    srcs = {SOURCE_OF[t] for t in tags if t in SOURCE_OF}
    if paid and "county tax collector" in srcs:
        if before_sale:
            notes.append(f"taxes for {paid.get('tax_year')} were paid {paid['paid_on']}, before any sale, so the tax signal is gone")
            srcs.discard("county tax collector")
        else:
            notes.append(f"county shows {paid.get('tax_year')} taxes paid {paid['paid_on']}, AFTER the tax sale: "
                         "that can be the owner redeeming OR the tax-sale buyer's payment, so ask the Tax Collector "
                         "whether it was redeemed")
            score = min(score, 60)
    if "probate court" in srcs and not _probate_is_owner(lead, raw):
        notes.append("the probate is for someone NOT on the deed (likely a relative), so it doesn't count")
        srcs.discard("probate court")
    if not srcs:
        score = min(score, 10)
        notes.append("no live distress left")
    elif srcs == {"PropStream"}:
        score = min(score, 40)
        notes.append("PropStream only, not confirmed by the county or court; check who filed any lis pendens")
    elif srcs == {"city permits"}:
        score = min(score, 20)
        notes.append("an expired permit alone is not distress")
    elif srcs <= {"city permits", "PropStream"}:
        score = min(score, 40)
    rf = _recent_full_price(raw)
    if rf:
        score = min(score, 30)
        notes.append(rf)
    # 2026-11-12 (Tala): how reachable the best number is counts too, so the freshest,
    # most reachable leads sit at the top. Only traced leads (sync attaches "_callable").
    if lead.get("skiptrace_status") == "traced" and "_callable" in lead:
        pen, reason = pg.confidence_penalty(lead["_callable"], lead.get("phone_meta") or {},
                                            traced_deceased=bool(lead.get("trace_deceased")))
        if pen:
            score = max(0, score - pen)
        notes.append(reason)
    score = min(score, AUTO_MAX)
    if notes:
        why += "; " + "; ".join(notes)
    return score, why


def _confidence_auto(lead, raw):
    srcs = []
    for t in lead.get("source_tags") or []:
        s = SOURCE_OF.get(t)
        if s and s not in srcs:
            srcs.append(s)
    if not srcs:
        return 0, "no live distress source"
    county = [s for s in srcs if s != "PropStream"]
    fresh = [s for s in county if _fresh(raw, s)]
    score = 45 if (fresh or not county) else 35
    if len(srcs) >= 2:
        score += 25
    if len(srcs) >= 3:
        score += 10
    reasons = set(lead.get("review_reasons") or [])
    id_ok = not (reasons & ID_DOUBT)
    if id_ok:
        score += 15
    if county and len(fresh) == len(county):
        score += 15
    elif not county:
        score += 5  # PropStream only: monthly data, never rechecked live
    if lead.get("distress_any_stale"):
        score -= 20
    if not id_ok:
        score -= 15
    score = max(0, min(100, score))
    bits = [f"{s} (checked this week)" if s in fresh else s for s in srcs]
    why = " + ".join(bits) + ("; owner identity clean" if id_ok else "; owner identity uncertain")
    gone = past_deadline(raw)
    if gone:
        score = min(score, 50)
        why += "; " + gone
    return score, why

# ------------------------------------------------------------------ building a contact
def row_dict(lead, today):
    body = list(csv.reader(io.StringIO(to_csv([lead], today))))
    return dict(zip(body[0], body[1])) if len(body) > 1 else {}


SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def nice_name(n):
    """'HALL, JOHN ESPER JR' / 'HALL , JOHN' -> 'John Esper Hall Jr'."""
    n = " ".join((n or "").split())
    if "," in n:
        last, rest = [x.strip() for x in n.split(",", 1)]
        parts = rest.split()
        suf = [p for p in parts if p.upper().strip(".") in SUFFIXES]
        given = [p for p in parts if p not in suf]
        n = " ".join(given + [last] + suf)
    return n.title()


def heat_tag(days):
    if days >= 90:
        return "very hot"
    if days >= 60:
        return "hot"
    if days >= 30:
        return "warm"
    return None


def days_on_list(lead):
    c = lead.get("created_at")
    if not c:
        return 0
    if c.tzinfo is None:
        c = c.replace(tzinfo=timezone.utc)
    return max(0, (datetime.now(timezone.utc) - c).days)


def years_owned(d):
    s = (d.get("Last Recorded Sale Date") or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return max(0, (date.today() - datetime.strptime(s, fmt).date()).days // 365)
        except ValueError:
            pass
    return None


BANKRUPT = {}  # lead id -> (chapter, date_filed, case_number), filled in main()
# 2026-11-10: the new lead-flow changes that touch GHL (deed names, holding new leads during a
# backlog, "delete in 5 days" tags and removals) run REPORT-ONLY until Tala approves them after
# seeing the first report. Approval = morbius_state key 'flow_live' set to 1.
FLOW_LIVE = False


RECHECK_TAG = "check: distress may be gone"


def build(lead, d, phones, fields, readded=False, readd_reason=""):
    tags_src = set(lead.get("source_tags") or [])
    owner = owner_trace_names(lead.get("owner_name"))
    probate = bool(tags_src & PROBATE_TAGS)
    raw = lead.get("raw") or {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    days = days_on_list(lead)

    call_first, call_last, _, _ = compute_skiptrace_identity(lead, list(tags_src))
    info = []
    if probate and d.get("Personal Representative"):
        call_line = f"Call: {nice_name(d['Personal Representative'])} (Personal Representative)"
        info.append(call_line)
    elif probate and (call_first or call_last):
        call_line = f"Call: {nice_name(call_first + ' ' + call_last)} (family contact)"
        info.append(call_line)
    else:
        call_line = "Call: owner"
    info.append(f"Traced: {str(d.get('Last Skip-Traced') or '').strip() or 'yes'}")
    # 2026-11-12: every number with its grade (A best .. D worst), best first
    meta = lead.get("phone_meta") or {}
    dec = bool(lead.get("trace_deceased"))
    for p_, g_ in pg.graded(phones, meta, traced_deceased=dec):
        info.append(pg.describe(p_, meta.get(p_), g_))
    if lead.get("_best_dnc"):
        info.append("CAUTION: the best number for this person is on the Do Not Call registry, so it is not "
                    "listed. Mail or door knock (Larry is reviewing whether it can be called).")
    if lead.get("_bad_note"):
        info.append(lead["_bad_note"])

    rp = raw.get("redemption_period") or {}
    if d.get("Foreclosure Auction Date"):
        nxt = f"Foreclosure auction {d['Foreclosure Auction Date']}"
    elif rp.get("redemption_deadline"):
        nxt = f"Tax sale redemption deadline {rp['redemption_deadline']} (sold at tax sale {rp.get('sale_date', '')})"
    else:
        nxt = d.get("Tax Sale Status") or ""
    heirs = probate_heirs(raw.get("probate"), d.get("Personal Representative"), lead.get("owner_name")) if probate else ""

    conf, why = confidence(lead, raw)
    story = (lead.get("story") or d.get("Call Brief") or "").strip()
    cf = {
        "crawler id": str(lead["id"]),
        "confidence": conf,
        "their story": (f"{readd_reason}\n\n" if readd_reason else "") + (
            f"Bankruptcy: Chapter {BANKRUPT[str(lead['id'])][0]} filed {BANKRUPT[str(lead['id'])][1]} "
            f"(SC case {BANKRUPT[str(lead['id'])][2]}). Check: confirm it's the same owner and this property "
            f"before calling.\n\n" if str(lead["id"]) in BANKRUPT else "") + f"Confidence {conf}%: {why}.\n\n{story}",
        "skip trace info": "\n".join(info),
        "mailing address": d.get("Mailing Address") or "",
        "county": f"{lead.get('county') or 'Greenville'} County, SC",
        "distress signals": d.get("Distress List") or "",
        "days on list": days,
        "next deadline": nxt,
        "heirs": heirs or "",
        "name 2": (flow.second_owner(lead.get("owner_name"), deed_name(lead.get("owner_name"))[1]) if FLOW_LIVE
                   else " ".join(x for x in (owner.get("first2"), owner.get("last2")) if x)),
        # 2026-11-10 (Tala): county parcel number + BatchData backup email on every card
        "apn": (lead.get("pin") or "").strip(),
        # 2026-11-11 (Tala): every owner exactly as on the deed (General Info folder)
        "sellers name on deed": flow.deed_full_name(lead.get("owner_name")),
        "email 2": (lead.get("email_2") or "").strip(),
    }
    for i, p in enumerate(phones[1:5], start=2):
        cf[f"phone {i}"] = p
    # 2026-11-13 (Tala): probate court details into the Probate folder (Flo Rida's fields)
    if probate:
        cf.update(probate_fields(raw.get("probate") or {}, d, phones))
    yo = years_owned(d)
    if yo is not None:
        cf["years owned"] = yo

    tags = ["crawler"] + sorted({t.strip().lower() for t in (d.get("Distress List") or "").split(",") if t.strip()})
    if probate and "probate" not in tags:
        tags.append("probate")
    h = heat_tag(days)
    if h:
        tags.append(h)
    if readded:
        tags.append("re-added")
    if conf >= 95:
        tags.append("verified 95+")
    if "code_violation" in tags_src:
        tags.append("condemned")
    if str(lead["id"]) in BANKRUPT:
        tags.append("bankruptcy")
    if any(isinstance(v, dict) and v.get("mls_status") for k, v in raw.items() if k.startswith("propstream")):
        tags.append("listed on mls")
    if lead.get("_best_dnc"):
        tags.append(pg.BEST_DNC_TAG)
    # 2026-11-13 (Tala): the weekly live re-check found at least one problem already gone
    if lead.get("distress_any_stale"):
        tags.append(RECHECK_TAG)
    # 2026-11-12 (Tala): the best properties also go into the email campaign (Margaret segments them)
    if (lead.get("email") or "").strip() and conf >= pg.EMAIL_MIN_CONF:
        tags.append(pg.EMAIL_TAG)

    # 2026-11-10 (Tala): the owner exactly as on the deed -- middle names/initials and Jr/Sr kept,
    # a trust/estate/entity keeps its full name in First Name
    if FLOW_LIVE:
        first, last = deed_name(lead.get("owner_name"))
    else:
        first, last = (owner.get("first") or "").title(), (owner.get("last") or "").title()
    body = {
        "firstName": first, "lastName": last,
        "name": f"{first} {last}".strip(),
        "phone": phones[0],
        "address1": d.get("Property Address") or lead.get("address") or "",
        "city": d.get("Property City") or "",
        "state": d.get("Property State") or "SC",
        "postalCode": d.get("Property Zip") or "",
        "country": "US",
        "source": "Crawler",
        "customFields": [{"id": fields[k], "field_value": v} for k, v in cf.items()
                         if k in fields and v not in (None, "")],
    }
    return body, tags, call_line


def _iso(v):
    try:
        return datetime.strptime(str(v or "").strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return ""


def probate_fields(pr, d, phones):
    """Probate case -> the GHL Probate folder fields. Only facts from the court record:
    PR relationship and creditor deadline stay blank (the case file doesn't give them).
    PR Phone = Phone 1 only when the number was traced for the Personal Representative."""
    sub = (pr.get("case_subtype") or "").lower()
    will = "Will" if "testate" in sub and "intestate" not in sub else ("No will" if "intestate" in sub else "Unknown")
    pr_name = d.get("Personal Representative") or pr.get("pr_name") or ""
    out = {
        "personal representative": nice_name(pr_name) if pr_name else "",
        "probate case number": (pr.get("case_number") or "").strip(),
        "will": will,
        "date of death": _iso(pr.get("date_of_death")),
    }
    if pr_name and phones:
        out["pr phone"] = phones[0]
    return {k: v for k, v in out.items() if v}


def other_property_text(o):
    """One owner, several distressed properties, one GHL card (2026-11-11, Tala): each extra
    property gets its own score, signals, deadline and story under 'Other Properties Owned',
    so nothing about the second house is lost when she calls."""
    raw = o.get("raw") or {}
    if isinstance(raw, str):
        raw = json.loads(raw)
    conf, why = confidence(o, raw)
    rp = raw.get("redemption_period") or {}
    fm = raw.get("foreclosure_mie") or {}
    if fm.get("sale_date"):
        nxt = f"foreclosure sale {fm['sale_date']}"
    elif rp.get("redemption_deadline"):
        nxt = f"tax sale buy-back deadline {rp['redemption_deadline']} (sold {rp.get('sale_date', '')})"
    else:
        nxt = "no hard deadline found yet"
    signals = ", ".join(t.replace("_", " ") for t in (o.get("source_tags") or []))
    head = (f"{o.get('address') or ''}, {o.get('city') or ''} | Confidence {conf}% | {signals or 'no list'} | "
            f"{nxt}\nWhy: {why}.")
    story = (o.get("story") or "").strip()
    return f"{head}\n{story}" if story else head


def _norm(k, v):
    if isinstance(v, list):
        v = ", ".join(str(x) for x in v)
    v = str(v if v is not None else "").strip()
    if k == "phone" or k.startswith("phone ") or k == "pr phone":
        return "".join(ch for ch in v if ch.isdigit())[-10:]
    if k in ("date of death", "creditor deadline"):
        # GHL hands date fields back as epoch milliseconds or ISO text: compare the day
        if v.isdigit() and len(v) >= 12:
            return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date().isoformat()
        return v[:10]
    try:
        f = float(v.replace(",", "").replace("$", ""))
        return str(int(f)) if f == int(f) else str(f)
    except ValueError:
        pass
    return " ".join(v.lower().split())


def her_edits(contact, written, fields=None):
    """Fields whose GHL value no longer matches what Morbius last wrote = Tala's edits.
    Covers the standard fields (TRACKED) and every custom field Morbius writes
    (stored in `written` as "cf:<field name>"). GHL reformats values (phone ->
    +1XXXXXXXXXX, casing, 95 -> 95.0), so values are compared normalized; only a
    real change counts as her edit."""
    out = set()
    for k in TRACKED:
        if k in written and _norm(k, contact.get(k)) != _norm(k, written.get(k)):
            out.add(k)
    if fields:
        have = {f.get("id"): f.get("value", f.get("field_value")) for f in (contact.get("customFields") or [])}
        for k, v in written.items():
            if not k.startswith("cf:"):
                continue
            name = k[3:]
            fid = fields.get(name)
            if fid and _norm(name, have.get(fid)) != _norm(name, v):
                out.add(k)
    return out


def fill_email(api, cid, lead, contact, c):
    """2026-11-10 (Tala): put the BatchData email (best/tested first) on the card, but only when
    the GHL email box is empty, so her own typed email is never overwritten. Sent as its own
    update: if GHL refuses it (same email already on another contact) the lead still syncs."""
    em = (lead.get("email") or "").strip()
    if not em or (contact or {}).get("email"):
        return
    try:
        api.update_contact(cid, {"email": em})
        c["emails_added"] += 1
    except Exception as e:
        c["email_skipped"] += 1
        # Card id only (the run log is public): enough to find the card in GHL.
        print(f"  email not added to card {cid} ({type(e).__name__}): {str(e)[:80]}")


FIRST_STAGE_ID = None  # the Sellers pipeline's first stage, set in main()


def protected(api, cid):
    """Worked lead: a pipeline card moved off the first stage (e.g. to "Contacted"),
    closed/won/lost, or any note on the contact. GHL's search returns the stage as an
    id (pipelineStageId), so compare ids; fall back to the stage name if one is given."""
    for o in api.opportunities(cid):
        if (o.get("status") or "open") != "open":
            return True
        sid = o.get("pipelineStageId")
        if sid and FIRST_STAGE_ID and sid != FIRST_STAGE_ID:
            return True
        stage = (o.get("pipelineStageName") or o.get("stageName") or "").strip().lower()
        if not sid and stage and stage != FIRST_STAGE.lower():
            return True
    return api.has_notes(cid)


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="actually write to GHL")
    ap.add_argument("--limit", type=int, default=-1, help="max new contacts this run (0 = none, -1 = no limit)")
    a = ap.parse_args()
    if not (TOKEN and LOC and os.environ.get("DATABASE_URL")):
        print("Missing GHL_MORBIUS_TOKEN, GHL_LOCATION_ID or DATABASE_URL.")
        sys.exit(1)
    mode = "LIVE" if a.live else "PRACTICE (nothing written to GHL)"
    print(f"Morbius sync -- {mode}" + (f", limit {a.limit} new" if a.limit >= 0 else ", no limit on new"))

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    ensure_tables(conn)
    global FLOW_LIVE
    with conn.cursor() as cur:
        cur.execute("select value from morbius_state where key='flow_live'")
        r_ = cur.fetchone()
    FLOW_LIVE = bool(r_ and r_[0] == 1)
    print("Lead-flow changes: " + ("LIVE" if FLOW_LIVE else "REPORT-ONLY (waiting for Tala's okay)"))
    OPEN_ERRORS.clear()
    OPEN_ERRORS.update(open_errors(conn))
    load_tax_sale_info(conn)
    api = GHL(a.live)
    fields = api.custom_fields()
    if "probate case number" not in fields:  # Flo Rida's label may be "Probate Case #" etc.
        alt_ = next((n for n in fields if "case" in n and ("probate" in n or "#" in n)), None)
        if alt_:
            fields["probate case number"] = fields[alt_]
            print(f"Probate case number field found as '{alt_}'")
    miss_ = [n for n in ("personal representative", "pr phone", "probate case number", "will", "date of death")
             if n not in fields]
    print("Probate fields in GHL: " + ("all found" if not miss_ else "MISSING " + ", ".join(miss_)))
    missing = [n for n in ("crawler id", "their story", "skip trace info") if n not in fields]
    if missing:
        print(f"Missing custom fields in GHL: {', '.join(missing)} -- run Morbius setup first.")
        sys.exit(1)
    pipeline_id, stage_id, offer_ids = api.sellers_stage()
    global FIRST_STAGE_ID
    FIRST_STAGE_ID = stage_id
    print(f"Sellers pipeline found: {bool(pipeline_id)}; first stage found: {bool(stage_id)}")

    today = date.today()
    rows = lead_rows(conn, "house_leads")
    # Morbius's stories live on the leads table (the house_leads view doesn't carry them)
    with conn.cursor() as cur:
        cur.execute("select id::text, story from leads where story is not null")
        stories = dict(cur.fetchall())
    for r in rows:
        r["story"] = stories.get(str(r["id"]))
    print(f"Stories attached: {sum(1 for r in rows if r.get('story'))}")
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.bankruptcy_filings') is not null")
        if cur.fetchone()[0]:
            cur.execute("""select matched_lead_id, chapter, to_char(date_filed,'MM/DD/YYYY'), case_number
                           from bankruptcy_filings where matched_lead_id is not null""")
            for lid_, ch, df, cn in cur.fetchall():
                BANKRUPT[lid_] = (ch, df, cn)
    c_bk = len(BANKRUPT)
    print(f"Bankruptcy matches on file: {c_bk}")
    by_id = {str(r["id"]): r for r in rows}
    ok = clean_phones(conn)
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.phone_dnc') is not null")
        if cur.fetchone()[0]:
            cur.execute("select phone from phone_dnc where on_dnc")
            on_dnc = {x[0] for x in cur.fetchall()}
        else:
            on_dnc = set()
    # 2026-11-12: phone details, litigator / deceased flags and call feedback live on the leads table
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""select id::text, phone_meta, litigator, trace_deceased, trace_history, bad_phones,
                              retrace_reason, reach_status, skiptrace_input from leads
                       where skiptrace_status = 'traced'""")
        extra = {x["id"]: x for x in cur.fetchall()}
    for r in rows:
        x = extra.get(str(r["id"]))
        if x:
            for k in ("phone_meta", "litigator", "trace_deceased", "trace_history", "bad_phones",
                      "retrace_reason", "reach_status", "skiptrace_input"):
                r[k] = x[k]
            r["_callable"] = [p for p in good_phones(r) if ok.get(p)]
    mapped = load_map(conn)
    c = dict(new=0, linked_existing=0, updated=0, kept_her_edit=0, retrace=0, deleted_dead=0,
             deleted_tagged=0, protected_kept=0, skipped_no_clean_phone=0, readded=0, errors=0,
             emails_added=0, email_skipped=0, litigators_blocked=0, deleted_litigator=0, held_best_dnc=0,
             bad_numbers_found=0, retrace_next_name=0, idicore_search=0, unreachable=0,
             never_call_removed=0, never_call_dnd=0)
    litigator_cards = []
    new_budget = a.limit if a.limit >= 0 else 10 ** 9
    edit_keys = {}

    # ---------- FOLLOW-UP CHECK (2026-11-10, Tala's lead-flow safeguards; see morbius_flow.py)
    # For every crawler contact in GHL: has she worked it, is its next call overdue, how many
    # no-answers, did she ever talk to them, is it Do Not Call. Read-only.
    now = datetime.now(timezone.utc)
    activity = {}  # ghl contact id -> summary
    for lid, m in mapped.items():
        cid = m.get("ghl_contact_id")
        if not cid or m.get("deleted_at") or cid in activity:
            continue
        try:
            contact = api.get_contact(cid)
            if contact is None:
                continue
            have_ = {f.get("id"): f.get("value", f.get("field_value")) for f in (contact.get("customFields") or [])}

            def num_(name):
                try:
                    return float(str(have_.get(fields.get(name)) or 0).replace(",", ""))
                except ValueError:
                    return 0
            # the Power Dialer keeps its own count in "Call attempts"; Confidence sets its wait (2 or 3 days)
            notes_ = api.notes(cid)
            s = flow.summarize(contact, api.opportunities(cid), notes_, api.call_log(cid),
                               offer_ids, stage_id, confidence=num_("confidence"),
                               call_attempts=num_("call attempts"))
            # 2026-11-12: what her Call Result / Call Outcome / Working Numbers / notes say about the numbers
            id_name_ = {v: k for k, v in fields.items()}
            cf_ = {id_name_.get(fid, ""): val for fid, val in have_.items()}
            s["numbers"] = pg.bad_numbers(contact, cf_, notes_)
            s["card_numbers"] = set(pg.card_numbers(contact, cf_).values())
            # 2026-11-13 (Tala): she deletes bad numbers off the card by hand -- a number Morbius
            # wrote that's gone from the card is bad for good (never re-added in another slot)
            gone_ = pg.deleted_numbers(m.get("written"), s["card_numbers"])
            if gone_:
                fb_ = s["numbers"]
                fb_["bad"] = set(fb_.get("bad") or set()) | gone_
                fb_.setdefault("reasons", []).append("you deleted " + ", ".join(sorted(gone_)) + " from the card")
                c["deleted_by_tala"] = c.get("deleted_by_tala", 0) + len(gone_)
            s["added_at"] = m.get("created_at")
            s["tags"] = {t.strip().lower() for t in contact.get("tags") or []}
            s["lead_id"] = lid
            activity[cid] = s
        except Exception as e:
            print(f"  follow-up check skipped one contact: {type(e).__name__}")
    behind = sum(1 for s in activity.values() if flow.is_behind(s, s["added_at"], now))
    paused = behind >= flow.BACKLOG_LIMIT
    c["behind_on_follow_up"] = behind
    c["new_leads_paused"] = paused
    c["call_log"] = {True: "read", False: "not readable (token scope)", None: "no contacts"}[api.call_log_ok]
    c["held_for_backlog"] = 0
    print(f"Follow-up: {behind} of {len(activity)} crawler contacts behind; "
          f"{'NEW LEADS PAUSED' if paused else 'new leads flowing'} (pause at {flow.BACKLOG_LIMIT})")
    # ---------- BAD NUMBERS (2026-11-12, Tala): a wrong / disconnected / bad number never deletes a
    # lead. The number is marked dead; if no good number is left (or she reached the wrong person),
    # the next person tied to the property is queued for a re-trace (her yes first, as always);
    # when every name has been traced and nothing works -> "idicore search" (hot) / "unreachable".
    retrace_rows = []
    for cid, s in activity.items():
        fb = s.get("numbers") or {}
        lead = by_id.get(s["lead_id"])
        if not lead or not fb.get("bad"):
            continue
        new_bad = set(fb["bad"]) - set(lead.get("bad_phones") or [])
        if not new_bad and not fb.get("wrong_person"):
            continue
        c["bad_numbers_found"] += len(new_bad)
        lead["bad_phones"] = sorted(set(lead.get("bad_phones") or []) | set(fb["bad"]))
        lead["_callable"] = [p for p in good_phones(lead) if ok.get(p)]
        api.add_tags(cid, [pg.BAD_NUMBER_TAG])
        reason = "; ".join(fb.get("reasons") or [])[:200]
        status_ = None
        retrace_ = None
        if fb.get("wrong_person") or not lead["_callable"]:
            tags_ = set(lead.get("source_tags") or [])
            raw_ = lead.get("raw") or {}
            if isinstance(raw_, str):
                raw_ = json.loads(raw_)
            sf, sl, _, _ = compute_skiptrace_identity(lead, list(tags_))
            own = owner_trace_names(lead.get("owner_name"))
            prob = raw_.get("probate") or {}
            heirs_ = probate_heirs(prob, prob.get("pr_name"), lead.get("owner_name")) if prob else ""
            cands = pg.trace_candidates((sf, sl), heirs_, (own.get("first2"), own.get("last2")),
                                        (own.get("first"), own.get("last")),
                                        bool(tags_ & PROBATE_TAGS) or bool(lead.get("trace_deceased")))
            tried = [(h.get("first"), h.get("last")) for h in (lead.get("trace_history") or [])]
            inp_ = lead.get("skiptrace_input") or {}
            tried.append((inp_.get("first"), inp_.get("last")))
            nxt_ = pg.next_trace_target(cands, tried)
            if nxt_:
                retrace_ = (f"Numbers didn't reach the right person ({reason}). Next: trace "
                            f"{nxt_[0]} {nxt_[1]} ({nxt_[2]}).")
                retrace_rows.append((s["lead_id"], nxt_[0], nxt_[1], retrace_))
                c["retrace_next_name"] += 1
            else:
                conf_, _ = confidence(lead, raw_)
                status_ = pg.IDICORE_TAG if conf_ >= pg.IDICORE_MIN_CONF else pg.UNREACHABLE_TAG
                api.add_tags(cid, [status_])
                c["idicore_search" if status_ == pg.IDICORE_TAG else "unreachable"] += 1
        lead["retrace_reason"] = retrace_ or lead.get("retrace_reason")
        lead["_bad_note"] = f"Marked bad from your calls: {', '.join(sorted(new_bad)) or 'none new'} ({reason})"
        if a.live:
            with conn.cursor() as cur:
                cur.execute("""update leads set bad_phones = %s,
                                      ghl_feedback = coalesce(ghl_feedback, '{}') || %s::text[],
                                      retrace_reason = coalesce(%s, retrace_reason),
                                      reach_status = coalesce(%s, reach_status)
                               where id = %s""",
                            (lead["bad_phones"], [f"{today:%Y-%m-%d}: {reason}"], retrace_, status_, s["lead_id"]))
            conn.commit()

    # one owner, several properties -> one GHL contact: the first lead linked owns the card,
    # the others are listed under "Other Properties Owned" instead of overwriting it
    shared = {}
    for lid_, m_ in sorted(mapped.items(), key=lambda x: (x[1].get("created_at") is None, str(x[1].get("created_at")))):
        if m_.get("ghl_contact_id") and not m_.get("deleted_at"):
            shared.setdefault(m_["ghl_contact_id"], []).append(lid_)

    # ---------- GREEN / YELLOW / ORANGE
    for lead in rows:
        lid = str(lead["id"])
        if lead.get("lead_status") or lead.get("skiptrace_status") != "traced" or not_worked(lead):
            continue
        m = mapped.get(lid)
        # 2026-11-12 (Tala): litigators never go into GHL; one already there is removed below
        if lead.get("litigator"):
            c["litigators_blocked"] += 1
            if m and m.get("ghl_contact_id") and not m.get("deleted_at"):
                litigator_cards.append((lid, m["ghl_contact_id"]))
            continue
        meta_ = lead.get("phone_meta") or {}
        dec_ = bool(lead.get("trace_deceased"))
        # best graded number first (A..D), only numbers clean on Do Not Call and never reported wrong
        phones = [p for p, _g in pg.graded([p for p in good_phones(lead) if ok.get(p)], meta_, traced_deceased=dec_)]
        lead["_best_dnc"] = pg.best_is_dnc(good_phones(lead), meta_, ok, on_dnc, traced_deceased=dec_)
        readd_reason = ""
        if m and m.get("deleted_at"):
            src = set(lead.get("source_tags") or [])
            if (m.get("delete_reason") or "").startswith("tagged delete"):
                # Tala tagged it "delete": it stays gone unless it lands on a NEW list
                new_lists = sorted(src - set(m.get("deleted_tags") or []))
                if not new_lists:
                    continue
                readd_reason = (f"Re-added: now also on {', '.join(t.replace('_', ' ') for t in new_lists)} "
                                f"(you deleted it {m['deleted_at']:%m/%d/%Y}).")
            else:
                readd_reason = (f"Re-added: back on {', '.join(t.replace('_', ' ') for t in sorted(src))} "
                                f"(removed {m['deleted_at']:%m/%d/%Y} as a dead lead).")
        live_map = m and m.get("ghl_contact_id") and not m.get("deleted_at")
        if not phones:
            c["skipped_no_clean_phone"] += 1
            continue
        try:
            d = row_dict(lead, today)
            readded = bool(m and m.get("deleted_at"))
            body, tags, _ = build(lead, d, phones, fields, readded, readd_reason)
            if needs_retrace(lead):
                if live_map:
                    api.add_tags(m["ghl_contact_id"], ["retrace"])
                    c["retrace"] += 1
                continue
            written = {k: body.get(k) for k in TRACKED}
            id_to_name = {v: k for k, v in fields.items()}
            for f in body["customFields"]:
                if f["id"] in id_to_name:
                    written["cf:" + id_to_name[f["id"]]] = f["field_value"]
            if live_map:
                cid = m["ghl_contact_id"]
                prev_ = m.get("written") or {}
                if (prev_.get("firstName"), prev_.get("lastName")) != deed_name(lead.get("owner_name")):
                    c["names_to_fix"] = c.get("names_to_fix", 0) + 1
                owners = shared.get(cid, [lid])
                if owners[0] != lid:
                    continue  # second property of the same owner: shown on the first lead's card
                others = [by_id[x] for x in owners[1:] if x in by_id]
                if others and "other properties owned" in fields:
                    body["customFields"] = [f for f in body["customFields"] if f["id"] != fields["other properties owned"]]
                    body["customFields"].append({"id": fields["other properties owned"],
                                                 "field_value": "\n\n".join(other_property_text(o) for o in others)})
                contact = api.get_contact(cid)
                if contact is None:
                    continue  # she deleted it herself; it stays deleted
                prev = m.get("written") or {}
                # once Tala edits a field it stays hers forever (locked), not just for one run
                edits = her_edits(contact, prev, fields) | set(prev.get("_locked") or [])
                # 2026-11-13 (Tala): a phone number in a name field is a bad import, never her edit --
                # put the deed name back
                for k_ in ("firstName", "lastName"):
                    if pg.has_phone_digits(contact.get(k_)) and not pg.has_phone_digits(body.get(k_)):
                        edits.discard(k_)
                        c["names_with_number_fixed"] = c.get("names_with_number_fixed", 0) + 1
                upd = {k: v for k, v in body.items() if k not in edits and k != "source"}
                upd["customFields"] = [f for f in body["customFields"]
                                       if "cf:" + id_to_name.get(f["id"], "") not in edits]
                c["kept_her_edit"] += len(edits)
                for k in edits:
                    edit_keys[k] = edit_keys.get(k, 0) + 1
                api.update_contact(cid, upd)
                if live_map:
                    fill_email(api, cid, lead, contact, c)
                api.add_tags(cid, tags)
                stale = {"warm", "hot", "very hot", "verified 95+", pg.BEST_DNC_TAG, pg.EMAIL_TAG, RECHECK_TAG} - set(tags)
                api.remove_tags(cid, stale)
                have = {f.get("id"): f.get("value", f.get("field_value")) for f in (contact.get("customFields") or [])}
                for k in edits:
                    written[k] = have.get(fields.get(k[3:])) if k.startswith("cf:") else contact.get(k)
                written["_locked"] = sorted(edits)
                c["updated"] += 1
                if a.live:
                    with conn.cursor() as cur:
                        cur.execute("update morbius_contacts set written=%s, updated_at=now() where lead_id=%s",
                                    (json.dumps(written), lid))
            else:
                if new_budget <= 0:
                    continue
                if lead.get("_best_dnc"):
                    # her best number is on Do Not Call: no worse number gets dialed instead (mail / door knock)
                    c["held_best_dnc"] += 1
                    continue
                if paused and not FLOW_LIVE:
                    c["held_for_backlog"] += 1  # report-only: counted, still added
                elif paused:
                    raw_ = lead.get("raw") or {}
                    if isinstance(raw_, str):
                        raw_ = json.loads(raw_)
                    if not urgent_lead(lead, raw_, today):
                        # 100+ leads behind on follow-up: it waits (traced and ready) until she catches up
                        c["held_for_backlog"] += 1
                        continue
                new_budget -= 1
                cid, existed = api.create_contact(dict(body, tags=tags))
                if existed:
                    # already in GHL (a form lead or one she added): add the crawler info, keep her fields
                    api.add_tags(cid, tags)
                    api.update_contact(cid, {"customFields": body["customFields"]})
                    c["linked_existing"] += 1
                    written["_locked"] = list(TRACKED)  # her own contact: name/phone/address stay hers
                else:
                    c["new"] += 1
                if readded:
                    c["readded"] += 1
                opp = None
                if not existed and a.live:
                    fill_email(api, cid, lead, {}, c)
                if not existed:
                    opp = api.create_opportunity(cid, f"{body['name']} - {body['address1']}", pipeline_id, stage_id)
                if a.live:
                    with conn.cursor() as cur:
                        cur.execute("""insert into morbius_contacts (lead_id, ghl_contact_id, opportunity_id, written,
                                                                      updated_at, readded_count)
                                       values (%s,%s,%s,%s,now(),%s)
                                       on conflict (lead_id) do update set ghl_contact_id=excluded.ghl_contact_id,
                                         opportunity_id=coalesce(excluded.opportunity_id, morbius_contacts.opportunity_id),
                                         written=excluded.written, updated_at=now(), deleted_at=null, delete_reason=null,
                                         readded_count=morbius_contacts.readded_count + %s""",
                                    (lid, cid, opp, json.dumps(written), 1 if readded else 0, 1 if readded else 0))
            if a.live:
                conn.commit()
        except Exception as e:  # one bad lead never stops the run
            conn.rollback()
            c["errors"] += 1
            print(f"  lead skipped: {type(e).__name__}: {str(e)[:120]}")

    # ---------- NEVER-CALL NUMBERS (2026-11-14, Tala): a number on phone_suppression (STOP replies,
    # "wrong number" texts, anything she says must never be called) comes off every card. Phone 2-5 are
    # cleared, Phone 1 is replaced by the best good number, and a card with no good number left gets
    # Do Not Disturb. This overrides her own edits to those phone fields: it's a safety rule.
    with conn.cursor() as cur:
        cur.execute("select to_regclass('public.phone_suppression') is not null")
        if cur.fetchone()[0]:
            cur.execute("select phone from phone_suppression")
            blocked = {x[0] for x in cur.fetchall()}
        else:
            blocked = set()
    for cid, s in activity.items():
        if not (s.get("card_numbers") or set()) & blocked:
            continue
        try:
            contact = api.get_contact(cid)
            if contact is None:
                continue
            id_name_ = {v: k for k, v in fields.items()}
            cf_ = {id_name_.get(f.get("id"), ""): f.get("value", f.get("field_value"))
                   for f in (contact.get("customFields") or [])}
            lead = by_id.get(s["lead_id"]) or {}
            good_ = [p for p, _g in pg.graded([p for p in good_phones(lead) if ok.get(p)],
                                               lead.get("phone_meta") or {},
                                               traced_deceased=bool(lead.get("trace_deceased")))]
            p_ = never_call.plan(contact, cf_, blocked, good_)
            if not p_:
                continue
            body_ = never_call.update_body(p_, fields)
            if body_:
                api.update_contact(cid, body_)
            api.add_tags(cid, [never_call.NEVER_CALL_TAG])
            api.add_note(cid, never_call.note_text(p_))
            c["never_call_removed"] += len(p_["removed"])
            c["never_call_dnd"] += 1 if p_["dnd"] else 0
            if a.live:
                # keep `written` in step so these changes are never mistaken for her edits
                patch_ = {"cf:" + name: "" for name in p_["clear"]}
                unlock_ = ["cf:" + name for name in p_["clear"]]
                if p_["phone"]:
                    patch_["phone"] = p_["phone"]
                    unlock_.append("phone")
                with conn.cursor() as cur:
                    # read fresh (the update loop above may have rewritten `written` this run)
                    cur.execute("select lead_id, written from morbius_contacts where ghl_contact_id=%s", (cid,))
                    for lid_, w_ in cur.fetchall():
                        w_ = dict(w_ or {})
                        w_.update(patch_)
                        w_["_locked"] = [k for k in (w_.get("_locked") or []) if k not in unlock_]
                        cur.execute("update morbius_contacts set written=%s, updated_at=now() where lead_id=%s",
                                    (json.dumps(w_), lid_))
                conn.commit()
        except Exception as e:
            conn.rollback()
            c["errors"] += 1
            print(f"  never-call cleanup skipped one card: {type(e).__name__}")
    print(f"Never-call numbers: {c['never_call_removed']} removed from cards, {c['never_call_dnd']} cards set to DND")

    # ---------- SKIP-TRACE QUEUE (purple = on hold)
    # Tala's rule (Nov 7 2026): no skip tracing until at least 50 crawler leads in GHL have
    # been called/updated (pipeline card moved off New, closed, or a note logged). Until then
    # untraced leads wait PURPLE. Exception -> URGENT (she gets a chat + phone alert asking to
    # approve): Confidence 95+, on 3+ lists, or a hard deadline (foreclosure auction / tax-sale
    # redemption) within 60 days. Nothing is ever traced without her approval.
    # Every lead she has ever called/updated is remembered (deleting it later doesn't undo the count).
    now_worked = [cid_ for cid_, s in activity.items() if s["worked"]]
    with conn.cursor() as cur:
        if a.live:
            for cid_ in now_worked:
                cur.execute("insert into morbius_worked (ghl_contact_id) values (%s) on conflict do nothing", (cid_,))
            conn.commit()
        cur.execute("select count(*) from morbius_worked")
        worked = max(cur.fetchone()[0], len(now_worked))
        cur.execute("select value from morbius_state where key='released'")
        r_ = cur.fetchone()
        released = r_[0] if r_ else 0
        cur.execute("select lead_id from morbius_skiptrace_queue where status in ('ready','approved')")
        already_ready = {x[0] for x in cur.fetchall()}
    c["called_or_updated"] = worked
    # Tala's rule (2026-11-10): every 50 leads called/updated releases the next 50 hottest untraced
    # leads -- but nothing is released while 100+ leads are behind on follow-up (urgent ones still are).
    # Releases she earns during a pause aren't lost; they go out once she's caught up.
    earned = flow.earned_release(worked)
    to_release = 0 if paused else max(0, earned - released)
    queue = []
    for lead in rows:
        if lead.get("lead_status") or lead.get("skiptrace_status") in ("traced",) or not_worked(lead):
            continue
        if not (lead.get("list_count") or 0) or lead.get("is_duplicate"):
            continue
        raw = lead.get("raw") or {}
        if isinstance(raw, str):
            raw = json.loads(raw)
        conf, _ = confidence(lead, raw)
        days = days_on_list(lead)
        lc = lead.get("list_count") or 0
        why = []
        urgent = urgent_lead(lead, raw, today)
        if past_deadline(raw, today):
            # may already be sold: hold it (purple), never urgent, until a source confirms it's still live
            lid = str(lead["id"])
            queue.append([lid, "ready" if lid in already_ready else "purple", None, conf, lc, days,
                          (2, 999, -conf, -lc, -days)])  # 2 = never auto-released in a batch
            continue
        if urgent:
            why.append(f"Confidence {conf}%, confirmed by {n_sources(lead)} sources")
        if urgent and lc >= 3:
            why.append(f"on {lc} lists")
        d = row_dict(lead, today)
        soon = None
        for label, val in (("foreclosure auction", d.get("Foreclosure Auction Date")),
                           ("tax-sale redemption deadline", (raw.get("redemption_period") or {}).get("redemption_deadline"))):
            try:
                dd = date.fromisoformat(str(val)[:10]) if val else None
            except ValueError:
                dd = None
            if dd and today <= dd <= today + timedelta(days=60):
                if urgent:
                    why.append(f"{label} {dd:%m/%d/%Y}")
                soon = min(soon or dd, dd)
        lid = str(lead["id"])
        status = "urgent" if urgent else ("ready" if lid in already_ready else "purple")
        # hottest first: urgent, then the nearest deadline, Confidence, lists, days on list
        rank = (0 if urgent else 1, (soon - today).days if soon else 999, -conf, -lc, -days)
        queue.append([lid, status, "; ".join(why) or None, conf, lc, days, rank])
    queue.sort(key=lambda q: q[6])
    for q in queue:
        if to_release <= 0:
            break
        if q[1] == "purple" and q[6][0] != 2:
            q[1] = "ready"
            to_release -= 1
            released += 1
    queue = [tuple(q[:6]) for q in queue]
    # 2026-11-12: leads whose numbers failed, waiting for her yes to trace the next person
    retrace_names = {}
    for lid_, f_, l_, why_ in retrace_rows:
        lead_ = by_id.get(lid_) or {}
        raw_ = lead_.get("raw") or {}
        if isinstance(raw_, str):
            raw_ = json.loads(raw_)
        conf_, _ = confidence(lead_, raw_)
        queue.append((lid_, "retrace", why_, conf_, lead_.get("list_count") or 0, days_on_list(lead_)))
        retrace_names[lid_] = (f_, l_)
    with conn.cursor() as cur:
        cur.execute("""select q.lead_id, q.retrace_first, q.retrace_last, q.reason, q.confidence, q.list_count,
                              q.days_on_list from morbius_skiptrace_queue q join leads l on l.id::text = q.lead_id
                       where q.status in ('retrace', 'approved') and q.retrace_first is not null
                         and l.retrace_reason is not null""")
        for row_ in cur.fetchall():
            if row_[0] not in retrace_names:
                queue.append((row_[0], "retrace", row_[3], row_[4], row_[5], row_[6]))
                retrace_names[row_[0]] = (row_[1], row_[2])
    c["skiptrace_retrace"] = len(retrace_names)
    c["released_total"] = released
    c["next_batch_at"] = (worked // flow.RELEASE_PER + 1) * flow.RELEASE_PER
    c["skiptrace_urgent"] = sum(1 for q in queue if q[1] == "urgent")
    c["skiptrace_purple"] = sum(1 for q in queue if q[1] == "purple")
    c["skiptrace_ready"] = sum(1 for q in queue if q[1] == "ready")
    if a.live:
        with conn.cursor() as cur:
            cur.execute("""insert into morbius_state (key, value) values ('released', %s)
                           on conflict (key) do update set value = excluded.value""", (released,))
            ids = [q[0] for q in queue]
            cur.execute("delete from morbius_skiptrace_queue where not (lead_id = any(%s))", (ids,))
            for q in queue:
                cur.execute("""insert into morbius_skiptrace_queue (lead_id, status, reason, confidence, list_count,
                                 days_on_list, updated_at) values (%s,%s,%s,%s,%s,%s,now())
                               on conflict (lead_id) do update set
                                 status = case when morbius_skiptrace_queue.approved_at is not null then 'approved'
                                               else excluded.status end,
                                 reason=excluded.reason, confidence=excluded.confidence,
                                 list_count=excluded.list_count, days_on_list=excluded.days_on_list, updated_at=now(),
                                 alerted_at = case when excluded.status in ('urgent', 'retrace')
                                                   then morbius_skiptrace_queue.alerted_at end""", q)
            for lid_, (f_, l_) in retrace_names.items():
                cur.execute("update morbius_skiptrace_queue set retrace_first=%s, retrace_last=%s where lead_id=%s",
                            (f_, l_, lid_))
        conn.commit()

    # ---------- VERIFY: read back up to 10 GHL cards whose lead has a Morbius story
    if a.live and "their story" in fields:
        checked = shown = 0
        for lid, m in load_map(conn).items():
            lead = by_id.get(lid)
            if checked >= 10 or not lead or not lead.get("story") or m.get("deleted_at") or not m.get("ghl_contact_id"):
                continue
            if "cf:their story" in ((m.get("written") or {}).get("_locked") or []):
                continue
            contact = api.get_contact(m["ghl_contact_id"])
            if contact is None:
                continue
            checked += 1
            val = next((f.get("value") for f in contact.get("customFields") or [] if f.get("id") == fields["their story"]), "")
            shown += "Here's the stitch" in str(val)
        print(f"Verified in GHL: {shown}/{checked} cards show Morbius's story")

    # ---------- RED: dead leads + her "delete" tag
    dead = []
    for lid, m in mapped.items():
        if not m.get("ghl_contact_id") or m.get("deleted_at"):
            continue
        lead = by_id.get(lid)
        if lead is None or lead.get("lead_status") == "dead":
            dead.append((lid, m["ghl_contact_id"], "dead lead: no longer on any list / resolved / sold"))
        elif not_worked(lead):
            dead.append((lid, m["ghl_contact_id"], "mobile/manufactured home: not worked (kept in master list)"))
    tagged, status = api.tagged_delete()
    if status != 200:
        print(f"Could not search for 'delete' tags (HTTP {status}); dead-lead deletes still checked.")
    by_contact = {m["ghl_contact_id"]: lid for lid, m in mapped.items() if m.get("ghl_contact_id")}
    to_delete = []
    for lid, cid, why in dead:
        if protected(api, cid):
            c["protected_kept"] += 1
            continue
        to_delete.append((lid, cid, why, "deleted_dead"))
    seen = {x[1] for x in to_delete}
    for lid, cid in litigator_cards:
        # Two leads (same owner) can share one merged card: delete the card once, mark both leads.
        if cid in seen:
            continue
        seen.add(cid)
        to_delete.append((lid, cid, "litigator (BatchData): never in GHL -- Tala's rule", "deleted_litigator"))
    for cid in tagged:
        if cid not in seen:
            to_delete.append((by_contact.get(cid), cid, "tagged delete by Tala", "deleted_tagged"))
    if len(to_delete) > DELETE_BRAKE:
        print(f"SAFETY BRAKE: {len(to_delete)} deletes planned (limit {DELETE_BRAKE}). Nothing deleted -- "
              "Tala approves first.")
        c["brake_held"] = len(to_delete)
    else:
        for lid, cid, why, key in to_delete:
            contact = api.get_contact(cid)
            if contact is None:
                # Card already gone from GHL: record it so no lead keeps pointing at a missing card.
                if a.live:
                    with conn.cursor() as cur:
                        cur.execute("""update morbius_contacts set deleted_at=now(), delete_reason=%s
                                       where ghl_contact_id=%s and deleted_at is null""",
                                    (why + " (card already gone)", cid))
                    conn.commit()
                continue
            if api.delete_contact(cid):
                c[key] += 1
                if a.live:
                    with conn.cursor() as cur:
                        if lid:
                            src = list((by_id.get(lid) or {}).get("source_tags") or [])
                            cur.execute("""update morbius_contacts set deleted_at=now(), delete_reason=%s, backup=%s,
                                           deleted_tags=%s where lead_id=%s""", (why, json.dumps(contact), src, lid))
                            # Other leads sharing this merged card are gone from GHL too.
                            cur.execute("""update morbius_contacts set deleted_at=now(), delete_reason=%s
                                           where ghl_contact_id=%s and deleted_at is null""", (why, cid))
                        else:
                            cur.execute("""insert into morbius_contacts (lead_id, ghl_contact_id, deleted_at,
                                             delete_reason, backup)
                                           values (gen_random_uuid(), %s, now(), %s, %s)""",
                                        (cid, why, json.dumps(contact)))
                    conn.commit()

    # ---------- DEAD WEIGHT (2026-11-10): 6+ no-answers, never talked -> "delete in 5 days" tag,
    # removed 5 days later unless Tala took the tag off. Do Not Call / talked-to / offer-stage
    # contacts are never tagged or removed.
    with conn.cursor() as cur:
        cur.execute("select ghl_contact_id, tagged_at, kept_at, deleted_at from morbius_delete_watch")
        watch = {r[0]: r for r in cur.fetchall()}
    c.update(tagged_delete_5d=0, kept_by_tala=0, untagged_now_safe=0, deleted_no_answer=0, due_delete=0)
    due = []

    def keep(cid_, tag_on):
        if tag_on:
            api.remove_tags(cid_, [flow.DELETE_TAG])
        if a.live:
            with conn.cursor() as cur:
                cur.execute("""insert into morbius_delete_watch (ghl_contact_id, kept_at) values (%s, now())
                               on conflict (ghl_contact_id) do update set kept_at = now()""", (cid_,))
            conn.commit()

    if not FLOW_LIVE:
        # report-only: what the first live run would tag (nothing is tagged or removed yet)
        c["would_tag_delete_5d"] = sum(1 for s in activity.values()
                                       if flow.dead_weight(s) and flow.DELETE_TAG not in s["tags"])
    for cid, s in (activity.items() if FLOW_LIVE else []):
        w = watch.get(cid)
        tag_on = flow.DELETE_TAG in s["tags"]
        if w and (w[2] or w[3]):
            continue  # she kept it, or it's already gone
        if s["dnc"] or s["talked"] or s["past_offer"] or (s["tags"] & {pg.BAD_NUMBER_TAG, pg.IDICORE_TAG,
                                                                         pg.UNREACHABLE_TAG, pg.BEST_DNC_TAG}):
            if tag_on or w:
                keep(cid, tag_on)  # never delete a Do Not Call / talked-to / offer-stage contact
                c["untagged_now_safe"] += 1
            continue
        if w:
            if not tag_on:
                keep(cid, False)  # Tala took the tag off: keep this lead for good
                c["kept_by_tala"] += 1
            elif (now - flow.parse_dt(w[1])).total_seconds() >= flow.DELETE_AFTER_DAYS * 86400:
                due.append(cid)
            continue
        if tag_on or flow.dead_weight(s):
            if not tag_on:
                api.add_tags(cid, [flow.DELETE_TAG])
            c["tagged_delete_5d"] += 1
            if a.live:
                with conn.cursor() as cur:
                    cur.execute("insert into morbius_delete_watch (ghl_contact_id) values (%s) on conflict do nothing",
                                (cid,))
                conn.commit()
    c["due_delete"] = len(due)
    if due and len(to_delete) + len(due) > DELETE_BRAKE:
        print(f"SAFETY BRAKE: {len(to_delete) + len(due)} deletes planned (limit {DELETE_BRAKE}). "
              "No 'delete in 5 days' contacts removed -- Tala approves first.")
        c["brake_held"] = c.get("brake_held", 0) + len(due)
    else:
        for cid in due:
            contact = api.get_contact(cid)
            if contact is None or flow.DELETE_TAG not in {t.strip().lower() for t in contact.get("tags") or []}:
                continue  # gone already, or she removed the tag a moment ago
            if api.delete_contact(cid):
                c["deleted_no_answer"] += 1
                if a.live:
                    with conn.cursor() as cur:
                        # "tagged delete..." = it stays gone unless it lands on a NEW list
                        cur.execute("""update morbius_contacts set deleted_at=now(), backup=%s,
                                         delete_reason='tagged delete in 5 days: 6+ no answers, never reached',
                                         deleted_tags=(select source_tags from leads where id=morbius_contacts.lead_id)
                                       where ghl_contact_id=%s and deleted_at is null""", (json.dumps(contact), cid))
                        cur.execute("update morbius_delete_watch set deleted_at=now() where ghl_contact_id=%s", (cid,))
                    conn.commit()

    c["api_calls"] = api.calls
    if a.live:
        with conn.cursor() as cur:
            cur.execute("insert into morbius_runs (mode, counts) values (%s,%s)", ("live", json.dumps(c)))
        conn.commit()
    conn.close()
    print("Morbius counts: " + ", ".join(f"{k} {v}" for k, v in c.items()))
    if edit_keys:
        print("Her edits kept, by field: " + ", ".join(f"{k} x{v}" for k, v in sorted(edit_keys.items())))


if __name__ == "__main__":
    main()
