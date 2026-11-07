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
import requests

from export_call_lists import OPEN_ERRORS, clean_phones, lead_rows, load_tax_sale_info, needs_retrace, open_errors, to_csv
from ghl_sync import good_phones
from skiptrace_prep import compute_skiptrace_identity, owner_trace_names, probate_heirs

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
        r = self._req("GET", "/opportunities/pipelines", params={"locationId": LOC})
        r.raise_for_status()
        for p in r.json().get("pipelines", []):
            if p.get("name", "").strip().lower() == PIPELINE_NAME.lower():
                stages = p.get("stages") or []
                first = next((s for s in stages if s.get("name", "").strip().lower() == FIRST_STAGE.lower()),
                             stages[0] if stages else None)
                return p["id"], (first or {}).get("id")
        return None, None

    def get_contact(self, cid):
        r = self._req("GET", f"/contacts/{cid}")
        if r is None or r.status_code == 404:
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

    def has_notes(self, cid):
        r = self._req("GET", f"/contacts/{cid}/notes")
        return bool(r is not None and r.status_code == 200 and r.json().get("notes"))

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
        cur.execute("""create table if not exists morbius_skiptrace_queue (
                         lead_id text primary key, status text not null, reason text, confidence int,
                         list_count int, days_on_list int, updated_at timestamptz default now(),
                         alerted_at timestamptz, approved_at timestamptz)""")
        cur.execute("alter table morbius_skiptrace_queue enable row level security")
        cur.execute("revoke all on morbius_skiptrace_queue from anon, authenticated")
        cur.execute("""create table if not exists morbius_worked (ghl_contact_id text primary key,
                         first_seen timestamptz default now())""")
        cur.execute("""create table if not exists morbius_state (key text primary key, value bigint not null default 0)""")
        cur.execute("alter table morbius_worked enable row level security")
        cur.execute("alter table morbius_state enable row level security")
        cur.execute("revoke all on morbius_worked, morbius_state from anon, authenticated")
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


def confidence(lead, raw):
    """0-100: how sure we are this owner is really in this situation right now.
    95+ needs 2+ independent live sources, a clean identity and fresh data."""
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
        "name 2": " ".join(x for x in (owner.get("first2"), owner.get("last2")) if x),
    }
    for i, p in enumerate(phones[1:5], start=2):
        cf[f"phone {i}"] = p
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

    first = (owner.get("first") or "").title()
    last = (owner.get("last") or "").title()
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


def _norm(k, v):
    if isinstance(v, list):
        v = ", ".join(str(x) for x in v)
    v = str(v if v is not None else "").strip()
    if k == "phone" or k.startswith("phone "):
        return "".join(ch for ch in v if ch.isdigit())[-10:]
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
    OPEN_ERRORS.clear()
    OPEN_ERRORS.update(open_errors(conn))
    load_tax_sale_info(conn)
    api = GHL(a.live)
    fields = api.custom_fields()
    missing = [n for n in ("crawler id", "their story", "skip trace info") if n not in fields]
    if missing:
        print(f"Missing custom fields in GHL: {', '.join(missing)} -- run Morbius setup first.")
        sys.exit(1)
    pipeline_id, stage_id = api.sellers_stage()
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
    mapped = load_map(conn)
    c = dict(new=0, linked_existing=0, updated=0, kept_her_edit=0, retrace=0, deleted_dead=0,
             deleted_tagged=0, protected_kept=0, skipped_no_clean_phone=0, readded=0, errors=0)
    new_budget = a.limit if a.limit >= 0 else 10 ** 9
    edit_keys = {}
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
        phones = [p for p in good_phones(lead) if ok.get(p)]
        m = mapped.get(lid)
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
                owners = shared.get(cid, [lid])
                if owners[0] != lid:
                    continue  # second property of the same owner: shown on the first lead's card
                others = [by_id[x] for x in owners[1:] if x in by_id]
                if others and "other properties owned" in fields:
                    body["customFields"] = [f for f in body["customFields"] if f["id"] != fields["other properties owned"]]
                    body["customFields"].append({"id": fields["other properties owned"], "field_value": "\n".join(
                        f"{o.get('address') or ''} (crawler: {', '.join(o.get('source_tags') or [])})" for o in others)})
                contact = api.get_contact(cid)
                if contact is None:
                    continue  # she deleted it herself; it stays deleted
                prev = m.get("written") or {}
                # once Tala edits a field it stays hers forever (locked), not just for one run
                edits = her_edits(contact, prev, fields) | set(prev.get("_locked") or [])
                upd = {k: v for k, v in body.items() if k not in edits and k != "source"}
                upd["customFields"] = [f for f in body["customFields"]
                                       if "cf:" + id_to_name.get(f["id"], "") not in edits]
                c["kept_her_edit"] += len(edits)
                for k in edits:
                    edit_keys[k] = edit_keys.get(k, 0) + 1
                api.update_contact(cid, upd)
                api.add_tags(cid, tags)
                stale = {"warm", "hot", "very hot", "verified 95+"} - set(tags)
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

    # ---------- SKIP-TRACE QUEUE (purple = on hold)
    # Tala's rule (Nov 7 2026): no skip tracing until at least 50 crawler leads in GHL have
    # been called/updated (pipeline card moved off New, closed, or a note logged). Until then
    # untraced leads wait PURPLE. Exception -> URGENT (she gets a chat + phone alert asking to
    # approve): Confidence 95+, on 3+ lists, or a hard deadline (foreclosure auction / tax-sale
    # redemption) within 60 days. Nothing is ever traced without her approval.
    # Every lead she has ever called/updated is remembered (deleting it later doesn't undo the count).
    now_worked = []
    for lid, m in mapped.items():
        if m.get("ghl_contact_id") and not m.get("deleted_at"):
            try:
                if protected(api, m["ghl_contact_id"]):
                    now_worked.append(m["ghl_contact_id"])
            except Exception:
                pass
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
    # Tala's rule: every 50 leads called/updated releases the 100 hottest untraced leads.
    earned = (worked // 50) * 100
    to_release = max(0, earned - released)
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
        if conf >= 95:
            why.append(f"Confidence {conf}%")
        if lc >= 3:
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
                why.append(f"{label} {dd:%m/%d/%Y}")
                soon = min(soon or dd, dd)
        lid = str(lead["id"])
        status = "urgent" if why else ("ready" if lid in already_ready else "purple")
        # hottest first: urgent, then the nearest deadline, Confidence, lists, days on list
        rank = (0 if why else 1, (soon - today).days if soon else 999, -conf, -lc, -days)
        queue.append([lid, status, "; ".join(why) or None, conf, lc, days, rank])
    queue.sort(key=lambda q: q[6])
    for q in queue:
        if to_release <= 0:
            break
        if q[1] == "purple":
            q[1] = "ready"
            to_release -= 1
            released += 1
    queue = [tuple(q[:6]) for q in queue]
    c["released_total"] = released
    c["next_batch_at"] = (worked // 50 + 1) * 50
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
                                 alerted_at = case when excluded.status='urgent' then morbius_skiptrace_queue.alerted_at end""", q)
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
                continue
            if api.delete_contact(cid):
                c[key] += 1
                if a.live:
                    with conn.cursor() as cur:
                        if lid:
                            src = list((by_id.get(lid) or {}).get("source_tags") or [])
                            cur.execute("""update morbius_contacts set deleted_at=now(), delete_reason=%s, backup=%s,
                                           deleted_tags=%s where lead_id=%s""", (why, json.dumps(contact), src, lid))
                        else:
                            cur.execute("""insert into morbius_contacts (lead_id, ghl_contact_id, deleted_at,
                                             delete_reason, backup)
                                           values (gen_random_uuid(), %s, now(), %s, %s)""",
                                        (cid, why, json.dumps(contact)))
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
