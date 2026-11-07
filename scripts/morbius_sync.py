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
from datetime import date, datetime, timezone

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
    conn.commit()


def load_map(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("select * from morbius_contacts")
        return {str(r["lead_id"]): r for r in cur.fetchall()}


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


def build(lead, d, phones, fields, readded=False):
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
    info.append(f"Traced: {d.get('Last Skip-Traced') or 'yes'}")

    rp = raw.get("redemption_period") or {}
    if d.get("Foreclosure Auction Date"):
        nxt = f"Foreclosure auction {d['Foreclosure Auction Date']}"
    elif rp.get("redemption_deadline"):
        nxt = f"Tax sale redemption deadline {rp['redemption_deadline']} (sold at tax sale {rp.get('sale_date', '')})"
    else:
        nxt = d.get("Tax Sale Status") or ""
    heirs = probate_heirs(raw.get("probate"), d.get("Personal Representative"), lead.get("owner_name")) if probate else ""

    cf = {
        "crawler id": str(lead["id"]),
        "their story": (lead.get("story") or d.get("Call Brief") or "").strip(),
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


def her_edits(contact, written):
    """Tracked fields whose GHL value no longer matches what Morbius last wrote."""
    out = set()
    for k in TRACKED:
        if k in written and (contact.get(k) or "") != (written.get(k) or ""):
            out.add(k)
    return out


def protected(api, cid):
    """Worked lead: a pipeline card past the first stage / not open, or any note."""
    for o in api.opportunities(cid):
        stage = (o.get("pipelineStageName") or o.get("stageName") or "").strip().lower()
        if (o.get("status") or "open") != "open" or (stage and stage != FIRST_STAGE.lower()):
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
    print(f"Sellers pipeline found: {bool(pipeline_id)}; first stage found: {bool(stage_id)}")

    today = date.today()
    rows = lead_rows(conn, "house_leads")
    by_id = {str(r["id"]): r for r in rows}
    ok = clean_phones(conn)
    mapped = load_map(conn)
    c = dict(new=0, linked_existing=0, updated=0, kept_her_edit=0, retrace=0, deleted_dead=0,
             deleted_tagged=0, protected_kept=0, skipped_no_clean_phone=0, readded=0, errors=0)
    new_budget = a.limit if a.limit >= 0 else 10 ** 9

    # ---------- GREEN / YELLOW / ORANGE
    for lead in rows:
        lid = str(lead["id"])
        if lead.get("lead_status") or lead.get("skiptrace_status") != "traced":
            continue
        phones = [p for p in good_phones(lead) if ok.get(p)]
        m = mapped.get(lid)
        live_map = m and m.get("ghl_contact_id") and not m.get("deleted_at")
        if not phones:
            c["skipped_no_clean_phone"] += 1
            continue
        try:
            d = row_dict(lead, today)
            readded = bool(m and m.get("deleted_at"))
            body, tags, _ = build(lead, d, phones, fields, readded)
            if needs_retrace(lead):
                if live_map:
                    api.add_tags(m["ghl_contact_id"], ["retrace"])
                    c["retrace"] += 1
                continue
            written = {k: body.get(k) for k in TRACKED}
            if live_map:
                cid = m["ghl_contact_id"]
                contact = api.get_contact(cid)
                if contact is None:
                    continue  # she deleted it herself; it stays deleted
                edits = her_edits(contact, m.get("written") or {})
                upd = {k: v for k, v in body.items() if k not in edits and k != "source"}
                c["kept_her_edit"] += len(edits)
                api.update_contact(cid, upd)
                api.add_tags(cid, tags)
                stale = {"warm", "hot", "very hot"} - set(tags)
                api.remove_tags(cid, stale)
                for k in edits:
                    written[k] = contact.get(k)
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
                                         written=excluded.written, updated_at=now(), deleted_at=null,
                                         readded_count=morbius_contacts.readded_count + %s""",
                                    (lid, cid, opp, json.dumps(written), 1 if readded else 0, 1 if readded else 0))
            if a.live:
                conn.commit()
        except Exception as e:  # one bad lead never stops the run
            conn.rollback()
            c["errors"] += 1
            print(f"  lead skipped: {type(e).__name__}: {str(e)[:120]}")

    # ---------- RED: dead leads + her "delete" tag
    dead = []
    for lid, m in mapped.items():
        if not m.get("ghl_contact_id") or m.get("deleted_at"):
            continue
        lead = by_id.get(lid)
        if lead is None or lead.get("lead_status") == "dead":
            dead.append((lid, m["ghl_contact_id"], "dead lead: no longer on any list / resolved / sold"))
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
                            cur.execute("""update morbius_contacts set deleted_at=now(), delete_reason=%s, backup=%s
                                           where lead_id=%s""", (why, json.dumps(contact), lid))
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


if __name__ == "__main__":
    main()
