#!/usr/bin/env python3
"""
GHL SYNC (added 2026-10-16, T Dawg's ask: "every lead in GHL up to date week
over week, no backtracking, even at tens of thousands of leads").

GoHighLevel's import can only find an existing contact by PHONE or EMAIL --
never by name or property address. Everything that went wrong on the 10.1 /
10.7 / 10.11 lists came from the system not knowing which phone each GHL
contact actually has, and from the same change being sent to GHL more than
once. This script and the GHL section of export_call_lists.py fix that:

1. GHL CONTACT EXPORT -> SYSTEM (this script, nightly, after skip-trace intake).
   She exports her GHL contacts (Contacts -> select all -> Export) and drops
   the CSV into the private Supabase Storage bucket "ghl-sync" (any name).
   The newest unread file is read once. For every lead it records:
     ghl_phone       the phone the GHL contact has (the import's match key)
     ghl_contact_id  the GHL contact id
     ghl_in_crm      true = contact is in GHL; false = it was there (or was
                     sent) and is gone now (deleted)
     ghl_tags        the contact's GHL tags
   A lead is matched to a contact by any phone the skip trace returned for it
   (or the phone already recorded). One contact can be several leads (one
   owner, several properties).

   Feedback from calling: a contact tagged "wrong number" in GHL marks that
   phone bad (it's never exported again); a contact tagged "wrong person"
   marks the lead for a retrace. Both go on the error list so they show in
   the next GHL file.

2. SENT LOG (ghl_sent). Every row export_call_lists.py puts in the daily GHL
   file is logged (lead, action, detail). A change goes to GHL ONCE; it only
   goes again if a newer GHL export shows it didn't take (a dead contact is
   still there, a new lead isn't there). So importing every file once never
   re-creates a contact she already deleted.

Nothing here prints names, phones or addresses -- the repo and its logs are
public. Counts only.
"""
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

import psycopg2

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://izxqrskybwtqzbbbzlux.supabase.co")
BUCKET = "ghl-sync"
EXPORT_BUCKET = "lead-exports"

# Changes found before this moment were handled by hand on the
# "10.15 updated leads ghl import list" sheet, so the first automatic GHL file
# starts after it (00:00 Eastern on 10/16/2026).
CUTOFF = datetime(2026, 10, 16, 4, 0, tzinfo=timezone.utc)

WRONG_NUMBER_TAGS = {"wrong number", "wrong #", "bad number", "wrong-number"}
WRONG_PERSON_TAGS = {"wrong person", "wrong-person", "delete - wrong person"}


def ensure_columns(conn):
    with conn.cursor() as cur:
        for col, typ in [("ghl_phone", "text"), ("ghl_contact_id", "text"), ("ghl_in_crm", "boolean"),
                         ("ghl_seen_at", "timestamptz"), ("ghl_tags", "text[]"), ("ghl_reset_at", "timestamptz"),
                         ("bad_phones", "text[]"), ("retrace_reason", "text"), ("ghl_feedback", "text[]"),
                         ("trace_offers", "jsonb")]:
            cur.execute(f"alter table leads add column if not exists {col} {typ}")
        cur.execute("create index if not exists idx_leads_ghl_phone on leads(ghl_phone) where ghl_phone is not null")
        cur.execute("""create table if not exists ghl_sent (
                         lead_id uuid not null, action text not null, detail text not null default '',
                         file_date date not null, sent_at timestamptz not null default now(),
                         primary key (lead_id, action, detail))""")
        cur.execute("""create table if not exists ghl_sync_files (
                         name text primary key, file_time timestamptz, contacts int, matched_leads int,
                         gone int, wrong_number int, wrong_person int, unmatched_contacts int,
                         read_at timestamptz not null default now())""")
        cur.execute("""create table if not exists lead_error_log (
                         id bigserial primary key, lead_id uuid not null, found_at timestamptz not null default now(),
                         reason text not null)""")


def norm_phone(v):
    d = re.sub(r"\D", "", str(v or ""))
    d = d[1:] if len(d) == 11 and d.startswith("1") else d
    return d if len(d) == 10 else ""


def _col(row, *names):
    low = {(k or "").strip().lower(): v for k, v in row.items()}
    for n in names:
        v = low.get(n.lower())
        if v not in (None, ""):
            return str(v).strip()
    return ""


def parse_contacts(rows):
    """GHL export rows -> [{id, first, last, phone, email, tags}]."""
    out = []
    for r in rows:
        phone = norm_phone(_col(r, "Phone", "phone"))
        email = _col(r, "Email", "email").lower()
        tags = {t.strip().lower() for t in re.split(r"[,|]", _col(r, "Tags", "tags")) if t.strip()}
        out.append({"id": _col(r, "Contact Id", "contact id", "Id"), "first": _col(r, "First Name"),
                    "last": _col(r, "Last Name"), "phone": phone, "email": email, "tags": tags})
    return out


def plan_sync(leads, contacts):
    """Pure. leads: [{id, skiptrace_phones, ghl_phone, ghl_in_crm, bad_phones, retrace_reason}].
    Returns (updates {lead_id: {...}}, feedback [(lead_id, kind, detail)], unmatched_contact_count)."""
    by_phone = {}
    for c in contacts:
        if c["phone"]:
            by_phone.setdefault(c["phone"], c)
    updates, feedback, used = {}, [], set()
    for l in leads:
        bad = set(l.get("bad_phones") or [])
        order = [p for p in [l.get("ghl_phone")] + list(l.get("skiptrace_phones") or []) if p]
        hit = next((by_phone[p] for p in order if p in by_phone), None)
        if hit:
            used.add(hit["phone"])
            u = {"ghl_phone": hit["phone"], "ghl_contact_id": hit["id"], "ghl_in_crm": True,
                 "ghl_tags": sorted(hit["tags"])}
            # each caller tag is acted on ONCE per GHL contact: a tag left on an
            # old contact never re-triggers after the lead was retraced
            done = set(l.get("ghl_feedback") or [])
            cid = hit["id"] or hit["phone"]
            k_num, k_person = f"{cid}|number|{hit['phone']}", f"{cid}|person"
            if hit["tags"] & WRONG_NUMBER_TAGS and k_num not in done:
                u["ghl_feedback"] = sorted(done | {k_num})
                done.add(k_num)
                if hit["phone"] not in bad:
                    u["bad_phones"] = sorted(bad | {hit["phone"]})
                    feedback.append((l["id"], "wrong_number", hit["phone"][-4:]))
            if hit["tags"] & WRONG_PERSON_TAGS and k_person in done:
                pass
            elif hit["tags"] & WRONG_PERSON_TAGS and not l.get("retrace_reason"):
                u["ghl_feedback"] = sorted(done | {k_person})
                u["retrace_reason"] = "caller reported wrong person"
                feedback.append((l["id"], "wrong_person", ""))
            elif ("bad_phones" in u and not l.get("retrace_reason") and l.get("skiptrace_phones")
                  and set(l["skiptrace_phones"]) <= set(u["bad_phones"])):
                # every number this trace found is wrong: same as a wrong person --
                # needs a different search (ORANGE), never the same one again
                u["retrace_reason"] = "caller reported every number wrong"
                feedback.append((l["id"], "wrong_person", "all numbers"))
            updates[l["id"]] = u
        elif l.get("ghl_in_crm") is True or l.get("ghl_phone") or l.get("skiptrace_phones"):
            # not in GHL now: deleted, or never imported. (ghl_contact_id is
            # kept, so "it WAS there and she deleted it" stays known -- the
            # system never re-creates a contact she deleted on purpose.)
            updates[l["id"]] = {"ghl_in_crm": False}
    unmatched = sum(1 for c in contacts if c["phone"] and c["phone"] not in used
                    and any(t.startswith("list-") for t in c["tags"]))
    return updates, feedback, unmatched


def apply_sync(conn, contacts, file_time, now=None):
    with conn.cursor() as cur:
        cur.execute("""select id, skiptrace_phones, ghl_phone, ghl_in_crm, bad_phones, retrace_reason, ghl_feedback
                       from leads where skiptrace_phones is not null or ghl_phone is not null""")
        cols = [d[0] for d in cur.description]
        leads = [dict(zip(cols, r)) for r in cur.fetchall()]
    for l in leads:
        l["id"] = str(l["id"])
    updates, feedback, unmatched = plan_sync(leads, contacts)
    gone = [k for k, u in updates.items() if u == {"ghl_in_crm": False}]
    with conn.cursor() as cur:
        # one statement for every lead not in GHL (light on the free plan)
        cur.execute("update leads set ghl_in_crm = false, ghl_seen_at = %s where id = any(%s::uuid[])",
                    (file_time, gone))
        for lead_id, u in updates.items():
            if u == {"ghl_in_crm": False}:
                continue
            u = dict(u, ghl_seen_at=file_time)
            sets = ", ".join(f"{k} = %s" for k in u)
            cur.execute(f"update leads set {sets} where id = %s", list(u.values()) + [lead_id])
        for lead_id, kind, detail in feedback:
            reason = ("caller reported wrong number (phone ending " + detail + ") -- that number is no longer exported"
                      if kind == "wrong_number" else
                      "caller reported every number wrong -- needs a different search" if detail else
                      "caller reported wrong person -- retrace")
            cur.execute("select 1 from lead_error_log where lead_id = %s and reason = %s", (lead_id, reason))
            if not cur.fetchone():
                cur.execute("insert into lead_error_log (lead_id, reason, found_at) values (%s, %s, coalesce(%s, now()))",
                            (lead_id, reason, now))
    stats = {"contacts": len(contacts), "matched": sum(1 for u in updates.values() if u.get("ghl_in_crm")),
             "gone": sum(1 for u in updates.values() if u.get("ghl_in_crm") is False),
             "wrong_number": sum(1 for f in feedback if f[1] == "wrong_number"),
             "wrong_person": sum(1 for f in feedback if f[1] == "wrong_person"), "unmatched": unmatched}
    return stats


# ------------------------------------------------------------------
# THE DAILY GHL FILE: which leads go in it, and how (pure, tested in
# tests/test_ghl_flow.py). export_call_lists.py gathers the leads and writes
# the file; this decides.
#
#   RED     DELETE   dead, and its GHL contact is still there. Import tags it
#                    "delete - dead lead"; then filter that tag and bulk delete.
#   ORANGE  RETRACE  the person in GHL is wrong / changed and SHE must edit the
#                    lead before tracing it ("Edit Before Trace" says what).
#                    Import as is (tags the old contact "retrace"), bulk delete
#                    "retrace", fix, trace.
#   YELLOW  RETRACE  same, but the system already has the right person in the
#                    Skiptrace columns -- ready to trace, no edit.
#   YELLOW  UPDATE   same person, something changed (mailing address, list
#                    added/dropped...). Import updates the contact in place.
#   GREEN   NEW      traced, alive, not in GHL yet. Import creates it.
#
# Every row is matched to GHL by the phone GHL actually has (ghl_phone), so
# an import always finds the contact. One owner with several properties is
# ONE GHL contact: those leads share a phone and come out as one row (the
# others listed in "Other Properties"), and a dead property never deletes a
# contact that still stands for a live one.
ACTION_ORDER = {"DELETE": 0, "RETRACE": 1, "UPDATE": 2, "NEW": 3}


def good_phones(r):
    bad = set(r.get("bad_phones") or [])
    return [p for p in (r.get("skiptrace_phones") or []) if p and p not in bad]


def _aware(d):
    if d is None:
        return None
    if not isinstance(d, datetime):
        if hasattr(d, "year"):  # a plain date -> its midnight UTC
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


class SentIndex(dict):
    """{lead_id: [(action, detail, sent_at)]} -- built once per plan so a
    lookup is per lead, not a scan of every row ever sent."""


def index_sent(sent):
    if isinstance(sent, SentIndex):
        return sent
    idx = SentIndex()
    for (k_id, action, detail), at in (sent or {}).items():
        idx.setdefault(k_id, []).append((action, detail, _aware(at)))
    return idx


def _sends(r, sent):
    """{action: (newest sent_at, its detail)} for the lead's CURRENT person --
    rows sent before a new person was traced (ghl_reset_at, skiptrace_intake)
    belong to a contact since deleted. A NEW
    only counts for one of its current phones."""
    out = {}
    if not sent:
        return out
    lid, phones = str(r["id"]), set(r.get("skiptrace_phones") or [])
    traced = _aware(r.get("ghl_reset_at"))  # set when a NEW person was traced
    for action, detail, at in index_sent(sent).get(lid, ()):
        if at is None or (traced and at < traced):
            continue
        if action == "NEW" and detail not in phones:
            continue
        if action not in out or at > out[action][0]:
            out[action] = (at, detail)
    return out


def ghl_state(r, sent=None, cutoff=CUTOFF):
    """(in GHL?, the phone GHL has for the contact) -- what an import matches on.
    The NEWEST of these wins:
      a GHL export (ghl_in_crm, read at ghl_seen_at) -- the truth when newest
      a NEW row sent        -> in GHL with that phone (she imports every file)
      a DELETE / RETRACE row sent -> gone (she bulk deletes those tags)
    With none of them: a phone a GHL export matched earlier, else one of the
    hand-imported 10.1 / 10.7 leads (traced before the cutoff)."""
    good = good_phones(r)
    first = good[0] if good else ""
    events = []
    if r.get("ghl_in_crm") is not None:
        seen = _aware(r.get("ghl_seen_at")) or datetime(2000, 1, 1, tzinfo=timezone.utc)
        events.append((seen, 0, bool(r["ghl_in_crm"]), r.get("ghl_phone") or first))
    sends = _sends(r, sent)
    if "NEW" in sends:
        events.append((sends["NEW"][0], 1, True, sends["NEW"][1]))
    for act in ("DELETE", "RETRACE"):
        if act in sends:
            events.append((sends[act][0], 1, False, r.get("ghl_phone") or first))
    if events:
        _, _, here, phone = max(events, key=lambda e: (e[0], e[1]))
        return here, phone
    if r.get("ghl_phone"):
        return True, r["ghl_phone"]
    when = _aware(r.get("skiptrace_date"))
    legacy = bool(r.get("skiptrace_status") == "traced" and good and when and when < cutoff)
    return legacy, first


def key_phone(r, sent=None):
    return ghl_state(r, sent)[1]


def in_ghl(r, cutoff=CUTOFF, sent=None):
    return ghl_state(r, sent, cutoff)[0]


def plan_ghl_rows(leads, sent, cutoff=CUTOFF):
    """leads: dicts with the lead columns plus three computed keys:
         error      open error text ('' if none)        error_at  its newest found_at
         retrace    None, or {"edit": "" | what she must fix first}
    sent: {(lead_id, action, detail): sent_at}
    Returns [{action, color, edit, lead, others, detail, phones}] sorted
    DELETE, RETRACE, UPDATE, NEW."""
    idx = index_sent(sent)
    state = {str(r["id"]): ghl_state(r, idx, cutoff) for r in leads}
    last_sent = {k: max((at for _, _, at in v if at), default=None) for k, v in idx.items()}
    alive_by_phone = {}
    for r in leads:
        here, kp = state[str(r["id"])]
        if kp and here and r.get("lead_status") != "dead":
            alive_by_phone.setdefault(kp, []).append(r)

    # a row that undoes an earlier one makes it due again: a lead that died
    # (DELETE) and came back needs its NEW again, and the reverse
    undone_by = {"DELETE": ("NEW",), "NEW": ("DELETE", "RETRACE"), "RETRACE": ("NEW",), "UPDATE": ()}

    def due(r, action, detail, event_at, contradicted):
        """Send once. Send again only when a GHL export read AFTER it was
        sent shows it didn't take, or a later row undid it."""
        k = (str(r["id"]), action, detail)
        seen = _aware(r.get("ghl_seen_at"))
        sends = _sends(r, idx)
        later = [sends[a][0] for a in undone_by[action] if a in sends]
        if k in sent:
            at = _aware(sent[k])
            return bool((contradicted and seen and seen > at) or any(t > at for t in later))
        ev = _aware(event_at)
        return bool((ev and ev >= cutoff) or contradicted or later)

    rows = []
    for r in leads:
        phones = good_phones(r)
        here, kp = state[str(r["id"])]
        synced = r.get("ghl_in_crm") is not None
        tags = set(r.get("ghl_tags") or [])
        if r.get("lead_status") == "dead":
            if not (here and kp):
                continue
            if any(x is not r for x in alive_by_phone.get(kp, [])):
                continue  # the contact still stands for a live property of the same owner
            if due(r, "DELETE", "dead", r.get("lead_status_at"), synced and r.get("ghl_in_crm") is True):
                rows.append({"action": "DELETE", "color": "RED", "edit": "", "lead": r, "detail": "dead",
                             "phones": [kp]})
        elif r.get("retrace") is not None:
            if not (here and kp):
                continue  # nothing in GHL to tag; the lead is on the pull list to trace
            who = (r["retrace"].get("who") or "").strip()
            if due(r, "RETRACE", who, r.get("retrace_at") or r.get("error_at") or r.get("skiptrace_date"),
                   synced and r.get("ghl_in_crm") is True and "retrace" not in tags):
                edit = r["retrace"].get("edit") or ""
                rows.append({"action": "RETRACE", "color": "ORANGE" if edit else "YELLOW",
                             "edit": edit or "Ready to trace -- the right person is already in the Skiptrace columns",
                             "lead": r, "detail": who, "phones": [kp]})
        elif r.get("error") and here and kp:
            # only errors found AFTER the last row sent for this lead (and after
            # its current trace): older ones were in that row already, and an
            # old error dropping out of the 30-day window isn't news
            err_at = _aware(r.get("error_at"))
            prior = [t for t in (last_sent.get(str(r["id"])), _aware(r.get("skiptrace_date"))
                                 if r.get("skiptrace_status") == "traced" else None) if t]
            if err_at is None or any(err_at <= t for t in prior):
                continue
            if due(r, "UPDATE", r["error"], err_at, False):
                rows.append({"action": "UPDATE", "color": "YELLOW", "edit": "Update in place -- import as is",
                             "lead": r, "detail": r["error"], "phones": [kp] + [p for p in phones if p != kp]})
        elif (not here and r.get("skiptrace_status") == "traced" and phones
              and r.get("lead_status") != "wrong_info"):
            # re-send a NEW only when it never got in (a GHL export after it
            # doesn't have it and never had it); a contact she deleted herself
            # stays deleted
            if (synced and r.get("ghl_in_crm") is False and r.get("ghl_contact_id")
                    and "DELETE" not in _sends(r, idx)):
                continue  # she deleted it herself (not on a DELETE row of ours)
            never_in = synced and r.get("ghl_in_crm") is False and not r.get("ghl_contact_id")
            k_new = (str(r["id"]), "NEW", phones[0])
            if due(r, "NEW", phones[0], r.get("skiptrace_date"), never_in and k_new in sent):
                rows.append({"action": "NEW", "color": "GREEN", "edit": "", "lead": r, "detail": phones[0],
                             "phones": phones})
    # one row per GHL contact (phone): the most urgent action wins, the other
    # properties of the same owner ride along on it
    pri = {"RETRACE": 0, "UPDATE": 1, "NEW": 2, "DELETE": 3}
    best = {}
    for x in rows:
        p = x["phones"][0]
        cur = best.get(p)
        if cur is None:
            best[p] = dict(x, others=[])
        elif pri[x["action"]] < pri[cur["action"]]:
            best[p] = dict(x, others=cur["others"] + [dict(cur, others=[])])
        else:
            cur["others"].append(x)
    out = list(best.values())
    out.sort(key=lambda x: (ACTION_ORDER[x["action"]], str(x["lead"].get("address") or "")))
    return out


def list_files(session, key, url=SUPABASE_URL):
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    session.post(f"{url}/storage/v1/bucket", headers=h, json={"id": BUCKET, "name": BUCKET, "public": False},
                 timeout=60)
    r = session.post(f"{url}/storage/v1/object/list/{BUCKET}", headers=h,
                     json={"prefix": "", "limit": 1000, "sortBy": {"column": "updated_at", "order": "asc"}},
                     timeout=60)
    if r.status_code != 200:
        return []
    return [(o["name"], o.get("updated_at") or o.get("created_at") or "") for o in r.json()
            if o.get("id") and (o.get("name") or "").lower().endswith((".csv", ".xlsx"))]


def sync(conn, key):
    import requests
    from skiptrace_intake import read_rows
    s = requests.Session()
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    with conn.cursor() as cur:
        cur.execute("select name from ghl_sync_files")
        done = {r[0] for r in cur.fetchall()}
    files = [f for f in list_files(s, key) if f[0] not in done]
    if not files:
        return None
    # only the newest export matters -- it is the whole GHL contact list
    # (newest by upload time, whatever the file is called)
    files.sort(key=lambda f: f[1] or "")
    name, updated = files[-1]
    r = s.get(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{name}", headers=h, timeout=180)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(name)[1]) as tmp:
        tmp.write(r.content)
        tmp.flush()
        contacts = parse_contacts(read_rows(tmp.name))
    try:
        file_time = datetime.fromisoformat(updated.replace("Z", "+00:00"))
    except ValueError:
        file_time = datetime.now(timezone.utc)
    stats = apply_sync(conn, contacts, file_time)
    with conn.cursor() as cur:
        for n, _ in files:
            cur.execute("""insert into ghl_sync_files (name, file_time, contacts, matched_leads, gone, wrong_number,
                                                       wrong_person, unmatched_contacts)
                           values (%s,%s,%s,%s,%s,%s,%s,%s) on conflict (name) do nothing""",
                        (n, file_time if n == name else None,
                         *((stats["contacts"], stats["matched"], stats["gone"], stats["wrong_number"],
                            stats["wrong_person"], stats["unmatched"]) if n == name else (0, 0, 0, 0, 0, 0))))
    conn.commit()
    return stats


def main():
    db_url = os.environ.get("DATABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    ensure_columns(conn)
    conn.commit()
    if not key:
        print("SUPABASE_SERVICE_KEY is not set -- can't read the ghl-sync bucket")
        return
    stats = sync(conn, key)
    if stats is None:
        print("GHL sync: no new GHL contact export in ghl-sync.")
        return
    with conn.cursor() as cur:
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) values "
                    "('ghl_sync', %s, %s, %s)",
                    (stats["contacts"], stats["matched"],
                     "ok: " + " ".join(f"{k}:{v}" for k, v in stats.items())))
    conn.commit()
    conn.close()
    print("GHL sync: " + ", ".join(f"{k} {v}" for k, v in stats.items()))


if __name__ == "__main__":
    main()
