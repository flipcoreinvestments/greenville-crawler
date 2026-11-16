"""
build_trace_list.py  (2026-11-15, Tala)

One skip-trace upload file, in this order:
  PART A  leads already in GHL whose information changed and need a retrace
          (the person to trace changed, or the name/address was fixed after an empty trace)
  PART B  new leads to trace, best first, up to NEW_AMOUNT (her "150 new leads" order)
          - retraces of leads not in GHL, then approved, then released, then the hottest
            traceable leads (Confidence, lists, days on list)

Rules kept: only living, traceable people (Morbius's traceability check: heir first when
there is no Personal Representative, never the deceased owner); a lead whose retrace would
search the same name and address again is left out (never retrace the same information).
Every number that comes back is checked against the Bad Numbers list and Do Not Call by
skiptrace_intake / Morbius before anything reaches GHL.

Writes the list to the private trace_list_export table (nothing else in the database changes).
"""

import csv
import os
import sys

import psycopg2
import psycopg2.extras

from export_call_lists import split_contact_address
from skiptrace_prep import compute_skiptrace_identity

NEW_AMOUNT = int(os.environ.get("NEW_AMOUNT") or 150)

HEAD = ["Group", "First Name", "Last Name", "Street", "City", "State", "Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Owner on Title", "Why", "Confidence", "Ref ID"]


def norm(s):
    return " ".join((s or "").upper().split())


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        select q.lead_id, q.status, q.reason, q.confidence, q.list_count, q.days_on_list,
               q.retrace_first, q.retrace_last,
               (m.lead_id is not null) as in_ghl,
               l.skiptrace_status, l.skiptrace_fix_status, l.skiptrace_fix_note, l.retrace_reason,
               l.skiptrace_input
        from morbius_skiptrace_queue q
        join leads l on l.id::text = q.lead_id
        left join morbius_contacts m on m.lead_id = l.id and m.deleted_at is null and m.ghl_contact_id is not null
        where coalesce(q.reason, '') not like 'Not traceable%%'
          and coalesce(l.lead_status, '') = ''
          and coalesce(l.skiptrace_fix_status, '') <> 'needs_human'
    """)
    queue = cur.fetchall()
    ids = [q["lead_id"] for q in queue]
    cur.execute("select * from house_leads where id::text = any(%s)", (ids,))
    leads = {str(r["id"]): dict(r) for r in cur.fetchall()}

    part_a, part_b, skipped = [], [], {"not_in_house_leads": 0, "no_name": 0, "same_search": 0}
    for q in queue:
        lead = leads.get(q["lead_id"])
        if not lead:
            skipped["not_in_house_leads"] += 1
            continue
        tags = list(lead.get("source_tags") or [])
        first, last, addr, _note = compute_skiptrace_identity(lead, tags)
        retrace = q["status"] == "retrace" or (q["status"] == "approved" and q.get("retrace_first"))
        if retrace and q.get("retrace_first"):
            first, last = q["retrace_first"], q["retrace_last"]
        fixed = lead.get("skiptrace_status") == "failed" and q.get("skiptrace_fix_status") == "fixed"
        if not (first or "").strip() or not (last or "").strip():
            skipped["no_name"] += 1
            continue
        inp = q.get("skiptrace_input") or {}
        if (retrace or fixed) and inp and norm(inp.get("first")) == norm(first) \
                and norm(inp.get("last")) == norm(last) \
                and norm(inp.get("address")) and norm(addr).startswith(norm(inp.get("address"))) \
                and not fixed:
            skipped["same_search"] += 1  # nothing changed: never pay to search the same thing again
            continue
        street, city, st, z = split_contact_address(addr)
        if not st:
            street, city, st, z = addr or lead.get("address"), lead.get("city") or "", "SC", lead.get("zip") or ""
        why = (q.get("retrace_reason") or q.get("reason") if retrace
               else (q.get("skiptrace_fix_note") if fixed else (q.get("reason") or q["status"])))
        row = {"First Name": first.strip().title(), "Last Name": last.strip().title(),
               "Street": street, "City": city, "State": st, "Zip": z,
               "Property Address": lead.get("address"), "Property City": lead.get("city"),
               "Property State": "SC", "Property Zip": lead.get("zip"),
               "Owner on Title": lead.get("owner_name"), "Why": (why or "")[:160],
               "Confidence": q.get("confidence"), "Ref ID": q["lead_id"]}
        status_rank = {"retrace": 0, "approved": 1, "urgent": 2, "ready": 3}.get(q["status"], 4)
        key = (status_rank, -(q.get("confidence") or 0), -(q.get("list_count") or 0), -(q.get("days_on_list") or 0))
        if q["in_ghl"] and (retrace or fixed):
            row["Group"] = "A - in GHL, info changed: retrace"
            part_a.append((key, row))
        elif not q["in_ghl"]:
            row["Group"] = "B - new lead" + (" (retrace, next person)" if retrace else "")
            part_b.append((key, row))
    part_a.sort(key=lambda x: x[0])
    part_b.sort(key=lambda x: x[0])
    part_b = part_b[:NEW_AMOUNT]

    # the list goes to a private table (never a public log or artifact: the repo is public)
    cols = ["grp", "first_name", "last_name", "street", "city", "state", "zip", "property_address",
            "property_city", "property_state", "property_zip", "owner_on_title", "why", "confidence", "lead_id"]
    with conn.cursor() as wc:
        wc.execute("delete from trace_list_export")
        for i, (_k, r) in enumerate(part_a + part_b, 1):
            wc.execute(f"insert into trace_list_export (pos, {', '.join(cols)}) values (%s, {', '.join(['%s'] * len(cols))})",
                       [i] + [r[h] for h in HEAD])
    conn.commit()
    print(f"PART A (in GHL, retrace): {len(part_a)}")
    print(f"PART B (new leads): {len(part_b)}")
    print(f"left out: {skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
