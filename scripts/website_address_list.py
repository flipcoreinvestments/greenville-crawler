#!/usr/bin/env python3
"""
Nightly hand-off of the trouble-address list to the Unstuck Homes website.
Added 2026-11-19 (Tala approved the plan; Morbius okayed it).

WHAT IT DOES
------------
The website's free address check (/roadmap) needs to know, in seconds,
whether a typed Greenville County address is on a tax sale, foreclosure or
probate list. This step, the LAST one of the nightly run, reads the leads
database and sends the website ONE complete list:

    every Greenville County address currently tagged tax_sale,
    foreclosure_mie or probate -- skipping leads marked dead, sold
    or duplicate -- with the owner name(s) on the deed, which list(s),
    key dates and the amount due.

The website swaps in the whole list at once; its old list stays until a new
one fully arrives.

RULES
-----
- Read-only on the database. Changes nothing in the crawler.
- No AI calls, no Scrapfly, no paid service. Takes seconds.
- Runs only if every earlier nightly step passed (the workflow step has
  `if: success()`), so a broken night never sends a half-empty list.
- SWITCHED OFF until both GitHub secrets exist:
      WEBSITE_LIST_URL  -- the website backend function's address
      WEBSITE_LIST_KEY  -- the shared password (same value in Base44 secrets)
  With either missing it prints a note and exits cleanly (no red X).
- Never sends an empty list (that would wipe the website's list); an empty
  result fails the step so it gets noticed.

    python scripts/website_address_list.py            # build + send
    python scripts/website_address_list.py --dry-run  # build + print counts only
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from lead_common import address_core  # noqa: E402

LISTS = ("tax_sale", "foreclosure_mie", "probate")
SEND_TRIES = 3
SEND_WAIT_SECONDS = 20
SEND_TIMEOUT_SECONDS = 60

QUERY = r"""
select l.address, l.city, l.zip, l.owner_name, l.source_tags,
       l.raw->'tax_sale'->>'amount_due'       as tax_amount_due,
       ts.sale_date                           as tax_sale_date,
       l.raw->'foreclosure_mie'->>'sale_date' as foreclosure_sale_date,
       l.raw->'probate'->>'file_date'         as probate_file_date
from leads l
left join lateral (
    -- the county's sale date for the tax sale list this lead was last on:
    -- tax_sale_ingest logs it as "sale_date:YYYY-MM-DD" on each run
    select substring(r.notes from 'sale_date:(\d{4}-\d{2}-\d{2})') as sale_date
    from source_runs r
    where 'tax_sale' = any(l.source_tags)
      and r.source_name = 'tax_sale'
      and r.notes ~ 'sale_date:\d{4}-\d{2}-\d{2}'
      and r.run_at <= coalesce((l.raw->'tax_sale'->>'fetched_at')::timestamptz, now()) + interval '1 day'
    order by r.run_at desc
    limit 1
) ts on true
where lower(coalesce(l.county, '')) = 'greenville'
  and l.source_tags && array['tax_sale', 'foreclosure_mie', 'probate']::text[]
  and not coalesce(l.is_sold, false)
  and not coalesce(l.is_duplicate, false)
  and coalesce(l.lead_status, '') <> 'dead'
"""


def iso_date(value):
    """'1/2/2026', '01/02/2026' or '2026-01-02' -> '2026-01-02'; junk -> None."""
    s = (value or "").strip()
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        return f"{int(m.group(3)):04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def money(value):
    try:
        return round(float(str(value).replace(",", "").replace("$", "")), 2)
    except (TypeError, ValueError):
        return None


def split_address(address):
    """'123 N Main St' -> ('123', 'N MAIN'). No house number -> (None, None)."""
    a = re.sub(r"\s+", " ", (address or "").strip())
    m = re.match(r"^(\d+[A-Za-z]?)\s+(.+)$", a)
    if not m:
        return None, None
    street = address_core(m.group(2)) or ""
    return m.group(1).upper(), street.upper() or None


def build_entries(rows, today=None):
    """rows: dicts shaped like QUERY's columns. Returns the list to send."""
    today = today or datetime.now(timezone.utc).date().isoformat()
    out = {}
    for r in rows:
        house, street = split_address(r.get("address"))
        if not house or not street:
            continue  # the website matches on house number + street
        tags = set(r.get("source_tags") or [])
        lists = []
        if "tax_sale" in tags:
            sale = iso_date(r.get("tax_sale_date"))
            lists.append({
                "list": "tax sale",
                "sale_date": sale,
                "after_sale": bool(sale and sale < today),
                "amount_due": money(r.get("tax_amount_due")),
            })
        if "foreclosure_mie" in tags:
            lists.append({"list": "foreclosure", "sale_date": iso_date(r.get("foreclosure_sale_date"))})
        if "probate" in tags:
            lists.append({"list": "probate", "filed_date": iso_date(r.get("probate_file_date"))})
        if not lists:
            continue
        key = f"{house} {street}"
        entry = {
            "address": re.sub(r"\s+", " ", r["address"].strip()).upper(),
            "house_number": house,
            "street": street,
            "city": (r.get("city") or "").strip().upper() or None,
            "zip": (r.get("zip") or "").strip() or None,
            "owner_name": (r.get("owner_name") or "").strip() or None,
            "lists": lists,
        }
        if key in out:  # same house twice: keep one entry, merge lists + owners
            old = out[key]
            seen = {l["list"] for l in old["lists"]}
            old["lists"] += [l for l in lists if l["list"] not in seen]
            if entry["owner_name"] and entry["owner_name"] != old["owner_name"]:
                old["owner_name"] = "; ".join(n for n in (old["owner_name"], entry["owner_name"]) if n)
        else:
            out[key] = entry
    return list(out.values())


def fetch_rows(db_url):
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(db_url)
    try:
        conn.set_session(readonly=True)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(QUERY)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def send(url, key, payload):
    import requests
    body = json.dumps(payload)
    last = None
    for attempt in range(1, SEND_TRIES + 1):
        try:
            resp = requests.post(url, data=body, timeout=SEND_TIMEOUT_SECONDS,
                                 headers={"Content-Type": "application/json",
                                          "Authorization": f"Bearer {key}"})
            if 200 <= resp.status_code < 300:
                return True, f"HTTP {resp.status_code}"
            last = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if resp.status_code < 500:
                break  # wrong password / bad request: retrying won't help
        except requests.RequestException as e:
            last = f"{type(e).__name__}: {e}"
        if attempt < SEND_TRIES:
            time.sleep(SEND_WAIT_SECONDS)
    return False, last


def main():
    dry_run = "--dry-run" in sys.argv
    url = os.environ.get("WEBSITE_LIST_URL", "").strip()
    key = os.environ.get("WEBSITE_LIST_KEY", "").strip()
    if not dry_run and (not url or not key):
        print("Website address list: switched off (WEBSITE_LIST_URL / WEBSITE_LIST_KEY "
              "not set yet). Nothing sent.")
        return 0

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    entries = build_entries(fetch_rows(db_url))
    counts = {}
    for e in entries:
        for l in e["lists"]:
            counts[l["list"]] = counts.get(l["list"], 0) + 1
    print(f"Website address list: {len(entries)} addresses "
          + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))

    if not entries:
        print("Empty list -- NOT sent (it would wipe the website's list). "
              "The website keeps its last good list.", file=sys.stderr)
        return 1
    if dry_run:
        print("Dry run: nothing sent.")
        return 0

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(entries),
        "addresses": entries,
    }
    ok, detail = send(url, key, payload)
    if ok:
        print(f"Sent to the website ({detail}).")
        return 0
    print(f"Website did not accept the list ({detail}). It keeps its last good list.",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
