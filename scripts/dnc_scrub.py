#!/usr/bin/env python3
"""
DO NOT CALL SCRUB (added 2026-10-13, T Dawg's ask). Runs every night after the
skip-trace intake and before the call lists are written.

What she does (once a month):
  1. Log in at telemarketing.donotcall.gov (Restart Homes' subscription; the
     first 5 area codes are free) and download the full list for each area
     code she subscribed to.
  2. Drop the downloaded files into the private Supabase Storage bucket
     "dnc-registry" (top level; .txt, .csv or .zip are all fine).
  3. Opt-outs ("STOP" replies) exported from the texting platform go into the
     same bucket under opt-outs/ -- any file, any columns; every 10-digit
     number in it is suppressed for 5 years (SC Code 37-21-70).

What this script does:
  - reads every registry file once, records which area codes it covers and
    when it was uploaded (a registry copy counts for 31 days -- the SC
    good-faith defense in 37-21-70 and the federal safe harbor both use a
    copy no older than 31 days),
  - checks every phone a skip trace returned (leads.skiptrace_phones)
    against the registry and the opt-out list,
  - stores the answer per phone in phone_dnc, which export_call_lists.py
    uses to write lead-exports/text_ready/text_ready.csv: only phones that
    were checked against a fresh registry copy and came back clean.

A phone whose area code has no fresh registry copy is NOT clean -- it can't
be checked, so it never goes on the texting list. Nothing here is printed to
the (public) GitHub log except counts.
"""
import io
import os
import re
import sys
import zipfile
from datetime import datetime, timedelta, timezone

import psycopg2

from lead_common import ensure_schema
from skiptrace_intake import ensure_columns

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://izxqrskybwtqzbbbzlux.supabase.co")
BUCKET = "dnc-registry"
FRESH_DAYS = 31
OPT_OUT_YEARS = 5
DIGITS = re.compile(r"\d")


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""create table if not exists dnc_registry_files (
                         name text primary key, area_codes text[], numbers int,
                         uploaded_at timestamptz, read_at timestamptz not null default now())""")
        cur.execute("""create table if not exists phone_suppression (
                         phone text primary key, reason text not null, source_file text,
                         added_at timestamptz not null default now())""")
        cur.execute("""create table if not exists phone_dnc (
                         phone text primary key, area_code text not null, on_dnc boolean,
                         suppressed boolean not null default false,
                         registry_as_of timestamptz, checked_at timestamptz not null default now())""")
    conn.commit()


def ten_digit_numbers(text):
    """Every phone number in a registry/opt-out file, one per line. Handles
    '8645551234', '864,5551234', '(864) 555-1234', '1-864-555-1234'."""
    for line in text.splitlines():
        d = "".join(DIGITS.findall(line))
        if len(d) == 11 and d.startswith("1"):
            d = d[1:]
        if len(d) == 10:
            yield d


def file_text(name, content):
    if name.lower().endswith(".zip"):
        out = []
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            for n in z.namelist():
                if not n.endswith("/"):
                    out.append(z.read(n).decode("utf-8", "ignore"))
        return "\n".join(out)
    return content.decode("utf-8", "ignore")


def list_objects(s, key, prefix=""):
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    s.post(f"{SUPABASE_URL}/storage/v1/bucket", headers=h,
           json={"id": BUCKET, "name": BUCKET, "public": False}, timeout=60)
    r = s.post(f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}", headers=h,
               json={"prefix": prefix, "limit": 1000, "sortBy": {"column": "name", "order": "asc"}}, timeout=60)
    if r.status_code != 200:
        return []
    return [(prefix + o["name"], o.get("created_at") or o.get("updated_at")) for o in r.json()
            if o.get("id") and o["name"].lower().endswith((".txt", ".csv", ".zip"))]


def download(s, key, name):
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    r = s.get(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{name}", headers=h, timeout=300)
    r.raise_for_status()
    return r.content


def our_phones(conn):
    with conn.cursor() as cur:
        cur.execute("select distinct unnest(skiptrace_phones) from leads where skiptrace_phones is not null")
        return {r[0] for r in cur.fetchall() if r[0]}


def scrub(conn, key):
    import requests
    s = requests.Session()
    phones = our_phones(conn)
    now = datetime.now(timezone.utc)

    # 1. opt-outs (small files -- read every night, new numbers added)
    added = 0
    with conn.cursor() as cur:
        for name, _ in list_objects(s, key, "opt-outs/"):
            for p in set(ten_digit_numbers(file_text(name, download(s, key, name)))):
                cur.execute("""insert into phone_suppression (phone, reason, source_file) values (%s, 'opt-out', %s)
                               on conflict (phone) do nothing""", (p, name))
                added += cur.rowcount
        cur.execute("delete from phone_suppression where added_at < now() - interval '%s years'" % OPT_OUT_YEARS)
        cur.execute("select phone from phone_suppression")
        suppressed = {r[0] for r in cur.fetchall()}
    conn.commit()

    # 2. registry files: which area codes are covered, by which upload
    files = list_objects(s, key, "")
    with conn.cursor() as cur:
        cur.execute("select name, area_codes, uploaded_at from dnc_registry_files")
        known = {r[0]: (r[1] or [], r[2]) for r in cur.fetchall()}
        cur.execute("select phone from phone_dnc")
        checked = {r[0] for r in cur.fetchall()}
    new_files = [f for f in files if f[0] not in known]
    unchecked = phones - checked
    on_dnc, covered = set(), {}
    # Re-read the registry only when there's something new to check: a new
    # download (monthly) or phones from a new skip-trace file.
    if new_files or unchecked:
        for name, created in files:
            text = file_text(name, download(s, key, name))
            nums = list(ten_digit_numbers(text))
            codes = sorted({n[:3] for n in nums})
            up = datetime.fromisoformat(created.replace("Z", "+00:00")) if created else now
            for c in codes:
                if c not in covered or up > covered[c]:
                    covered[c] = up
            on_dnc.update(n for n in nums if n in phones)
            with conn.cursor() as cur:
                cur.execute("""insert into dnc_registry_files (name, area_codes, numbers, uploaded_at)
                               values (%s, %s, %s, %s) on conflict (name) do update
                               set area_codes = excluded.area_codes, numbers = excluded.numbers""",
                            (name, codes, len(nums), up))
        conn.commit()
        with conn.cursor() as cur:
            for p in phones:
                as_of = covered.get(p[:3])
                cur.execute("""insert into phone_dnc (phone, area_code, on_dnc, suppressed, registry_as_of, checked_at)
                               values (%s, %s, %s, %s, %s, now())
                               on conflict (phone) do update set on_dnc = excluded.on_dnc,
                                 suppressed = excluded.suppressed, registry_as_of = excluded.registry_as_of,
                                 checked_at = now()""",
                            (p, p[:3], (p in on_dnc) if as_of else None, p in suppressed, as_of))
    # opt-outs can arrive any night, even without a new registry file
    with conn.cursor() as cur:
        cur.execute("update phone_dnc set suppressed = (phone = any(%s))", (list(suppressed),))
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"""select count(*) filter (where on_dnc is false and not suppressed
                                                  and registry_as_of > now() - interval '{FRESH_DAYS} days'),
                               count(*) filter (where on_dnc),
                               count(*) filter (where suppressed),
                               count(*) filter (where on_dnc is null or registry_as_of <= now() - interval '{FRESH_DAYS} days')
                        from phone_dnc""")
        clean, dnc, supp, unknown = cur.fetchone()
    return {"files": len(files), "new_files": len(new_files), "phones": len(phones), "clean": clean,
            "on_dnc": dnc, "opted_out": supp, "not_checkable": unknown, "opt_outs_added": added}


def main():
    db_url = os.environ.get("DATABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not db_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    ensure_columns(conn)
    ensure_schema(conn)
    ensure_tables(conn)
    if not key:
        print("SUPABASE_SERVICE_KEY is not set -- can't read the dnc-registry bucket")
        return
    t = scrub(conn, key)
    with conn.cursor() as cur:
        cur.execute("insert into source_runs (source_name, records_found, records_new, notes) "
                    "values ('dnc_scrub', %s, %s, %s)",
                    (t["phones"], t["clean"],
                     f"ok: registry_files:{t['files']} new:{t['new_files']} clean:{t['clean']} "
                     f"on_dnc:{t['on_dnc']} opted_out:{t['opted_out']} not_checkable:{t['not_checkable']}"))
    conn.commit()
    conn.close()
    print(f"Do Not Call scrub: {t['phones']} skip-traced phone(s); {t['clean']} clean, {t['on_dnc']} on the "
          f"registry, {t['opted_out']} opted out, {t['not_checkable']} not checkable "
          f"(no registry file under {FRESH_DAYS} days old for that area code). "
          f"{t['files']} registry file(s), {t['new_files']} new.")


if __name__ == "__main__":
    main()
