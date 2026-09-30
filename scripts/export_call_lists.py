#!/usr/bin/env python3
"""
CALL LISTS + SYSTEM HEALTH, written every night (added 2026-09-30).

T Dawg doesn't write SQL, so the lists come to her instead of her querying
for them. After every nightly run this script writes three files into the
PRIVATE Supabase Storage bucket "lead-exports" (never the public repo --
these hold owner names and addresses):

  house_leads.csv   houses, duplex/multiplex, mobile homes, ag improved,
                    unknown land use -- person-owned, residential
  land_leads.csv    residential vacant lots + ag vacant land
  system_health.txt one-page status: when it last ran, list sizes, database
                    space left, Scrapfly credits left, PropStream list ages

Each CSV is sorted the way she calls: '***' (vacant + behind on taxes)
first, then highest score. Same rows as the house_leads / land_leads views.
Files are overwritten every night, so the bucket never grows.

Download: Supabase -> Storage -> lead-exports -> click the file -> Download.
The CSV opens in Excel/Google Sheets and uploads straight into PropStream,
BatchData or BatchDialer (property address, city, state, zip are separate
columns, which is what their skip-trace importers ask for).
"""
import csv
import io
import os
import re
import sys
from datetime import date, datetime, timezone

import psycopg2
import psycopg2.extras

from lead_common import ensure_schema

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://izxqrskybwtqzbbbzlux.supabase.co")
BUCKET = "lead-exports"
DB_LIMIT_MB = 500          # Supabase free plan
DB_WARN_PCT = 80
PROPSTREAM_STALE_DAYS = 60  # propstream_import.py drops a list after this
PROPSTREAM_WARN_DAYS = 45

LIST_LABELS = {
    "tax_sale": "Tax sale list",
    "repeat_tax_delinquent": "Tax sale 2 years running",
    "foreclosure_mie": "Foreclosure auction scheduled",
    "hoa_foreclosure": "HOA foreclosure",
    "permit_expired": "Expired/stalled permit",
    "permit_demolition": "Demolition permit",
    "insurance_damage": "Storm/fire/water damage permit",
    "code_violation": "Code violation (unfit structure)",
    "probate": "Probate (owner deceased)",
    "pre_probate": "Pre-probate (owner deceased)",
    "pre_foreclosure": "Pre-foreclosure",
    "involuntary_lien": "Involuntary lien",
    "failed_listing": "Failed MLS listing",
}

REVIEW_LABELS = {
    "missing_owner": "no owner name",
    "absentee_flag_but_same_address": "absentee flag conflicts",
    "corrupted_address": "bad address",
    "invalid_mailing_address": "mailing address is a name",
    "probate_owner_mismatch": "probate address may not be the estate's",
    "land_use_unknown": "property type unknown",
    "probate_name_match": "probate matched by owner name -- confirm it's the same person",
}

COLUMNS = [
    "Rank", "Flag", "Score", "Property Address", "Property City", "Property State", "Property Zip",
    "Owner Name", "Mailing Address", "Mailing State", "Mailing Zip",
    "Lists", "List Count", "Tax Owed", "Tax Sale Status", "Foreclosure Auction Date",
    "Personal Representative", "PR Address", "Date of Death",
    "Vacant (USPS)", "Absentee Owner", "Tired Landlord", "Out-of-State Owner", "Assumable Loan",
    "Land Use", "Parcel #", "Check Before Calling", "Exported",
]

MAIL_TAIL = re.compile(r",?\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$", re.I)


def split_mailing(mailing):
    """'412 Simsbury Way Greer, SC 29650' -> ('SC', '29650'). The county
    field has no comma between street and city, so the city is left inside
    the full address rather than guessed."""
    m = MAIL_TAIL.search(mailing or "")
    return (m.group(1).upper(), m.group(2)) if m else ("", "")


def yn(v):
    return "Yes" if v is True else ("No" if v is False else "")


def tax_sale_status(raw, tags):
    if "tax_sale" not in tags:
        return ""
    fetched = ((raw or {}).get("tax_sale") or {}).get("fetched_at") or ""
    try:
        seen = datetime.fromisoformat(fetched.replace("Z", "+00:00")).date()
    except ValueError:
        return "On county tax sale list"
    if (date.today() - seen).days <= 3:
        return "On county tax sale list"
    return (f"Sale held -- last on county list {seen:%m/%d/%Y}; owner has 12 months "
            f"from the sale to redeem")


def lead_rows(conn, view):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            select v.*, (v.is_vacant is true and v.source_tags && array['tax_sale', 'repeat_tax_delinquent'])
                        as triple
            from {view} v
            order by (v.is_vacant is true and v.source_tags && array['tax_sale', 'repeat_tax_delinquent']) desc,
                     v.score desc, v.address
            """
        )
        return cur.fetchall()


def to_csv(rows, today):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(COLUMNS)
    for i, r in enumerate(rows, start=1):
        raw = r.get("raw") or {}
        tags = list(r.get("source_tags") or [])
        probate = raw.get("probate") or {}
        mst, mzip = split_mailing(r.get("mailing_address"))
        amount = (raw.get("tax_sale") or {}).get("amount_due") or ""
        reasons = [REVIEW_LABELS.get(x, x.replace("_", " ")) for x in (r.get("review_reasons") or [])
                   if x not in ("single_source", "incomplete_address")]
        flag = "***" if r.get("triple") else ("*" if r.get("needs_review") else "")
        w.writerow([
            i, flag, int(r.get("score") or 0), r["address"], (r.get("city") or "").title(),
            r.get("state") or "SC", r.get("zip") or "",
            r.get("owner_name") or "", r.get("mailing_address") or "", mst, mzip,
            "; ".join(LIST_LABELS.get(t, t) for t in tags), r.get("list_count") or 0,
            f"${float(amount):,.2f}" if re.match(r"^\d+(\.\d+)?$", str(amount)) else "",
            tax_sale_status(raw, tags),
            ((raw.get("foreclosure_mie") or {}).get("sale_date") or "") if "foreclosure_mie" in tags else "",
            (probate.get("pr_name") or "") if "probate" in tags else "",
            (probate.get("pr_address") or "") if "probate" in tags else "",
            (probate.get("date_of_death") or "") if "probate" in tags else "",
            yn(r.get("is_vacant")), yn(r.get("is_absentee")), yn(r.get("is_tired_landlord")),
            yn(r.get("is_out_of_state_land")), yn(r.get("has_assumable_loan")),
            r.get("land_use") or "", r.get("pin") or "", "; ".join(reasons), today.isoformat(),
        ])
    return buf.getvalue()


# ------------------------------------------------------------------ health
def health_report(conn, counts, today):
    lines = [f"Restart Homes lead system -- status as of {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC", ""]
    warn = []
    with conn.cursor() as cur:
        lines.append("LEAD LISTS")
        for name, n in counts.items():
            lines.append(f"  {name:<22}{n:>7,}")
        cur.execute("select count(*) from business_owned_leads")
        lines.append(f"  {'business_owned_leads':<22}{cur.fetchone()[0]:>7,}  (not exported)")
        lines.append("")

        cur.execute("select pg_database_size(current_database())")
        mb = cur.fetchone()[0] / 1024 / 1024
        pct = mb / DB_LIMIT_MB * 100
        lines.append("DATABASE SPACE (free plan limit 500 MB)")
        lines.append(f"  {mb:,.0f} MB used = {pct:.0f}%")
        if pct >= DB_WARN_PCT:
            warn.append(f"Database is {pct:.0f}% full ({mb:,.0f} of {DB_LIMIT_MB} MB). "
                        f"Run 'vacuum full analyze leads;' in the Supabase SQL editor, or upgrade to Pro.")
        lines.append("")

        cur.execute("select source_name, max(run_at) from source_runs where source_name <> 'scrapfly_credits' "
                    "group by source_name having max(run_at) > now() - interval '60 days' order by 1")
        lines.append("LAST RUN PER SOURCE")
        for src, at in cur.fetchall():
            age = (datetime.now(timezone.utc) - at).days if at else None
            lines.append(f"  {src:<28}{at:%Y-%m-%d}" + (f"  ({age} days ago)" if age else ""))
            if age is not None and age > (8 if src == "foreclosure_mie" else 2):
                warn.append(f"{src} has not run in {age} days.")
        lines.append("")

        cur.execute("select notes, run_at from source_runs where source_name = 'scrapfly_credits' "
                    "order by run_at desc limit 1")
        r = cur.fetchone()
        lines.append("SCRAPFLY (weekly foreclosure check, ~150 credits per run)")
        if r:
            lines.append(f"  {r[0]}  (checked {r[1]:%Y-%m-%d})")
            m = re.search(r"remaining=(\d+)", r[0] or "")
            if m and int(m.group(1)) < 450:
                warn.append(f"Scrapfly has {m.group(1)} credits left -- about {int(m.group(1)) // 150} "
                            f"more weekly foreclosure runs. Free credits never refill.")
        else:
            lines.append("  not checked yet (next Monday's run records it)")
        lines.append("")

        lines.append("PROPSTREAM EXPORTS (dropped from leads at 60 days old)")
        for key, label in [("propstream_pre_probate", "Pre-Probate"),
                           ("propstream_pre_foreclosure", "Pre-Foreclosures"),
                           ("propstream_vacant", "Vacant"),
                           ("propstream_involuntary_lien", "Liens"),
                           ("propstream_failed_listing", "Failed Listings"),
                           ("propstream_assumable", "Assumable")]:
            cur.execute("select max(raw->%s->>'file_date') from leads where raw ? %s", (key, key))
            d = cur.fetchone()[0]
            if not d:
                lines.append(f"  {label:<18}not uploaded")
                continue
            age = (today - date.fromisoformat(d[:10])).days
            lines.append(f"  {label:<18}exported {d[:10]}  ({age} days old)")
            if age >= PROPSTREAM_WARN_DAYS:
                warn.append(f"PropStream {label} export is {age} days old -- re-pull it before day "
                            f"{PROPSTREAM_STALE_DAYS} or it drops off every lead.")
    head = ["ACTION NEEDED:"] + [f"  - {w}" for w in warn] if warn else ["ACTION NEEDED: nothing"]
    return "\n".join(lines[:2] + head + [""] + lines[2:]) + "\n", warn


# ------------------------------------------------------------------ upload
def upload(files, key, url=SUPABASE_URL, session=None):
    import requests
    s = session or requests.Session()
    h = {"Authorization": f"Bearer {key}", "apikey": key}
    # create the private bucket the first time (409/400 = already exists)
    r = s.post(f"{url}/storage/v1/bucket", headers=h,
               json={"id": BUCKET, "name": BUCKET, "public": False}, timeout=60)
    if r.status_code not in (200, 201, 400, 409):
        r.raise_for_status()
    for name, body, ctype in files:
        r = s.post(f"{url}/storage/v1/object/{BUCKET}/{name}",
                   headers={**h, "x-upsert": "true", "Content-Type": ctype},
                   data=body.encode("utf-8"), timeout=120)
        r.raise_for_status()
        print(f"  uploaded {BUCKET}/{name} ({len(body):,} bytes)")


def build(conn, today=None):
    today = today or date.today()
    out, counts = [], {}
    for view in ("house_leads", "land_leads"):
        rows = lead_rows(conn, view)
        counts[view] = len(rows)
        out.append((f"{view}.csv", to_csv(rows, today), "text/csv"))
    report, warn = health_report(conn, counts, today)
    out.append(("system_health.txt", report, "text/plain"))
    return out, warn


def main():
    db_url = os.environ.get("DATABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not db_url or not key:
        print("DATABASE_URL and SUPABASE_SERVICE_KEY must be set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(db_url)
    ensure_schema(conn)
    conn.commit()
    files, warn = build(conn)
    conn.close()
    upload(files, key)
    print(files[-1][1])
    for w in warn:
        print(f"::warning::{w}")


if __name__ == "__main__":
    main()
