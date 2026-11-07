"""
BANKRUPTCY INGEST (added 2026-11-07 for Tala's Unstuck Homes "bankruptcy list").

Once a week: asks PACER's official Case Locator (PCL) API for new Chapter 7 and
Chapter 13 cases filed in the U.S. Bankruptcy Court for South Carolina (court
id "scbk"), saves them in table bankruptcy_filings, and matches each debtor to
a crawler lead's owner. Tala's common-name rule applies: a match needs the same
last name, the same first name AND the same middle initial when both sides have
one; anything weaker is never matched. Matched leads get the "bankruptcy" tag
in GHL through Morbius (morbius_sync.py) with a "check the property" note,
because PCL results carry no property address.

Cost: PACER bills about $0.10 per page of 54 results and waives bills under
$30 a quarter. This script stops after MAX_PAGES pages per run.

  python bankruptcy_ingest.py --check      log in and out only (free; proves the secrets work)
  python bankruptcy_ingest.py --days 8     search the last 8 days (default)

Secrets: PACER_USERNAME, PACER_PASSWORD, optional PACER_TOTP_SECRET (authenticator
setup key, if MFA is on), DATABASE_URL. Logs counts only, never names.
"""
import argparse
import base64
import hashlib
import hmac
import os
import re
import struct
import sys
import time
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
import requests

from skiptrace_prep import owner_trace_names

AUTH_URL = "https://pacer.login.uscourts.gov/services/cso-auth"
LOGOUT_URL = "https://pacer.login.uscourts.gov/services/cso-logout"
PCL = "https://pcl.uscourts.gov/pcl-public-api/rest"
COURT = "scbk"
MAX_PAGES = 12  # ~$1.20 worst case per run

USER = os.environ.get("PACER_USERNAME", "").strip()
PASS = os.environ.get("PACER_PASSWORD", "").strip()
TOTP = os.environ.get("PACER_TOTP_SECRET", "").replace(" ", "").strip()


def totp_now(secret):
    key = base64.b32decode(secret.upper() + "=" * (-len(secret) % 8))
    msg = struct.pack(">Q", int(time.time()) // 30)
    h = hmac.new(key, msg, hashlib.sha1).digest()
    o = h[-1] & 0x0F
    return f"{(struct.unpack('>I', h[o:o + 4])[0] & 0x7FFFFFFF) % 1000000:06d}"


def login():
    body = {"loginId": USER, "password": PASS, "redactFlag": "1"}
    if TOTP:
        body["otpCode"] = totp_now(TOTP)
    r = requests.post(AUTH_URL, json=body, headers={"Accept": "application/json"}, timeout=30)
    try:
        j = r.json()
    except ValueError:
        j = {}
    token = j.get("nextGenCSO")
    if r.status_code != 200 or not token:
        msg = (j.get("errorDescription") or j.get("loginResult") or f"HTTP {r.status_code}")
        print(f"PACER login failed: {str(msg)[:160]}")
        if "otp" in str(msg).lower() or "multi" in str(msg).lower():
            print("Looks like MFA is on: add the authenticator setup key as the PACER_TOTP_SECRET secret.")
        sys.exit(1)
    print(f"PACER login result: {j.get('loginResult')} {str(j.get('errorDescription') or '')[:120]}".strip())
    return token


def logout(token):
    try:
        requests.post(LOGOUT_URL, json={"nextGenCSO": token}, timeout=15)
    except requests.RequestException:
        pass


SUFFIX = {"JR", "SR", "II", "III", "IV", "V"}


def people_from_title(title):
    """'John A. Smith and Mary B. Smith' / 'Smith, John A' -> [(first, middle_initial, last)]."""
    out = []
    for part in re.split(r"\s+(?:and|&)\s+", title or "", flags=re.I):
        part = re.sub(r"\(.*?\)|\bet al\b\.?|\ba/k/a\b.*|\baka\b.*|\bdba\b.*|\bf/k/a\b.*", " ", part, flags=re.I)
        if re.search(r"\b(LLC|INC|CORP|COMPANY|TRUST|L\.L\.C)\b", part, re.I):
            continue
        if "," in part:
            last, rest = part.split(",", 1)
            toks = [t for t in re.findall(r"[A-Za-z][A-Za-z'\-]*", rest) if t.upper() not in SUFFIX]
            if not toks:
                continue
            first, mid = toks[0], (toks[1][0] if len(toks) > 1 else "")
            last = re.sub(r"[^A-Za-z'\-]", "", last)
        else:
            toks = [t for t in re.findall(r"[A-Za-z][A-Za-z'\-]*", part) if t.upper() not in SUFFIX]
            if len(toks) < 2:
                continue
            first, last = toks[0], toks[-1]
            mid = toks[1][0] if len(toks) > 2 else ""
        out.append((first.upper(), mid.upper(), last.upper()))
    return out


def owner_people(owner_name):
    """County 'LAST FIRST MIDDLE' (+ co-owners) -> [(first, middle_initial, last)]."""
    n = owner_trace_names(owner_name)
    if n.get("kind") != "Person":
        return []
    out = []
    raw = (owner_name or "").upper()
    for f, l in ((n.get("first"), n.get("last")), (n.get("first2"), n.get("last2"))):
        if not f or not l:
            continue
        f, l = f.upper(), l.upper()
        mid = ""
        m = re.search(rf"\b{re.escape(l)}\b\s*,?\s*{re.escape(f)}\s+([A-Z])", raw)
        if m:
            mid = m.group(1)
        out.append((f, mid, l))
    return out


def same_person(a, b):
    if a[0] != b[0] or a[2] != b[2]:
        return False
    return not (a[1] and b[1] and a[1] != b[1]) and bool(a[1] and b[1])


def search(token, d_from, d_to):
    rows, page, pages = [], 0, 1
    body = {"courtId": [COURT], "jurisdictionType": "bk",
            "dateFiledFrom": d_from.isoformat(), "dateFiledTo": d_to.isoformat(),
            "federalBankruptcyChapter": ["7", "13"]}
    head = {"Content-type": "application/json", "Accept": "application/json", "X-NEXT-GEN-CSO": token}
    while page < pages and page < MAX_PAGES:
        r = requests.post(f"{PCL}/cases/find", params={"page": page}, json=body, headers=head, timeout=60)
        if r.status_code == 401 and page == 0:
            for alt in ("X-NEXT-GENCSO", "X-NEXTGEN-CSO"):
                h2 = {k: v for k, v in head.items() if k != "X-NEXT-GEN-CSO"}
                h2[alt] = token
                r = requests.post(f"{PCL}/cases/find", params={"page": page}, json=body, headers=h2, timeout=60)
                if r.status_code != 401:
                    head = h2
                    break
            if r.status_code == 401:
                r = requests.post(f"{PCL}/cases/find", params={"page": page}, json=body, headers=head,
                                  cookies={"NextGenCSO": token}, timeout=60)
            print(f"PCL retry: HTTP {r.status_code} {r.text[:200].replace(token, '***')}")
        if r.status_code != 200:
            print(f"PCL search failed on page {page}: HTTP {r.status_code}")
            break
        j = r.json()
        rows += j.get("content") or []
        pages = (j.get("pageInfo") or {}).get("totalPages") or 1
        page += 1
    print(f"PCL: {len(rows)} cases, {page} page(s) billed (stopped at {MAX_PAGES} max)" if page >= MAX_PAGES
          else f"PCL: {len(rows)} cases, {page} page(s) billed")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--days", type=int, default=8)
    a = ap.parse_args()
    if not (USER and PASS):
        print("Missing PACER_USERNAME or PACER_PASSWORD secret.")
        sys.exit(1)
    token = login()
    if a.check:
        print("PACER login OK (no search run, nothing billed).")
        logout(token)
        return
    try:
        cases = search(token, date.today() - timedelta(days=a.days), date.today())
    finally:
        logout(token)

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    with conn.cursor() as cur:
        cur.execute("""create table if not exists bankruptcy_filings (
                         case_number text primary key, title text, chapter text, date_filed date,
                         office text, matched_lead_id text, seen_at timestamptz default now())""")
        cur.execute("alter table bankruptcy_filings enable row level security")
        cur.execute("revoke all on bankruptcy_filings from anon, authenticated")
        cur.execute("""select id::text, owner_name from leads
                       where coalesce(is_duplicate,false)=false and owner_name is not null""")
        owners = [(lid, owner_people(on)) for lid, on in cur.fetchall()]
    by_name = {}
    for lid, ppl in owners:
        for p in ppl:
            by_name.setdefault((p[0], p[2]), []).append((lid, p))
    matched = 0
    with conn.cursor() as cur:
        for c in cases:
            num = c.get("caseNumberFull") or c.get("caseNumber")
            if not num:
                continue
            title = c.get("caseTitle") or ""
            hit = None
            for p in people_from_title(title):
                cands = [lid for lid, op in by_name.get((p[0], p[2]), []) if same_person(p, op)]
                if len(set(cands)) == 1:
                    hit = cands[0]
                    break
            matched += bool(hit)
            cur.execute("""insert into bankruptcy_filings (case_number, title, chapter, date_filed, office, matched_lead_id)
                           values (%s,%s,%s,%s,%s,%s)
                           on conflict (case_number) do update set matched_lead_id =
                             coalesce(excluded.matched_lead_id, bankruptcy_filings.matched_lead_id)""",
                        (num, title, str(c.get("bankruptcyChapter") or ""), c.get("dateFiled"),
                         c.get("caseOffice"), hit))
    conn.commit()
    conn.close()
    print(f"Bankruptcy: {len(cases)} SC filings saved, {matched} matched to a crawler lead "
          "(same first, middle initial and last name).")


if __name__ == "__main__":
    main()
