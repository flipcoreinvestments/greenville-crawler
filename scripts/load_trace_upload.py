#!/usr/bin/env python3
"""
Load an encrypted skip-trace result file into the private skiptrace-results bucket (2026-11-16).

The repo is public, so a result file is only ever committed ENCRYPTED (scripts/data/uploads/*.enc).
Its one-time key sits in the private morbius_upload_key table (RLS on, no public access). This
script decrypts the file, uploads it to the bucket's top level (skiptrace_intake.py reads it next),
then deletes the key so the committed copy can never be read again. Logs show counts only.
"""
import os
import subprocess
import sys

import psycopg2
import requests

URL = os.environ.get("SUPABASE_URL", "https://izxqrskybwtqzbbbzlux.supabase.co")
HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "uploads")


def main():
    key = os.environ["SUPABASE_SERVICE_KEY"]
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("select name, k from morbius_upload_key")
    jobs = cur.fetchall()
    done = 0
    for name, k in jobs:
        enc = os.path.join(HERE, name + ".enc")
        if not os.path.exists(enc):
            print("one encrypted file is missing from the repo")
            continue
        out = subprocess.run(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-pass", "stdin", "-in", enc],
                             input=k.encode(), capture_output=True)
        if out.returncode != 0:
            print("could not decrypt one file")
            continue
        h = {"Authorization": f"Bearer {key}", "apikey": key, "Content-Type": "text/csv", "x-upsert": "true"}
        r = requests.post(f"{URL}/storage/v1/object/skiptrace-results/{name}", headers=h, data=out.stdout, timeout=120)
        if not r.ok:
            print(f"upload failed: HTTP {r.status_code}")
            continue
        cur.execute("delete from morbius_upload_key where name=%s", (name,))
        conn.commit()
        done += 1
    print(f"Uploaded {done} of {len(jobs)} skip-trace file(s) to the private bucket")
    if jobs and not done:
        sys.exit(1)


if __name__ == "__main__":
    main()
