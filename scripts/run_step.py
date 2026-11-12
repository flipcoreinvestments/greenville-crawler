#!/usr/bin/env python3
"""
Run one nightly step, and retry it ONCE if it died from a passing database
problem. Added 2026-11-12 (Tala's call after the November failures).

    python scripts/run_step.py scripts/probate_ingest.py [args...]

A retry happens only when the step's own output shows a database hiccup that
usually clears on its own (deadlock, statement timeout, no free connection,
dropped connection). Any other failure (a county site down, a bug, bad data)
is NOT retried, so real problems still show red right away. Every step is
safe to run twice: they upsert and re-check rather than append.
"""

import os
import subprocess
import sys
import time

RETRY_WAIT_SECONDS = int(os.environ.get("RUN_STEP_RETRY_WAIT", "90"))
TRANSIENT_DB_ERRORS = (
    "deadlock detected",
    "canceling statement due to statement timeout",
    "canceling statement due to lock timeout",
    "ECHECKOUTTIMEOUT",
    "authentication did not complete",
    "server closed the connection unexpectedly",
    "could not connect to server",
    "SSL connection has been closed unexpectedly",
    "terminating connection due to administrator command",
)


def run_once(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1,
                            env={**os.environ, "PYTHONUNBUFFERED": "1"})
    tail = []
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        tail.append(line)
        if len(tail) > 200:
            tail.pop(0)
    return proc.wait(), "".join(tail)


def main():
    if len(sys.argv) < 2:
        print("usage: run_step.py <script.py> [args...]", file=sys.stderr)
        sys.exit(2)
    cmd = [sys.executable] + sys.argv[1:]
    code, tail = run_once(cmd)
    if code == 0:
        return
    hit = next((m for m in TRANSIENT_DB_ERRORS if m in tail), None)
    if not hit:
        sys.exit(code)
    print(f"\n[run_step] step failed on a passing database problem ({hit!r}); "
          f"retrying once in {RETRY_WAIT_SECONDS}s...", flush=True)
    time.sleep(RETRY_WAIT_SECONDS)
    code, _ = run_once(cmd)
    if code == 0:
        print("[run_step] retry succeeded.", flush=True)
    sys.exit(code)


if __name__ == "__main__":
    main()
