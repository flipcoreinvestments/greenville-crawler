#!/usr/bin/env python3
"""
One-off debug helper: prints what the foreclosure_mie sale-date dropdown
looks like from inside a GitHub Actions runner, with no DB dependency.

Exists to root-cause the 2026-09-22 discrepancy: a real browser session
(user's own Chrome) showed 5 future sale dates on the site, but the same
fetch_future_sale_dates() call from inside a GitHub Actions run returned 0.
Delete this file once that's resolved -- it's throwaway diagnostic tooling,
not part of the pipeline.
"""
import sys
from playwright.sync_api import sync_playwright
import foreclosure_mie_ingest as fm

with sync_playwright() as p:
    browser = p.chromium.launch()
    context, page = fm.new_browser_context(browser)
    dates = fm.fetch_future_sale_dates(page)
    print(f"RESULT: fetch_future_sale_dates() returned {len(dates)} future date(s): {dates}")
    browser.close()
