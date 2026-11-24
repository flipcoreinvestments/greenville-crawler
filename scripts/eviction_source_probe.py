#!/usr/bin/env python3
"""
ONE-TIME PROBE (2026-11-24, Tala's go via Morbius): find out where Greenville
County magistrate eviction (ejectment) filings are published and what the
search form lets us ask for. It reads page STRUCTURE only: titles, form field
names, dropdown option labels, link texts. It runs no searches, reads no case
data, writes nothing to the database and needs no secrets. Output goes to the
run log (this repo is public, so nothing personal may be printed).
"""
import re
import sys
from playwright.sync_api import sync_playwright

CANDIDATES = [
    "https://www2.greenvillecounty.org/scjd/publicindex/",
    "https://www2.greenvillecounty.org/scjd/publicindex",
    "http://www.13th-judicial-circuit.org",
    "https://publicindex.sccourts.org/greenville/publicindex/",
    "https://publicindex.sccourts.org/",
    "https://www.gcgis.org/apps/magistrateinfonj/",
]
KEYWORDS = re.compile(r"magistrat|summary|civil|evict|eject|landlord|case type|filed|disclaim|agree|captcha", re.I)


def describe(page, url):
    print(f"\n=== {url}")
    try:
        resp = page.goto(url, timeout=45000, wait_until="domcontentloaded")
    except Exception as e:
        print(f"  LOAD FAILED: {type(e).__name__}: {str(e)[:160]}")
        return
    page.wait_for_timeout(2500)
    print(f"  status={resp.status if resp else '?'} final_url={page.url}")
    print(f"  title={page.title()[:120]!r}")
    body = re.sub(r"\s+", " ", page.inner_text("body"))
    print(f"  body_chars={len(body)} captcha_hint={'captcha' in body.lower() or bool(page.query_selector('iframe[src*=recaptcha], .g-recaptcha, .h-captcha'))}")
    for m in sorted({m.group(0).lower() for m in KEYWORDS.finditer(body)}):
        print(f"  mentions: {m}")
    for f in page.query_selector_all("form")[:5]:
        print(f"  FORM action={f.get_attribute('action')!r} method={f.get_attribute('method')!r}")
    for el in page.query_selector_all("input, select, button, textarea")[:60]:
        tag = el.evaluate("e => e.tagName.toLowerCase()")
        print(f"   {tag} name={el.get_attribute('name')!r} id={el.get_attribute('id')!r} type={el.get_attribute('type')!r} value={(el.get_attribute('value') or '')[:40]!r}")
        if tag == "select":
            opts = [re.sub(r'\s+', ' ', o.inner_text()).strip() for o in el.query_selector_all("option")]
            print(f"     options({len(opts)}): {opts[:80]}")
    links = [re.sub(r"\s+", " ", a.inner_text()).strip() for a in page.query_selector_all("a")]
    links = [l for l in links if l]
    print(f"  links({len(links)}): {links[:60]}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent="UnstuckHomesResearch/1.0 (+tala@unstuckhomes.com; one-time structure check)")
        page = ctx.new_page()
        for url in CANDIDATES:
            try:
                describe(page, url)
            except Exception as e:
                print(f"  PROBE ERROR on {url}: {type(e).__name__}: {str(e)[:160]}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
