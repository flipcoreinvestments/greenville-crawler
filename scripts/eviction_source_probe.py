#!/usr/bin/env python3
"""
ONE-TIME PROBE (2026-11-24, Tala's go via Morbius): find out where Greenville
County magistrate eviction (ejectment) filings are published and what the
search form lets us ask for. Reads page STRUCTURE only: titles, form field
names, dropdown option labels, link texts. Tala approved clicking the public
index disclaimer's Accept button. It runs NO searches, reads NO case data,
writes nothing to the database and needs no secrets. Output goes to the run
log (this repo is public, so nothing personal may be printed).
"""
import re
import sys
from playwright.sync_api import sync_playwright

CANDIDATES = [
    "https://www2.greenvillecounty.org/scjd/publicindex/",
    "https://publicindex.sccourts.org/greenville/publicindex/",
    "https://www.gcgis.org/apps/magistrateinfonj/",
]


def describe_loaded(page, label):
    print(f"\n=== {label}")
    print(f"  url={page.url} title={page.title()[:120]!r}")
    body = re.sub(r"\s+", " ", page.inner_text("body"))
    print(f"  body_chars={len(body)} captcha_hint={'captcha' in body.lower() or bool(page.query_selector('iframe[src*=recaptcha], .g-recaptcha, .h-captcha'))}")
    for f in page.query_selector_all("form")[:5]:
        print(f"  FORM action={f.get_attribute('action')!r} method={f.get_attribute('method')!r}")
    for el in page.query_selector_all("input, select, button, textarea")[:80]:
        tag = el.evaluate("e => e.tagName.toLowerCase()")
        if (el.get_attribute("type") or "") == "hidden":
            continue
        print(f"   {tag} name={el.get_attribute('name')!r} type={el.get_attribute('type')!r} value={(el.get_attribute('value') or '')[:40]!r}")
        if tag == "select":
            opts = [re.sub(r"\s+", " ", o.inner_text()).strip() for o in el.query_selector_all("option")]
            print(f"     options({len(opts)}): {opts[:120]}")
    links = [re.sub(r"\s+", " ", a.inner_text()).strip() for a in page.query_selector_all("a")]
    print(f"  links({len([l for l in links if l])}): {[l for l in links if l][:60]}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent="UnstuckHomesResearch/1.0 (+tala@unstuckhomes.com; one-time structure check)")
        for url in CANDIDATES:
            page = ctx.new_page()
            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                page.wait_for_timeout(2500)
                describe_loaded(page, url)
                btn = page.query_selector("input[value='Accept']")
                if btn:
                    print("  -> clicking the disclaimer's Accept button (no search is run)")
                    btn.click()
                    page.wait_for_load_state("domcontentloaded", timeout=45000)
                    page.wait_for_timeout(2500)
                    describe_loaded(page, f"{url} [after Accept]")
                    # one more hop if there is a plain link to a case search page
                    for a in page.query_selector_all("a"):
                        t = re.sub(r"\s+", " ", a.inner_text()).strip()
                        if re.search(r"case|civil|magistrate|search", t, re.I) and len(t) < 60:
                            print(f"  candidate link: {t!r} -> {a.get_attribute('href')!r}")
            except Exception as e:
                print(f"  PROBE ERROR on {url}: {type(e).__name__}: {str(e)[:200]}")
            finally:
                page.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
