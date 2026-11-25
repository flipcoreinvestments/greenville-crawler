#!/usr/bin/env python3
"""
ONE-TIME PROBE v3 (2026-11-24, Tala's go via Morbius). Greenville County Public
Index, Summary Court, Civil, eviction sub-types, Case Filed in the last ~3
weeks. Prints ONLY structure and counts: row counts, table column headers and
the field LABELS on one case page. It never prints a name, address or case
number (this repo is public). No database, no secrets.
"""
import re
import sys
from playwright.sync_api import sync_playwright

START = "https://www2.greenvillecounty.org/scjd/publicindex/"
SUBTYPES = ["Eject Unlaw Oc (470)", "Eject Unlaw Occ $150", "Eject Unlaw Occ $160", "Ejectm of Trespasser"]
DATE_FROM, DATE_TO = "11/01/2026", "11/24/2026"
P = "ctl00$ContentPlaceHolder1$"


def sel(page, name, label):
    page.select_option(f"select[name='{P}{name}']", label=label)
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(1200)


def fresh_form(ctx):
    page = ctx.new_page()
    page.goto(START, timeout=45000, wait_until="domcontentloaded")
    page.click("input[value='Accept']")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(1500)
    return page


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent="UnstuckHomesResearch/1.0 (+tala@unstuckhomes.com; one-time structure check)")
        shown_detail = False
        for sub in SUBTYPES:
            page = fresh_form(ctx)
            try:
                sel(page, "DropDownListCourtType", "Summary Court")
                sel(page, "DropDownListCaseTypes", "Civil")
                sel(page, "DropdownlistCaseSubType", sub)
                sel(page, "DropDownListDateFilter", "Case Filed")
                page.fill(f"input[name='{P}TextBoxDateFrom']", DATE_FROM)
                page.fill(f"input[name='{P}TextBoxDateTo']", DATE_TO)
                page.click(f"input[name='{P}ButtonSearch']")
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)
                tables = page.query_selector_all("table")
                best = None
                for t in tables:
                    rows = t.query_selector_all("tr")
                    if len(rows) > 2 and (best is None or len(rows) > len(best.query_selector_all("tr"))):
                        best = t
                print(f"\n=== sub-type {sub!r}  url_path={page.url.split('.org')[-1]}")
                body = re.sub(r"\s+", " ", page.inner_text("body"))
                m = re.search(r"(\d[\d,]*)\s+(records?|results?|cases?)\s+(found|returned)", body, re.I)
                print(f"  count_text={m.group(0) if m else None!r} body_chars={len(body)} has_no_results={'no records' in body.lower() or 'no results' in body.lower()}")
                if best is not None:
                    rows = best.query_selector_all("tr")
                    hdr = [re.sub(r"\s+", " ", c.inner_text()).strip() for c in rows[0].query_selector_all("th,td")]
                    print(f"  result_table rows={len(rows)-1} columns={hdr}")
                    nlinks = len(best.query_selector_all("a"))
                    print(f"  links_in_table={nlinks}")
                    pager = [re.sub(r"\s+", " ", a.inner_text()).strip() for a in page.query_selector_all("a") if re.fullmatch(r"\d+|\.\.\.|Next|>|>>", re.sub(r"\s+", " ", a.inner_text()).strip())]
                    print(f"  pager_links={pager[:15]}")
                    if not shown_detail and nlinks:
                        a = best.query_selector_all("a")[0]
                        try:
                            a.click()
                            page.wait_for_load_state("domcontentloaded", timeout=45000)
                            page.wait_for_timeout(2500)
                            text = page.inner_text("body")
                            labels = sorted({re.sub(r"\s+", " ", l).strip() for l in text.splitlines() if l.strip().endswith(":") and len(l.strip()) < 50})
                            print(f"  DETAIL url_path={page.url.split('.org')[-1]} labels={labels}")
                            heads = sorted({re.sub(r"\s+", " ", c.inner_text()).strip() for c in page.query_selector_all("th")})
                            print(f"  DETAIL table_headers={heads[:60]}")
                            print(f"  DETAIL mentions_address={bool(re.search(r'address|street|property|premises', text, re.I))}")
                            shown_detail = True
                        except Exception as e:
                            print(f"  DETAIL click failed: {type(e).__name__}: {str(e)[:160]}")
                else:
                    print("  no result table found")
            except Exception as e:
                print(f"  PROBE ERROR for {sub!r}: {type(e).__name__}: {str(e)[:220]}")
            finally:
                page.close()
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
