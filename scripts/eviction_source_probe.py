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
    from collections import Counter
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent="UnstuckHomesResearch/1.0 (+tala@unstuckhomes.com; one-time structure check)")
        page = fresh_form(ctx)
        try:
            sel(page, "DropDownListCourtType", "Summary Court")
            sel(page, "DropDownListCaseTypes", "Civil")
            page.wait_for_timeout(1500)
            sel(page, "DropdownlistCaseSubType", "Eject Unlaw Occ $150")
            chosen = page.eval_on_selector(f"select[name='{P}DropdownlistCaseSubType']", "e => e.options[e.selectedIndex].text")
            print(f"selected sub-type after postbacks: {chosen!r}")
            sel(page, "DropDownListDateFilter", "Case Filed")
            page.fill(f"input[name='{P}TextBoxDateFrom']", DATE_FROM)
            page.fill(f"input[name='{P}TextBoxDateTo']", DATE_TO)
            print("form state before search:", {n: page.eval_on_selector(f"select[name='{P}{n}']", "e => e.options[e.selectedIndex].text") for n in ["DropDownListCourtType","DropDownListCaseTypes","DropdownlistCaseSubType","DropDownListDateFilter"]})
            page.click(f"input[name='{P}ButtonSearch']")
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            best = None
            for t in page.query_selector_all("table"):
                n = len(t.query_selector_all("tr"))
                if n > 2 and (best is None or n > len(best.query_selector_all("tr"))):
                    best = t
            rows = best.query_selector_all("tr")
            hdr = [re.sub(r"\s+", " ", c.inner_text()).strip() for c in rows[0].query_selector_all("th,td")]
            data = []
            for r in rows[1:]:
                cells = [re.sub(r"\s+", " ", c.inner_text()).strip() for c in r.query_selector_all("td")]
                if len(cells) == len(hdr):
                    data.append(dict(zip(hdr, cells)))
            print(f"parsed rows={len(data)} of {len(rows)-1}")
            for col in ["Party Type", "Case Status", "Type", "Subtype", "Court Agency"]:
                print(f"  {col}: {dict(Counter(d.get(col) for d in data).most_common(15))}")
            dates = sorted({d.get("Filed Date") for d in data if d.get("Filed Date")})
            print(f"  filed_date range: {dates[:1]} .. {dates[-1:]} distinct_days={len(dates)}")
            print(f"  distinct case numbers: {len({d.get('Case Number') for d in data})}")
            ev = [d for d in data if d.get("Subtype") and re.search(r"eject", d["Subtype"], re.I)]
            print(f"  rows whose Subtype mentions eject: {len(ev)}; party types among them: {dict(Counter(d['Party Type'] for d in ev))}")
            target = ev[0]["Case Number"] if ev else data[0]["Case Number"]
            link = None
            for a in best.query_selector_all("a"):
                if target in a.inner_text():
                    link = a
                    break
            if link:
                link.evaluate("e => e.click()")
                page.wait_for_load_state("domcontentloaded", timeout=45000)
                page.wait_for_timeout(2500)
                text = page.inner_text("body")
                labels = sorted({re.sub(r"\s+", " ", l).strip() for l in text.splitlines() if l.strip().endswith(":") and len(l.strip()) < 50})
                print(f"DETAIL url_path={page.url.split('.org')[-1]} labels={labels}")
                heads = sorted({re.sub(r"\s+", " ", c.inner_text()).strip() for c in page.query_selector_all("th")})
                print(f"DETAIL table_headers={heads[:60]}")
                print(f"DETAIL mentions_address_words={sorted({m.lower() for m in re.findall(r'address|street|premises|property', text, re.I)})}")
                street_re = re.compile(r"\b\d{2,5} [A-Z][A-Za-z]+ (St|Rd|Dr|Ave|Ln|Ct|Cir|Way|Blvd)\b")
                print("DETAIL has_street_pattern=" + str(bool(street_re.search(text))))
            else:
                print("no link for chosen case number")
        except Exception as e:
            print(f"PROBE ERROR: {type(e).__name__}: {str(e)[:300]}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
