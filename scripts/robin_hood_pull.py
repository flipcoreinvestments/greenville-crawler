#!/usr/bin/env python3
"""
ROBIN HOOD DAILY PULL (added 2026-11-06, Tala's ask: "eyes everywhere we exist").

Every morning this reads Unstuck Homes' numbers from every source, VIEW-ONLY,
and saves one snapshot per source per day in the private Supabase table
robin_hood_daily. Claude's morning Robin Hood report reads that table, writes
the report, the Company Health Tracker row and (on the 1st) the monthly
health report, and shares the numbers with Margaret Marketer.

Sources (each runs on its own; one failing never stops the others):
  ghl         GoHighLevel (view-only Private Integration token)
  ga4         Google Analytics 4 (robot account, Viewer)
  gsc         Google Search Console (robot account, Restricted)
  bing        Bing Webmaster Tools
  clarity     Microsoft Clarity
  pagespeed   PageSpeed Insights (phone + computer, Core Web Vitals)
  site        Site up / response time / trackers present / robots + sitemap

THIS REPO IS PUBLIC. Logs print COUNTS ONLY -- never names, phones, emails,
search terms, tokens or URLs that carry keys. Errors are reduced to the
source name, HTTP status and a short reason.

Secrets (GitHub > Settings > Secrets and variables > Actions):
  DATABASE_URL, GHL_TOKEN, GHL_LOCATION_ID, GOOGLE_SA_JSON,
  CLARITY_TOKEN, BING_API_KEY
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import requests

SITE = "https://unstuckhomes.com/"
GA_PROPERTY = "563019976"
GA_ID = "G-TZ8SMTF26B"
CLARITY_ID = "z9m11hl4b4"
GSC_SITES = ["sc-domain:unstuckhomes.com", "https://unstuckhomes.com/"]
GHL_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"
ET = ZoneInfo("America/New_York")
TIMEOUT = 45
COLD_DAYS = 7          # an open opportunity untouched this long is "going cold"
MAX_PAGES = 60         # safety cap on paging (6,000 records per list)

NOW_ET = datetime.now(ET)
TODAY = NOW_ET.date()
YESTERDAY = TODAY - timedelta(days=1)


class SourceError(Exception):
    """A short, log-safe reason (no URLs, keys or personal data)."""


def safe_get(url, *, headers=None, params=None, label=""):
    try:
        r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise SourceError(f"{label} network error ({type(e).__name__})") from None
    if r.status_code == 429:
        time.sleep(8)
        try:
            r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            raise SourceError(f"{label} network error ({type(e).__name__})") from None
    if r.status_code >= 400:
        raise SourceError(f"{label} HTTP {r.status_code}")
    try:
        return r.json()
    except ValueError:
        raise SourceError(f"{label} returned non-JSON") from None


def safe_post(url, *, headers=None, body=None, label=""):
    try:
        r = requests.post(url, headers=headers, json=body, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise SourceError(f"{label} network error ({type(e).__name__})") from None
    if r.status_code >= 400:
        raise SourceError(f"{label} HTTP {r.status_code}")
    try:
        return r.json()
    except ValueError:
        raise SourceError(f"{label} returned non-JSON") from None


def parse_dt(v):
    if not v:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def on_day(dt, day):
    return dt is not None and dt.astimezone(ET).date() == day


# --------------------------------------------------------------------------- GHL
def ghl_paged(path, params, key, headers):
    """Follows GHL's startAfter/startAfterId paging. Returns the full list."""
    out, params = [], dict(params)
    for _ in range(MAX_PAGES):
        data = safe_get(GHL_BASE + path, headers=headers, params=params, label="ghl" + path.split("?")[0])
        items = data.get(key) or []
        out.extend(items)
        meta = data.get("meta") or {}
        if len(items) < int(params.get("limit", 100)) or not (meta.get("startAfterId") or meta.get("startAfter")):
            break
        if meta.get("startAfterId"):
            params["startAfterId"] = meta["startAfterId"]
        if meta.get("startAfter"):
            params["startAfter"] = meta["startAfter"]
    return out


def pull_ghl():
    token, loc = os.environ.get("GHL_TOKEN"), os.environ.get("GHL_LOCATION_ID")
    if not token or not loc:
        raise SourceError("GHL_TOKEN / GHL_LOCATION_ID secret missing")
    h = {"Authorization": f"Bearer {token}", "Version": GHL_VERSION, "Accept": "application/json"}
    m, notes = {}, []

    # Contacts
    contacts = ghl_paged("/contacts/", {"locationId": loc, "limit": 100}, "contacts", h)
    m["contacts_total"] = len(contacts)
    m["contacts_new_yesterday"] = sum(on_day(parse_dt(c.get("dateAdded")), YESTERDAY) for c in contacts)
    week_ago = NOW_ET - timedelta(days=7)
    m["contacts_new_7d"] = sum(1 for c in contacts if (parse_dt(c.get("dateAdded")) or week_ago) > week_ago)
    src = {}
    for c in contacts:
        if (parse_dt(c.get("dateAdded")) or week_ago) > NOW_ET - timedelta(days=30):
            s = (c.get("source") or "unknown").strip().lower()[:40]
            src[s] = src.get(s, 0) + 1
    m["new_contacts_by_source_30d"] = src

    # Pipelines + opportunities
    stages = {}
    try:
        pipes = safe_get(GHL_BASE + "/opportunities/pipelines", headers=h, params={"locationId": loc}, label="ghl pipelines")
        for p in pipes.get("pipelines") or []:
            for s in p.get("stages") or []:
                stages[s.get("id")] = f"{p.get('name', '')} › {s.get('name', '')}"
    except SourceError as e:
        notes.append(str(e))
    opps = ghl_paged("/opportunities/search", {"location_id": loc, "limit": 100}, "opportunities", h)
    by_status, by_stage, cold = {}, {}, []
    won_value_30d, won_30d = 0.0, 0
    for o in opps:
        st = (o.get("status") or "open").lower()
        by_status[st] = by_status.get(st, 0) + 1
        if st == "open":
            nm = stages.get(o.get("pipelineStageId"), "unknown stage")
            by_stage[nm] = by_stage.get(nm, 0) + 1
            last = parse_dt(o.get("lastStageChangeAt")) or parse_dt(o.get("updatedAt")) or parse_dt(o.get("createdAt"))
            if last:
                days = (datetime.now(timezone.utc) - last).days
                if days >= COLD_DAYS:
                    cold.append({"name": (o.get("name") or (o.get("contact") or {}).get("name") or "Unnamed")[:60],
                                 "stage": nm, "days_untouched": days,
                                 "value": o.get("monetaryValue") or 0})
        if st == "won":
            when = parse_dt(o.get("lastStatusChangeAt")) or parse_dt(o.get("updatedAt"))
            if when and when > datetime.now(timezone.utc) - timedelta(days=30):
                won_30d += 1
                won_value_30d += float(o.get("monetaryValue") or 0)
    cold.sort(key=lambda x: -x["days_untouched"])
    m.update({"opportunities_total": len(opps), "opportunities_by_status": by_status,
              "open_by_stage": by_stage, "going_cold": cold[:15], "going_cold_count": len(cold),
              "won_30d": won_30d, "won_value_30d": round(won_value_30d, 2),
              "opps_new_yesterday": sum(on_day(parse_dt(o.get("createdAt")), YESTERDAY) for o in opps)})

    # Conversations
    try:
        conv = safe_get(GHL_BASE + "/conversations/search", headers=h,
                        params={"locationId": loc, "limit": 100, "sort": "desc", "sortBy": "last_message_date"},
                        label="ghl conversations")
        convs = conv.get("conversations") or []
        m["conversations_unread"] = sum(1 for c in convs if (c.get("unreadCount") or 0) > 0)
        m["conversations_waiting_on_us"] = sum(
            1 for c in convs if str(c.get("lastMessageDirection") or "").lower() == "inbound")
        m["conversations_active_yesterday"] = sum(on_day(parse_dt(c.get("lastMessageDate")), YESTERDAY) for c in convs)
    except SourceError as e:
        notes.append(str(e))

    # Appointments: yesterday + next 7 days
    try:
        cals = safe_get(GHL_BASE + "/calendars/", headers=h, params={"locationId": loc}, label="ghl calendars")
        start = datetime.combine(YESTERDAY, datetime.min.time(), ET)
        end = start + timedelta(days=8)
        appts_y, appts_next, noshow_y = 0, 0, 0
        for c in (cals.get("calendars") or [])[:20]:
            ev = safe_get(GHL_BASE + "/calendars/events", headers=h,
                          params={"locationId": loc, "calendarId": c.get("id"),
                                  "startTime": int(start.timestamp() * 1000), "endTime": int(end.timestamp() * 1000)},
                          label="ghl events")
            for e in ev.get("events") or []:
                t = parse_dt(e.get("startTime"))
                status = str(e.get("appointmentStatus") or e.get("status") or "").lower()
                if on_day(t, YESTERDAY):
                    appts_y += 1
                    noshow_y += status in ("noshow", "no_show", "no-show")
                elif t and t.astimezone(ET).date() >= TODAY:
                    appts_next += 1
        m.update({"appointments_yesterday": appts_y, "no_shows_yesterday": noshow_y,
                  "appointments_next_7d": appts_next})
    except SourceError as e:
        notes.append(str(e))

    # Form submissions
    try:
        fs = safe_get(GHL_BASE + "/forms/submissions", headers=h,
                      params={"locationId": loc, "startAt": YESTERDAY.isoformat(), "endAt": YESTERDAY.isoformat(),
                              "limit": 100}, label="ghl forms")
        m["form_submissions_yesterday"] = (fs.get("meta") or {}).get("total", len(fs.get("submissions") or []))
    except SourceError as e:
        notes.append(str(e))

    if notes:
        m["partial_errors"] = notes
    return m


# ------------------------------------------------------------------ Google (GA4/GSC)
_google_token = {}


def google_token(scope):
    if scope in _google_token:
        return _google_token[scope]
    raw = os.environ.get("GOOGLE_SA_JSON")
    if not raw:
        raise SourceError("GOOGLE_SA_JSON secret missing")
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request
        creds = service_account.Credentials.from_service_account_info(json.loads(raw), scopes=[scope])
        creds.refresh(Request())
    except Exception as e:  # never print the key
        raise SourceError(f"google auth failed ({type(e).__name__})") from None
    _google_token[scope] = creds.token
    return creds.token


def ga_report(token, body):
    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{GA_PROPERTY}:runReport"
    return safe_post(url, headers={"Authorization": f"Bearer {token}"}, body=body, label="ga4")


def ga_rows(resp):
    dims = [d["name"] for d in resp.get("dimensionHeaders") or []]
    mets = [x["name"] for x in resp.get("metricHeaders") or []]
    out = []
    for r in resp.get("rows") or []:
        row = {d: v.get("value") for d, v in zip(dims, r.get("dimensionValues") or [])}
        for mname, v in zip(mets, r.get("metricValues") or []):
            try:
                row[mname] = float(v.get("value"))
            except (TypeError, ValueError):
                row[mname] = 0.0
        out.append(row)
    return out


def pull_ga4():
    t = google_token("https://www.googleapis.com/auth/analytics.readonly")
    y = YESTERDAY.isoformat()
    core = ["activeUsers", "newUsers", "sessions", "engagedSessions", "engagementRate",
            "averageSessionDuration", "screenPageViews", "bounceRate"]
    m = {}
    tot = ga_rows(ga_report(t, {"dateRanges": [{"startDate": y, "endDate": y},
                                               {"startDate": "28daysAgo", "endDate": "yesterday"}],
                                "metrics": [{"name": n} for n in core]}))
    # with two date ranges GA adds a dateRange dimension
    for r in tot:
        key = "yesterday" if r.get("dateRange") in ("date_range_0", None) else "last_28d"
        m[key] = {n: round(r.get(n, 0), 3) for n in core}
    m["channels_28d"] = [{"channel": r.get("sessionDefaultChannelGroup"), "sessions": r.get("sessions"),
                          "users": r.get("activeUsers")} for r in ga_rows(ga_report(t, {
        "dateRanges": [{"startDate": "28daysAgo", "endDate": "yesterday"}],
        "dimensions": [{"name": "sessionDefaultChannelGroup"}],
        "metrics": [{"name": "sessions"}, {"name": "activeUsers"}],
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}], "limit": 10}))]
    m["top_pages_28d"] = [{"page": r.get("pagePath"), "views": r.get("screenPageViews"),
                           "avg_engagement_s": round(r.get("userEngagementDuration", 0) / max(r.get("activeUsers", 1), 1), 1)}
                          for r in ga_rows(ga_report(t, {
                              "dateRanges": [{"startDate": "28daysAgo", "endDate": "yesterday"}],
                              "dimensions": [{"name": "pagePath"}],
                              "metrics": [{"name": "screenPageViews"}, {"name": "activeUsers"},
                                          {"name": "userEngagementDuration"}],
                              "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}], "limit": 10}))]
    m["events_28d"] = {r.get("eventName"): r.get("eventCount") for r in ga_rows(ga_report(t, {
        "dateRanges": [{"startDate": "28daysAgo", "endDate": "yesterday"}],
        "dimensions": [{"name": "eventName"}], "metrics": [{"name": "eventCount"}], "limit": 25}))}
    m["devices_28d"] = {r.get("deviceCategory"): r.get("sessions") for r in ga_rows(ga_report(t, {
        "dateRanges": [{"startDate": "28daysAgo", "endDate": "yesterday"}],
        "dimensions": [{"name": "deviceCategory"}], "metrics": [{"name": "sessions"}]}))}
    m["cities_28d"] = [{"city": r.get("city"), "users": r.get("activeUsers")} for r in ga_rows(ga_report(t, {
        "dateRanges": [{"startDate": "28daysAgo", "endDate": "yesterday"}],
        "dimensions": [{"name": "city"}], "metrics": [{"name": "activeUsers"}],
        "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}], "limit": 10}))]
    return m


def pull_gsc():
    t = google_token("https://www.googleapis.com/auth/webmasters.readonly")
    h = {"Authorization": f"Bearer {t}"}
    end = TODAY - timedelta(days=3)          # Search Console runs ~2-3 days behind
    start7, start28 = end - timedelta(days=6), end - timedelta(days=27)
    last_err = None
    for site in GSC_SITES:
        base = "https://searchconsole.googleapis.com/webmasters/v3/sites/" + requests.utils.quote(site, safe="")
        try:
            def q(body):
                return safe_post(base + "/searchAnalytics/query", headers=h, body=body, label="gsc")
            tot7 = q({"startDate": start7.isoformat(), "endDate": end.isoformat()}).get("rows") or []
            tot28 = q({"startDate": start28.isoformat(), "endDate": end.isoformat()}).get("rows") or []
            queries = q({"startDate": start28.isoformat(), "endDate": end.isoformat(),
                         "dimensions": ["query"], "rowLimit": 15}).get("rows") or []
            pages = q({"startDate": start28.isoformat(), "endDate": end.isoformat(),
                       "dimensions": ["page"], "rowLimit": 10}).get("rows") or []
            def tot(rows):
                r = rows[0] if rows else {}
                return {"clicks": r.get("clicks", 0), "impressions": r.get("impressions", 0),
                        "ctr": round(r.get("ctr", 0), 4), "position": round(r.get("position", 0), 1)}
            m = {"property": site, "through": end.isoformat(), "last_7d": tot(tot7), "last_28d": tot(tot28),
                 "top_queries_28d": [{"query": r["keys"][0], "clicks": r.get("clicks"), "impressions": r.get("impressions"),
                                      "position": round(r.get("position", 0), 1)} for r in queries],
                 "top_pages_28d": [{"page": r["keys"][0], "clicks": r.get("clicks"),
                                    "impressions": r.get("impressions")} for r in pages]}
            try:
                sm = safe_get(base + "/sitemaps", headers=h, label="gsc sitemaps")
                m["sitemaps"] = [{"path": s.get("path"), "errors": s.get("errors"), "warnings": s.get("warnings"),
                                  "last_downloaded": s.get("lastDownloaded")} for s in sm.get("sitemap") or []]
            except SourceError:
                m["sitemaps"] = []
            return m
        except SourceError as e:
            last_err = e
    raise last_err or SourceError("gsc no property")


# --------------------------------------------------------------------------- Bing
def bing_date(v):
    mt = re.search(r"/Date\((\d+)", str(v or ""))
    return datetime.fromtimestamp(int(mt.group(1)) / 1000, tz=timezone.utc).date() if mt else None


def pull_bing():
    key = os.environ.get("BING_API_KEY")
    if not key:
        raise SourceError("BING_API_KEY secret missing")
    base = "https://ssl.bing.com/webmaster/api.svc/json/"
    p = {"siteUrl": SITE, "apikey": key}
    traffic = safe_get(base + "GetRankAndTrafficStats", params=p, label="bing traffic").get("d") or []
    cutoff7, cutoff28 = TODAY - timedelta(days=7), TODAY - timedelta(days=28)
    def sum_since(rows, cut):
        rs = [r for r in rows if (bing_date(r.get("Date")) or TODAY) >= cut]
        return {"clicks": sum(r.get("Clicks", 0) for r in rs), "impressions": sum(r.get("Impressions", 0) for r in rs)}
    m = {"last_7d": sum_since(traffic, cutoff7), "last_28d": sum_since(traffic, cutoff28)}
    try:
        qs = safe_get(base + "GetQueryStats", params=p, label="bing queries").get("d") or []
        agg = {}
        for r in qs:
            if (bing_date(r.get("Date")) or TODAY) >= cutoff28:
                a = agg.setdefault(r.get("Query", ""), {"clicks": 0, "impressions": 0})
                a["clicks"] += r.get("Clicks", 0)
                a["impressions"] += r.get("Impressions", 0)
        m["top_queries_28d"] = sorted(({"query": k, **v} for k, v in agg.items()),
                                      key=lambda x: -x["impressions"])[:15]
    except SourceError as e:
        m["partial_errors"] = [str(e)]
    try:
        cs = safe_get(base + "GetCrawlStats", params=p, label="bing crawl").get("d") or []
        if cs:
            last = max(cs, key=lambda r: bing_date(r.get("Date")) or TODAY - timedelta(days=999))
            m["crawl"] = {"in_index": last.get("InIndex"), "crawled_pages": last.get("CrawledPages"),
                          "crawl_errors": last.get("CrawlErrors"), "blocked_by_robots": last.get("BlockedByRobotsTxt")}
    except SourceError:
        pass
    return m


# ------------------------------------------------------------------------ Clarity
def pull_clarity():
    tok = os.environ.get("CLARITY_TOKEN")
    if not tok:
        raise SourceError("CLARITY_TOKEN secret missing")
    url = "https://www.clarity.ms/export-data/api/v1/project-live-insights"
    h = {"Authorization": f"Bearer {tok}", "Content-type": "application/json"}
    m = {"window": "last 24 hours"}
    overall = safe_get(url, headers=h, params={"numOfDays": "1"}, label="clarity")
    for block in overall or []:
        name = block.get("metricName", "")
        info = block.get("information") or []
        m[name] = info[0] if len(info) == 1 else info[:10]
    try:
        by_url = safe_get(url, headers=h, params={"numOfDays": "1", "dimension1": "URL"}, label="clarity url")
        m["by_page"] = {b.get("metricName"): (b.get("information") or [])[:10] for b in by_url or []
                        if b.get("metricName") in ("Traffic", "ScrollDepth", "RageClickCount", "DeadClickCount",
                                                   "QuickbackClick", "ScriptErrorCount", "EngagementTime")}
    except SourceError as e:
        m["partial_errors"] = [str(e)]
    return m


# ---------------------------------------------------------------------- PageSpeed
def pull_pagespeed():
    # Without a key, Google's shared free quota is often used up on GitHub's
    # servers (HTTP 429). PAGESPEED_KEY = an API key from the Unstuck Robin
    # Hood Google Cloud project, restricted to the PageSpeed Insights API.
    key = os.environ.get("PAGESPEED_KEY")
    out = {}
    for strategy in ("mobile", "desktop"):
        data = None
        params = {"url": SITE, "strategy": strategy, "category": "performance"}
        if key:
            params["key"] = key
        for attempt in range(2):
            try:
                data = safe_get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
                                params=params, label=f"pagespeed {strategy}")
                break
            except SourceError:
                if attempt:
                    raise
                time.sleep(20)
        lh = data.get("lighthouseResult") or {}
        audits = lh.get("audits") or {}
        def a(k):
            x = audits.get(k) or {}
            return {"value": x.get("numericValue"), "display": x.get("displayValue"), "score": x.get("score")}
        opps = []
        for k, x in audits.items():
            sav = ((x.get("details") or {}).get("overallSavingsMs") or 0)
            if (x.get("details") or {}).get("type") == "opportunity" and sav >= 100:
                opps.append({"fix": x.get("title"), "saves_ms": round(sav)})
        field = (data.get("loadingExperience") or {})
        fm = field.get("metrics") or {}
        out[strategy] = {
            "score": round((((lh.get("categories") or {}).get("performance") or {}).get("score") or 0) * 100),
            "lcp": a("largest-contentful-paint"), "cls": a("cumulative-layout-shift"),
            "tbt": a("total-blocking-time"), "fcp": a("first-contentful-paint"), "speed_index": a("speed-index"),
            "top_fixes": sorted(opps, key=lambda o: -o["saves_ms"])[:5],
            "real_visitors": {
                "overall": field.get("overall_category"),
                "lcp_ms": (fm.get("LARGEST_CONTENTFUL_PAINT_MS") or {}).get("percentile"),
                "inp_ms": (fm.get("INTERACTION_TO_NEXT_PAINT") or {}).get("percentile"),
                "cls_x100": (fm.get("CUMULATIVE_LAYOUT_SHIFT_SCORE") or {}).get("percentile"),
            } if fm else None,
        }
    return out


# ---------------------------------------------------------------------- Site check
def pull_site():
    m = {}
    t0 = time.time()
    try:
        r = requests.get(SITE, timeout=TIMEOUT, headers={"User-Agent": "UnstuckHomesRobinHood/1.0"})
        m["status"] = r.status_code
        m["response_ms"] = round((time.time() - t0) * 1000)
        m["up"] = r.status_code == 200
        html = r.text
        m["ga4_tag_present"] = GA_ID in html
        m["clarity_tag_present"] = CLARITY_ID in html
        m["html_kb"] = round(len(r.content) / 1024, 1)
    except requests.RequestException as e:
        m.update({"up": False, "status": None, "error": type(e).__name__})
    for path in ("robots.txt", "sitemap.xml"):
        try:
            rr = requests.get(SITE + path, timeout=TIMEOUT)
            m[path.replace(".", "_") + "_ok"] = rr.status_code == 200
            if path == "sitemap.xml" and rr.status_code == 200:
                m["sitemap_urls"] = rr.text.count("<loc>")
        except requests.RequestException:
            m[path.replace(".", "_") + "_ok"] = False
    return m


# ------------------------------------------------------------------------- store
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""create table if not exists robin_hood_daily (
                         run_date date not null, source text not null, ok boolean not null,
                         metrics jsonb, error text, pulled_at timestamptz not null default now(),
                         primary key (run_date, source))""")
        # private: no anon/public access through the Supabase API
        cur.execute("alter table robin_hood_daily enable row level security")


SOURCES = [("site", pull_site), ("ghl", pull_ghl), ("ga4", pull_ga4), ("gsc", pull_gsc),
           ("bing", pull_bing), ("clarity", pull_clarity), ("pagespeed", pull_pagespeed)]


def main():
    results = []
    for name, fn in SOURCES:
        try:
            metrics, ok, err = fn(), True, None
        except SourceError as e:
            metrics, ok, err = None, False, str(e)
        except Exception as e:  # unexpected bug: log type only
            metrics, ok, err = None, False, f"{name} unexpected {type(e).__name__}"
        results.append((name, ok, metrics, err))
        print(f"[robin hood] {name}: {'ok' if ok else 'FAILED - ' + err}")

    url = os.environ.get("DATABASE_URL")
    if not url:
        print("[robin hood] DATABASE_URL missing; nothing saved")
        sys.exit(1)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    ensure_table(conn)
    with conn.cursor() as cur:
        for name, ok, metrics, err in results:
            cur.execute("""insert into robin_hood_daily (run_date, source, ok, metrics, error, pulled_at)
                           values (%s, %s, %s, %s, %s, now())
                           on conflict (run_date, source) do update
                           set ok = excluded.ok, metrics = excluded.metrics, error = excluded.error,
                               pulled_at = now()""",
                        (TODAY, name, ok, psycopg2.extras.Json(metrics) if metrics is not None else None, err))
    conn.close()
    good = sum(1 for r in results if r[1])
    print(f"[robin hood] saved {len(results)} sources for {TODAY} ({good} ok, {len(results) - good} failed)")


if __name__ == "__main__":
    main()
