# app.py - NEWS BIAS TERMINAL v2 - Clean Redesign
# Auto-refresh, Bloomberg-style, Progress bars, Fixed USD detection

import os, json, time, re, hashlib, math, hmac, calendar as pycalendar, threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
import feedparser, psycopg2, socket
from psycopg2 import extras
from concurrent.futures import ThreadPoolExecutor, as_completed

socket.setdefaulttimeout(float(os.environ.get("HTTP_TIMEOUT", "12")))

try:
    from psycopg2.pool import ThreadedConnectionPool
except: ThreadedConnectionPool = None

try:
    from psycopg2.pool import PoolError
except:
    class PoolError(Exception): pass

try: import requests
except: requests = None

from fastapi import FastAPI, Header
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

ASSETS = ["XAU", "US500", "WTI"]
GATE_PROFILE = os.environ.get("GATE_PROFILE", "STRICT").strip().upper()
if GATE_PROFILE not in ("STRICT", "MODERATE"): GATE_PROFILE = "STRICT"

GATE_THRESHOLDS = {
    "STRICT": {"quality_v2_min": 55, "conflict_max": 0.55, "min_opp_flip_dist": 0.35, "neutral_allow": False,
               "event_mode_block": True, "event_override_quality": 70, "event_override_conflict": 0.45},
    "MODERATE": {"quality_v2_min": 42, "conflict_max": 0.70, "min_opp_flip_dist": 0.20, "neutral_allow": True,
                 "event_mode_block": False, "event_override_quality": 60, "event_override_conflict": 0.60}
}

HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC
BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

EVENT_CFG = {"enabled": True, "lookahead_hours": float(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
             "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")), "max_upcoming": 12}

ALERT_CFG = {"enabled": True, "q2_drop": 12, "conflict_spike": 0.18, "feeds_degraded_ratio": 0.80}

FRED_CFG = {"enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
            "api_key": os.environ.get("FRED_API_KEY", "").strip(), "window_days": 120}
FRED_SERIES = {"DGS10": {}, "DFII10": {}, "T10YIE": {}, "DTWEXBGS": {}, "VIXCLS": {}, "BAA10Y": {}}

RUN_MODE_DEFAULT = os.environ.get("RUN_MODE_DEFAULT", "quick").strip().lower()
RUN_MAX_SEC = float(os.environ.get("RUN_MAX_SEC", "10"))
RUN_LOCK_TIMEOUT_SEC = float(os.environ.get("RUN_LOCK_TIMEOUT_SEC", "25"))
RSS_LIMIT_QUICK, RSS_LIMIT_FULL = 18, 40
CAL_LIMIT_QUICK, CAL_LIMIT_FULL = 120, 250
FEEDS_HEALTH_TTL_SEC = 90
CAL_INGEST_TTL_SEC = 300
RSS_PARSE_WORKERS = 8
FRED_ON_RUN = os.environ.get("FRED_ON_RUN", "1").strip() == "1"

_PIPELINE_LOCK = threading.Lock()
_FEEDS_HEALTH_CACHE = None
_FEEDS_HEALTH_TS = 0

def get_run_tokens():
    rt = os.environ.get("RUN_TOKEN", "").strip()
    rts = os.environ.get("RUN_TOKENS", "").strip()
    toks = [rt] if rt else []
    if rts: toks.extend([x.strip() for x in rts.split(",") if x.strip()])
    return list(dict.fromkeys(toks))

MYFXBOOK_IFRAME = "https://widget.myfxbook.com/widget/calendar.html?lang=en&impacts=0,1,2,3&symbols=USD"

TV_SYMBOLS = [{"proName": "ICMARKETS:XAUUSD", "title": "XAUUSD"}, {"proName": "ICMARKETS:EURUSD", "title": "EURUSD"},
              {"proName": "ICMARKETS:US500", "title": "US500"}, {"proName": "ICMARKETS:XTIUSD", "title": "WTI"},
              {"proName": "ICMARKETS:USDJPY", "title": "USDJPY"}]

RSS_FEEDS = {
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/news_release.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",
    "OILPRICE": "https://oilprice.com/rss/main",
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "MARKETWATCH_TOP": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REALTIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "INV_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INV_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "TRUMP_HEADLINES": "https://rss.politico.com/donald-trump.xml",
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    "MYFX_CAL": "https://www.myfxbook.com/rss/forex-economic-calendar-events",
    "MYFX_NEWS": "https://www.myfxbook.com/rss/latest-forex-news",
    "NASDAQ_STOCKS": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
    "WSJ_MARKETS_MAIN": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
}

CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR", "MYFX_CAL"}
SOURCE_WEIGHT = {"FED": 3.0, "BLS": 3.0, "BEA": 2.8, "FXSTREET_NEWS": 1.4, "MARKETWATCH_TOP": 1.2,
                 "MARKETWATCH_REALTIME": 1.3, "OILPRICE": 1.2, "INV_NEWS_11": 1.0, "INV_MKT_FUND": 1.0,
                 "TRUMP_HEADLINES": 1.2, "FOREXFACTORY_CALENDAR": 0.0, "MYFX_CAL": 0.0, "MYFX_NEWS": 1.15,
                 "NASDAQ_STOCKS": 1.3, "WSJ_MARKETS_MAIN": 1.6, "FRED": 1.0}

RULES = {
    "XAU": [(r"\b(fed|fomc|powell|hawkish|rate hike)\b", -0.7, "Hawkish Fed weighs on gold"),
            (r"\b(rate cut|dovish|easing)\b", +0.6, "Dovish Fed supports gold"),
            (r"\b(cpi|inflation)\b", +0.2, "Inflation supports gold"),
            (r"\b(strong dollar|usd strengthens|yields rise)\b", -0.8, "Strong USD weighs on gold"),
            (r"\b(geopolitical|safe[- ]haven|risk aversion)\b", +0.5, "Safe-haven demand")],
    "US500": [(r"\b(earnings beat|guidance raised)\b", +0.6, "Earnings optimism"),
              (r"\b(earnings miss|guidance cut)\b", -0.7, "Earnings disappointment"),
              (r"\b(yields rise|rates higher|hawkish)\b", -0.6, "Higher yields pressure equities"),
              (r"\b(rate cut|dovish|easing)\b", +0.5, "Easing supports equities"),
              (r"\b(risk-off|selloff|crash)\b", -0.7, "Risk-off pressure"),
              (r"\b(rally|rebound|risk-on)\b", +0.3, "Risk-on momentum")],
    "WTI": [(r"\b(crude|oil|wti|brent)\b", +0.1, "Oil headlines"),
            (r"\b(opec|output cut|production cut)\b", +0.8, "Supply cuts support oil"),
            (r"\b(inventories rise|inventory build)\b", -0.8, "Inventory build"),
            (r"\b(inventories fall|inventory draw)\b", +0.8, "Inventory draw supports oil"),
            (r"\b(disruption|outage|sanctions)\b", +0.7, "Supply disruption")]
}

# DB
_DB_POOL = None
_DB_READY = False

def _make_dsn():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if db_url: return db_url
    return f"host={os.environ.get('PGHOST','localhost')} port={os.environ.get('PGPORT','5432')} dbname={os.environ.get('PGDATABASE','postgres')} user={os.environ.get('PGUSER','postgres')} password={os.environ.get('PGPASSWORD','')} connect_timeout=5"

def _get_pool():
    global _DB_POOL
    if _DB_POOL: return _DB_POOL
    if not ThreadedConnectionPool: return None
    try: _DB_POOL = ThreadedConnectionPool(1, 10, _make_dsn())
    except: pass
    return _DB_POOL

def db_conn():
    pool = _get_pool()
    if pool:
        try:
            c = pool.getconn()
            setattr(c, "_from_pool", True)
            return c
        except: pass
    c = psycopg2.connect(_make_dsn())
    setattr(c, "_from_pool", False)
    return c

def db_put(conn):
    if not conn: return
    pool = _get_pool()
    if pool and getattr(conn, "_from_pool", False):
        try: pool.putconn(conn); return
        except: pass
    try: conn.close()
    except: pass

def db_init():
    global _DB_READY
    if _DB_READY: return
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS news_items(id BIGSERIAL PRIMARY KEY,source TEXT,title TEXT,link TEXT,published_ts BIGINT,fingerprint TEXT UNIQUE);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(published_ts DESC);")
                cur.execute("CREATE TABLE IF NOT EXISTS bias_state(id SMALLINT PRIMARY KEY DEFAULT 1,updated_ts BIGINT,payload_json TEXT);")
                cur.execute("CREATE TABLE IF NOT EXISTS fred_series(series_id TEXT,obs_date DATE,value DOUBLE PRECISION,PRIMARY KEY(series_id,obs_date));")
                cur.execute("CREATE TABLE IF NOT EXISTS econ_events(id BIGSERIAL PRIMARY KEY,source TEXT,title TEXT,country TEXT,currency TEXT,impact TEXT,event_ts BIGINT,link TEXT,fingerprint TEXT UNIQUE);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_econ_ts ON econ_events(event_ts DESC);")
                cur.execute("CREATE TABLE IF NOT EXISTS kv_state(k TEXT PRIMARY KEY,v TEXT,updated_ts BIGINT);")
        _DB_READY = True
    finally: db_put(conn)

def kv_get(key):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM kv_state WHERE k=%s;", (key,))
            row = cur.fetchone()
            return str(row[0]) if row else None
    except: return None
    finally: db_put(conn)

def kv_set(key, value):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO kv_state(k,v,updated_ts) VALUES(%s,%s,%s) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_ts=EXCLUDED.updated_ts;", (key, str(value), int(time.time())))
    finally: db_put(conn)

def save_bias(payload):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO bias_state(id,updated_ts,payload_json) VALUES(1,%s,%s) ON CONFLICT(id) DO UPDATE SET updated_ts=EXCLUDED.updated_ts,payload_json=EXCLUDED.payload_json;",
                            (int(time.time()), json.dumps(payload)))
    finally: db_put(conn)

def load_bias():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
            return json.loads(row[0]) if row else None
    except: return None
    finally: db_put(conn)

# Helpers
def fingerprint(title, link): return hashlib.sha1(f"{title}||{link}".encode()).hexdigest()
def decay_weight(age_sec): return math.exp(-LAMBDA * max(0, age_sec))
def _fresh_bucket(age): return "0-2h" if age <= 7200 else ("2-8h" if age <= 28800 else "8-24h")

def match_rules(asset, title):
    return [{"w": w, "why": why} for pat, w, why in RULES.get(asset, []) if re.search(pat, title or "", re.I)]

def _ts_from_entry(e, now):
    for k in ["published_parsed", "updated_parsed"]:
        if e.get(k):
            try: return int(pycalendar.timegm(e[k]))
            except: pass
    return now

def _impact_norm(x):
    if not x: return None
    s = str(x).upper()
    if "HI" in s: return "HIGH"
    if "ME" in s: return "MED"
    if "LO" in s: return "LOW"
    return s[:6]

def _fmt_countdown(now, future):
    d = future - now
    if d <= 0: return "now"
    return f"{int(d/60)}m" if d < 3600 else f"{d/3600:.1f}h"

# USD Detection - Enhanced
_USD_KW = re.compile(r"\b(USD|Fed|FOMC|Powell|NFP|Non.?Farm|Payroll|CPI|PPI|GDP|Retail\s*Sales|Jobless|Unemployment|ISM|PMI|Treasury|Michigan|JOLTS|ADP|Philadelphia|Empire\s*State|Durable\s*Goods|Housing|Consumer\s*Confidence)\b", re.I)
_CCY_PAT = re.compile(r"\b(USD|EUR|GBP|JPY|CHF|AUD|CAD|NZD|CNY)\b", re.I)

def _detect_ccy(title):
    if not title: return None
    m = _CCY_PAT.search(title)
    if m: return m.group(1).upper()
    if _USD_KW.search(title): return "USD"
    for pat, ccy in [(r"\b(US|U\.S\.|American)\b", "USD"), (r"\b(Euro|ECB)\b", "EUR"), (r"\b(UK|British|BoE)\b", "GBP"), (r"\b(Japan|BoJ)\b", "JPY")]:
        if re.search(pat, title, re.I): return ccy
    return None

# Feeds health
def feeds_health_live(force=False):
    global _FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS
    now = int(time.time())
    if not force and _FEEDS_HEALTH_CACHE and (now - _FEEDS_HEALTH_TS) <= FEEDS_HEALTH_TTL_SEC:
        return _FEEDS_HEALTH_CACHE
    res = {}
    for src, url in RSS_FEEDS.items():
        try:
            if src == "TRUMP_HEADLINES" and not TRUMP_ENABLED:
                res[src] = {"ok": True, "skipped": True}; continue
            d = feedparser.parse(url)
            res[src] = {"ok": True, "entries": len(d.entries or [])}
        except Exception as e:
            res[src] = {"ok": False, "error": str(e)}
    _FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS = res, now
    return res

# Ingest news
def _parse_feed(src, url, now, limit):
    if src in CALENDAR_FEEDS or (src == "TRUMP_HEADLINES" and not TRUMP_ENABLED): return []
    try: d = feedparser.parse(url)
    except: return []
    out = []
    for e in (d.entries or [])[:limit]:
        title, link = (e.get("title") or "").strip(), (e.get("link") or "").strip()
        if title and link:
            out.append((src, title, link, _ts_from_entry(e, now), fingerprint(title, link)))
    return out

def ingest_news(limit=40, max_sec=None):
    now = int(time.time())
    deadline = time.time() + max_sec if max_sec else None
    tasks = [(s, u) for s, u in RSS_FEEDS.items() if s not in CALENDAR_FEEDS and not (s == "TRUMP_HEADLINES" and not TRUMP_ENABLED)]
    batch = []
    with ThreadPoolExecutor(max_workers=RSS_PARSE_WORKERS) as ex:
        futs = [ex.submit(_parse_feed, s, u, now, limit) for s, u in tasks]
        for f in as_completed(futs, timeout=max_sec):
            if deadline and time.time() > deadline: break
            try: batch.extend(f.result(timeout=0))
            except: pass
    if not batch: return 0
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, "INSERT INTO news_items(source,title,link,published_ts,fingerprint) VALUES %s ON CONFLICT DO NOTHING;", batch)
        return len(batch)
    finally: db_put(conn)

# Ingest calendar
def ingest_calendar(limit=250):
    inserted = 0
    for src in CALENDAR_FEEDS:
        url = RSS_FEEDS.get(src)
        if not url: continue
        try: d = feedparser.parse(url)
        except: continue
        conn = db_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for e in (d.entries or [])[:limit]:
                        title = (e.get("title") or "").strip()
                        if not title: continue
                        link = (e.get("link") or "").strip()
                        event_ts = _ts_from_entry(e, int(time.time()))
                        ccy = _detect_ccy(title)
                        impact = _impact_norm(e.get("impact") or e.get("importance"))
                        fp = fingerprint(f"{src}||{title}", link or title)
                        try:
                            cur.execute("INSERT INTO econ_events(source,title,currency,impact,event_ts,link,fingerprint) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING 1;",
                                        (src, title, ccy, impact, event_ts, link, fp))
                            if cur.fetchone(): inserted += 1
                        except: pass
        finally: db_put(conn)
    return inserted

# Events
def _get_upcoming(now):
    horizon = now + int(EVENT_CFG["lookahead_hours"] * 3600)
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,link,event_ts,currency,impact FROM econ_events WHERE event_ts BETWEEN %s AND %s ORDER BY event_ts LIMIT 12;", (now, horizon))
            return [{"source": s, "title": t, "link": l, "ts": ts, "currency": _detect_ccy(t) if not c else c, "impact": _impact_norm(i)} for s, t, l, ts, c, i in cur.fetchall()]
    finally: db_put(conn)

def _event_risk(upcoming, recent_macro):
    risk = 0.6 if recent_macro else 0.0
    if not upcoming: return risk
    impacts = [x.get("impact") for x in upcoming if x.get("ts")]
    if "HIGH" in impacts: return max(risk, 1.0)
    if "MED" in impacts: return max(risk, 0.7)
    return max(risk, 0.4)

# FRED
def fred_ingest(sid, days=120):
    if not (FRED_CFG["enabled"] and FRED_CFG["api_key"] and requests): return 0
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": sid, "api_key": FRED_CFG["api_key"], "file_type": "json", "sort_order": "desc", "limit": days*2}, timeout=12)
        obs = r.json().get("observations", [])
    except: return 0
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for o in obs:
                    d, v = o.get("date"), o.get("value")
                    if d and v not in (None, ".", ""):
                        try: cur.execute("INSERT INTO fred_series VALUES(%s,%s,%s) ON CONFLICT DO UPDATE SET value=EXCLUDED.value;", (sid, d, float(v)))
                        except: pass
        return len(obs)
    finally: db_put(conn)

def fred_last(sid, n=90):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM fred_series WHERE series_id=%s AND value IS NOT NULL ORDER BY obs_date DESC LIMIT %s;", (sid, n))
            return [float(x[0]) for x in cur.fetchall()]
    finally: db_put(conn)

def compute_fred_drivers():
    out = {"XAU": [], "US500": [], "WTI": []}
    dfii = fred_last("DFII10", 90)
    dgs = fred_last("DGS10", 90)
    usd = fred_last("DTWEXBGS", 120)
    vix = fred_last("VIXCLS", 120)
    if len(dfii) >= 6:
        d = dfii[0] - dfii[5]
        out["XAU"].append({"w": -1.2 if d > 0 else 1.0, "why": "Real yields move"})
    if len(dgs) >= 6:
        d = dgs[0] - dgs[5]
        out["US500"].append({"w": -0.9 if d > 0 else 0.5, "why": "Rates pressure"})
    if len(usd) >= 6:
        d = (usd[0] - usd[5]) / max(abs(usd[5]), 1)
        out["XAU"].append({"w": -0.9 if d > 0 else 0.7, "why": "USD move"})
    if len(vix) >= 6:
        d = (vix[0] - vix[5]) / max(abs(vix[5]), 1)
        out["US500"].append({"w": -1.0 if d > 0 else 0.4, "why": "VIX regime"})
    return out

# Compute bias
def compute_bias():
    now = int(time.time())
    cutoff = now - 86400
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,link,published_ts FROM news_items WHERE published_ts>=%s ORDER BY published_ts DESC LIMIT 1200;", (cutoff,))
            rows = cur.fetchall()
    finally: db_put(conn)

    fred_ok = FRED_CFG["enabled"] and FRED_CFG["api_key"] and requests
    fred_drivers = compute_fred_drivers() if fred_ok else {"XAU": [], "US500": [], "WTI": []}

    upcoming = _get_upcoming(now)
    macro_sources = {"FED", "BLS", "BEA"}
    recent_macro = any(s in macro_sources and (now - ts) <= EVENT_CFG["recent_hours"] * 3600 for s, _, _, ts in rows)
    event_mode = EVENT_CFG["enabled"] and (recent_macro or any(x.get("ts") for x in upcoming))
    event_risk = _event_risk(upcoming, recent_macro)

    assets_out = {}
    for asset in ASSETS:
        score, contribs, freshness = 0.0, [], {"0-2h": 0, "2-8h": 0, "8-24h": 0}
        for source, title, link, ts in rows:
            age = max(0, now - ts)
            w_src, w_time = SOURCE_WEIGHT.get(source, 1.0), decay_weight(age)
            for m in match_rules(asset, title):
                contrib = m["w"] * w_src * w_time
                score += contrib
                freshness[_fresh_bucket(age)] += 1
                contribs.append({"source": source, "title": title, "contrib": round(contrib, 4), "why": m["why"], "age_min": int(age/60)})

        for d in fred_drivers.get(asset, []):
            score += d["w"]
            contribs.append({"source": "FRED", "contrib": round(d["w"], 4), "why": d["why"]})

        th = BIAS_THRESH.get(asset, 1.0)
        bias = "BULLISH" if score >= th else ("BEARISH" if score <= -th else "NEUTRAL")

        net = sum(x["contrib"] for x in contribs)
        abs_sum = sum(abs(x["contrib"]) for x in contribs)
        conflict = round(1 - abs(net) / max(abs_sum, 0.001), 4)
        src_div = len(set(x["source"] for x in contribs))

        fresh_total = sum(freshness.values())
        fresh_score = (freshness["0-2h"] + freshness["2-8h"] * 0.6) / max(1, fresh_total) if fresh_total else 0

        raw = 0.45 * min(1, abs(score)/th) + 0.20 * min(1, len(contribs)/18) + 0.10 * min(1, src_div/7) + 0.10 * fresh_score + 0.15 * (1-conflict) - 0.35 * conflict - (0.15 * event_risk if event_mode else 0)
        quality = int(max(0, min(100, round(raw * 100))))

        top3 = sorted(contribs, key=lambda x: abs(x["contrib"]), reverse=True)[:3]
        assets_out[asset] = {"bias": bias, "score": round(score, 4), "threshold": th, "quality_v2": quality,
                            "evidence_count": len(contribs), "source_diversity": src_div, "conflict_index": conflict,
                            "freshness": freshness, "top3_drivers": [{"why": x["why"]} for x in top3],
                            "why_top5": top3[:5]}

    return {"updated_utc": datetime.now(timezone.utc).isoformat(), "assets": assets_out,
            "meta": {"gate_profile": GATE_PROFILE},
            "event": {"enabled": EVENT_CFG["enabled"], "event_mode": event_mode, "event_risk": round(event_risk, 3),
                     "upcoming_events": upcoming}}

# Pipeline
def pipeline_run(mode="quick"):
    if not _PIPELINE_LOCK.acquire(timeout=RUN_LOCK_TIMEOUT_SEC):
        db_init()
        return load_bias() or {}
    try:
        db_init()
        ingest_news(RSS_LIMIT_QUICK if mode == "quick" else RSS_LIMIT_FULL, RUN_MAX_SEC if mode == "quick" else None)
        last_cal = int(kv_get("cal_ts") or 0)
        if (int(time.time()) - last_cal) >= CAL_INGEST_TTL_SEC:
            ingest_calendar(CAL_LIMIT_QUICK if mode == "quick" else CAL_LIMIT_FULL)
            kv_set("cal_ts", str(int(time.time())))
        if FRED_ON_RUN:
            for sid in FRED_SERIES: fred_ingest(sid, 120)
        payload = compute_bias()
        payload["meta"]["feeds_status"] = feeds_health_live(mode == "full")
        save_bias(payload)
        return payload
    finally: _PIPELINE_LOCK.release()

# Trade gate
def eval_gate(a, event_mode, profile):
    cfg = GATE_THRESHOLDS.get(profile, GATE_THRESHOLDS["STRICT"])
    bias, quality, conflict = a.get("bias", "NEUTRAL"), a.get("quality_v2", 0), a.get("conflict_index", 1.0)
    qmin, cmax = cfg["quality_v2_min"], cfg["conflict_max"]
    blockers, to_fix = [], []
    if bias == "NEUTRAL" and not cfg.get("neutral_allow"):
        blockers.append("No clear direction"); to_fix.append("Wait for BULLISH or BEARISH")
    if quality < qmin:
        blockers.append(f"Quality {quality} (need {qmin})"); to_fix.append(f"Need +{qmin-quality} quality")
    if conflict > cmax:
        blockers.append(f"High conflict {int(conflict*100)}%"); to_fix.append(f"Need below {int(cmax*100)}%")
    if event_mode and cfg.get("event_mode_block"):
        oq, oc = cfg["event_override_quality"], cfg["event_override_conflict"]
        if not (quality >= oq and conflict <= oc and bias != "NEUTRAL"):
            blockers.append("Macro event window"); to_fix.append("Wait for release")
    return {"ok": len(blockers) == 0, "blockers": blockers, "to_fix": to_fix, "quality_min": qmin}

# FastAPI
app = FastAPI(title="News Bias Terminal")

@app.get("/", include_in_schema=False)
def root(): return RedirectResponse("/dashboard")

@app.get("/health")
def health(): return {"ok": True, "profile": GATE_PROFILE}

@app.get("/api/data")
def api_data():
    db_init()
    return load_bias() or pipeline_run()

@app.post("/api/refresh")
def api_refresh(token: str = "", x_run_token: Optional[str] = Header(None)):
    t = (token or x_run_token or "").strip()
    toks = get_run_tokens()
    if toks and not any(hmac.compare_digest(t, a) for a in toks):
        return JSONResponse({"ok": False}, 401)
    return {"ok": True, "updated": pipeline_run().get("updated_utc")}

@app.get("/latest")
def latest(limit: int = 30):
    db_init()
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,link,published_ts FROM news_items ORDER BY published_ts DESC LIMIT %s;", (min(60, limit),))
            return {"items": [{"source": s, "title": t, "ts": ts} for s, t, _, ts in cur.fetchall()]}
    finally: db_put(conn)

@app.get("/feeds")
def feeds():
    db_init()
    status = feeds_health_live(True)
    return {"total": len(status), "ok": sum(1 for v in status.values() if v.get("ok")), "feeds": status}

@app.get("/myfx_calendar", response_class=HTMLResponse)
def myfx_cal():
    return HTMLResponse(f'<!doctype html><html><head><meta charset="utf-8"><style>html,body{{height:100%;margin:0;background:#0a0e14}}</style></head><body><iframe src="{MYFXBOOK_IFRAME}" style="width:100%;height:100%;border:0"></iframe></body></html>')


# Dashboard HTML
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias() or pipeline_run()
    assets = payload.get("assets", {})
    event = payload.get("event", {})
    meta = payload.get("meta", {})

    now_ts = int(time.time())
    event_mode = event.get("event_mode", False)
    event_risk = int(event.get("event_risk", 0) * 100)
    upcoming = event.get("upcoming_events", [])[:8]

    # Find next USD event
    next_ev = None
    for ev in upcoming:
        if ev.get("currency") == "USD" and ev.get("ts"):
            next_ev = ev; break
    if not next_ev:
        for ev in upcoming:
            if ev.get("ts"): next_ev = ev; break

    # Gates
    for sym in ASSETS:
        a = assets.get(sym, {})
        a["gate"] = eval_gate(a, event_mode, GATE_PROFILE)

    feeds_status = meta.get("feeds_status", {})
    feeds_ok = sum(1 for v in feeds_status.values() if v.get("ok"))
    feeds_total = len(feeds_status) or len(RSS_FEEDS)

    def asset_card(sym):
        a = assets.get(sym, {})
        bias = a.get("bias", "NEUTRAL")
        gate = a.get("gate", {})
        ok = gate.get("ok", False)
        quality = a.get("quality_v2", 0)
        blockers = gate.get("blockers", [])
        reason = blockers[0] if blockers else (a.get("top3_drivers", [{}])[0].get("why", "") if ok else "")
        bias_cls = "green" if bias == "BULLISH" else ("red" if bias == "BEARISH" else "gray")
        trade_cls = "green" if ok else "yellow"
        bar_cls = "green" if ok else ("yellow" if quality >= 40 else "red")
        return f'''<div class="card" onclick="openView('{sym}')">
            <div class="card-head"><span class="sym">{sym}</span>
                <div class="badges"><span class="badge {bias_cls}">{bias}</span>
                <span class="badge {trade_cls}">{"TRADE OK" if ok else "WAIT"}</span></div></div>
            <div class="bar"><div class="fill {bar_cls}" style="width:{min(100,quality)}%"></div></div>
            <div class="reason"><span>{reason[:50] or "Analysis complete"}</span><span class="arrow">→</span></div></div>'''

    next_ev_html = ""
    if next_ev:
        ccy = next_ev.get("currency") or "—"
        title = (next_ev.get("title") or "")[:50]
        impact = next_ev.get("impact") or ""
        countdown = _fmt_countdown(now_ts, next_ev["ts"]) if next_ev.get("ts") else ""
        impact_cls = "red" if impact == "HIGH" else ("yellow" if impact == "MED" else "green")
        next_ev_html = f'<span class="ccy">{ccy}</span> {title} <span class="impact {impact_cls}">{impact}</span>'
    else:
        next_ev_html = "No upcoming events"

    js_payload = json.dumps(payload).replace("</", "<\\/")
    tv_json = json.dumps(TV_SYMBOLS)
    gate_cfg = json.dumps(GATE_THRESHOLDS[GATE_PROFILE])

    return HTMLResponse(f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NEWS BIAS // TERMINAL</title>
<style>
:root{{--bg:#0a0e14;--card:#0f1419;--border:rgba(255,255,255,0.06);--text:#e6edf3;--muted:#7d8590;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff}}
*{{box-sizing:border-box}}html,body{{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:14px}}
.container{{max-width:1100px;margin:0 auto;padding:16px}}
.header{{display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-bottom:1px solid var(--border);margin-bottom:16px;flex-wrap:wrap;gap:12px}}
.logo{{font-weight:700;font-size:15px}}.logo span{{color:#39c5cf}}
.pills{{display:flex;gap:8px;flex-wrap:wrap}}
.pill{{font-size:11px;font-weight:600;padding:4px 10px;border-radius:12px;background:rgba(255,255,255,0.04);border:1px solid var(--border)}}
.pill.green{{color:var(--green);border-color:rgba(63,185,80,0.3)}}.pill.yellow{{color:var(--yellow);border-color:rgba(210,153,34,0.3)}}
.tv-wrap{{border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:16px}}
.ticker-wrap{{background:var(--card);border:1px solid var(--border);border-radius:8px;margin-bottom:16px;overflow:hidden}}
.ticker-label{{font-size:10px;font-weight:600;color:var(--muted);padding:8px 12px 4px;letter-spacing:0.5px}}
.ticker{{overflow:hidden;white-space:nowrap;padding:0 12px 10px}}
.ticker-inner{{display:inline-flex;gap:32px;animation:scroll 120s linear infinite}}
.ticker:hover .ticker-inner{{animation-play-state:paused}}
@keyframes scroll{{0%{{transform:translateX(0)}}100%{{transform:translateX(-50%)}}}}
.ticker-item{{font-size:12px;color:var(--muted)}}.ticker-item b{{color:#39c5cf;margin-right:6px}}
.next-event{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px 16px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}}
.next-event-label{{font-size:10px;color:var(--muted);font-weight:600}}.next-event-main{{font-size:13px;margin-top:2px}}
.ccy{{background:rgba(88,166,255,0.15);color:var(--blue);padding:2px 6px;border-radius:4px;font-weight:600;font-size:11px;margin-right:6px}}
.impact{{font-size:10px;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:8px}}
.impact.red{{background:rgba(248,81,73,0.15);color:var(--red)}}.impact.yellow{{background:rgba(210,153,34,0.15);color:var(--yellow)}}.impact.green{{background:rgba(63,185,80,0.15);color:var(--green)}}
.next-event-time{{font-size:12px;color:var(--muted)}}
.cards{{display:flex;flex-direction:column;gap:12px}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;cursor:pointer;transition:border-color 0.15s}}
.card:hover{{border-color:rgba(255,255,255,0.12)}}
.card-head{{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}}
.sym{{font-size:18px;font-weight:700}}.badges{{display:flex;gap:6px}}
.badge{{font-size:11px;font-weight:600;padding:4px 10px;border-radius:6px}}
.badge.green{{background:rgba(63,185,80,0.12);color:var(--green)}}.badge.red{{background:rgba(248,81,73,0.12);color:var(--red)}}
.badge.yellow{{background:rgba(210,153,34,0.12);color:var(--yellow)}}.badge.gray{{background:rgba(125,133,144,0.12);color:var(--muted)}}
.bar{{height:6px;background:rgba(255,255,255,0.06);border-radius:3px;overflow:hidden;margin-bottom:10px}}
.fill{{height:100%;border-radius:3px}}.fill.green{{background:var(--green)}}.fill.yellow{{background:var(--yellow)}}.fill.red{{background:var(--red)}}
.reason{{font-size:12px;color:var(--muted);display:flex;justify-content:space-between}}.arrow{{color:var(--muted)}}
.btn-row{{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap}}
.btn{{background:var(--card);border:1px solid var(--border);color:var(--text);padding:10px 16px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer}}
.btn:hover{{background:rgba(255,255,255,0.04)}}
.footer{{margin-top:24px;padding-top:16px;border-top:1px solid var(--border);font-size:11px;color:var(--muted);display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px}}
.modal{{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:1000;padding:16px;overflow-y:auto}}
.modal.open{{display:flex;justify-content:center;align-items:flex-start;padding-top:5vh}}
.modal-box{{background:var(--card);border:1px solid var(--border);border-radius:12px;max-width:600px;width:100%;max-height:85vh;overflow-y:auto}}
.modal-header{{display:flex;justify-content:space-between;align-items:center;padding:16px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--card)}}
.modal-title{{font-size:16px;font-weight:700}}.modal-close{{background:none;border:none;color:var(--muted);font-size:24px;cursor:pointer}}
.modal-body{{padding:16px}}
.section{{margin-bottom:20px}}.section-label{{font-size:10px;font-weight:600;color:var(--muted);letter-spacing:0.5px;margin-bottom:8px}}
.bar-row{{display:flex;align-items:center;gap:12px;margin-bottom:8px}}.bar-label{{width:100px;font-size:12px;color:var(--muted)}}
.bar-track{{flex:1;height:8px;background:rgba(255,255,255,0.06);border-radius:4px;overflow:hidden}}.bar-fill{{height:100%;border-radius:4px}}
.bar-val{{width:60px;text-align:right;font-size:12px;font-weight:600}}
.blocker{{font-size:12px;padding:8px 12px;border-radius:6px;background:rgba(248,81,73,0.08);color:var(--red);border-left:3px solid var(--red);margin-bottom:6px}}
.fix{{font-size:12px;padding:8px 12px;border-radius:6px;background:rgba(63,185,80,0.08);color:var(--green);border-left:3px solid var(--green);margin-bottom:6px}}
.driver{{font-size:12px;padding:8px 12px;background:rgba(255,255,255,0.02);border-radius:6px;border:1px solid var(--border);margin-bottom:6px}}
.feed-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px}}
.feed-item{{font-size:11px;padding:8px;border-radius:6px;background:rgba(255,255,255,0.02);border:1px solid var(--border);display:flex;justify-content:space-between}}
.feed-ok{{color:var(--green)}}.feed-bad{{color:var(--red)}}
</style>
</head>
<body>
<div class="container">
<div class="header">
<div class="logo"><span>NEWS BIAS</span> // TERMINAL</div>
<div class="pills">
<span class="pill {"yellow" if event_mode else "green"}">RISK {event_risk}%</span>
<span class="pill green" onclick="openFeeds()" style="cursor:pointer">FEEDS {feeds_ok}/{feeds_total}</span>
<span class="pill" id="updateTime">{payload.get("updated_utc","")[:19].replace("T"," ")}</span>
</div>
</div>

<div class="tv-wrap">
<div class="tradingview-widget-container"><div class="tradingview-widget-container__widget"></div>
<script src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
{{"symbols":{tv_json},"showSymbolLogo":true,"isTransparent":true,"displayMode":"adaptive","colorTheme":"dark"}}
</script></div></div>

<div class="ticker-wrap">
<div class="ticker-label">HEADLINES</div>
<div class="ticker"><div class="ticker-inner" id="newsTicker">Loading...</div></div>
</div>

<div class="next-event">
<div><div class="next-event-label">NEXT EVENT</div><div class="next-event-main">{next_ev_html}</div></div>
<div class="next-event-time">{f"in {_fmt_countdown(now_ts, next_ev['ts'])}" if next_ev and next_ev.get("ts") else ""}</div>
</div>

<div class="cards">{asset_card("XAU")}{asset_card("US500")}{asset_card("WTI")}</div>

<div class="btn-row">
<button class="btn" onclick="openMyfx()">📅 Economic Calendar</button>
<button class="btn" onclick="openFeeds()">📡 Feeds Status</button>
</div>

<div class="footer"><span>Auto-refresh: 30s • Profile: {GATE_PROFILE}</span><span>Keys: 1-3 view • Esc close</span></div>
</div>

<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
<div class="modal-box">
<div class="modal-header"><div class="modal-title" id="modalTitle">View</div><button class="modal-close" onclick="closeModal()">×</button></div>
<div class="modal-body" id="modalBody"></div>
</div>
</div>

<script>
const P={js_payload};
const CFG={gate_cfg};

setInterval(async()=>{{try{{const r=await fetch("/api/data");const d=await r.json();if(d.updated_utc)document.getElementById("updateTime").textContent=d.updated_utc.slice(0,19).replace("T"," ")}}catch{{}}}},30000);

async function loadNews(){{try{{const r=await fetch("/latest?limit=20");const d=await r.json();const h=(d.items||[]).map(i=>`<span class="ticker-item"><b>${{esc(i.source)}}</b>${{esc(i.title)}}</span>`).join("");document.getElementById("newsTicker").innerHTML=h+h}}catch{{}}}}
loadNews();setInterval(loadNews,60000);

function esc(s){{return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}}
function openModal(t,h){{document.getElementById("modalTitle").textContent=t;document.getElementById("modalBody").innerHTML=h;document.getElementById("modal").classList.add("open")}}
function closeModal(){{document.getElementById("modal").classList.remove("open")}}

function openView(sym){{
const a=P.assets?.[sym]||{{}};const ev=P.event||{{}};
const q=a.quality_v2||0,qmin=CFG.quality_v2_min,qneed=Math.max(0,qmin-q);
const conflict=Math.round((a.conflict_index||0)*100),agree=100-conflict;
const risk=Math.round((ev.event_risk||0)*100);
const fr=a.freshness||{{}},ft=(fr["0-2h"]||0)+(fr["2-8h"]||0)+(fr["8-24h"]||0);
const fresh=ft?Math.round(((fr["0-2h"]||0)+(fr["2-8h"]||0)*0.6)/ft*100):0;

const bias=a.bias||"NEUTRAL";
const blockers=[],toFix=[];
if(bias==="NEUTRAL"&&!CFG.neutral_allow){{blockers.push("No clear direction");toFix.push("Wait for BULLISH or BEARISH")}}
if(q<qmin){{blockers.push(`Quality ${{q}} (need ${{qmin}})`);toFix.push(`Need +${{qneed}} quality`)}}
if((a.conflict_index||0)>CFG.conflict_max){{blockers.push(`High conflict ${{conflict}}%`);toFix.push(`Need below ${{Math.round(CFG.conflict_max*100)}}%`)}}
if(ev.event_mode&&CFG.event_mode_block){{if(!(q>=CFG.event_override_quality&&(a.conflict_index||0)<=CFG.event_override_conflict&&bias!=="NEUTRAL")){{blockers.push("Macro event window");toFix.push("Wait for release")}}}}

const drivers=(a.top3_drivers||[]).slice(0,3);

let h=`<div class="section"><div class="section-label">SIGNAL HEALTH</div>
${{barRow("Quality",q,100,q>=qmin?"var(--green)":"var(--yellow)",qneed>0?`need +${{qneed}}`:"")}}
${{barRow("Agreement",agree,100,agree>=50?"var(--green)":"var(--yellow)","")}}
${{barRow("Event Risk",risk,100,risk>50?"var(--red)":"var(--green)",risk>50?"macro window":"low")}}
${{barRow("Freshness",fresh,100,fresh>=50?"var(--green)":"var(--yellow)","")}}
</div>`;

if(blockers.length)h+=`<div class="section"><div class="section-label">BLOCKERS</div>${{blockers.map(b=>`<div class="blocker">${{esc(b)}}</div>`).join("")}}</div>`;
if(toFix.length)h+=`<div class="section"><div class="section-label">TO UNBLOCK</div>${{toFix.map(f=>`<div class="fix">${{esc(f)}}</div>`).join("")}}</div>`;
if(drivers.length)h+=`<div class="section"><div class="section-label">TOP DRIVERS</div>${{drivers.map((d,i)=>`<div class="driver">${{i+1}}. ${{esc(d.why||"")}}</div>`).join("")}}</div>`;

openModal(`VIEW ${{sym}}`,h);
}}

function barRow(label,val,max,color,note){{
const pct=Math.min(100,Math.max(0,val/max*100));
return `<div class="bar-row"><div class="bar-label">${{label}}</div><div class="bar-track"><div class="bar-fill" style="width:${{pct}}%;background:${{color}}"></div></div><div class="bar-val">${{val}}${{note?` <span style="color:var(--muted);font-size:10px">${{note}}</span>`:""}}</div></div>`;
}}

function openFeeds(){{
const f=P.meta?.feeds_status||{{}};
const items=Object.entries(f).sort((a,b)=>a[0].localeCompare(b[0]));
let h=`<div class="feed-grid">${{items.map(([n,i])=>`<div class="feed-item"><span>${{esc(n)}}</span><span class="${{i.ok?"feed-ok":"feed-bad"}}">${{i.ok?"✓":"✗"}}</span></div>`).join("")}}</div>`;
openModal("FEEDS STATUS",h);
}}

function openMyfx(){{
openModal("ECONOMIC CALENDAR",`<div style="height:70vh;border-radius:8px;overflow:hidden"><iframe src="/myfx_calendar" style="width:100%;height:100%;border:0;filter:invert(1) hue-rotate(180deg) contrast(0.9)"></iframe></div>`);
}}

document.addEventListener("keydown",e=>{{
if(e.key==="Escape")closeModal();
if(e.key==="1")openView("XAU");
if(e.key==="2")openView("US500");
if(e.key==="3")openView("WTI");
}});
</script>
</body>
</html>''')
