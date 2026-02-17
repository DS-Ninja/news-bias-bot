# app.py - NEWS BIAS TERMINAL v3 - Bloomberg Clean Style
# Simple output: BIAS + TRADE/WAIT + human-readable reasons

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

try: import requests
except: requests = None

# === AUTO-SCHEDULER ===
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    _APSCHEDULER_AVAILABLE = True
except ImportError:
    _APSCHEDULER_AVAILABLE = False

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

FRED_CFG = {"enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
            "api_key": os.environ.get("FRED_API_KEY", "").strip(), "window_days": 120}
FRED_SERIES = {"DGS10": {}, "DFII10": {}, "T10YIE": {}, "DTWEXBGS": {}, "VIXCLS": {}, "BAA10Y": {}}

RUN_MAX_SEC = float(os.environ.get("RUN_MAX_SEC", "10"))
RUN_LOCK_TIMEOUT_SEC = 25
RSS_LIMIT, CAL_LIMIT = 18, 120
FEEDS_HEALTH_TTL_SEC, CAL_INGEST_TTL_SEC = 90, 300
RSS_PARSE_WORKERS = 8
FRED_ON_RUN = os.environ.get("FRED_ON_RUN", "1").strip() == "1"

# Auto-refresh interval in minutes (change this to adjust frequency)
AUTO_REFRESH_MINUTES = int(os.environ.get("AUTO_REFRESH_MINUTES", "15"))

_PIPELINE_LOCK = threading.Lock()
_FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS = None, 0
_CONN_FROM_POOL = set()

def get_run_tokens():
    rt = os.environ.get("RUN_TOKEN", "").strip()
    rts = os.environ.get("RUN_TOKENS", "").strip()
    toks = [rt] if rt else []
    if rts: toks.extend([x.strip() for x in rts.split(",") if x.strip()])
    return list(dict.fromkeys(toks))

MYFXBOOK_IFRAME = "https://widget.myfxbook.com/widget/calendar.html?lang=en&impacts=0,1,2,3&symbols=USD"
TV_SYMBOLS = [{"proName": "ICMARKETS:XAUUSD", "title": "XAU"}, {"proName": "ICMARKETS:EURUSD", "title": "EUR"},
              {"proName": "ICMARKETS:US500", "title": "US500"}, {"proName": "ICMARKETS:XTIUSD", "title": "WTI"},
              {"proName": "ICMARKETS:USDJPY", "title": "JPY"}]

RSS_FEEDS = {
    # === EXISTING FEEDS ===
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/news_release.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",
    "OILPRICE": "https://oilprice.com/rss/main",
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "MARKETWATCH_TOP": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REALTIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "INV_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INV_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INV_COMMOD": "https://www.investing.com/rss/commodities_Technical.rss",
    "TRUMP_HEADLINES": "https://rss.politico.com/donald-trump.xml",
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    "MYFX_CAL": "https://www.myfxbook.com/rss/forex-economic-calendar-events",
    "MYFX_NEWS": "https://www.myfxbook.com/rss/latest-forex-news",
    "NASDAQ_STOCKS": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
    "WSJ_MARKETS": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",

    # === GOLD / PRECIOUS METALS ===
    "KITCO_NEWS": "https://www.kitco.com/rss/news.xml",
    "KITCO_GOLD": "https://www.kitco.com/rss/gold.xml",
    "MINING_COM": "https://www.mining.com/feed/",
    "GOLD_PRICE_ORG": "https://goldprice.org/rss",
    "BULLION_VAULT": "https://www.bullionvault.com/gold-news/rss",
    "SPROTT_GOLD": "https://www.sprott.com/rss/insights/",
    "GOLD_SWISS": "https://www.gold.org/rss",

    # === STOCKS / EQUITIES ===
    "CNBC_TOP": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "CNBC_STOCKS": "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "CNBC_EARNINGS": "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "YAHOO_FIN": "https://finance.yahoo.com/news/rssindex",
    "SEEKINGALPHA": "https://seekingalpha.com/market_currents.xml",
    "SEEKINGALPHA_TOP": "https://seekingalpha.com/feed.xml",
    "BENZINGA": "https://www.benzinga.com/feed",
    "BENZINGA_MARKETS": "https://www.benzinga.com/markets/feed",
    "ZACKS_COMMENTARY": "https://www.zacks.com/commentary/rss",
    "MOTLEY_FOOL": "https://www.fool.com/feeds/index.aspx",
    "THESTREET": "https://www.thestreet.com/rss",
    "BARRONS": "https://feeds.content.dowjones.io/public/rss/barrons",
    "IBD": "https://www.investors.com/feed/",
    "STOCKTWITS": "https://api.stocktwits.com/api/2/streams/trending.json",

    # === OIL / ENERGY ===
    "OILPRICE_ENERGY": "https://oilprice.com/rss/energy",
    "OILPRICE_OIL": "https://oilprice.com/rss/oil",
    "OILPRICE_GAS": "https://oilprice.com/rss/natural-gas",
    "RIGZONE": "https://www.rigzone.com/news/rss/rigzone_news.aspx",
    "EIA_TODAY": "https://www.eia.gov/rss/todayinenergy.xml",
    "WORLDOIL": "https://www.worldoil.com/rss/news",
    "OFFSHORE_MAG": "https://www.offshore-mag.com/rss",
    "ENERGY_VOICE": "https://www.energyvoice.com/feed/",
    "ARGUS_MEDIA": "https://www.argusmedia.com/en/rss-feeds/news",
    "PLATTS": "https://www.spglobal.com/commodityinsights/en/rss-feed/oil",
    "OIL_GAS_JOURNAL": "https://www.ogj.com/rss",

    # === FOREX / CURRENCIES ===
    "FOREXLIVE": "https://www.forexlive.com/feed/news",
    "DAILYFX_NEWS": "https://www.dailyfx.com/feeds/market-news",
    "DAILYFX_TOP": "https://www.dailyfx.com/feeds/top-stories",
    "FX_EMPIRE": "https://www.fxempire.com/news/feed",
    "ACTIONFOREX": "https://www.actionforex.com/feed/",
    "EARNFOREX": "https://www.earnforex.com/rss/news/",
    "FXLEADERS": "https://www.fxleaders.com/feed/",
    "BABYPIPS": "https://www.babypips.com/feed",

    # === CENTRAL BANKS ===
    "FED_SPEECHES": "https://www.federalreserve.gov/feeds/speeches.xml",
    "FED_TESTIMONY": "https://www.federalreserve.gov/feeds/testimony.xml",
    "ECB_PRESS": "https://www.ecb.europa.eu/rss/press.html",
    "BOE_NEWS": "https://www.bankofengland.co.uk/rss/news",
    "BOJ_ANNOUNCE": "https://www.boj.or.jp/en/announcements/release_2024/rel240101a.htm/rss.xml",
    "SNB_NEWS": "https://www.snb.ch/en/mmr/reference/rss_snb/source/rss_snb.en.xml",
    "RBA_MEDIA": "https://www.rba.gov.au/rss/rss-cb-media-releases.xml",
    "BOC_ANNOUNCE": "https://www.bankofcanada.ca/feed/",

    # === MACRO / ECONOMIC ===
    "TRADINGECONOMICS": "https://tradingeconomics.com/rss/news.aspx",
    "ECONODAY": "https://www.econoday.com/rss/",
    "BRIEFING": "https://www.briefing.com/rss",
    "CALCWATCH": "https://www.calcalistech.com/ctech/home.rss",
    "TREASURY_PRESS": "https://home.treasury.gov/news/press-releases/rss.xml",
    "IMF_NEWS": "https://www.imf.org/en/News/rss",
    "WORLDBANK": "https://www.worldbank.org/en/news/rss.xml",
    "BIS_PRESS": "https://www.bis.org/doclist/press.rss",

    # === GENERAL FINANCIAL NEWS ===
    "REUTERS_BIZ": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "REUTERS_MARKETS": "https://www.reutersagency.com/feed/?best-topics=markets&post_type=best",
    "AP_BUSINESS": "https://rsshub.app/apnews/topics/business",
    "BBC_BUSINESS": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "FT_MARKETS": "https://www.ft.com/markets?format=rss",
    "GUARDIAN_BIZ": "https://www.theguardian.com/uk/business/rss",
    "ECONOMIST": "https://www.economist.com/finance-and-economics/rss.xml",
    "FORTUNE": "https://fortune.com/feed/",
    "FORBES_MARKETS": "https://www.forbes.com/markets/feed/",

    # === GEOPOLITICS (affects safe-haven) ===
    "REUTERS_WORLD": "https://www.reutersagency.com/feed/?best-topics=world&post_type=best",
    "AP_WORLD": "https://rsshub.app/apnews/topics/world-news",
    "BBC_WORLD": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "ALJAZEERA": "https://www.aljazeera.com/xml/rss/all.xml",
    "POLITICO_ECON": "https://rss.politico.com/economy.xml",
    "POLITICO_FIN": "https://rss.politico.com/finance.xml",

    # === COMMODITIES GENERAL ===
    "INV_COMMODITIES": "https://www.investing.com/rss/commodities.rss",
    "COMMODITY_NEWS": "https://www.commodities-now.com/rss",
    "METAL_BULLETIN": "https://www.metalbulletin.com/rss",
    "FASTMARKETS": "https://www.fastmarkets.com/rss",
    "MINING_WEEKLY": "https://www.miningweekly.com/rss",

    # === CRYPTO (can correlate with gold) ===
    "COINDESK": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "COINTELEGRAPH": "https://cointelegraph.com/rss",

    # === ADDITIONAL INVESTING.COM ===
    "INV_STOCK_NEWS": "https://www.investing.com/rss/stock_stock_news.rss",
    "INV_FOREX": "https://www.investing.com/rss/forex.rss",
    "INV_CENTRAL_BANKS": "https://www.investing.com/rss/news_287.rss",
    "INV_ECONOMY": "https://www.investing.com/rss/news_14.rss",
    "INV_STOCK_MARKETS": "https://www.investing.com/rss/news_25.rss",
}

CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR", "MYFX_CAL"}
SOURCE_WEIGHT = {
    # === HIGH PRIORITY (Official / Institutional) ===
    "FED": 3.0, "FED_SPEECHES": 2.8, "FED_TESTIMONY": 2.8,
    "BLS": 3.0, "BEA": 2.8, "TREASURY_PRESS": 2.5,
    "ECB_PRESS": 2.5, "BOE_NEWS": 2.4, "BOJ_ANNOUNCE": 2.3,
    "SNB_NEWS": 2.2, "RBA_MEDIA": 2.2, "BOC_ANNOUNCE": 2.2,
    "IMF_NEWS": 2.0, "WORLDBANK": 1.8, "BIS_PRESS": 2.0,

    # === GOLD SPECIALIZED ===
    "KITCO_NEWS": 2.0, "KITCO_GOLD": 2.2,
    "MINING_COM": 1.6, "GOLD_PRICE_ORG": 1.5,
    "BULLION_VAULT": 1.5, "SPROTT_GOLD": 1.4, "GOLD_SWISS": 1.6,

    # === OIL SPECIALIZED ===
    "OILPRICE": 1.8, "OILPRICE_ENERGY": 1.8, "OILPRICE_OIL": 2.0, "OILPRICE_GAS": 1.5,
    "RIGZONE": 1.7, "EIA_TODAY": 2.2, "WORLDOIL": 1.5,
    "OFFSHORE_MAG": 1.3, "ENERGY_VOICE": 1.4,
    "ARGUS_MEDIA": 1.8, "PLATTS": 2.0, "OIL_GAS_JOURNAL": 1.6,

    # === MAJOR FINANCIAL MEDIA ===
    "REUTERS_BIZ": 2.0, "REUTERS_MARKETS": 2.0, "REUTERS_WORLD": 1.5,
    "WSJ_MARKETS": 2.0, "BARRONS": 1.8, "FT_MARKETS": 1.9,
    "CNBC_TOP": 1.6, "CNBC_STOCKS": 1.7, "CNBC_EARNINGS": 1.8,
    "MARKETWATCH_TOP": 1.5, "MARKETWATCH_REALTIME": 1.6,
    "BBC_BUSINESS": 1.4, "ECONOMIST": 1.6, "FORTUNE": 1.3, "FORBES_MARKETS": 1.3,
    "AP_BUSINESS": 1.5, "AP_WORLD": 1.3, "BBC_WORLD": 1.3,

    # === FOREX SPECIALIZED ===
    "FXSTREET_NEWS": 1.5, "FXSTREET_ANALYSIS": 1.4,
    "FOREXLIVE": 1.6, "DAILYFX_NEWS": 1.5, "DAILYFX_TOP": 1.5,
    "FX_EMPIRE": 1.3, "ACTIONFOREX": 1.2, "EARNFOREX": 1.1,
    "FXLEADERS": 1.2, "BABYPIPS": 1.0,

    # === STOCK ANALYSIS ===
    "SEEKINGALPHA": 1.5, "SEEKINGALPHA_TOP": 1.4,
    "BENZINGA": 1.4, "BENZINGA_MARKETS": 1.5,
    "ZACKS_COMMENTARY": 1.4, "MOTLEY_FOOL": 1.2,
    "THESTREET": 1.3, "IBD": 1.5, "YAHOO_FIN": 1.3,
    "NASDAQ_STOCKS": 1.4,

    # === INVESTING.COM ===
    "INV_NEWS_11": 1.2, "INV_MKT_FUND": 1.3, "INV_COMMOD": 1.3,
    "INV_COMMODITIES": 1.3, "INV_STOCK_NEWS": 1.2, "INV_FOREX": 1.2,
    "INV_CENTRAL_BANKS": 1.5, "INV_ECONOMY": 1.4, "INV_STOCK_MARKETS": 1.3,

    # === MACRO / ECONOMIC ===
    "TRADINGECONOMICS": 1.6, "ECONODAY": 1.4, "BRIEFING": 1.3,

    # === GEOPOLITICS ===
    "POLITICO_ECON": 1.4, "POLITICO_FIN": 1.4, "TRUMP_HEADLINES": 1.3,
    "ALJAZEERA": 1.2, "GUARDIAN_BIZ": 1.2,

    # === COMMODITIES GENERAL ===
    "COMMODITY_NEWS": 1.2, "METAL_BULLETIN": 1.4, "FASTMARKETS": 1.3, "MINING_WEEKLY": 1.3,

    # === CRYPTO (correlation signals) ===
    "COINDESK": 0.8, "COINTELEGRAPH": 0.7,

    # === CALENDAR (no bias weight) ===
    "FOREXFACTORY_CALENDAR": 0.0, "MYFX_CAL": 0.0,
    "MYFX_NEWS": 1.2,

    # === FRED ===
    "FRED": 1.5,
}

RULES = {
    "XAU": [(r"\b(fed|fomc|powell|hawkish|rate hike|rates higher)\b", -0.7, "Hawkish Fed weighs on gold"),
            (r"\b(rate cut|dovish|easing)\b", +0.6, "Dovish Fed supports gold"),
            (r"\b(cpi|inflation)\b", +0.2, "Inflation data supports gold"),
            (r"\b(strong dollar|usd strengthens|yields rise|real yields)\b", -0.8, "Strong USD weighs on gold"),
            (r"\b(geopolitical|safe[- ]haven|risk aversion)\b", +0.5, "Safe-haven demand rises"),
            (r"\b(risk-on|stocks rally)\b", -0.2, "Risk-on mood pressures gold")],
    "US500": [(r"\b(earnings beat|guidance raised)\b", +0.6, "Strong earnings support stocks"),
              (r"\b(earnings miss|guidance cut)\b", -0.7, "Weak earnings pressure stocks"),
              (r"\b(yields rise|rates higher|hawkish)\b", -0.6, "Rising yields pressure stocks"),
              (r"\b(rate cut|dovish|easing)\b", +0.5, "Fed easing supports stocks"),
              (r"\b(risk-off|selloff|crash)\b", -0.7, "Risk-off mood in markets"),
              (r"\b(rally|rebound|risk-on)\b", +0.3, "Risk-on momentum building")],
    "WTI": [(r"\b(crude|oil|wti|brent)\b", +0.1, "Oil market focus"),
            (r"\b(opec|output cut|production cut)\b", +0.8, "OPEC cuts support prices"),
            (r"\b(inventory build|inventories rise)\b", -0.8, "Inventory build pressures oil"),
            (r"\b(inventory draw|inventories fall)\b", +0.8, "Inventory draw supports oil"),
            (r"\b(disruption|outage|sanctions)\b", +0.7, "Supply disruption risk"),
            (r"\b(demand weak|recession)\b", -0.6, "Demand concerns weigh on oil")]
}

# ============ DB ============
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
            _CONN_FROM_POOL.add(id(c))
            return c
        except: pass
    return psycopg2.connect(_make_dsn())

def db_put(conn):
    if not conn: return
    pool, cid = _get_pool(), id(conn)
    if pool and cid in _CONN_FROM_POOL:
        _CONN_FROM_POOL.discard(cid)
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
                cur.execute("CREATE TABLE IF NOT EXISTS econ_events(id BIGSERIAL PRIMARY KEY,source TEXT,title TEXT,currency TEXT,impact TEXT,event_ts BIGINT,link TEXT,fingerprint TEXT UNIQUE);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_econ_ts ON econ_events(event_ts DESC);")
                cur.execute("CREATE TABLE IF NOT EXISTS kv_state(k TEXT PRIMARY KEY,v TEXT,updated_ts BIGINT);")
        _DB_READY = True
    finally: db_put(conn)

def kv_get(k):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM kv_state WHERE k=%s;", (k,))
            r = cur.fetchone()
            return r[0] if r else None
    except: return None
    finally: db_put(conn)

def kv_set(k, v):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO kv_state(k,v,updated_ts) VALUES(%s,%s,%s) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_ts=EXCLUDED.updated_ts;", (k, str(v), int(time.time())))
    finally: db_put(conn)

def save_bias(p):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO bias_state(id,updated_ts,payload_json) VALUES(1,%s,%s) ON CONFLICT(id) DO UPDATE SET updated_ts=EXCLUDED.updated_ts,payload_json=EXCLUDED.payload_json;", (int(time.time()), json.dumps(p)))
    finally: db_put(conn)

def load_bias():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload_json FROM bias_state WHERE id=1;")
            r = cur.fetchone()
            return json.loads(r[0]) if r else None
    except: return None
    finally: db_put(conn)

# ============ HELPERS ============
def fingerprint(t, l): return hashlib.sha1(f"{t}||{l}".encode()).hexdigest()
def decay_weight(age): return math.exp(-LAMBDA * max(0, age))
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
    return "HIGH" if "HI" in s else ("MED" if "ME" in s else ("LOW" if "LO" in s else None))

def _fmt_time(ts):
    if not ts: return ""
    d = ts - int(time.time())
    if d <= 0: return "now"
    if d < 3600: return f"{d//60}m"
    if d < 86400: return f"{d//3600}h {(d%3600)//60}m"
    return f"{d//86400}d"

# USD Detection
_USD_KW = re.compile(r"\b(USD|Fed|FOMC|Powell|NFP|Payroll|CPI|PPI|GDP|Retail Sales|Jobless|Unemployment|ISM|PMI|Treasury|Michigan|JOLTS|ADP|Empire State|Durable Goods|Housing|Consumer Confidence|Initial Claims|Core PCE|Factory Orders|Trade Balance)\b", re.I)
_CCY_PAT = re.compile(r"\b(USD|EUR|GBP|JPY|CHF|AUD|CAD|NZD|CNY)\b", re.I)

def _detect_ccy(title):
    if not title: return None
    m = _CCY_PAT.search(title)
    if m: return m.group(1).upper()
    if _USD_KW.search(title): return "USD"
    for pat, ccy in [(r"\b(US|U\.S\.|American)\b", "USD"), (r"\b(Euro|ECB)\b", "EUR"), (r"\b(UK|BoE|British)\b", "GBP"), (r"\b(Japan|BoJ)\b", "JPY")]:
        if re.search(pat, title, re.I): return ccy
    return None

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

# ============ INGEST ============
def _parse_feed(src, url, now, limit):
    if src in CALENDAR_FEEDS or (src == "TRUMP_HEADLINES" and not TRUMP_ENABLED): return []
    try: d = feedparser.parse(url)
    except: return []
    out = []
    for e in (d.entries or [])[:limit]:
        t, l = (e.get("title") or "").strip(), (e.get("link") or "").strip()
        if t and l: out.append((src, t, l, _ts_from_entry(e, now), fingerprint(t, l)))
    return out

def ingest_news():
    now = int(time.time())
    tasks = [(s, u) for s, u in RSS_FEEDS.items() if s not in CALENDAR_FEEDS and not (s == "TRUMP_HEADLINES" and not TRUMP_ENABLED)]
    batch = []
    with ThreadPoolExecutor(max_workers=RSS_PARSE_WORKERS) as ex:
        for f in as_completed([ex.submit(_parse_feed, s, u, now, RSS_LIMIT) for s, u in tasks], timeout=RUN_MAX_SEC):
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

def ingest_calendar():
    for src in CALENDAR_FEEDS:
        url = RSS_FEEDS.get(src)
        if not url: continue
        try: d = feedparser.parse(url)
        except: continue
        conn = db_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for e in (d.entries or [])[:CAL_LIMIT]:
                        t = (e.get("title") or "").strip()
                        if not t: continue
                        l = (e.get("link") or "").strip()
                        ts = _ts_from_entry(e, int(time.time()))
                        ccy = _detect_ccy(t)
                        imp = _impact_norm(e.get("impact") or e.get("importance"))
                        fp = fingerprint(f"{src}||{t}", l or t)
                        try: cur.execute("INSERT INTO econ_events(source,title,currency,impact,event_ts,link,fingerprint) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;", (src, t, ccy, imp, ts, l, fp))
                        except: pass
        finally: db_put(conn)

def _get_upcoming(now):
    horizon = now + int(EVENT_CFG["lookahead_hours"] * 3600)
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT title, event_ts, currency, impact FROM econ_events 
                          WHERE event_ts BETWEEN %s AND %s AND currency='USD' AND impact IN ('HIGH','MED')
                          ORDER BY event_ts LIMIT 10;""", (now, horizon))
            rows = cur.fetchall()
            if not rows:
                cur.execute("""SELECT title, event_ts, currency, impact FROM econ_events 
                              WHERE event_ts BETWEEN %s AND %s ORDER BY event_ts LIMIT 5;""", (now, horizon))
                rows = cur.fetchall()
            return [{"title": t, "ts": ts, "currency": _detect_ccy(t) if not c else c, "impact": _impact_norm(i)} for t, ts, c, i in rows]
    finally: db_put(conn)

def _event_risk(upcoming, recent_macro):
    if recent_macro: return 0.7
    if not upcoming: return 0.1
    impacts = [x.get("impact") for x in upcoming if x.get("currency") == "USD"]
    if "HIGH" in impacts: return 0.9
    if "MED" in impacts: return 0.5
    return 0.2

# ============ FRED ============
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
        out["XAU"].append({"w": -1.2 if d > 0 else 1.0, "why": "Real yields rising" if d > 0 else "Real yields falling"})
    if len(dgs) >= 6:
        d = dgs[0] - dgs[5]
        out["US500"].append({"w": -0.9 if d > 0 else 0.5, "why": "Bond yields rising" if d > 0 else "Bond yields falling"})
    if len(usd) >= 6:
        d = (usd[0] - usd[5]) / max(abs(usd[5]), 1)
        out["XAU"].append({"w": -0.9 if d > 0 else 0.7, "why": "Dollar strengthening" if d > 0 else "Dollar weakening"})
    if len(vix) >= 6:
        d = (vix[0] - vix[5]) / max(abs(vix[5]), 1)
        out["US500"].append({"w": -1.0 if d > 0 else 0.4, "why": "Volatility spiking" if d > 0 else "Volatility calming"})
    return out

# ============ COMPUTE BIAS ============
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
    event_mode = EVENT_CFG["enabled"] and (recent_macro or any(x.get("ts") and x.get("currency") == "USD" and x.get("impact") in ("HIGH", "MED") for x in upcoming))
    event_risk = _event_risk(upcoming, recent_macro)

    assets_out = {}
    for asset in ASSETS:
        score, contribs, drivers_why, freshness = 0.0, [], [], {"0-2h": 0, "2-8h": 0, "8-24h": 0}
        for source, title, link, ts in rows:
            age = max(0, now - ts)
            w_src, w_time = SOURCE_WEIGHT.get(source, 1.0), decay_weight(age)
            for m in match_rules(asset, title):
                contrib = m["w"] * w_src * w_time
                score += contrib
                freshness[_fresh_bucket(age)] += 1
                contribs.append({"contrib": contrib, "why": m["why"]})
                if m["why"] not in drivers_why: drivers_why.append(m["why"])

        for d in fred_drivers.get(asset, []):
            score += d["w"]
            contribs.append({"contrib": d["w"], "why": d["why"]})
            if d["why"] not in drivers_why: drivers_why.append(d["why"])

        th = BIAS_THRESH.get(asset, 1.0)
        bias = "BULLISH" if score >= th else ("BEARISH" if score <= -th else "NEUTRAL")

        net = sum(x["contrib"] for x in contribs)
        abs_sum = sum(abs(x["contrib"]) for x in contribs) or 1
        conflict = 1 - abs(net) / abs_sum
        src_div = len(set(row[0] for row in rows if match_rules(asset, row[1])))

        fresh_total = sum(freshness.values()) or 1
        fresh_score = (freshness["0-2h"] + freshness["2-8h"] * 0.6) / fresh_total

        raw = 0.45 * min(1, abs(score)/th) + 0.20 * min(1, len(contribs)/18) + 0.10 * min(1, src_div/7) + 0.10 * fresh_score + 0.15 * (1-conflict) - 0.35 * conflict - (0.15 * event_risk if event_mode else 0)
        quality = int(max(0, min(100, round(raw * 100))))

        top_drivers = sorted(contribs, key=lambda x: abs(x["contrib"]), reverse=True)[:5]
        why_bias = [d["why"] for d in top_drivers if (d["contrib"] > 0) == (score > 0)][:3]
        why_opposite = [d["why"] for d in top_drivers if (d["contrib"] > 0) != (score > 0)][:2]

        assets_out[asset] = {
            "bias": bias, "score": round(score, 3), "threshold": th,
            "quality": quality, "conflict": round(conflict, 2),
            "evidence_count": len(contribs),
            "why_bias": why_bias or ["Market signals unclear"],
            "why_opposite": why_opposite,
            "freshness": freshness,
            "event_mode": event_mode
        }

    return {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "event": {"event_mode": event_mode, "event_risk": round(event_risk, 2), "upcoming": upcoming[:5]},
        "meta": {"profile": GATE_PROFILE}
    }

# ============ TRADE GATE ============
def eval_gate(a, event):
    cfg = GATE_THRESHOLDS[GATE_PROFILE]
    bias, quality, conflict = a.get("bias", "NEUTRAL"), a.get("quality", 0), a.get("conflict", 1)
    event_mode = event.get("event_mode", False)
    upcoming = event.get("upcoming", [])

    blockers, unlock = [], []

    if bias == "NEUTRAL":
        blockers.append("No clear market direction")
        unlock.append("Wait for clearer signal")

    if quality < cfg["quality_v2_min"]:
        blockers.append("Signal not strong enough")
        unlock.append("Wait for more confirming news")

    if conflict > cfg["conflict_max"]:
        blockers.append("Sources disagree on direction")
        unlock.append("Wait for consensus")

    if event_mode and cfg["event_mode_block"]:
        next_ev = next((e for e in upcoming if e.get("currency") == "USD" and e.get("impact") in ("HIGH", "MED")), None)
        if next_ev:
            blockers.append(f"Macro event coming ({next_ev.get('title', 'USD event')[:30]})")
            unlock.append("Wait for event to pass")
        elif not (quality >= cfg["event_override_quality"] and conflict <= cfg["event_override_conflict"]):
            blockers.append("Macro event window active")
            unlock.append("Wait for window to close")

    return {"ok": len(blockers) == 0, "blockers": blockers, "unlock": unlock}

# ============ PIPELINE ============
def pipeline_run():
    if not _PIPELINE_LOCK.acquire(timeout=RUN_LOCK_TIMEOUT_SEC):
        db_init()
        return load_bias() or {}
    try:
        db_init()
        ingest_news()
        last_cal = int(kv_get("cal_ts") or 0)
        if (int(time.time()) - last_cal) >= CAL_INGEST_TTL_SEC:
            ingest_calendar()
            kv_set("cal_ts", str(int(time.time())))
        if FRED_ON_RUN:
            for sid in FRED_SERIES: fred_ingest(sid, 120)
        payload = compute_bias()
        payload["meta"]["feeds_status"] = feeds_health_live()
        save_bias(payload)
        return payload
    finally: _PIPELINE_LOCK.release()

def _scheduled_pipeline():
    """Called by APScheduler every AUTO_REFRESH_MINUTES minutes."""
    try:
        print(f"[scheduler] Running pipeline at {datetime.now(timezone.utc).isoformat()}")
        pipeline_run()
        print(f"[scheduler] Done.")
    except Exception as e:
        print(f"[scheduler] Error: {e}")

# ============ API ============
app = FastAPI(title="News Bias Terminal")

@app.on_event("startup")
def startup_event():
    """Start the background scheduler when FastAPI starts."""
    db_init()
    # Запускаем первый пересчёт сразу при старте в фоне
    t = threading.Thread(target=_scheduled_pipeline, daemon=True)
    t.start()
    print("[scheduler] Initial pipeline run started.")
    if _APSCHEDULER_AVAILABLE:
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            _scheduled_pipeline,
            trigger="interval",
            minutes=AUTO_REFRESH_MINUTES,
            id="pipeline_refresh",
            replace_existing=True,
            max_instances=1,
        )
        scheduler.start()
        print(f"[scheduler] Started — pipeline runs every {AUTO_REFRESH_MINUTES} minutes.")
    else:
        print("[scheduler] APScheduler not installed — auto-refresh disabled. Run: pip install apscheduler")

@app.get("/", include_in_schema=False)
def root(): return RedirectResponse("/dashboard")

@app.get("/health")
def health():
    return {
        "ok": True,
        "profile": GATE_PROFILE,
        "auto_refresh_minutes": AUTO_REFRESH_MINUTES,
        "scheduler": "apscheduler" if _APSCHEDULER_AVAILABLE else "disabled"
    }

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
def latest(limit: int = 20):
    db_init()
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,published_ts FROM news_items ORDER BY published_ts DESC LIMIT %s;", (min(50, limit),))
            return {"items": [{"source": s, "title": t, "ts": ts} for s, t, ts in cur.fetchall()]}
    finally: db_put(conn)

@app.get("/feeds")
def feeds():
    db_init()
    st = feeds_health_live(True)
    return {"total": len(st), "ok": sum(1 for v in st.values() if v.get("ok")), "feeds": st}

@app.get("/myfx_calendar", response_class=HTMLResponse)
def myfx_cal():
    return HTMLResponse(f'<!doctype html><html><head><meta charset="utf-8"><style>html,body{{height:100%;margin:0;background:#000}}</style></head><body><iframe src="{MYFXBOOK_IFRAME}" style="width:100%;height:100%;border:0"></iframe></body></html>')


# ============ DASHBOARD ============
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias() or pipeline_run()
    assets = payload.get("assets", {})
    event = payload.get("event", {})
    meta = payload.get("meta", {})

    now_ts = int(time.time())
    event_mode = event.get("event_mode", False)
    upcoming = event.get("upcoming", [])[:5]

    next_ev = next((e for e in upcoming if e.get("currency") == "USD" and e.get("impact") in ("HIGH", "MED")), None)
    if not next_ev and upcoming:
        next_ev = upcoming[0]

    feeds_st = meta.get("feeds_status", {})
    feeds_ok = sum(1 for v in feeds_st.values() if v.get("ok"))
    feeds_total = len(feeds_st) or len(RSS_FEEDS)

    asset_data = []
    for sym in ASSETS:
        a = assets.get(sym, {})
        gate = eval_gate(a, event)
        a["gate"] = gate
        asset_data.append((sym, a))

    def asset_row(sym, a):
        bias = a.get("bias", "NEUTRAL")
        gate = a.get("gate", {})
        ok = gate.get("ok", False)
        blockers = gate.get("blockers", [])
        why_bias = a.get("why_bias", [])

        bias_icon = "▲" if bias == "BULLISH" else ("▼" if bias == "BEARISH" else "●")
        bias_color = "#00d4aa" if bias == "BULLISH" else ("#ff5f56" if bias == "BEARISH" else "#888")
        trade_text = "TRADE" if ok else "WAIT"
        trade_color = "#00d4aa" if ok else "#ffbd2e"

        reason = blockers[0] if blockers else (why_bias[0] if why_bias else "Analyzing...")

        return f'''
        <div class="asset-row" onclick="openView('{sym}')">
            <div class="asset-main">
                <span class="asset-sym">{sym}</span>
                <span class="asset-bias" style="color:{bias_color}">{bias} {bias_icon}</span>
            </div>
            <div class="asset-reason">{reason[:45]}</div>
            <div class="asset-trade" style="background:{trade_color}20;color:{trade_color}">{trade_text}</div>
        </div>'''

    if next_ev:
        ev_ccy = next_ev.get("currency") or "—"
        ev_title = (next_ev.get("title") or "Event")[:40]
        ev_impact = next_ev.get("impact") or ""
        ev_time = _fmt_time(next_ev.get("ts"))
        impact_color = "#ff5f56" if ev_impact == "HIGH" else ("#ffbd2e" if ev_impact == "MED" else "#888")
        next_ev_html = f'<span class="ev-ccy">{ev_ccy}</span> {ev_title} <span class="ev-impact" style="color:{impact_color}">{ev_impact}</span> <span class="ev-time">in {ev_time}</span>'
    else:
        next_ev_html = '<span class="ev-none">No major USD events upcoming</span>'

    js_payload = json.dumps(payload).replace("</", "<\\/")
    tv_json = json.dumps(TV_SYMBOLS)
    updated = payload.get("updated_utc", "")[:19].replace("T", " ")

    return HTMLResponse(f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>NEWS BIAS TERMINAL</title>
<style>
:root {{
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #c9d1d9;
    --text-muted: #8b949e;
    --orange: #ff9500;
    --green: #00d4aa;
    --red: #ff5f56;
    --yellow: #ffbd2e;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'SF Mono', 'Consolas', 'Monaco', monospace;
    font-size: 13px;
    line-height: 1.4;
    min-height: 100vh;
}}

.container {{ max-width: 900px; margin: 0 auto; }}

.header {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 8px 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}}
.logo {{ color: var(--orange); font-weight: 700; font-size: 12px; letter-spacing: 1px; }}
.logo span {{ color: var(--text-muted); }}
.header-right {{ display: flex; align-items: center; gap: 12px; }}
.status {{ display: flex; align-items: center; gap: 5px; font-size: 10px; }}
.live-dot {{
    width: 6px; height: 6px;
    background: var(--green);
    border-radius: 50%;
    animation: pulse 2s infinite;
}}
@keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} }}
.time {{ color: var(--text-muted); font-size: 10px; }}
.feeds-badge {{
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 10px;
    cursor: pointer;
}}
.feeds-badge:hover {{ border-color: var(--orange); }}
.refresh-badge {{
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 10px;
    cursor: pointer;
    color: var(--text-muted);
}}
.refresh-badge:hover {{ border-color: var(--green); color: var(--green); }}

.tv-wrap {{ border-bottom: 1px solid var(--border); background: #000; }}

.news-ticker {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 6px 0;
    overflow: hidden;
    font-size: 11px;
}}
.ticker-label {{ color: var(--orange); font-size: 9px; font-weight: 700; padding: 0 12px; display: inline-block; min-width: 60px; }}
.ticker-scroll {{ display: inline-block; white-space: nowrap; animation: ticker 180s linear infinite; }}
.ticker-scroll:hover {{ animation-play-state: paused; }}
@keyframes ticker {{ 0% {{ transform: translateX(0); }} 100% {{ transform: translateX(-50%); }} }}
.ticker-item {{ display: inline; margin-right: 30px; color: var(--text-muted); }}
.ticker-item b {{ color: var(--orange); margin-right: 6px; }}

.next-event {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 8px 16px;
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 11px;
    max-width: 900px;
    margin: 0 auto;
}}
.ev-label {{ color: var(--text-muted); font-size: 9px; font-weight: 700; min-width: 70px; }}
.ev-ccy {{ background: #1f6feb33; color: #58a6ff; padding: 2px 5px; border-radius: 2px; font-size: 10px; font-weight: 700; margin-right: 6px; }}
.ev-impact {{ font-size: 9px; font-weight: 700; margin-left: 6px; }}
.ev-time {{ color: var(--text-muted); margin-left: 6px; }}
.ev-none {{ color: var(--text-muted); font-style: italic; }}

.assets {{ padding: 12px 16px; max-width: 900px; margin: 0 auto; }}
.section-header {{ color: var(--orange); font-size: 9px; font-weight: 700; letter-spacing: 1px; margin-bottom: 8px; }}
.asset-row {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 10px 14px;
    margin-bottom: 4px;
    cursor: pointer;
    transition: border-color 0.15s;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
}}
.asset-row:hover {{ border-color: var(--orange); }}
.asset-main {{ display: flex; align-items: center; gap: 16px; min-width: 180px; }}
.asset-sym {{ font-size: 14px; font-weight: 700; color: var(--text); min-width: 55px; }}
.asset-bias {{ font-size: 11px; font-weight: 700; min-width: 80px; }}
.asset-reason {{ flex: 1; color: var(--text-muted); font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.asset-trade {{ padding: 4px 12px; border-radius: 3px; font-size: 10px; font-weight: 700; text-align: center; min-width: 60px; }}

/* Countdown */
.next-refresh {{
    background: var(--surface);
    border-top: 1px solid var(--border);
    padding: 6px 16px;
    font-size: 10px;
    color: var(--text-muted);
    text-align: center;
}}
.next-refresh span {{ color: var(--green); }}

.footer {{
    background: var(--surface);
    border-top: 1px solid var(--border);
    padding: 8px 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
}}
.footer-btns {{ display: flex; gap: 6px; }}
.btn {{
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 5px 10px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 600;
    cursor: pointer;
    font-family: inherit;
}}
.btn:hover {{ border-color: var(--orange); color: var(--orange); }}
.footer-info {{ color: var(--text-muted); font-size: 9px; }}
.footer-info span {{ margin-left: 12px; }}

.modal {{
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.85);
    z-index: 100;
    padding: 20px;
    overflow-y: auto;
}}
.modal.open {{ display: flex; justify-content: center; align-items: flex-start; padding-top: 10vh; }}
.modal-box {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    width: 100%;
    max-width: 500px;
    max-height: 80vh;
    overflow-y: auto;
}}
.modal-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px;
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--surface);
}}
.modal-title {{ font-size: 14px; font-weight: 700; color: var(--orange); }}
.modal-close {{ background: none; border: none; color: var(--text-muted); font-size: 20px; cursor: pointer; padding: 0; }}
.modal-close:hover {{ color: var(--text); }}
.modal-body {{ padding: 16px; }}
.view-section {{ margin-bottom: 20px; }}
.view-label {{ font-size: 10px; font-weight: 700; color: var(--text-muted); letter-spacing: 0.5px; margin-bottom: 10px; }}
.view-item {{ padding: 10px 12px; background: var(--bg); border-radius: 4px; margin-bottom: 6px; font-size: 12px; border-left: 3px solid var(--border); }}
.view-item.why {{ border-left-color: var(--green); color: var(--green); }}
.view-item.block {{ border-left-color: var(--red); color: var(--red); }}
.view-item.unlock {{ border-left-color: var(--yellow); color: var(--yellow); }}
.view-item.opposite {{ border-left-color: var(--text-muted); color: var(--text-muted); }}

.feed-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 6px; }}
.feed-item {{ background: var(--bg); padding: 8px; border-radius: 4px; font-size: 10px; display: flex; justify-content: space-between; }}
.feed-ok {{ color: var(--green); }}
.feed-bad {{ color: var(--red); }}

.spacer {{ height: 50px; }}

@media (max-width: 600px) {{
    .container {{ max-width: 100%; }}
    .asset-row {{ flex-direction: column; align-items: flex-start; gap: 8px; }}
    .asset-main {{ width: 100%; justify-content: space-between; }}
    .asset-reason {{ width: 100%; white-space: normal; }}
    .asset-trade {{ align-self: flex-start; }}
    .footer {{ flex-direction: column; gap: 8px; }}
}}
</style>
</head>
<body>

<div class="header">
    <div class="logo">NEWS BIAS <span>// TERMINAL</span></div>
    <div class="header-right">
        <div class="status">
            <div class="live-dot"></div>
            <span>LIVE</span>
        </div>
        <div class="time" id="clock">{updated} UTC</div>
        <div class="refresh-badge" onclick="manualRefresh()" id="refreshBtn">↻ REFRESH</div>
        <div class="feeds-badge" onclick="openFeeds()">FEEDS {feeds_ok}/{feeds_total}</div>
    </div>
</div>

<div class="tv-wrap">
    <div class="tradingview-widget-container">
        <div class="tradingview-widget-container__widget"></div>
        <script src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
        {{"symbols":{tv_json},"showSymbolLogo":true,"isTransparent":true,"displayMode":"compact","colorTheme":"dark"}}
        </script>
    </div>
</div>

<div class="news-ticker">
    <span class="ticker-label">▶ NEWS</span>
    <div class="ticker-scroll" id="newsTicker">Loading headlines...</div>
</div>

<div class="next-event">
    <span class="ev-label">NEXT EVENT</span>
    {next_ev_html}
</div>

<div class="assets">
    <div class="section-header">&lt;&lt; TRADE SIGNALS &gt;&gt;</div>
    {''.join(asset_row(sym, a) for sym, a in asset_data)}
</div>

<div class="next-refresh">
    Auto-refresh every {AUTO_REFRESH_MINUTES} min &nbsp;|&nbsp; Next update in: <span id="countdown">—</span>
</div>

<div class="spacer"></div>

<div class="footer">
    <div class="footer-btns">
        <button class="btn" onclick="openCalendar()">📅 CALENDAR</button>
        <button class="btn" onclick="openFeeds()">📡 FEEDS</button>
    </div>
    <div class="footer-info">
        <span>Profile: {GATE_PROFILE}</span>
        <span>Keys: 1-3 view • Esc close</span>
    </div>
</div>

<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
    <div class="modal-box">
        <div class="modal-header">
            <div class="modal-title" id="modalTitle">View</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div class="modal-body" id="modalBody"></div>
    </div>
</div>

<script>
const P = {js_payload};
const ASSETS = ['XAU', 'US500', 'WTI'];
const CFG = {json.dumps(GATE_THRESHOLDS[GATE_PROFILE])};
const AUTO_REFRESH_MS = {AUTO_REFRESH_MINUTES} * 60 * 1000;

let _lastUpdate = Date.now();
let _nextRefreshAt = _lastUpdate + AUTO_REFRESH_MS;

// ---- Countdown timer ----
function updateCountdown() {{
    const remaining = Math.max(0, Math.round((_nextRefreshAt - Date.now()) / 1000));
    const m = Math.floor(remaining / 60);
    const s = remaining % 60;
    document.getElementById('countdown').textContent = m + 'm ' + String(s).padStart(2,'0') + 's';
}}
setInterval(updateCountdown, 1000);
updateCountdown();

// ---- Auto-reload the page every AUTO_REFRESH_MINUTES ----
// This reloads the whole dashboard so UI stays in sync with backend data
setInterval(() => {{
    window.location.reload();
}}, AUTO_REFRESH_MS);

// ---- Also update clock & news every 30s without reloading ----
setInterval(async () => {{
    try {{
        const r = await fetch('/api/data');
        const d = await r.json();
        if (d.updated_utc) {{
            document.getElementById('clock').textContent = d.updated_utc.slice(0,19).replace('T',' ') + ' UTC';
        }}
    }} catch {{}}
}}, 30000);

// ---- Manual refresh button ----
async function manualRefresh() {{
    const btn = document.getElementById('refreshBtn');
    btn.textContent = '↻ ...';
    try {{
        // POST запускает пересчёт данных, GET только читает
        await fetch('/api/refresh', {{ method: 'POST' }});
        _nextRefreshAt = Date.now() + AUTO_REFRESH_MS;
        window.location.reload();
    }} catch {{
        btn.textContent = '↻ REFRESH';
    }}
}}

// ---- Load news ticker ----
async function loadNews() {{
    try {{
        const r = await fetch('/latest?limit=15');
        const d = await r.json();
        const items = (d.items || []).map(i => `<span class="ticker-item"><b>${{esc(i.source)}}</b>${{esc(i.title)}}</span>`).join('');
        document.getElementById('newsTicker').innerHTML = items + items;
    }} catch {{}}
}}
loadNews();
setInterval(loadNews, 120000);

function esc(s) {{ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}

function openModal(title, html) {{
    document.getElementById('modalTitle').textContent = title;
    document.getElementById('modalBody').innerHTML = html;
    document.getElementById('modal').classList.add('open');
}}
function closeModal() {{ document.getElementById('modal').classList.remove('open'); }}

function openView(sym) {{
    const a = P.assets?.[sym] || {{}};
    const ev = P.event || {{}};
    const bias = a.bias || 'NEUTRAL';
    const whyBias = a.why_bias || [];
    const whyOpp = a.why_opposite || [];
    const blockers = [], unlock = [];
    const quality = a.quality || 0;
    const conflict = a.conflict || 1;
    const eventMode = ev.event_mode;
    const upcoming = ev.upcoming || [];

    if (bias === 'NEUTRAL') {{ blockers.push('No clear market direction'); unlock.push('Wait for clearer signal'); }}
    if (quality < CFG.quality_v2_min) {{ blockers.push('Signal not strong enough'); unlock.push('Wait for more confirming news'); }}
    if (conflict > CFG.conflict_max) {{ blockers.push('Sources disagree on direction'); unlock.push('Wait for consensus'); }}
    if (eventMode && CFG.event_mode_block) {{
        const nextUSD = upcoming.find(e => e.currency === 'USD' && ['HIGH','MED'].includes(e.impact));
        if (nextUSD) {{
            blockers.push('Macro event coming (' + (nextUSD.title || 'USD event').slice(0,25) + ')');
            unlock.push('Wait for event to pass');
        }} else if (!(quality >= CFG.event_override_quality && conflict <= CFG.event_override_conflict)) {{
            blockers.push('Macro event window active');
            unlock.push('Wait for window to close');
        }}
    }}

    const ok = blockers.length === 0;
    const biasColor = bias === 'BULLISH' ? '#00d4aa' : (bias === 'BEARISH' ? '#ff5f56' : '#888');
    const tradeText = ok ? 'TRADE' : 'WAIT';
    const tradeColor = ok ? '#00d4aa' : '#ffbd2e';

    let html = `
        <div style="text-align:center;padding:20px 0;border-bottom:1px solid var(--border);margin-bottom:16px;">
            <div style="font-size:24px;font-weight:700;color:${{biasColor}};margin-bottom:8px;">${{bias}}</div>
            <div style="display:inline-block;background:${{tradeColor}}22;color:${{tradeColor}};padding:8px 20px;border-radius:4px;font-weight:700;">${{tradeText}}</div>
        </div>
    `;
    if (whyBias.length) {{
        html += `<div class="view-section"><div class="view-label">WHY ${{bias}}</div>`;
        whyBias.forEach(w => {{ html += `<div class="view-item why">${{esc(w)}}</div>`; }});
        html += `</div>`;
    }}
    if (blockers.length) {{
        html += `<div class="view-section"><div class="view-label">WHY WAIT</div>`;
        blockers.forEach(b => {{ html += `<div class="view-item block">${{esc(b)}}</div>`; }});
        html += `</div>`;
    }}
    if (unlock.length) {{
        html += `<div class="view-section"><div class="view-label">WILL UNLOCK WHEN</div>`;
        unlock.forEach(u => {{ html += `<div class="view-item unlock">${{esc(u)}}</div>`; }});
        html += `</div>`;
    }}
    if (whyOpp.length) {{
        html += `<div class="view-section"><div class="view-label">OPPOSING SIGNALS</div>`;
        whyOpp.forEach(w => {{ html += `<div class="view-item opposite">${{esc(w)}}</div>`; }});
        html += `</div>`;
    }}
    openModal(sym, html);
}}

function openFeeds() {{
    const f = P.meta?.feeds_status || {{}};
    const items = Object.entries(f).sort((a,b) => a[0].localeCompare(b[0]));
    const ok = items.filter(([,v]) => v.ok).length;
    let html = `<div style="margin-bottom:16px;color:var(--text-muted);">Active: ${{ok}}/${{items.length}}</div>`;
    html += `<div class="feed-grid">`;
    items.forEach(([name, info]) => {{
        html += `<div class="feed-item"><span>${{esc(name)}}</span><span class="${{info.ok ? 'feed-ok' : 'feed-bad'}}">${{info.ok ? '●' : '○'}}</span></div>`;
    }});
    html += `</div>`;
    openModal('FEEDS STATUS', html);
}}

function openCalendar() {{
    openModal('ECONOMIC CALENDAR', `
        <div style="height:65vh;margin:-16px;margin-top:0;">
            <iframe src="/myfx_calendar" style="width:100%;height:100%;border:0;filter:invert(1) hue-rotate(180deg) contrast(0.9);"></iframe>
        </div>
    `);
}}

document.addEventListener('keydown', e => {{
    if (e.key === 'Escape') closeModal();
    if (e.key === '1') openView('XAU');
    if (e.key === '2') openView('US500');
    if (e.key === '3') openView('WTI');
}});
</script>
</body>
</html>''')
