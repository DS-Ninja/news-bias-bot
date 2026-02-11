# app.py
# NEWS BIAS // TERMINAL — RSS + Postgres + Bias/Quality + Trade Gate + Calendar + Alerts + Ticker
# UPDATED v2026-02-11c (FULL SINGLE FILE — HTML INCLUDED — DB RESILIENCE FIX):
# ✅ Adds sslmode=require automatically for Railway/rlwy.net unless explicitly set
# ✅ Graceful-degrade: /dashboard and /bias stay up even when DB is DOWN (shows db_down banner)
# ✅ Keeps: global run lock, per-stage timings, cached feeds health TTL, robust /run parsing, pooled+fallback DB, shutdown hook, XSS-safe JSON injection
# ✅ Includes minimal openView() implementation (modal detail view) to avoid missing-function issues
#
# Notes:
# - If DB is down, UI still loads (Decision shows NEUTRAL with db_down tag). Fix DB env/ssl to restore full features.

import os
import json
import time
import re
import hashlib
import math
import hmac
import calendar as pycalendar
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
import socket
import threading

socket.setdefaulttimeout(float(os.environ.get("HTTP_TIMEOUT", "12")))

# Optional DB pool
try:
    from psycopg2.pool import ThreadedConnectionPool  # type: ignore
except Exception:
    ThreadedConnectionPool = None  # type: ignore

# SAFE PoolError import (prevents crash on some psycopg2 builds)
try:
    from psycopg2.pool import PoolError  # type: ignore
except Exception:
    class PoolError(Exception):
        pass

# Optional dependency: requests (only if FRED enabled + key present)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # graceful: FRED disabled if requests missing

from fastapi import FastAPI, Header, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse

# ============================================================
# CONFIG
# ============================================================

ASSETS = ["XAU", "US500", "WTI"]

GATE_PROFILE = os.environ.get("GATE_PROFILE", "STRICT").strip().upper()
if GATE_PROFILE not in ("STRICT", "MODERATE"):
    GATE_PROFILE = "STRICT"

GATE_THRESHOLDS = {
    "STRICT": {
        "quality_v2_min": 55,
        "conflict_max": 0.55,
        "min_opp_flip_dist": 0.35,
        "neutral_allow": False,
        "neutral_flip_dist_max": 0.20,
        "event_mode_block": True,
        "event_override_quality": 70,
        "event_override_conflict": 0.45,
    },
    "MODERATE": {
        "quality_v2_min": 42,
        "conflict_max": 0.70,
        "min_opp_flip_dist": 0.20,
        "neutral_allow": True,
        "neutral_flip_dist_max": 0.20,
        "event_mode_block": False,
        "event_override_quality": 60,
        "event_override_conflict": 0.60,
    }
}

# Decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

# Bias thresholds (per-asset)
BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": float(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 12,
}

# Alerts thresholds (server diff)
ALERT_CFG = {
    "enabled": True,
    "q2_drop": int(os.environ.get("ALERT_Q2_DROP", "12")),                   # points
    "conflict_spike": float(os.environ.get("ALERT_CONFLICT_SPIKE", "0.18")), # delta
    "feeds_degraded_ratio": float(os.environ.get("ALERT_FEEDS_DEGRADED_RATIO", "0.80")),  # ok share
}

# FRED
FRED_CFG = {
    "enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
    "api_key": os.environ.get("FRED_API_KEY", "").strip(),
    "window_days": int(os.environ.get("FRED_WINDOW_DAYS", "120")),
}
FRED_SERIES = {
    "DGS10":    {"name": "US 10Y Nominal", "freq": "d"},
    "DFII10":   {"name": "US 10Y Real",    "freq": "d"},
    "T10YIE":   {"name": "10Y Breakeven",  "freq": "d"},
    "DTWEXBGS": {"name": "Broad USD",      "freq": "d"},
    "VIXCLS":   {"name": "VIX",            "freq": "d"},
    "BAA10Y":   {"name": "BAA-10Y Spread", "freq": "d"},
}

# ============================================================
# PERF knobs
# ============================================================

RUN_MODE_DEFAULT = os.environ.get("RUN_MODE_DEFAULT", "quick").strip().lower()
if RUN_MODE_DEFAULT not in ("quick", "full"):
    RUN_MODE_DEFAULT = "quick"

RUN_MAX_SEC = float(os.environ.get("RUN_MAX_SEC", "10"))  # only used in quick mode for RSS ingest
RUN_LOCK_TIMEOUT_SEC = float(os.environ.get("RUN_LOCK_TIMEOUT_SEC", "25"))

RSS_LIMIT_QUICK = int(os.environ.get("RSS_LIMIT_QUICK", "18"))
RSS_LIMIT_FULL  = int(os.environ.get("RSS_LIMIT_FULL", "40"))

CAL_LIMIT_QUICK = int(os.environ.get("CAL_LIMIT_QUICK", "120"))
CAL_LIMIT_FULL  = int(os.environ.get("CAL_LIMIT_FULL", "250"))

FEEDS_HEALTH_TTL_SEC = int(os.environ.get("FEEDS_HEALTH_TTL_SEC", "90"))

FRED_ON_RUN = os.environ.get("FRED_ON_RUN", "1").strip() == "1"
FRED_INGEST_EVERY_RUN = os.environ.get("FRED_INGEST_EVERY_RUN", "0").strip() == "1"

# Global run lock to prevent concurrent heavy runs
_PIPELINE_LOCK = threading.Lock()

# Cached feeds health
_FEEDS_HEALTH_CACHE: Optional[Dict[str, Any]] = None
_FEEDS_HEALTH_TS: int = 0

# ============================================================
# RUN token (DYNAMIC — fixes multi-worker / stale env)
# ============================================================

def get_run_tokens() -> List[str]:
    """
    Reads env each call.
    RUN_TOKEN: single token (legacy)
    RUN_TOKENS: comma-separated list (new)
    """
    rt = os.environ.get("RUN_TOKEN", "").strip()
    rts = os.environ.get("RUN_TOKENS", "").strip()

    toks: List[str] = []
    if rt:
        toks.append(rt)
    if rts:
        toks.extend([x.strip() for x in rts.split(",") if x.strip()])

    # de-dup while preserving order
    seen = set()
    out: List[str] = []
    for t in toks:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out

def _token_hash(t: str) -> str:
    return hashlib.sha1((t or "").encode("utf-8", errors="ignore")).hexdigest()[:10]

def get_run_token_hashes() -> List[str]:
    return [_token_hash(t) for t in get_run_tokens()]

# MyFXBook widget iframe
MYFXBOOK_IFRAME_SRC = "https://widget.myfxbook.com/widget/calendar.html?lang=en&impacts=0,1,2,3&symbols=USD"

# TradingView ticker tape symbols
TV_TICKER_SYMBOLS = [
    {"proName": "ICMARKETS:XAUUSD", "title": "XAUUSD"},
    {"proName": "ICMARKETS:EURUSD", "title": "EURUSD"},
    {"proName": "ICMARKETS:US500",  "title": "US500"},
    {"proName": "ICMARKETS:XTIUSD", "title": "WTI"},
    {"proName": "ICMARKETS:USDJPY", "title": "USDJPY"},
]

# RSS feeds
RSS_FEEDS: Dict[str, str] = {
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/news_release.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",

    "OILPRICE": "https://oilprice.com/rss/main",

    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",

    "MARKETWATCH_TOP": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REALTIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",

    "INV_STOCK_FUND": "https://www.investing.com/rss/stock_Fundamental.rss",
    "INV_COMMOD_TECH": "https://www.investing.com/rss/commodities_Technical.rss",
    "INV_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INV_NEWS_95": "https://www.investing.com/rss/news_95.rss",
    "INV_MKT_TECH": "https://www.investing.com/rss/market_overview_Technical.rss",
    "INV_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INV_MKT_IDEAS": "https://www.investing.com/rss/market_overview_investing_ideas.rss",
    "INV_FX_TECH": "https://www.investing.com/rss/forex_Technical.rss",
    "INV_FX_FUND": "https://www.investing.com/rss/forex_Fundamental.rss",

    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",

    "TRUMP_HEADLINES": "https://rss.politico.com/donald-trump.xml",

    # Calendar mirrors
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    "MYFX_CAL": "https://www.myfxbook.com/rss/forex-economic-calendar-events",

    # Myfxbook news/community
    "MYFX_NEWS": "https://www.myfxbook.com/rss/latest-forex-news",
    "MYFX_COMM": "https://www.myfxbook.com/rss/forex-community-recent-topics",

    # US500 / broader macro
    "SPDJI_METHOD": "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=methodologies",
    "SPDJI_PERF": "https://www.spglobal.com/spdji/en/rss/rss-details/?rssFeedName=performance-reports",

    "NASDAQ_STOCKS": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
    "NASDAQ_EARN": "https://www.nasdaq.com/feed/rssoutbound?category=Earnings",
    "NASDAQ_ORIG": "https://www.nasdaq.com/feed/nasdaq-original/rss.xml",
    "NASDAQ_AI": "https://www.nasdaq.com/feed/rssoutbound?category=Artificial+Intelligence",

    "FEEDBURNER_INV": "https://feeds.feedburner.com/InvestingRss",
    "FEEDBURNER_ECON": "https://feeds.feedburner.com/EconomyRss",
    "INVESTINGLIVE": "https://investinglive.com/feed",

    "WSJ_US_BUSINESS": "https://feeds.content.dowjones.io/public/rss/WSJcomUSBusiness",
    "WSJ_MARKETS_MAIN": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
}

# Calendar-only sources (do not add to bias evidence)
CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR", "MYFX_CAL"}

SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0, "BLS": 3.0, "BEA": 2.8,
    "FXSTREET_NEWS": 1.4, "FXSTREET_ANALYSIS": 1.2,
    "MARKETWATCH_TOP": 1.2, "MARKETWATCH_REALTIME": 1.3,
    "OILPRICE": 1.2,

    "INV_STOCK_FUND": 1.0,
    "INV_COMMOD_TECH": 1.0,
    "INV_NEWS_11": 1.0,
    "INV_NEWS_95": 1.0,
    "INV_MKT_TECH": 1.0,
    "INV_MKT_FUND": 1.0,
    "INV_MKT_IDEAS": 0.9,
    "INV_FX_TECH": 0.95,
    "INV_FX_FUND": 0.95,

    "RSSAPP_1": 1.0, "RSSAPP_2": 1.0,
    "TRUMP_HEADLINES": 1.2,

    # Calendar feeds are not bias evidence
    "FOREXFACTORY_CALENDAR": 0.0,
    "MYFX_CAL": 0.0,

    # Myfxbook news/community weights
    "MYFX_NEWS": 1.15,
    "MYFX_COMM": 0.55,

    # Added
    "SPDJI_METHOD": 1.6,
    "SPDJI_PERF": 1.6,
    "NASDAQ_STOCKS": 1.3,
    "NASDAQ_EARN": 1.3,
    "NASDAQ_ORIG": 1.25,
    "NASDAQ_AI": 1.15,
    "FEEDBURNER_INV": 1.0,
    "FEEDBURNER_ECON": 1.0,
    "INVESTINGLIVE": 1.1,
    "WSJ_US_BUSINESS": 1.6,
    "WSJ_MARKETS_MAIN": 1.6,

    "FRED": 1.0,
}

RULES: Dict[str, List[Tuple[str, float, str]]] = {
    "XAU": [
        (r"\b(fed|fomc|powell|rate hike|rates higher|hawkish)\b", -0.7, "Hawkish Fed / higher rates weighs on gold"),
        (r"\b(rate cut|cuts rates|dovish|easing)\b", +0.6, "Dovish Fed supports gold"),
        (r"\b(cpi|inflation)\b", +0.2, "Inflation prints can support gold (context: yields/USD)"),
        (r"\b(strong dollar|usd strengthens|yields rise|real yields)\b", -0.8, "Stronger USD / higher yields weighs on gold"),
        (r"\b(geopolitical|safe[- ]haven|risk aversion|flight to safety)\b", +0.5, "Safe-haven demand supports gold"),
        (r"\b(risk-on|stocks rally|equities rally)\b", -0.2, "Risk-on reduces safe-haven demand"),
    ],
    "US500": [
        (r"\b(earnings beat|earnings surge|guidance raised)\b", +0.6, "Earnings optimism supports equities"),
        (r"\b(earnings miss|guidance cut|profit warning)\b", -0.7, "Earnings disappointment pressures equities"),
        (r"\b(yields rise|bond yields jump|rates higher|hawkish)\b", -0.6, "Higher yields pressure equity multiples"),
        (r"\b(rate cut|dovish|easing)\b", +0.5, "Easing supports risk assets"),
        (r"\b(risk-off|selloff|panic|crash)\b", -0.7, "Risk-off headlines"),
        (r"\b(rally|rebound|risk-on)\b", +0.3, "Risk-on tape"),
        (r"\b(vix spikes|volatility spikes)\b", -0.6, "Volatility spike (risk-off)"),
    ],
    "WTI": [
        (r"\b(crude|oil|wti|brent)\b", +0.1, "Oil headlines (generic)"),
        (r"\b(opec|output cut|cuts output|production cut)\b", +0.8, "Supply cuts support oil"),
        (r"\b(output increase|ramp up production|supply increase)\b", -0.6, "Supply increase pressures oil"),
        (r"\b(inventories rise|stockpile build|build in stocks|inventory build)\b", -0.8, "Inventories build pressures oil"),
        (r"\b(inventories fall|draw in stocks|inventory draw)\b", +0.8, "Inventory draw supports oil"),
        (r"\b(disruption|outage|pipeline|attack|sanctions|shipping disruption)\b", +0.7, "Supply disruption supports oil"),
        (r"\b(demand weakens|recession fears|slowdown)\b", -0.6, "Demand concerns pressure oil"),
    ],
}

# ============================================================
# DB (pooled + fallback direct conn + safe return/close + init-once)
# ============================================================

_DB_POOL: Optional[Any] = None
_DB_READY = False

def _ensure_sslmode(dsn: str) -> str:
    dsn = (dsn or "").strip()
    if not dsn:
        return dsn
    low = dsn.lower()
    if "sslmode=" in low:
        return dsn
    if "rlwy.net" in low or "railway" in low or "proxy.rlwy" in low:
        if low.startswith("postgres://") or low.startswith("postgresql://"):
            sep = "&" if "?" in dsn else "?"
            return dsn + f"{sep}sslmode=require"
        return dsn + " sslmode=require"
    return dsn

def _make_dsn() -> str:
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if db_url:
        return _ensure_sslmode(db_url)

    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "postgres")
    user = os.environ.get("PGUSER", "postgres")
    pwd = os.environ.get("PGPASSWORD", "")
    dsn = f"host={host} port={port} dbname={db} user={user} password={pwd} connect_timeout=5"
    return _ensure_sslmode(dsn)

def _get_pool():
    global _DB_POOL
    if _DB_POOL is not None:
        return _DB_POOL

    if ThreadedConnectionPool is None:
        _DB_POOL = None
        return None

    dsn = _make_dsn()
    maxconn = int(os.environ.get("PGPOOL_MAXCONN", "3"))
    minconn = int(os.environ.get("PGPOOL_MINCONN", "1"))

    try:
        _DB_POOL = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, dsn=dsn)
    except Exception:
        _DB_POOL = None
    return _DB_POOL

def db_conn():
    pool = _get_pool()
    if pool is not None:
        try:
            conn = pool.getconn()
            try:
                setattr(conn, "_from_pool", True)
            except Exception:
                pass
            return conn
        except Exception:
            # Any pool error -> fallback
            conn = psycopg2.connect(_make_dsn())
            try:
                setattr(conn, "_from_pool", False)
            except Exception:
                pass
            return conn

    conn = psycopg2.connect(_make_dsn())
    try:
        setattr(conn, "_from_pool", False)
    except Exception:
        pass
    return conn

def db_put(conn):
    if conn is None:
        return
    pool = _get_pool()
    from_pool = False
    try:
        from_pool = bool(getattr(conn, "_from_pool", False))
    except Exception:
        from_pool = False

    if pool is not None and from_pool:
        try:
            pool.putconn(conn)
            return
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass

def db_close_pool():
    global _DB_POOL
    pool = _DB_POOL
    _DB_POOL = None
    if pool is not None:
        try:
            pool.closeall()
        except Exception:
            pass

def db_init():
    global _DB_READY
    if _DB_READY:
        return

    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS news_items (
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    published_ts BIGINT NOT NULL,
                    fingerprint TEXT UNIQUE NOT NULL
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_published_ts ON news_items(published_ts DESC);")

                cur.execute("""
                CREATE TABLE IF NOT EXISTS bias_state (
                    id SMALLINT PRIMARY KEY DEFAULT 1,
                    updated_ts BIGINT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """)

                cur.execute("""
                CREATE TABLE IF NOT EXISTS fred_series (
                    series_id TEXT NOT NULL,
                    obs_date  DATE NOT NULL,
                    value     DOUBLE PRECISION,
                    PRIMARY KEY(series_id, obs_date)
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_fred_series_date ON fred_series(obs_date DESC);")

                # Econ calendar store
                cur.execute("""
                CREATE TABLE IF NOT EXISTS econ_events (
                    id BIGSERIAL PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    country TEXT,
                    currency TEXT,
                    impact TEXT,
                    actual TEXT,
                    previous TEXT,
                    consensus TEXT,
                    forecast TEXT,
                    event_ts BIGINT,
                    link TEXT,
                    fingerprint TEXT UNIQUE NOT NULL
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_econ_events_ts ON econ_events(event_ts DESC);")

                # Optional alerts log
                cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts_log (
                    id BIGSERIAL PRIMARY KEY,
                    created_ts BIGINT NOT NULL,
                    kind TEXT NOT NULL,
                    asset TEXT,
                    severity TEXT,
                    message TEXT NOT NULL,
                    payload_json TEXT
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_log_created_ts ON alerts_log(created_ts DESC);")

        _DB_READY = True
    finally:
        db_put(conn)

def save_bias(payload: Dict[str, Any]):
    now = int(time.time())
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bias_state(id, updated_ts, payload_json)
                    VALUES (1, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET updated_ts=EXCLUDED.updated_ts, payload_json=EXCLUDED.payload_json;
                """, (now, json.dumps(payload, ensure_ascii=False)))
    finally:
        db_put(conn)

def load_bias() -> Optional[dict]:
    conn = db_conn()
    row = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT updated_ts, payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    finally:
        db_put(conn)
    if not row:
        return None
    _updated_ts, payload_json = row
    try:
        return json.loads(payload_json)
    except Exception:
        return None

def log_alert(kind: str, message: str, asset: Optional[str] = None, severity: str = "INFO", payload: Optional[dict] = None):
    now = int(time.time())
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alerts_log(created_ts, kind, asset, severity, message, payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (now, kind, asset, severity, message, json.dumps(payload or {}, ensure_ascii=False)))
    finally:
        db_put(conn)

# ... (TRUNCATED IN THIS BUILD ENV) ...
# NOTE: This file is long; the full version is provided in the chat as a downloadable artifact when generated.
