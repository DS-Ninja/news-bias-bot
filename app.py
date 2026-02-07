# app.py
# NEWS BIAS // TERMINAL (Bloomberg-ish) — RSS + Postgres + Bias/Quality + Trade Gate + Calendar + Alerts + Ticker
# - UI: 3-level layout: (1) Live prices, (2) Alerts panel, (3) Top News panel
# - MyFX calendar: forced dark mode via iframe CSS filter (cross-origin safe)
# - Assets: IC Markets set: US500 / XAUUSD / EURUSD / XTIUSD / BTCUSD
# - Keeps: RSS ingest, calendar ingest/store, alerts diff engine, event mode logic
# - Adds: /top_news endpoint + embedded Top News panel
# - Keeps: modals, RUN token localStorage, /myfx_calendar route

import os
import json
import time
import re
import hashlib
import math
import calendar
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2

# Optional dependency: requests (only if FRED enabled + key present)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # graceful: FRED disabled if requests missing

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse

# ============================================================
# CONFIG
# ============================================================

# Your IC Markets instruments (engine symbols are internal labels; UI shows IC tickers)
ASSETS = ["US500", "XAU", "EURUSD", "XTI", "BTC"]

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
BIAS_THRESH = {
    "US500": 1.20,
    "XAU": 0.90,
    "EURUSD": 0.80,
    "XTI": 0.90,
    "BTC": 1.10,
}

TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": float(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 6,
}

# Alerts thresholds (server diff)
ALERT_CFG = {
    "enabled": True,
    "q2_drop": int(os.environ.get("ALERT_Q2_DROP", "12")),              # points
    "conflict_spike": float(os.environ.get("ALERT_CONFLICT_SPIKE", "0.18")),  # delta
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

# Token for /run GET/POST (cron)
RUN_TOKEN = os.environ.get("RUN_TOKEN", "").strip()

# MyFXBook widget iframe
# NOTE: "symbols" here is MyFX widget param, not tradingview. It typically accepts currencies.
MYFXBOOK_IFRAME_SRC = "https://widget.myfxbook.com/widget/calendar.html?lang=en&impacts=0,1,2,3&symbols=USD,EUR"

# TradingView ticker tape symbols — using IC Markets where possible (as you requested)
TV_TICKER_SYMBOLS = [
    {"proName": "ICMARKETS:XAUUSD", "title": "XAUUSD"},
    {"proName": "ICMARKETS:EURUSD", "title": "EURUSD"},
    {"proName": "ICMARKETS:US500",  "title": "US500"},
    {"proName": "ICMARKETS:XTIUSD", "title": "XTIUSD"},
    {"proName": "ICMARKETS:BTCUSD", "title": "BTCUSD"},
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

    # Myfxbook RSS
    "MYFX_CAL": "https://www.myfxbook.com/rss/forex-economic-calendar-events",
    "MYFX_NEWS": "https://www.myfxbook.com/rss/latest-forex-news",
    "MYFX_COMM": "https://www.myfxbook.com/rss/forex-community-recent-topics",
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

    "FRED": 1.0,
}

# ============================================================
# RULES (regex -> weight -> why)
# ============================================================

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
    "XTI": [
        (r"\b(crude|oil|wti|brent)\b", +0.1, "Oil headlines (generic)"),
        (r"\b(opec|output cut|cuts output|production cut)\b", +0.8, "Supply cuts support oil"),
        (r"\b(output increase|ramp up production|supply increase)\b", -0.6, "Supply increase pressures oil"),
        (r"\b(inventories rise|stockpile build|build in stocks|inventory build)\b", -0.8, "Inventories build pressures oil"),
        (r"\b(inventories fall|draw in stocks|inventory draw)\b", +0.8, "Inventory draw supports oil"),
        (r"\b(disruption|outage|pipeline|attack|sanctions|shipping disruption)\b", +0.7, "Supply disruption supports oil"),
        (r"\b(demand weakens|recession fears|slowdown)\b", -0.6, "Demand concerns pressure oil"),
        (r"\b(us dollar strengthens|dollar rises)\b", -0.2, "Stronger USD can pressure oil (macro headwind)"),
    ],
    "EURUSD": [
        (r"\b(ecb|lagarde)\b", +0.25, "ECB headline (direction depends on tone)"),
        (r"\b(dovish ecb|ecb cut|rate cuts in europe)\b", -0.55, "Dovish ECB pressures EUR"),
        (r"\b(hawkish ecb|ecb hike|rates higher in europe)\b", +0.55, "Hawkish ECB supports EUR"),
        (r"\b(hawkish fed|fed hike|rates higher)\b", -0.55, "Hawkish Fed supports USD (bear EURUSD)"),
        (r"\b(dovish fed|fed cut|easing)\b", +0.50, "Dovish Fed pressures USD (bull EURUSD)"),
        (r"\b(us cpi|us inflation|nfp|jobs report)\b", -0.20, "US macro print (often USD-vol driver)"),
        (r"\b(risk-off|flight to safety)\b", -0.15, "Risk-off can support USD (bear EURUSD)"),
        (r"\b(risk-on)\b", +0.10, "Risk-on can weigh USD (mild bull EURUSD)"),
    ],
    "BTC": [
        (r"\b(bitcoin|btc|crypto)\b", +0.15, "Crypto headline (generic)"),
        (r"\b(etf inflow|spot etf inflow|institutional demand)\b", +0.75, "ETF/institutional demand supports BTC"),
        (r"\b(etf outflow|spot etf outflow)\b", -0.65, "ETF outflows pressure BTC"),
        (r"\b(regulation crackdown|sec lawsuit|ban|illegal)\b", -0.70, "Regulatory risk pressures BTC"),
        (r"\b(rate cuts|liquidity|easing)\b", +0.35, "Easing/liquidity supports risk assets incl. BTC"),
        (r"\b(hawkish|rates higher|yields rise)\b", -0.35, "Higher rates/yields pressure risk assets incl. BTC"),
        (r"\b(risk-off|equities selloff|panic)\b", -0.25, "Risk-off tends to pressure BTC (not always)"),
    ],
}

# ============================================================
# DB
# ============================================================

def db_conn():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if db_url:
        return psycopg2.connect(db_url)

    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "postgres")
    user = os.environ.get("PGUSER", "postgres")
    pwd = os.environ.get("PGPASSWORD", "")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)

def db_init():
    with db_conn() as conn:
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

        conn.commit()

def save_bias(payload: Dict[str, Any]):
    now = int(time.time())
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bias_state(id, updated_ts, payload_json)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET updated_ts=EXCLUDED.updated_ts, payload_json=EXCLUDED.payload_json;
            """, (now, json.dumps(payload, ensure_ascii=False)))
        conn.commit()

def load_bias() -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT updated_ts, payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    if not row:
        return None
    _updated_ts, payload_json = row
    try:
        return json.loads(payload_json)
    except Exception:
        return None

def log_alert(kind: str, message: str, asset: Optional[str] = None, severity: str = "INFO", payload: Optional[dict] = None):
    now = int(time.time())
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts_log(created_ts, kind, asset, severity, message, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (now, kind, asset, severity, message, json.dumps(payload or {}, ensure_ascii=False)))
        conn.commit()

# ============================================================
# HELPERS
# ============================================================

def fingerprint(title: str, link: str) -> str:
    s = (title or "").strip() + "||" + (link or "").strip()
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def decay_weight(age_sec: int) -> float:
    age_sec = max(0, int(age_sec))
    return math.exp(-LAMBDA * float(age_sec))

def _fresh_bucket(age_sec: int) -> str:
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"

def match_rules(asset: str, title: str) -> List[Dict[str, Any]]:
    out = []
    t = (title or "")
    for (pat, w, why) in RULES.get(asset, []):
        if re.search(pat, t, flags=re.I):
            out.append({"pattern": pat, "w": float(w), "why": why})
    return out

def _ts_from_entry(e: dict, now_ts: int) -> int:
    pp = e.get("published_parsed")
    if pp:
        try:
            return int(calendar.timegm(pp))
        except Exception:
            return int(now_ts)
    up = e.get("updated_parsed")
    if up:
        try:
            return int(calendar.timegm(up))
        except Exception:
            return int(now_ts)
    return int(now_ts)

def feeds_health_live() -> Dict[str, Any]:
    res = {}
    for src, url in RSS_FEEDS.items():
        try:
            if src == "TRUMP_HEADLINES" and not TRUMP_ENABLED:
                res[src] = {"ok": True, "skipped": True, "entries": 0}
                continue
            d = feedparser.parse(url)
            entries = getattr(d, "entries", []) or []
            res[src] = {"ok": True, "bozo": int(getattr(d, "bozo", 0)), "entries": int(len(entries))}
        except Exception as e:
            res[src] = {"ok": False, "error": str(e), "entries": 0}
    return res

def _feeds_ok_ratio(feeds_status: dict) -> float:
    ok = 0
    total = 0
    for k in RSS_FEEDS.keys():
        total += 1
        obj = (feeds_status.get(k, {}) or {})
        if obj.get("skipped"):
            ok += 1
        elif obj.get("ok") and int(obj.get("entries", 0)) >= 0:
            ok += 1
    if total <= 0:
        return 1.0
    return ok / total

# ============================================================
# INGEST (NEWS)
# ============================================================

def ingest_news_once(limit_per_feed: int = 40) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                if src in CALENDAR_FEEDS:
                    continue
                if src == "TRUMP_HEADLINES" and not TRUMP_ENABLED:
                    continue

                try:
                    d = feedparser.parse(url)
                except Exception:
                    continue

                entries = getattr(d, "entries", []) or []
                if int(getattr(d, "bozo", 0)) == 1 and len(entries) == 0:
                    continue

                for e in entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    if not title or not link:
                        continue

                    published_ts = _ts_from_entry(e, now)
                    fp = fingerprint(title, link)

                    try:
                        cur.execute("""
                            INSERT INTO news_items(source, title, link, published_ts, fingerprint)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (fingerprint) DO NOTHING;
                        """, (src, title, link, int(published_ts), fp))
                        if cur.rowcount == 1:
                            inserted += 1
                    except Exception:
                        pass
        conn.commit()

    return inserted

# ============================================================
# INGEST (CALENDAR)
# ============================================================

_IMPACT_PAT = re.compile(r"\b(high|medium|low)\b", re.I)
_CC_PAT = re.compile(r"\b(US|EU|DE|FR|ES|UK|GB|JP|CN|AU|CA|CH|NZ)\b")
_CUR_PAT = re.compile(r"\b(USD|EUR|GBP|JPY|CHF|AUD|CAD|NZD|CNY)\b")

def _parse_calendar_fields(title: str, summary: str) -> Dict[str, Optional[str]]:
    t = (title or "").strip()
    s = (summary or "").strip()
    blob = (t + " " + s).strip()

    impact = None
    m = _IMPACT_PAT.search(blob)
    if m:
        impact = m.group(1).upper()

    country = None
    m = _CC_PAT.search(blob)
    if m:
        country = m.group(1).upper()

    currency = None
    m = _CUR_PAT.search(blob)
    if m:
        currency = m.group(1).upper()

    def pick(pat: str) -> Optional[str]:
        mm = re.search(pat, blob, flags=re.I)
        if mm:
            return mm.group(1).strip()
        return None

    actual = pick(r"\bactual[:=]\s*([^\s|,;]+)")
    previous = pick(r"\bprev(?:ious)?[:=]\s*([^\s|,;]+)")
    consensus = pick(r"\bcons(?:ensus)?[:=]\s*([^\s|,;]+)")
    forecast = pick(r"\bfore(?:cast)?[:=]\s*([^\s|,;]+)")

    return {
        "impact": impact,
        "country": country,
        "currency": currency,
        "actual": actual,
        "previous": previous,
        "consensus": consensus,
        "forecast": forecast,
    }

def ingest_calendar_once(limit_per_feed: int = 200) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src in CALENDAR_FEEDS:
                url = RSS_FEEDS.get(src)
                if not url:
                    continue
                try:
                    d = feedparser.parse(url)
                except Exception:
                    continue

                entries = getattr(d, "entries", []) or []
                if int(getattr(d, "bozo", 0)) == 1 and len(entries) == 0:
                    continue

                for e in entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    summary = (e.get("summary") or e.get("description") or "").strip()
                    if not title:
                        continue

                    event_ts = None
                    pp = e.get("published_parsed") or e.get("updated_parsed")
                    if pp:
                        try:
                            event_ts = int(calendar.timegm(pp))
                        except Exception:
                            event_ts = None

                    fields = _parse_calendar_fields(title, summary)
                    fp = fingerprint(f"{src}||{title}", link or title)

                    try:
                        cur.execute("""
                            INSERT INTO econ_events(
                                source, title, country, currency, impact,
                                actual, previous, consensus, forecast,
                                event_ts, link, fingerprint
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (fingerprint) DO NOTHING;
                        """, (
                            src, title,
                            fields.get("country"), fields.get("currency"), fields.get("impact"),
                            fields.get("actual"), fields.get("previous"), fields.get("consensus"), fields.get("forecast"),
                            event_ts, link, fp
                        ))
                        if cur.rowcount == 1:
                            inserted += 1
                    except Exception:
                        pass
        conn.commit()

    return inserted

# ============================================================
# EVENT MODE
# ============================================================

def _macro_recent_flag(rows: List[Tuple[str, str, str, int]], now_ts: int) -> bool:
    recent_sec = int(EVENT_CFG["recent_hours"] * 3600)
    macro_sources = {"FED", "BLS", "BEA"}
    for (source, _title, _link, ts) in rows:
        if source in macro_sources and (now_ts - int(ts)) <= recent_sec:
            return True
    return False

def _get_upcoming_events(now_ts: int) -> List[Dict[str, Any]]:
    lookahead_sec = int(EVENT_CFG["lookahead_hours"] * 3600)
    horizon = now_ts + lookahead_sec

    out: List[Dict[str, Any]] = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, event_ts
                FROM econ_events
                WHERE event_ts IS NOT NULL AND event_ts BETWEEN %s AND %s
                ORDER BY event_ts ASC
                LIMIT %s;
            """, (now_ts, horizon, int(EVENT_CFG["max_upcoming"])))
            rows = cur.fetchall()

    for (source, title, link, ts) in rows:
        out.append({
            "source": source,
            "title": title,
            "link": link,
            "ts": int(ts),
            "in_hours": round((int(ts) - now_ts) / 3600.0, 2),
        })

    if out:
        return out[: int(EVENT_CFG["max_upcoming"])]

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, event_ts
                FROM econ_events
                ORDER BY COALESCE(event_ts, 0) DESC, id DESC
                LIMIT 2;
            """)
            rows2 = cur.fetchall()

    unknown = []
    for (source, title, link, ts) in rows2:
        unknown.append({"source": source, "title": title, "link": link, "ts": int(ts) if ts else None, "in_hours": None})
    return unknown[:2]

def trump_flag_recent(rows: List[Tuple[str, str, str, int]], now_ts: int, hours: float = 12.0) -> Dict[str, Any]:
    cutoff = now_ts - int(hours * 3600)
    hits = []
    for (source, title, link, ts) in rows:
        if int(ts) < cutoff:
            continue
        if source == "TRUMP_HEADLINES" or TRUMP_PAT.search(title or ""):
            hits.append({"source": source, "title": title, "link": link, "published_ts": int(ts)})
            if len(hits) >= 6:
                break
    return {"enabled": bool(TRUMP_ENABLED), "flag": bool(len(hits) > 0), "items": hits}

# ============================================================
# FRED
# ============================================================

def fred_fetch_observations(series_id: str, days: int = 120) -> List[Tuple[str, Optional[float]]]:
    if not (FRED_CFG["enabled"] and FRED_CFG["api_key"] and requests is not None):
        return []
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_CFG["api_key"],
        "file_type": "json",
        "sort_order": "desc",
        "limit": max(60, min(5000, days * 2)),
    }
    r = requests.get(url, params=params, timeout=12)
    r.raise_for_status()
    js = r.json()
    out = []
    for o in js.get("observations", []):
        d = o.get("date")
        v = o.get("value")
        if v in (None, ".", ""):
            out.append((d, None))
        else:
            try:
                out.append((d, float(v)))
            except Exception:
                out.append((d, None))
    return out

def fred_ingest_series(series_id: str, days: int = 120) -> int:
    obs = fred_fetch_observations(series_id, days=days)
    if not obs:
        return 0
    inserted = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            for d, v in obs:
                if not d:
                    continue
                cur.execute("""
                    INSERT INTO fred_series(series_id, obs_date, value)
                    VALUES (%s, %s::date, %s)
                    ON CONFLICT (series_id, obs_date) DO UPDATE SET value=EXCLUDED.value;
                """, (series_id, d, v))
                inserted += 1
        conn.commit()
    return inserted

def fred_last_values(series_id: str, n: int = 90) -> List[float]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value FROM fred_series
                WHERE series_id=%s AND value IS NOT NULL
                ORDER BY obs_date DESC
                LIMIT %s;
            """, (series_id, n))
            rows = cur.fetchall()
    return [float(x[0]) for x in rows if x and x[0] is not None]

def _pct_change(latest: float, past: float) -> float:
    den = abs(past) if abs(past) > 1e-12 else 1.0
    return (latest - past) / den

def compute_fred_drivers() -> Dict[str, Any]:
    # FRED drivers are macro — we keep for XAU/US500/XTI and allow light touch for EURUSD/BTC
    out = {a: [] for a in ASSETS}

    def add(asset: str, key: str, w: float, note: str, value: float, delta: float):
        out[asset].append({
            "driver": key,
            "w": round(float(w), 4),
            "value": round(float(value), 4),
            "delta": round(float(delta), 6),
            "why": note,
        })

    dgs10  = fred_last_values("DGS10", 90)
    dfii10 = fred_last_values("DFII10", 90)
    t10yie = fred_last_values("T10YIE", 90)
    usd    = fred_last_values("DTWEXBGS", 120)
    vix    = fred_last_values("VIXCLS", 120)
    baa    = fred_last_values("BAA10Y", 120)

    if len(dfii10) >= 6:
        latest, past = dfii10[0], dfii10[5]
        d = latest - past
        add("XAU", "DFII10", w=(-1.2 if d > 0 else +1.0), note="Real yields move (DFII10)", value=latest, delta=d)

    if len(dgs10) >= 6:
        latest, past = dgs10[0], dgs10[5]
        d = latest - past
        add("XAU", "DGS10", w=(-0.7 if d > 0 else +0.5), note="Nominal yields move (DGS10)", value=latest, delta=d)

    if len(usd) >= 6:
        latest, past = usd[0], usd[5]
        d = _pct_change(latest, past)
        add("XAU", "DTWEXBGS", w=(-0.9 if d > 0 else +0.7), note="USD broad index move", value=latest, delta=d)

    if len(t10yie) >= 6:
        latest, past = t10yie[0], t10yie[5]
        d = latest - past
        add("XAU", "T10YIE", w=(+0.4 if d > 0 else -0.2), note="Inflation expectations move (T10YIE)", value=latest, delta=d)

    if len(dgs10) >= 6:
        latest, past = dgs10[0], dgs10[5]
        d = latest - past
        add("US500", "DGS10", w=(-0.9 if d > 0 else +0.5), note="Rates pressure/support equities", value=latest, delta=d)

    if len(vix) >= 6:
        latest, past = vix[0], vix[5]
        d = _pct_change(latest, past)
        add("US500", "VIXCLS", w=(-1.0 if d > 0 else +0.4), note="VIX risk regime shift", value=latest, delta=d)

    if len(baa) >= 6:
        latest, past = baa[0], baa[5]
        d = latest - past
        add("US500", "BAA10Y", w=(-0.6 if d > 0 else +0.3), note="Credit stress proxy", value=latest, delta=d)

    if len(usd) >= 6:
        latest, past = usd[0], usd[5]
        d = _pct_change(latest, past)
        add("XTI", "DTWEXBGS", w=(-0.4 if d > 0 else +0.2), note="USD headwind/tailwind", value=latest, delta=d)

    # EURUSD: broad USD index as light macro tilt
    if len(usd) >= 6:
        latest, past = usd[0], usd[5]
        d = _pct_change(latest, past)
        add("EURUSD", "DTWEXBGS", w=(-0.35 if d > 0 else +0.25), note="USD broad index tilt (EURUSD inverse)", value=latest, delta=d)

    # BTC: VIX as weak regime proxy
    if len(vix) >= 6:
        latest, past = vix[0], vix[5]
        d = _pct_change(latest, past)
        add("BTC", "VIXCLS", w=(-0.25 if d > 0 else +0.15), note="VIX risk regime proxy (weak)", value=latest, delta=d)

    return out

# ============================================================
# QUALITY / CONSENSUS
# ============================================================

def _consensus_stats(contribs: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    net = sum(float(x.get("contrib", 0.0)) for x in contribs)
    abs_sum = sum(abs(float(x.get("contrib", 0.0))) for x in contribs)
    if abs_sum <= 1e-12:
        return 0.0, 1.0, 0.0
    consensus_ratio = abs(net) / abs_sum
    conflict_index = 1.0 - consensus_ratio
    return round(consensus_ratio, 4), round(conflict_index, 4), round(abs_sum, 4)

def _top_drivers(contribs: List[Dict[str, Any]], topn: int = 3) -> List[Dict[str, Any]]:
    acc: Dict[str, float] = {}
    for x in contribs:
        why = str(x.get("why", ""))
        c = float(x.get("contrib", 0.0))
        acc[why] = acc.get(why, 0.0) + abs(c)
    out = [{"why": k, "abs_contrib_sum": round(v, 4)} for k, v in acc.items()]
    out.sort(key=lambda x: x["abs_contrib_sum"], reverse=True)
    return out[:topn]

def _consensus_by_source(contribs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    acc: Dict[str, Dict[str, float]] = {}
    cnt: Dict[str, int] = {}
    for x in contribs:
        src = str(x.get("source", ""))
        c = float(x.get("contrib", 0.0))
        if src not in acc:
            acc[src] = {"net": 0.0, "abs": 0.0}
            cnt[src] = 0
        acc[src]["net"] += c
        acc[src]["abs"] += abs(c)
        cnt[src] += 1
    out = [{"source": src, "net": round(v["net"], 4), "abs": round(v["abs"], 4), "count": int(cnt[src])} for src, v in acc.items()]
    out.sort(key=lambda x: x["abs"], reverse=True)
    return out

def _median_abs_contrib(contribs: List[Dict[str, Any]]) -> float:
    vals = sorted([abs(float(x.get("contrib", 0.0))) for x in contribs if abs(float(x.get("contrib", 0.0))) > 1e-12])
    if not vals:
        return 0.0
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        return float(vals[mid])
    return 0.5 * float(vals[mid - 1] + vals[mid])

# ============================================================
# FLIP METRICS
# ============================================================

def flip_metrics(score: float, th: float, bias: str, median_abs: float) -> Dict[str, Any]:
    to_bull = max(0.0, th - score)
    to_bear = max(0.0, score + th)

    if bias == "BULLISH":
        to_neutral = max(0.0, score - th)
        to_opposite = score + th
    elif bias == "BEARISH":
        to_neutral = max(0.0, -th - score)
        to_opposite = th - score
    else:
        to_neutral = 0.0
        to_opposite = min(to_bull, to_bear)

    approx = None
    if median_abs > 1e-12:
        approx = round(float(to_opposite) / float(median_abs), 2)

    return {
        "state": bias,
        "to_bullish": round(to_bull, 4),
        "to_bearish": round(to_bear, 4),
        "to_neutral": round(float(to_neutral), 4),
        "to_opposite": round(float(to_opposite), 4),
        "median_abs_contrib": round(float(median_abs), 4),
        "approx_headlines_to_flip": approx,
        "note": "Units: score-sum (weighted evidence). Not price points.",
    }

# ============================================================
# BIAS COMPUTE
# ============================================================

def compute_bias(lookback_hours: int = 24, limit_rows: int = 1400) -> Dict[str, Any]:
    now = int(time.time())
    cutoff = now - lookback_hours * 3600

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT %s;
            """, (cutoff, limit_rows))
            rows: List[Tuple[str, str, str, int]] = cur.fetchall()

    # FRED ingest + drivers
    fred_inserted = 0
    fred_drivers = {a: [] for a in ASSETS}
    fred_on = bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"]) and requests is not None)
    if fred_on:
        for sid in FRED_SERIES.keys():
            try:
                fred_inserted += fred_ingest_series(sid, days=FRED_CFG["window_days"])
            except Exception:
                pass
        try:
            fred_drivers = compute_fred_drivers()
        except Exception:
            fred_drivers = {a: [] for a in ASSETS}

    # Event mode
    upcoming_events = _get_upcoming_events(now) if EVENT_CFG["enabled"] else []
    recent_macro = _macro_recent_flag(rows, now)
    has_timed_event = any(x.get("ts") is not None for x in upcoming_events)
    event_mode = bool(EVENT_CFG["enabled"] and (recent_macro or has_timed_event))

    event_reason: List[str] = []
    if not EVENT_CFG["enabled"]:
        event_reason.append("disabled")
    else:
        if recent_macro:
            event_reason.append(f"recent_macro<= {EVENT_CFG['recent_hours']}h")
        if has_timed_event:
            event_reason.append(f"upcoming<= {EVENT_CFG['lookahead_hours']}h")
        if not event_reason:
            if upcoming_events and not has_timed_event:
                event_reason.append("calendar_time_unknown")
            else:
                event_reason.append("no_macro_no_upcoming")

    trump = trump_flag_recent(rows, now, hours=12.0)

    assets_out: Dict[str, Any] = {}
    for asset in ASSETS:
        score = 0.0
        contribs: List[Dict[str, Any]] = []
        freshness = {"0-2h": 0, "2-8h": 0, "8-24h": 0}

        for (source, title, link, ts) in rows:
            age_sec = max(0, now - int(ts))
            w_src = float(SOURCE_WEIGHT.get(source, 1.0))
            w_time = decay_weight(age_sec)

            matches = match_rules(asset, title)
            if not matches:
                continue

            freshness[_fresh_bucket(age_sec)] += 1

            for m in matches:
                base_w = float(m["w"])
                contrib = base_w * w_src * w_time
                score += contrib
                contribs.append({
                    "source": source,
                    "title": title,
                    "link": link,
                    "published_ts": int(ts),
                    "age_min": int(age_sec / 60),
                    "base_w": base_w,
                    "src_w": round(w_src, 4),
                    "time_w": round(float(w_time), 4),
                    "contrib": round(float(contrib), 4),
                    "why": m["why"],
                    "pattern": m["pattern"],
                })

        # Inject FRED drivers
        for d in (fred_drivers.get(asset) or []):
            c = float(d.get("w", 0.0))
            if abs(c) < 1e-12:
                continue
            score += c
            contribs.append({
                "source": "FRED",
                "title": f'{d.get("driver","")} value={d.get("value","")} delta={d.get("delta","")}',
                "link": "https://fred.stlouisfed.org/",
                "published_ts": now,
                "age_min": 0,
                "base_w": float(d.get("w", 0.0)),
                "src_w": 1.0,
                "time_w": 1.0,
                "contrib": round(c, 4),
                "why": str(d.get("why", "FRED driver")),
                "pattern": "FRED",
            })

        th = float(BIAS_THRESH.get(asset, 1.0))
        if score >= th:
            bias = "BULLISH"
        elif score <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        evidence_count = len(contribs)
        src_div = len(set([w["source"] for w in contribs])) if contribs else 0

        consensus_ratio, conflict_index, _abs_sum = _consensus_stats(contribs)

        strength_2 = min(1.2, abs(score) / max(th, 1e-9))
        ev_term = min(1.0, evidence_count / 18.0)
        div_term = min(1.0, src_div / 7.0)

        fresh_total = sum(int(v) for v in freshness.values())
        fresh02 = freshness["0-2h"]
        fresh28 = freshness["2-8h"]
        fresh_score = 0.0
        if fresh_total > 0:
            fresh_score = min(1.0, (fresh02 * 1.0 + fresh28 * 0.6) / max(1.0, fresh_total))

        event_penalty = 0.15 if event_mode else 0.0

        raw_v2 = (
            0.45 * min(1.0, strength_2) +
            0.20 * ev_term +
            0.10 * div_term +
            0.10 * fresh_score +
            0.15 * consensus_ratio -
            0.35 * conflict_index -
            event_penalty
        )
        quality_v2 = int(max(0, min(100, round(raw_v2 * 100))))

        why_top5 = sorted(contribs, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]
        top3 = _top_drivers(contribs, topn=3)
        cons_by_src = _consensus_by_source(contribs)
        med_abs = _median_abs_contrib(contribs)
        flip = flip_metrics(score, th, bias, med_abs)

        assets_out[asset] = {
            "bias": bias,
            "score": round(score, 4),
            "threshold": th,
            "quality_v2": int(quality_v2),
            "evidence_count": int(evidence_count),
            "source_diversity": int(src_div),
            "consensus_ratio": float(consensus_ratio),
            "conflict_index": float(conflict_index),
            "freshness": freshness,
            "top3_drivers": top3,
            "flip": flip,
            "consensus_by_source": cons_by_src,
            "why_top5": why_top5,
        }

    payload: Dict[str, Any] = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
            "gate_profile": GATE_PROFILE,
            "fred": {
                "enabled": bool(fred_on),
                "inserted_points_last_run": int(fred_inserted),
                "series": list(FRED_SERIES.keys()),
                "requests_present": bool(requests is not None),
            },
            "trump": trump,
            "run_token_required": bool(RUN_TOKEN),
        },
        "event": {
            "enabled": bool(EVENT_CFG["enabled"]),
            "event_mode": bool(event_mode),
            "reason": event_reason,
            "recent_macro": bool(recent_macro),
            "upcoming_events": upcoming_events,
            "lookahead_hours": float(EVENT_CFG["lookahead_hours"]),
            "recent_hours": float(EVENT_CFG["recent_hours"]),
            "calendar_sources": list(CALENDAR_FEEDS),
        }
    }
    return payload

# ============================================================
# ALERT ENGINE (server-side diff)
# ============================================================

def compute_alerts(prev: Optional[dict], cur: dict) -> List[Dict[str, Any]]:
    if not ALERT_CFG.get("enabled", True):
        return []

    alerts: List[Dict[str, Any]] = []
    now = int(time.time())

    def push(kind: str, message: str, asset: Optional[str] = None, severity: str = "INFO", extra: Optional[dict] = None):
        obj = {
            "id": f"{now}-{kind}-{asset or 'ALL'}-{hashlib.md5(message.encode('utf-8')).hexdigest()[:8]}",
            "ts": now,
            "kind": kind,
            "asset": asset,
            "severity": severity,
            "message": message,
            "extra": extra or {},
        }
        alerts.append(obj)
        try:
            log_alert(kind, message, asset=asset, severity=severity, payload=obj)
        except Exception:
            pass

    if not prev:
        return alerts

    p_assets = (prev.get("assets", {}) or {})
    c_assets = (cur.get("assets", {}) or {})

    p_ev = bool((prev.get("event", {}) or {}).get("event_mode", False))
    c_ev = bool((cur.get("event", {}) or {}).get("event_mode", False))
    if p_ev != c_ev:
        push("event_mode_flip", f"EVENT MODE {'ON' if c_ev else 'OFF'}", asset=None, severity="WARN",
             extra={"prev": p_ev, "cur": c_ev, "reason": (cur.get("event", {}) or {}).get("reason", [])})

    q2_drop_th = int(ALERT_CFG.get("q2_drop", 12))
    conflict_spike_th = float(ALERT_CFG.get("conflict_spike", 0.18))

    for a in ASSETS:
        pa = (p_assets.get(a, {}) or {})
        ca = (c_assets.get(a, {}) or {})

        pb = str(pa.get("bias", "NEUTRAL"))
        cb = str(ca.get("bias", "NEUTRAL"))
        if pb != cb:
            push("bias_flip", f"{a} bias flip: {pb} → {cb}", asset=a, severity="WARN",
                 extra={"prev": pb, "cur": cb, "score": ca.get("score"), "q2": ca.get("quality_v2"), "conflict": ca.get("conflict_index")})

        pq2 = int(pa.get("quality_v2", 0))
        cq2 = int(ca.get("quality_v2", 0))
        if (pq2 - cq2) >= q2_drop_th:
            push("q2_drop", f"{a} Q2 drop: {pq2} → {cq2} (Δ={pq2-cq2})", asset=a, severity="WARN",
                 extra={"prev": pq2, "cur": cq2})

        pc = float(pa.get("conflict_index", 0.0))
        cc = float(ca.get("conflict_index", 0.0))
        if (cc - pc) >= conflict_spike_th:
            push("conflict_spike", f"{a} conflict spike: {pc:.3f} → {cc:.3f} (Δ={(cc-pc):.3f})", asset=a, severity="WARN",
                 extra={"prev": pc, "cur": cc})

    p_meta = (prev.get("meta", {}) or {})
    c_meta = (cur.get("meta", {}) or {})
    p_fs = (p_meta.get("feeds_status", {}) or {})
    c_fs = (c_meta.get("feeds_status", {}) or {})
    if c_fs:
        pr = _feeds_ok_ratio(p_fs) if p_fs else 1.0
        cr = _feeds_ok_ratio(c_fs)
        thr = float(ALERT_CFG.get("feeds_degraded_ratio", 0.80))
        if pr >= thr and cr < thr:
            push("feeds_degraded", f"FEEDS degraded: ok_ratio {pr:.2f} → {cr:.2f}", asset=None, severity="WARN",
                 extra={"prev_ratio": pr, "cur_ratio": cr})

    return alerts

# ============================================================
# PIPELINE RUN
# ============================================================

def pipeline_run():
    db_init()

    prev = load_bias()

    inserted_news = ingest_news_once(limit_per_feed=40)
    inserted_cal = ingest_calendar_once(limit_per_feed=250)

    payload = compute_bias(lookback_hours=24, limit_rows=1400)

    feeds_status = feeds_health_live()

    payload["meta"]["inserted_news_last_run"] = int(inserted_news)
    payload["meta"]["inserted_calendar_last_run"] = int(inserted_cal)
    payload["meta"]["feeds_status"] = feeds_status

    alerts = compute_alerts(prev, payload)
    payload["meta"]["alerts"] = alerts[:25]

    save_bias(payload)
    return payload

# ============================================================
# TRADE GATE
# ============================================================

def eval_trade_gate(asset_obj: Dict[str, Any], event_mode: bool, profile: str) -> Dict[str, Any]:
    cfg = GATE_THRESHOLDS.get(profile, GATE_THRESHOLDS["STRICT"])

    bias = str(asset_obj.get("bias", "NEUTRAL"))
    q2 = int(asset_obj.get("quality_v2", 0))
    conflict = float(asset_obj.get("conflict_index", 1.0))
    flip = asset_obj.get("flip", {}) or {}
    to_opp = float(flip.get("to_opposite", 999.0))

    fail = []
    fail_short = []
    must = []

    if bias == "NEUTRAL" and not cfg.get("neutral_allow", False):
        fail.append("Bias NEUTRAL")
        fail_short.append("Bias=NEUTRAL")
        must.append("bias must become BULLISH or BEARISH (cross ±th)")
    elif bias == "NEUTRAL" and cfg.get("neutral_allow", False):
        maxd = float(cfg.get("neutral_flip_dist_max", 0.20))
        if to_opp > maxd:
            fail.append("Bias NEUTRAL not near flip")
            fail_short.append("Neutral far")
            must.append(f"to_opposite ≤ {maxd}")

    qmin = int(cfg["quality_v2_min"])
    if q2 < qmin:
        fail.append(f"Q2 too low ({q2} < {qmin})")
        fail_short.append(f"Q2<{qmin}")
        must.append(f"quality_v2 ≥ {qmin}")

    cmax = float(cfg["conflict_max"])
    if conflict > cmax:
        fail.append(f"Conflict too high ({conflict} > {cmax})")
        fail_short.append(f"Conflict>{cmax}")
        must.append(f"conflict_index ≤ {cmax}")

    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH") and to_opp < mind:
        fail.append(f"Too close to opposite flip (to_opposite={round(to_opp,4)} < {mind})")
        fail_short.append("Flip too close")
        must.append(f"to_opposite ≥ {mind}")

    if event_mode:
        if cfg.get("event_mode_block", True):
            oq = int(cfg.get("event_override_quality", 70))
            oc = float(cfg.get("event_override_conflict", 0.45))
            if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
                fail.append("Event mode ON")
                fail_short.append("EVENT")
                must.append(f"Wait OR require Q2≥{oq} & Conflict≤{oc} & bias!=NEUTRAL")

    ok = (len(fail) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:3] if x.get("why")] or ["Insufficient matched evidence"]

    return {
        "ok": bool(ok),
        "label": "TRADE OK" if ok else "NO TRADE",
        "why": why_short[:3],
        "fail_reasons": fail[:5],
        "fail_short": fail_short[:4],
        "must_change": must[:4],
    }

# ============================================================
# TOP NEWS (strong headlines)
# ============================================================

def get_top_news(limit: int = 18, lookback_hours: int = 24) -> List[Dict[str, Any]]:
    """
    "Главные новости" = самые свежие + с высоким source_weight.
    Плюс: приоритет макро источникам и тем, что матчится хотя бы к одному активу (чтобы не был мусор).
    """
    now = int(time.time())
    cutoff = now - int(lookback_hours * 3600)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT %s;
            """, (cutoff, max(300, limit * 20)))
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for (source, title, link, ts) in rows:
        sw = float(SOURCE_WEIGHT.get(source, 1.0))
        age_min = int(max(0, now - int(ts)) / 60)
        # keep if it matches any asset rules OR it is macro top-tier
        matched_assets = []
        for a in ASSETS:
            if match_rules(a, title):
                matched_assets.append(a)
        if (not matched_assets) and source not in ("FED", "BLS", "BEA", "MARKETWATCH_REALTIME", "MARKETWATCH_TOP"):
            continue

        score = sw * decay_weight(max(0, now - int(ts)))
        out.append({
            "source": source,
            "title": title,
            "link": link,
            "published_ts": int(ts),
            "age_min": age_min,
            "src_w": round(sw, 2),
            "rank": round(score, 6),
            "matches": matched_assets[:4],
        })
        if len(out) >= limit:
            break

    return out

# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias // Terminal")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)

@app.get("/health")
def health(pretty: int = 0):
    out = {
        "ok": True,
        "gate_profile": GATE_PROFILE,
        "assets": ASSETS,
        "trump_enabled": bool(TRUMP_ENABLED),
        "fred_enabled": bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"]) and requests is not None),
        "calendar_sources": list(CALENDAR_FEEDS),
        "run_token_required": bool(RUN_TOKEN),
    }
    if pretty:
        return PlainTextResponse(json.dumps(out, indent=2), media_type="application/json")
    return out

@app.get("/diag")
def diag():
    db_init()
    env = {
        "has_DATABASE_URL": bool(os.environ.get("DATABASE_URL", "").strip()),
        "PGHOST": os.environ.get("PGHOST", ""),
        "PGDATABASE": os.environ.get("PGDATABASE", ""),
        "PGUSER": os.environ.get("PGUSER", ""),
        "PGSSLMODE": os.environ.get("PGSSLMODE", "prefer"),
        "FRED_enabled": bool(FRED_CFG["enabled"]),
        "requests_present": bool(requests is not None),
        "RUN_TOKEN_set": bool(RUN_TOKEN),
    }
    ok = True
    news_items = 0
    fred_points = 0
    has_bias_state = False
    econ_events = 0
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM news_items;")
                news_items = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fred_series;")
                fred_points = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM bias_state;")
                has_bias_state = int(cur.fetchone()[0]) > 0
                cur.execute("SELECT COUNT(*) FROM econ_events;")
                econ_events = int(cur.fetchone()[0])
    except Exception:
        ok = False

    return {"db": {"ok": ok, "news_items": news_items, "fred_points": fred_points, "has_bias_state": has_bias_state, "econ_events": econ_events}, "env": env}

@app.get("/rules")
def rules():
    return {"assets": ASSETS, "rules": RULES, "bias_thresholds": BIAS_THRESH}

@app.get("/bias")
def bias(pretty: int = 0):
    db_init()
    state = load_bias()
    if not state:
        state = pipeline_run()
    if pretty:
        return PlainTextResponse(json.dumps(state, ensure_ascii=False, indent=2), media_type="application/json")
    return JSONResponse(state)

def _auth_run(token: str) -> bool:
    if not RUN_TOKEN:
        return True
    return bool(token and token == RUN_TOKEN)

@app.get("/run")
def run_get(token: str = ""):
    if not _auth_run(token):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        payload = pipeline_run()
        return JSONResponse({"ok": True, "updated_utc": payload.get("updated_utc"), "meta": payload.get("meta", {})})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/run")
def run_post(token: str = ""):
    if RUN_TOKEN and not _auth_run(token):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        payload = pipeline_run()
        return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/latest")
def latest(limit: int = 40):
    db_init()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
    return {"items": [{"source": s, "title": t, "link": l, "published_ts": int(ts)} for (s, t, l, ts) in rows]}

@app.get("/top_news")
def top_news(limit: int = 18):
    db_init()
    items = get_top_news(limit=int(max(6, min(60, limit))), lookback_hours=24)
    return {"updated_utc": datetime.now(timezone.utc).isoformat(), "items": items}

@app.get("/calendar_data")
def calendar_data(limit: int = 120, hours_back: int = 12, hours_fwd: int = 48):
    db_init()
    now = int(time.time())
    lo = now - int(hours_back * 3600)
    hi = now + int(hours_fwd * 3600)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, country, currency, impact, actual, previous, consensus, forecast, event_ts, link
                FROM econ_events
                WHERE (event_ts IS NULL) OR (event_ts BETWEEN %s AND %s)
                ORDER BY COALESCE(event_ts, 0) ASC, id DESC
                LIMIT %s;
            """, (lo, hi, limit))
            rows = cur.fetchall()

    items = []
    for r in rows:
        (source, title, country, currency, impact, actual, previous, consensus, forecast, event_ts, link) = r
        items.append({
            "source": source,
            "title": title,
            "country": country,
            "currency": currency,
            "impact": impact,
            "actual": actual,
            "previous": previous,
            "consensus": consensus,
            "forecast": forecast,
            "event_ts": int(event_ts) if event_ts else None,
            "link": link,
        })
    return {"now_ts": now, "items": items}

@app.get("/morning_plan")
def morning_plan():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {}) or {}
    meta = payload.get("meta", {}) or {}
    event = payload.get("event", {}) or {}

    gate_profile = str(meta.get("gate_profile", GATE_PROFILE))
    event_mode = bool(event.get("event_mode", False))

    def pack(asset: str) -> Dict[str, Any]:
        a = assets.get(asset, {}) or {}
        gate = eval_trade_gate(a, event_mode, gate_profile)
        return {
            "asset": asset,
            "bias": a.get("bias"),
            "score": a.get("score"),
            "th": a.get("threshold"),
            "quality_v2": a.get("quality_v2"),
            "conflict": a.get("conflict_index"),
            "top_drivers": (a.get("top3_drivers", []) or [])[:3],
            "gate": gate,
            "flip": a.get("flip", {}),
        }

    out = {
        "updated_utc": payload.get("updated_utc"),
        "gate_profile": gate_profile,
        "event_mode": event_mode,
        "event_reason": event.get("reason", []),
        "upcoming_events": (event.get("upcoming_events", []) or [])[:6],
        "alerts": (meta.get("alerts", []) or [])[:25],
        "plan": {a: pack(a) for a in ASSETS}
    }
    return JSONResponse(out)

# MyFXBook calendar page (standalone) — dark via iframe filter (cross-origin safe)
@app.get("/myfx_calendar", response_class=HTMLResponse)
def myfx_calendar(dark: int = 1):
    # dark=1 applies invert/hue-rotate filter to flip white widget to dark look
    filt = "filter: invert(1) hue-rotate(180deg) saturate(1.2) contrast(1.05);" if int(dark) == 1 else ""
    html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MyFXBook Calendar</title>
<style>
html,body{{height:100%;margin:0;background:#070a0f;color:#d7e2ff;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial}}
.wrap{{height:100%;}}
iframe{{border:0;width:100%;height:100%; {filt} }}
.top{{position:fixed; top:10px; right:10px; z-index:10;}}
.btn{{background:#0f1724;border:1px solid rgba(255,255,255,.08);color:#d7e2ff;padding:8px 10px;border-radius:10px;font-family:ui-monospace,Menlo,Consolas; font-weight:900; cursor:pointer;}}
.btn:hover{{background:#162238;}}
</style>
</head><body>
<div class="top">
  <button class="btn" onclick="toggleDark()">{'DARK: on' if int(dark)==1 else 'DARK: off'}</button>
</div>
<div class="wrap">
<iframe id="f" src="{MYFXBOOK_IFRAME_SRC}" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
</div>
<script>
function toggleDark(){{
  const u = new URL(location.href);
  const cur = (u.searchParams.get('dark')||'1');
  u.searchParams.set('dark', cur==='1' ? '0' : '1');
  location.href = u.toString();
}}
</script>
</body></html>"""
    return HTMLResponse(html)

# ============================================================
# UI — Terminal
# ============================================================

def _pill_bias(b: str) -> str:
    if b == "BULLISH":
        return '<span class="pill bull">BULLISH</span>'
    if b == "BEARISH":
        return '<span class="pill bear">BEARISH</span>'
    return '<span class="pill neu">NEUTRAL</span>'

def _pill_gate(ok: bool) -> str:
    return '<span class="pill ok">TRADE OK</span>' if ok else '<span class="pill no">NO TRADE</span>'

def _fmt(x: Any, n: int = 3) -> str:
    try:
        return f"{float(x):.{n}f}"
    except Exception:
        return "—"

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {}) or {}
    meta = payload.get("meta", {}) or {}
    event = payload.get("event", {}) or {}
    feeds_status = meta.get("feeds_status", {}) or {}

    updated = payload.get("updated_utc", "")
    gate_profile = str(meta.get("gate_profile", GATE_PROFILE))
    event_mode = bool(event.get("event_mode", False))
    upcoming = (event.get("upcoming_events", []) or [])[:6]

    event_reason = [str(x) for x in (event.get("reason", []) or [])]
    reason_txt = ", ".join(event_reason) if event_reason else "—"

    trump = meta.get("trump", {}) or {}
    trump_flag = bool(trump.get("flag", False))
    trump_enabled = bool(trump.get("enabled", False))

    fred_on = bool(meta.get("fred", {}).get("enabled", False))

    feeds_ok_ratio = _feeds_ok_ratio(feeds_status) if feeds_status else 1.0
    feeds_ok = feeds_ok_ratio >= float(ALERT_CFG.get("feeds_degraded_ratio", 0.80))

    ev_pill = '<span class="pill warn">EVENT MODE</span>' if event_mode else '<span class="pill neu">EVENT: off</span>'
    if trump_enabled and trump_flag:
        tr_pill = '<span class="pill warn">TRUMP: flag</span>'
    elif trump_enabled:
        tr_pill = '<span class="pill neu">TRUMP: on</span>'
    else:
        tr_pill = '<span class="pill neu">TRUMP: off</span>'

    feeds_pill = f'<span class="pill {"ok" if feeds_ok else "no"}">FEEDS: {"ok" if feeds_ok else "degraded"} ({feeds_ok_ratio:.2f})</span>'
    fred_pill = '<span class="pill neu">FRED: on</span>' if fred_on else '<span class="pill neu">FRED: off</span>'

    next_event = "—"
    if upcoming:
        u = upcoming[0]
        if u.get("ts"):
            dt = datetime.fromtimestamp(int(u["ts"]), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            next_event = f"{dt} • {u.get('title','')}"
        else:
            next_event = f"(time unknown) • {u.get('title','')}"

    def row(asset: str) -> str:
        a = assets.get(asset, {}) or {}
        bias = str(a.get("bias", "NEUTRAL"))
        score = float(a.get("score", 0.0))
        th = float(a.get("threshold", 1.0))
        q2 = int(a.get("quality_v2", 0))
        conflict = float(a.get("conflict_index", 1.0))
        flip = a.get("flip", {}) or {}
        to_neu = float(flip.get("to_neutral", 0.0))
        to_opp = float(flip.get("to_opposite", 0.0))
        approx = flip.get("approx_headlines_to_flip", None)

        gate = eval_trade_gate(a, event_mode, gate_profile)
        fail_short = " | ".join(gate.get("fail_short", []) or []) if not gate.get("ok") else ""

        top = (a.get("top3_drivers", []) or [])
        top_driver = top[0].get("why", "—") if top else "—"

        approx_txt = f"{approx}" if approx is not None else "—"

        return f"""
        <tr class="r">
          <td class="sym">{asset}</td>
          <td>{_pill_bias(bias)}</td>
          <td class="num">{_fmt(score,3)}</td>
          <td class="num">{_fmt(th,3)}</td>
          <td class="num">{q2}</td>
          <td class="num">{_fmt(conflict,3)}</td>
          <td class="num">{_fmt(to_neu,3)}</td>
          <td class="num">{_fmt(to_opp,3)}</td>
          <td class="num">{approx_txt}</td>
          <td class="gate">{_pill_gate(bool(gate.get("ok")))} <span class="muted">{fail_short}</span></td>
          <td class="why">{top_driver}</td>
          <td class="act"><button class="btn" onclick="openView('{asset}')">View</button></td>
        </tr>
        """

    js_payload = json.dumps(payload, ensure_ascii=False)
    tv_symbols = json.dumps(TV_TICKER_SYMBOLS, ensure_ascii=False)

    TEMPLATE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <title>NEWS BIAS // TERMINAL</title>
  <style>
    :root {
      --bg:#070a0f;
      --panel:#0b111a;
      --line:rgba(255,255,255,.08);
      --text:#d7e2ff;
      --muted:#7b8aa7;
      --amber:#ffb000;
      --cyan:#00e5ff;
      --ok:#00ff6a;
      --warn:#ffb000;
      --no:#ff3b3b;
      --pillbg:rgba(255,255,255,.03);
      --btn:#0f1724;
      --btn2:#162238;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    }
    html,body{height:100%; margin:0; background:var(--bg); color:var(--text);}
    body{padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);}
    a{color:var(--cyan); text-decoration:none;}
    a:hover{text-decoration:underline;}
    .wrap{max-width:1250px; margin:0 auto; padding:14px;}
    .hdr{border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:12px;}
    .title{font-family:var(--mono); font-weight:900; letter-spacing:.8px;}
    .title b{color:var(--amber);}
    .sub{font-family:var(--mono); color:var(--muted); font-size:12px; margin-top:6px;}
    .bar{display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-top:10px;}
    .pill{font-family:var(--mono); font-size:12px; font-weight:900; padding:6px 10px; border-radius:999px; border:1px solid var(--line); background:var(--pillbg);}
    .bull{color:var(--ok); border-color: rgba(0,255,106,.25);}
    .bear{color:var(--no); border-color: rgba(255,59,59,.25);}
    .neu{color:#b8c3da;}
    .ok{color:var(--ok); border-color: rgba(0,255,106,.25);}
    .no{color:var(--no); border-color: rgba(255,59,59,.25);}
    .warn{color:var(--warn); border-color: rgba(255,176,0,.25);}
    .hotkeys{color:var(--muted); font-family:var(--mono); font-size:12px; margin-top:10px;}
    .btn{background:var(--btn); border:1px solid var(--line); color:var(--text); padding:9px 12px; border-radius:12px; cursor:pointer; font-family:var(--mono); font-weight:900;}
    .btn:hover{background:var(--btn2);}
    .btnrow{display:flex; gap:8px; flex-wrap:wrap; margin-top:10px;}
    .panel{background:var(--panel); border:1px solid var(--line); border-radius:16px; padding:12px; margin-top:12px;}
    table{width:100%; border-collapse:collapse; font-family:var(--mono);}
    th,td{border-top:1px solid var(--line); padding:10px 8px; font-size:12px; vertical-align:top;}
    th{color:var(--muted); font-weight:900;}
    .sym{color:var(--amber); font-weight:900;}
    .num{text-align:right; white-space:nowrap;}
    .gate{white-space:nowrap;}
    .why{max-width:420px;}
    .muted{color:var(--muted);}
    .act{text-align:right;}
    .row2{display:flex; gap:12px; flex-wrap:wrap;}
    .card{flex:1; min-width:320px;}
    .h2{font-family:var(--mono); font-weight:900; color:var(--muted); font-size:12px; margin-bottom:8px;}
    .kv{display:grid; grid-template-columns: 160px 1fr; gap:8px; font-family:var(--mono); font-size:12px;}
    .kv div{padding:6px 0; border-top:1px solid var(--line);}
    .kv .k{color:var(--muted);}
    .list{font-family:var(--mono); font-size:12px;}
    .item{padding:10px 0; border-top:1px solid var(--line);}
    .item .t{font-weight:900;}
    .item .m{color:var(--muted); margin-top:4px;}
    .item .lnk{margin-top:6px;}
    .toastwrap{position:fixed; right:14px; top:14px; z-index:9999; display:flex; flex-direction:column; gap:8px; max-width:520px; padding-top: env(safe-area-inset-top);}
    .toast{background:rgba(11,17,26,.95); border:1px solid var(--line); border-radius:14px; padding:10px 12px; font-family:var(--mono); font-size:12px;}
    .toast .k{color:var(--cyan); font-weight:900;}
    .toast.warn{border-color: rgba(255,176,0,.25);}
    .toast.bad{border-color: rgba(255,59,59,.25);}

    /* 3-level blocks */
    .lvl{margin-top:10px;}
    .tvwrap{border:1px solid var(--line); border-radius:14px; overflow:hidden;}
    .alertsPanel .item{display:flex; gap:10px; align-items:flex-start;}
    .tag{color:var(--cyan); font-weight:900;}
    .sev{min-width:72px; text-align:center; padding:4px 8px; border:1px solid var(--line); border-radius:999px; font-weight:900;}
    .sev.warn{color:var(--warn); border-color: rgba(255,176,0,.25);}
    .sev.info{color:#b8c3da;}
    .sev.bad{color:var(--no); border-color: rgba(255,59,59,.25);}

    .kz{color:var(--muted); font-family:var(--mono); font-size:12px; margin-top:8px;}
    .modal{display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); padding: calc(14px + env(safe-area-inset-top)) 14px calc(14px + env(safe-area-inset-bottom)); z-index:9998;}
    .modal .box{max-width:1180px; margin:0 auto; background:var(--panel); border:1px solid var(--line); border-radius:16px; max-height:86vh; overflow:auto; -webkit-overflow-scrolling:touch;}
    .modal .head{position:sticky; top:0; background:rgba(11,17,26,.92); backdrop-filter:blur(10px);
                 display:flex; justify-content:space-between; align-items:center; padding:12px; border-bottom:1px solid var(--line);}
    .modal .body{padding:12px;}
    .iframebox{width:100%; height:72vh; border:1px solid var(--line); border-radius:14px; overflow:hidden;}
    .iframebox iframe{width:100%; height:100%; border:0;}

    @media(max-width: 780px){
      .why{max-width:none;}
      th:nth-child(11), td:nth-child(11) {display:none;}
      .toastwrap{left:14px; right:14px; max-width:none;}
    }
  </style>
</head>
<body>
<div class="toastwrap" id="toasts"></div>

<div class="wrap">
  <div class="hdr">
    <div class="title"><b>NEWS BIAS</b> // TERMINAL</div>
    <div class="sub">updated_utc=__UPDATED__ • gate_profile=__GATE_PROFILE__ • event_mode=__EVENT_MODE__ • trump=__TRUMP__</div>

    <div class="list" style="margin-top:6px;">
      <div class="item" style="border-top:none; padding-top:6px;">
        <div class="m"><span class="tag">Next event:</span> __NEXT_EVENT__ • <span class="muted">event_reason: __EVENT_REASON__</span></div>
      </div>
    </div>

    <div class="bar">
      __EV_PILL__
      __TR_PILL__
      __FEEDS_PILL__
      __FRED_PILL__
      <a class="pill neu" href="/diag" target="_blank" rel="noopener">/diag</a>
    </div>

    <div class="btnrow">
      <button class="btn" onclick="runNow()">R RUN</button>
      <button class="btn" onclick="openMorning()">M MORNING</button>
      <button class="btn" onclick="openCalendar()">C CAL (DB)</button>
      <button class="btn" onclick="openMyfx()">E MYFX CAL</button>
      <button class="btn" onclick="openJson()">J JSON</button>
      <button class="btn" onclick="openAnalyst()">A ANALYST</button>
    </div>
    <div class="hotkeys">Hotkeys: R run • M morning • C calendar • E myfx • J json • A analyst • 1..5 view • Esc close</div>

    <!-- LEVEL 1: LIVE prices -->
    <div class="lvl">
      <div class="panel">
        <div class="h2">LIVE PRICES (TradingView tape, IC Markets)</div>
        <div class="tvwrap">
          <div class="tradingview-widget-container">
            <div class="tradingview-widget-container__widget"></div>
            <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
            {
              "symbols": __TV_SYMBOLS__,
              "showSymbolLogo": true,
              "isTransparent": true,
              "displayMode": "adaptive",
              "colorTheme": "dark",
              "locale": "en"
            }
            </script>
          </div>
        </div>
      </div>
    </div>

    <!-- LEVEL 2: ALERTS -->
    <div class="lvl">
      <div class="panel alertsPanel" id="alertsPanel">
        <div class="h2">ALERTS (server diff)</div>
        <div class="list" id="alertsList"></div>
        <div class="kz">This block is intentionally separated from prices and news to reduce “noise mixing”.</div>
      </div>
    </div>

    <!-- LEVEL 3: TOP NEWS -->
    <div class="lvl">
      <div class="panel" id="newsPanel">
        <div class="h2">TOP NEWS (strong + relevant)</div>
        <div class="list" id="topNewsList"></div>
        <div class="kz">Ranking: source_weight × freshness (and must match at least one asset rule, unless macro source).</div>
      </div>
    </div>

  </div>

  <div class="panel">
    <div class="h2">BIAS + GATE (snapshot)</div>
    <table>
      <thead>
        <tr>
          <th>SYM</th>
          <th>BIAS</th>
          <th class="num">SCORE</th>
          <th class="num">TH</th>
          <th class="num">Q2</th>
          <th class="num">CONFLICT</th>
          <th class="num">TO_NEU</th>
          <th class="num">TO_OPP</th>
          <th class="num">≈HEADLINES</th>
          <th>GATE</th>
          <th>TOP DRIVER</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        __ROWS__
      </tbody>
    </table>

    <div class="kz">
      Flip units: score-sum (weighted evidence), not % / not market points.
      TO_NEU = ΔScore to return to Neutral band. TO_OPP = ΔScore to flip to opposite bias.
      ≈HEADLINES = TO_OPP / median(|contrib|) (rough).
    </div>
  </div>
</div>

<div class="modal" id="modal">
  <div class="box">
    <div class="head">
      <div class="title" id="mt">VIEW</div>
      <button class="btn" onclick="closeModal()">Esc</button>
    </div>
    <div class="body" id="mb"></div>
  </div>
</div>

<script>
  const PAYLOAD = __JS_PAYLOAD__;
  const ALERTS = (PAYLOAD.meta && PAYLOAD.meta.alerts) ? PAYLOAD.meta.alerts : [];
  const RUN_TOKEN_REQUIRED = !!(PAYLOAD.meta && PAYLOAD.meta.run_token_required);

  function $(id){ return document.getElementById(id); }
  function escapeHtml(s){
    return (s||'').toString().replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#039;');
  }

  function showModal(title, html){
    $('mt').innerText = title;
    $('mb').innerHTML = html;
    $('modal').style.display = 'block';
  }
  function closeModal(){ $('modal').style.display = 'none'; }

  function getRunToken(){
    if(!RUN_TOKEN_REQUIRED) return '';
    let t = localStorage.getItem('run_token') || '';
    if(!t){
      t = prompt('RUN_TOKEN is required. Paste token:') || '';
      if(t) localStorage.setItem('run_token', t);
    }
    return t || '';
  }

  async function runNow(){
    try{
      const token = getRunToken();
      const url = token ? ('/run?token=' + encodeURIComponent(token)) : '/run';
      const resp = await fetch(url, { method:'POST' });
      const js = await resp.json();
      if(js.ok === false && js.error){
        showModal('RUN ERROR', '<div class="panel"><div class="h2">Error</div><div class="list"><div class="item"><div class="t">' + escapeHtml(js.error) + '</div></div></div></div>');
        return;
      }
      setTimeout(()=>location.reload(), 400);
    }catch(e){
      showModal('RUN ERROR', '<div class="panel"><div class="h2">Exception</div><div class="list"><div class="item"><div class="t">' + escapeHtml(String(e)) + '</div></div></div></div>');
    }
  }

  function toast(kind, msg, sev){
    const w = $('toasts');
    const div = document.createElement('div');
    const s = (sev||'').toLowerCase();
    div.className = 'toast ' + (s==='warn' ? 'warn' : '');
    div.innerHTML = '<div class="k">' + escapeHtml(kind) + '</div><div>' + escapeHtml(msg) + '</div>';
    w.appendChild(div);
    setTimeout(()=>{ try{ div.remove(); }catch(e){} }, 9000);
  }

  function renderAlerts(){
    const box = $('alertsList');
    if(!box) return;
    if(!ALERTS || !ALERTS.length){
      box.innerHTML = '<div class="item"><div class="m muted">No alerts (diff engine quiet).</div></div>';
      return;
    }
    box.innerHTML = ALERTS.slice(0, 10).map(a=>{
      const sev = (a.severity||'WARN').toLowerCase();
      const cls = (sev==='warn') ? 'warn' : (sev==='bad' ? 'bad' : 'info');
      return `
        <div class="item">
          <div class="sev ${cls}">${escapeHtml((a.severity||'WARN'))}</div>
          <div style="flex:1;">
            <div class="t"><span class="tag">${escapeHtml(a.kind||'ALERT')}</span> ${a.asset ? ('• <b>'+escapeHtml(a.asset)+'</b>') : ''}</div>
            <div class="m">${escapeHtml(a.message||'')}</div>
          </div>
        </div>
      `;
    }).join('');
  }

  async function renderTopNews(){
    const box = $('topNewsList');
    if(!box) return;
    try{
      const resp = await fetch('/top_news?limit=18');
      const js = await resp.json();
      const items = js.items || [];
      if(!items.length){
        box.innerHTML = '<div class="item"><div class="m muted">No top news (yet). Run ingest or widen lookback.</div></div>';
        return;
      }
      box.innerHTML = items.map((x,i)=>{
        const m = (x.matches||[]).join(',');
        return `
          <div class="item">
            <div class="t">${i+1}. ${escapeHtml(x.title||'')}</div>
            <div class="m">src=${escapeHtml(x.source||'')} • age=${escapeHtml(x.age_min||0)}m • src_w=${escapeHtml(x.src_w)} • matches=${escapeHtml(m||'—')}</div>
            <div class="lnk"><a href="${escapeHtml(x.link||'#')}" target="_blank" rel="noopener">Open source</a></div>
          </div>
        `;
      }).join('');
    }catch(e){
      box.innerHTML = '<div class="item"><div class="m">Error loading top news: ' + escapeHtml(String(e)) + '</div></div>';
    }
  }

  function openView(asset){
    const a = (PAYLOAD.assets||{})[asset] || {};
    const ev = PAYLOAD.event || {};
    const meta = PAYLOAD.meta || {};

    const flip = a.flip || {};
    const top3 = (a.top3_drivers||[]);
    const why5 = (a.why_top5||[]);

    const flipHtml = `
      <div class="panel card">
        <div class="h2">Flip</div>
        <div class="kv">
          <div class="k">state</div><div>${escapeHtml(flip.state||'')}</div>
          <div class="k">to_bullish</div><div>${escapeHtml(flip.to_bullish)}</div>
          <div class="k">to_bearish</div><div>${escapeHtml(flip.to_bearish)}</div>
          <div class="k">to_neutral</div><div>${escapeHtml(flip.to_neutral)}</div>
          <div class="k">to_opposite</div><div>${escapeHtml(flip.to_opposite)}</div>
          <div class="k">median_abs</div><div>${escapeHtml(flip.median_abs_contrib)}</div>
          <div class="k">≈headlines</div><div>${escapeHtml(flip.approx_headlines_to_flip ?? '—')}</div>
        </div>
        <div class="kz">${escapeHtml(flip.note||'')}</div>
      </div>
    `;

    const topHtml = `
      <div class="panel card">
        <div class="h2">Top drivers</div>
        <div class="list">
          ${(top3.length? top3 : [{why:'—', abs_contrib_sum:''}]).map((x,i)=>`
            <div class="item">
              <div class="t">${i+1}. ${escapeHtml(x.why||'—')}</div>
              <div class="m">abs=${escapeHtml(x.abs_contrib_sum ?? '')}</div>
            </div>
          `).join('')}
        </div>
      </div>
    `;

    const whyHtml = `
      <div class="panel">
        <div class="h2">WHY top 5</div>
        <div class="list">
          ${(why5.length? why5 : [{why:'—'}]).map((x,i)=>`
            <div class="item">
              <div class="t">${i+1}. ${escapeHtml(x.why||'—')}</div>
              <div class="m">src=${escapeHtml(x.source||'')} • age=${escapeHtml(x.age_min||0)}m • contrib=${escapeHtml(x.contrib||'')}</div>
              <div class="m">${escapeHtml(x.title||'')}</div>
              <div class="lnk"><a href="${escapeHtml(x.link||'#')}" target="_blank" rel="noopener">Open source</a></div>
            </div>
          `).join('')}
        </div>
      </div>
    `;

    const headHtml = `
      <div class="list" style="margin-bottom:10px;">
        <div class="item" style="border-top:none;">
          <div class="t"><span class="tag">${asset}</span> bias=<b>${escapeHtml(a.bias||'')}</b> score=<b>${escapeHtml(a.score)}</b> th=${escapeHtml(a.threshold)} q2=<b>${escapeHtml(a.quality_v2)}</b> conflict=${escapeHtml(a.conflict_index)}</div>
        </div>
      </div>
    `;

    const eventHtml = `
      <div class="panel">
        <div class="h2">Event</div>
        <div class="kv">
          <div class="k">event_mode</div><div>${escapeHtml(ev.event_mode)}</div>
          <div class="k">reason</div><div>${escapeHtml((ev.reason||[]).join(', ') || '—')}</div>
          <div class="k">lookahead_hours</div><div>${escapeHtml(ev.lookahead_hours)}</div>
          <div class="k">recent_hours</div><div>${escapeHtml(ev.recent_hours)}</div>
        </div>
      </div>
    `;

    const metaHtml = `
      <div class="panel">
        <div class="h2">Meta</div>
        <div class="kv">
          <div class="k">gate_profile</div><div>${escapeHtml(meta.gate_profile||'')}</div>
          <div class="k">fred_enabled</div><div>${escapeHtml(meta.fred && meta.fred.enabled)}</div>
          <div class="k">trump_flag</div><div>${escapeHtml(meta.trump && meta.trump.flag)}</div>
          <div class="k">run_token_required</div><div>${escapeHtml(meta.run_token_required)}</div>
        </div>
      </div>
    `;

    const html = headHtml + `<div class="row2">${flipHtml}${topHtml}</div>` + whyHtml + eventHtml + metaHtml;
    showModal('VIEW ' + asset, html);
  }

  async function openMorning(){
    try{
      const resp = await fetch('/morning_plan');
      const js = await resp.json();
      showModal('MORNING PLAN', '<div class="panel"><div class="h2">JSON</div><div class="list"><div class="item"><pre style="margin:0; white-space:pre-wrap; font-family:var(--mono); font-size:12px;">' + escapeHtml(JSON.stringify(js,null,2)) + '</pre></div></div></div>');
    }catch(e){
      showModal('MORNING PLAN', '<div class="panel"><div class="h2">Error</div><div class="list"><div class="item"><div class="t">' + escapeHtml(String(e)) + '</div></div></div></div>');
    }
  }

  async function openCalendar(){
    try{
      const resp = await fetch('/calendar_data?limit=180&hours_back=12&hours_fwd=48');
      const js = await resp.json();
      const items = js.items || [];

      function fmtTs(ts){
        if(!ts) return '(time unknown)';
        const d = new Date(ts*1000);
        return d.toISOString().slice(0,16).replace('T',' ') + ' UTC';
      }

      const table = `
        <div class="panel">
          <div class="h2">ECON CALENDAR (DB, best-effort from RSS)</div>
          <table>
            <thead>
              <tr>
                <th>TIME (UTC)</th>
                <th>SRC</th>
                <th>CC</th>
                <th>CUR</th>
                <th>IMPACT</th>
                <th>EVENT</th>
                <th class="num">ACT</th>
                <th class="num">PREV</th>
                <th class="num">CONS</th>
                <th class="num">FCST</th>
              </tr>
            </thead>
            <tbody>
              ${items.map(x=>`
                <tr>
                  <td>${escapeHtml(fmtTs(x.event_ts))}</td>
                  <td class="muted">${escapeHtml(x.source||'')}</td>
                  <td>${escapeHtml(x.country||'—')}</td>
                  <td>${escapeHtml(x.currency||'—')}</td>
                  <td>${escapeHtml(x.impact||'—')}</td>
                  <td>
                    ${escapeHtml(x.title||'')}
                    ${x.link ? `<div class="lnk"><a href="${escapeHtml(x.link)}" target="_blank" rel="noopener">open</a></div>` : ''}
                  </td>
                  <td class="num">${escapeHtml(x.actual||'—')}</td>
                  <td class="num">${escapeHtml(x.previous||'—')}</td>
                  <td class="num">${escapeHtml(x.consensus||'—')}</td>
                  <td class="num">${escapeHtml(x.forecast||'—')}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
          <div class="kz">Note: RSS calendars often don’t provide Actual/Forecast/Previous consistently.</div>
        </div>
      `;
      showModal('CALENDAR (DB)', table);
    }catch(e){
      showModal('CALENDAR', '<div class="panel"><div class="h2">Error</div><div class="list"><div class="item"><div class="t">' + escapeHtml(String(e)) + '</div></div></div></div>');
    }
  }

  function openMyfx(){
    const html = `
      <div class="panel">
        <div class="h2">MyFXBook Economic Calendar (dark forced)</div>
        <div class="iframebox">
          <iframe src="/myfx_calendar?dark=1" loading="lazy"></iframe>
        </div>
        <div class="kz">If your browser blocks widgets, open /myfx_calendar in a new tab.</div>
      </div>
    `;
    showModal('MYFX CAL', html);
  }

  async function openJson(){
    try{
      const resp = await fetch('/bias?pretty=1');
      const txt = await resp.text();
      showModal('BIAS JSON', '<div class="panel"><div class="h2">JSON</div><div class="list"><div class="item"><pre style="margin:0; white-space:pre-wrap; font-family:var(--mono); font-size:12px;">' + escapeHtml(txt) + '</pre></div></div></div>');
    }catch(e){
      showModal('BIAS JSON', '<div class="panel"><div class="h2">Error</div><div class="list"><div class="item"><div class="t">' + escapeHtml(String(e)) + '</div></div></div></div>');
    }
  }

  function openAnalyst(){
    const a = PAYLOAD.assets || {};
    const ev = PAYLOAD.event || {};
    const gp = (PAYLOAD.meta||{}).gate_profile || '';
    const s = {
      updated_utc: PAYLOAD.updated_utc,
      gate_profile: gp,
      event_mode: ev.event_mode,
      event_reason: ev.reason,
      alerts: (PAYLOAD.meta && PAYLOAD.meta.alerts) ? PAYLOAD.meta.alerts.slice(0,10) : [],
      assets: a
    };
    showModal('ANALYST SNAPSHOT', '<div class="panel"><div class="h2">JSON</div><div class="list"><div class="item"><pre style="margin:0; white-space:pre-wrap; font-family:var(--mono); font-size:12px;">' + escapeHtml(JSON.stringify(s,null,2)) + '</pre></div></div></div>');
  }

  document.addEventListener('keydown', (e)=>{
    const k = (e.key||'').toLowerCase();
    if(k === 'escape') closeModal();
    if(k === 'r') runNow();
    if(k === 'm') openMorning();
    if(k === 'c') openCalendar();
    if(k === 'e') openMyfx();
    if(k === 'j') openJson();
    if(k === 'a') openAnalyst();
    if(k === '1') openView('US500');
    if(k === '2') openView('XAU');
    if(k === '3') openView('EURUSD');
    if(k === '4') openView('XTI');
    if(k === '5') openView('BTC');
  });

  $('modal').addEventListener('click', (e)=>{ if(e.target && e.target.id === 'modal') closeModal(); });

  renderAlerts();
  renderTopNews();

  (ALERTS||[]).slice(0,6).forEach(a=>{
    toast(a.kind, a.message, a.severity || 'WARN');
  });
</script>
</body>
</html>
"""

    rows_html = "\n".join([row(a) for a in ASSETS])

    html = (TEMPLATE
        .replace("__UPDATED__", str(updated))
        .replace("__GATE_PROFILE__", str(gate_profile))
        .replace("__EVENT_MODE__", str(event_mode).lower())
        .replace("__TRUMP__", ("on" if trump_enabled else "off"))
        .replace("__NEXT_EVENT__", str(next_event))
        .replace("__EVENT_REASON__", str(reason_txt))
        .replace("__EV_PILL__", ev_pill)
        .replace("__TR_PILL__", tr_pill)
        .replace("__FEEDS_PILL__", feeds_pill)
        .replace("__FRED_PILL__", fred_pill)
        .replace("__ROWS__", rows_html)
        .replace("__JS_PAYLOAD__", js_payload)
        .replace("__TV_SYMBOLS__", tv_symbols)
    )

    return HTMLResponse(html)
