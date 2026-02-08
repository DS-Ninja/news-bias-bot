# app.py
# NEWS BIAS // TERMINAL — RSS + Postgres + Bias/Quality + Trade Gate + Calendar + Alerts + Ticker
# FIXES v2026-02-08b:
# - UI: named blocks (META / SWITCHBOARD / ACTIONS / MARKET TAPE / TICKERS / SUMMARY TABLE)
# - UI: table stability (fixed layout + colgroup + wrap) to prevent “pushing/misalignment”
# - Calendar: stronger parsing for MYFX_CAL + generic blob datetime parse (prevents USD events missing ts)
# - Next event: prefers USD; if USD exists but has no ts, shows USD "(time unknown)" instead of switching to JPY
# - Calendar: impact normalization LOW/MED/HIGH displayed everywhere
# - RUN token: supports RUN_TOKENS (comma-separated) + better mismatch UX + clear saved token
# - /diag: shows run_token_hashes (safe) to debug token mismatch
# - VIEW: keeps existing evidence view; calendar fields cleaner

import os
import json
import time
import re
import hashlib
import math
import calendar as pycalendar
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

# RUN token
# - RUN_TOKEN: single token (legacy)
# - RUN_TOKENS: comma-separated list (new)
RUN_TOKEN = os.environ.get("RUN_TOKEN", "").strip()
RUN_TOKENS_RAW = os.environ.get("RUN_TOKENS", "").strip()
RUN_TOKENS: List[str] = []
if RUN_TOKEN:
    RUN_TOKENS.append(RUN_TOKEN)
if RUN_TOKENS_RAW:
    RUN_TOKENS.extend([x.strip() for x in RUN_TOKENS_RAW.split(",") if x.strip()])

# de-dup while preserving order
_seen = set()
RUN_TOKENS = [t for t in RUN_TOKENS if not (t in _seen or _seen.add(t))]

def _token_hash(t: str) -> str:
    return hashlib.sha1((t or "").encode("utf-8", errors="ignore")).hexdigest()[:10]

RUN_TOKEN_HASHES = [_token_hash(t) for t in RUN_TOKENS]

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
    """
    RSS published_parsed is usually UTC-ish; use calendar.timegm() to avoid local-time mktime() shifts.
    """
    pp = e.get("published_parsed")
    if pp:
        try:
            return int(pycalendar.timegm(pp))
        except Exception:
            return int(now_ts)
    up = e.get("updated_parsed")
    if up:
        try:
            return int(pycalendar.timegm(up))
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

def _human_event_reason(reason_list: List[str]) -> str:
    if not reason_list:
        return "No macro trigger"
    out = []
    for r in reason_list:
        r = str(r)
        if r.startswith("upcoming<="):
            hh = r.split("<=")[-1].strip()
            out.append(f"Upcoming macro within {hh}")
        elif r.startswith("recent_macro<="):
            hh = r.split("<=")[-1].strip()
            out.append(f"Recent macro within {hh}")
        elif r == "calendar_time_unknown":
            out.append("Calendar time unknown (RSS)")
        elif r == "no_macro_no_upcoming":
            out.append("No macro triggers")
        elif r == "disabled":
            out.append("Disabled")
        else:
            out.append(r)
    return " | ".join(out)

def _fred_status(meta_fred: dict) -> Tuple[bool, str]:
    if not FRED_CFG.get("enabled", True):
        return False, "Disabled by env (FRED_ENABLED=0)"
    if requests is None:
        return False, "requests package missing"
    if not (FRED_CFG.get("api_key") or "").strip():
        return False, "Missing FRED_API_KEY"
    if not bool(meta_fred.get("enabled", False)):
        return False, "Not active (check key/requests)"
    return True, "OK"

def _run_token_status() -> Tuple[bool, str]:
    if not RUN_TOKENS:
        return False, "Open (no token required)"
    return True, f"Protected (token required). expected_hash={','.join(RUN_TOKEN_HASHES) or '—'}"

def _fmt_hhmm_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _fmt_countdown(now_ts: int, future_ts: int) -> str:
    dsec = int(future_ts - now_ts)
    if dsec <= 0:
        return "T+0.0h"
    return f"T-{round(dsec/3600.0, 1)}h"

def _impact_norm(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip().upper()
    # normalize common variants
    if s in ("HIGH", "H"):
        return "HIGH"
    if s in ("MEDIUM", "MED", "M"):
        return "MED"
    if s in ("LOW", "L"):
        return "LOW"
    # ForexFactory sometimes uses "High", "Medium", "Low"
    if s.startswith("HI"):
        return "HIGH"
    if s.startswith("ME"):
        return "MED"
    if s.startswith("LO"):
        return "LOW"
    return s[:8]

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
# INGEST (CALENDAR) — stronger parsing for ForexFactory/MyFXBook
# ============================================================

_CUR_PAT = re.compile(r"\b(USD|EUR|GBP|JPY|CHF|AUD|CAD|NZD|CNY)\b")
_IMPACT_PAT = re.compile(r"\b(high|medium|low)\b", re.I)

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def _get_entry_field(e: dict, keys: List[str]) -> Optional[str]:
    for k in keys:
        v = e.get(k)
        if v is None:
            continue
        # feedparser may return lists for some tags
        if isinstance(v, list) and v:
            vv = v[0]
            if vv is not None:
                return str(vv).strip()
        return str(v).strip()
    return None

def _try_parse_ff_datetime(e: dict) -> Optional[int]:
    """
    ForexFactory xml often has custom tags: date, time, datetime, etc.
    We try several keys and patterns. If we cannot parse, return None.
    """
    d_raw = _get_entry_field(e, ["date", "event_date", "ff_date", "ev_date"])
    t_raw = _get_entry_field(e, ["time", "event_time", "ff_time", "ev_time"])

    if d_raw and t_raw:
        d_raw2 = d_raw.strip()
        t_raw2 = t_raw.strip()

        # If time like "All Day" or "Tentative" -> not parseable
        if not re.search(r"\d", t_raw2):
            return None

        tm = re.search(r"(\d{1,2}):(\d{2})", t_raw2)
        if not tm:
            return None
        hh = int(tm.group(1))
        mm = int(tm.group(2))

        y = m = dd = None
        if re.match(r"^\d{4}-\d{2}-\d{2}$", d_raw2):
            y, m, dd = [int(x) for x in d_raw2.split("-")]
        elif re.match(r"^\d{2}-\d{2}-\d{4}$", d_raw2):
            m, dd, y = [int(x) for x in d_raw2.split("-")]
        else:
            m2 = re.search(r"(\d{4})-(\d{2})-(\d{2})", d_raw2)
            if m2:
                y, m, dd = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))

        if not (y and m and dd):
            return None

        dt = datetime(y, m, dd, hh, mm, tzinfo=timezone.utc)
        return int(dt.timestamp())

    dt_raw = _get_entry_field(e, ["datetime", "event_datetime", "ff_datetime"])
    if dt_raw:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2}).*?(\d{1,2}):(\d{2})", dt_raw)
        if m:
            y, mo, da, hh, mm = [int(m.group(i)) for i in range(1, 6)]
            dt = datetime(y, mo, da, hh, mm, tzinfo=timezone.utc)
            return int(dt.timestamp())

    return None

def _try_parse_any_datetime_from_blob(blob: str) -> Optional[int]:
    """
    Generic fallback: parse common datetime patterns from title/summary blob.
    Returns UTC ts or None.
    """
    if not blob:
        return None
    s = " ".join(str(blob).split())

    # 1) ISO: 2026-02-08 23:30
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})", s)
    if m:
        y, mo, da, hh, mm = [int(m.group(i)) for i in range(1, 6)]
        return int(datetime(y, mo, da, hh, mm, tzinfo=timezone.utc).timestamp())

    # 2) US-ish: Feb 8, 2026 23:30
    m = re.search(r"\b([A-Za-z]{3})\s+(\d{1,2}),\s*(\d{4})\s+(\d{1,2}):(\d{2})\b", s)
    if m:
        mon = _MONTHS.get(m.group(1).lower()[:3])
        da = int(m.group(2)); y = int(m.group(3)); hh = int(m.group(4)); mm = int(m.group(5))
        if mon:
            return int(datetime(y, mon, da, hh, mm, tzinfo=timezone.utc).timestamp())

    # 3) EU-ish: 08 Feb 2026 23:30
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s+(\d{1,2}):(\d{2})\b", s)
    if m:
        da = int(m.group(1)); mon = _MONTHS.get(m.group(2).lower()[:3]); y = int(m.group(3))
        hh = int(m.group(4)); mm = int(m.group(5))
        if mon:
            return int(datetime(y, mon, da, hh, mm, tzinfo=timezone.utc).timestamp())

    # 4) DD/MM/YYYY HH:MM
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\b", s)
    if m:
        da, mo, y, hh, mm = [int(m.group(i)) for i in range(1, 6)]
        if 1 <= mo <= 12 and 1 <= da <= 31:
            return int(datetime(y, mo, da, hh, mm, tzinfo=timezone.utc).timestamp())

    return None

def _parse_calendar_fields(src: str, e: dict) -> Dict[str, Optional[str]]:
    """
    Extract currency/impact/values from entry fields and text blob.
    Handles MYFXBook/FF quirks: currency often lives in tags/categories.
    """
    title = (e.get("title") or "").strip()
    summary = (e.get("summary") or e.get("description") or "").strip()
    blob = (title + " " + summary).strip()

    # Direct tags (best)
    currency = _get_entry_field(e, ["currency", "ccy", "cur", "ff_currency"])
    impact = _get_entry_field(e, ["impact", "importance", "ff_impact", "volatility"])

    # --- NEW: read tags/categories (MYFXBook часто кладёт валюту туда)
    if not currency:
        tags = e.get("tags") or e.get("categories") or []
        try:
            for tg in tags:
                term = ""
                if isinstance(tg, dict):
                    term = str(tg.get("term") or tg.get("label") or tg.get("name") or "")
                else:
                    term = str(tg)
                m = _CUR_PAT.search(term)
                if m:
                    currency = m.group(1).upper()
                    break
        except Exception:
            pass

    # Text fallback
    if not currency:
        m = _CUR_PAT.search(blob)
        if m:
            currency = m.group(1).upper()

    if not impact:
        m = _IMPACT_PAT.search(blob)
        if m:
            impact = m.group(1).upper()

    impact = _impact_norm(impact)

    def pick(pat: str) -> Optional[str]:
        mm = re.search(pat, blob, flags=re.I)
        if mm:
            return mm.group(1).strip()
        return None

    actual = pick(r"\bactual[:=]\s*([^\s|,;]+)")
    previous = pick(r"\bprev(?:ious)?[:=]\s*([^\s|,;]+)")
    consensus = pick(r"\bcons(?:ensus)?[:=]\s*([^\s|,;]+)")
    forecast = pick(r"\bfore(?:cast)?[:=]\s*([^\s|,;]+)")

    country = _get_entry_field(e, ["country", "ff_country"])
    if not country and currency:
        country = {"USD": "US", "EUR": "EU", "GBP": "UK", "JPY": "JP"}.get(currency.upper())

    return {
        "impact": impact,
        "country": country.upper() if country else None,
        "currency": currency.upper() if currency else None,
        "actual": actual,
        "previous": previous,
        "consensus": consensus,
        "forecast": forecast,
    }

def ingest_calendar_once(limit_per_feed: int = 250) -> int:
    inserted = 0

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
                    if not title:
                        continue

                    event_ts: Optional[int] = None
                    if src == "FOREXFACTORY_CALENDAR":
                        event_ts = _try_parse_ff_datetime(e)

                    if event_ts is None:
                        pp = e.get("published_parsed") or e.get("updated_parsed")
                        if pp:
                            try:
                                event_ts = int(pycalendar.timegm(pp))
                            except Exception:
                                event_ts = None

                    # NEW: generic blob parse (helps MYFX_CAL / messy feeds)
                    if event_ts is None:
                        blob = ((e.get("title") or "") + " " + (e.get("summary") or e.get("description") or "")).strip()
                        event_ts = _try_parse_any_datetime_from_blob(blob)

                    fields = _parse_calendar_fields(src, e)

                    fp = fingerprint(f"{src}||{title}", link or title)

                    try:
                        cur.execute("""
                            INSERT INTO econ_events(
                                source, title, country, currency, impact,
                                actual, previous, consensus, forecast,
                                event_ts, link, fingerprint
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (fingerprint) DO UPDATE SET
                                country=EXCLUDED.country,
                                currency=EXCLUDED.currency,
                                impact=EXCLUDED.impact,
                                actual=EXCLUDED.actual,
                                previous=EXCLUDED.previous,
                                consensus=EXCLUDED.consensus,
                                forecast=EXCLUDED.forecast,
                                event_ts=COALESCE(EXCLUDED.event_ts, econ_events.event_ts),
                                link=COALESCE(NULLIF(EXCLUDED.link,''), econ_events.link);
                        """, (
                            src, title,
                            fields.get("country"), fields.get("currency"), fields.get("impact"),
                            fields.get("actual"), fields.get("previous"), fields.get("consensus"), fields.get("forecast"),
                            event_ts, link, fp
                        ))
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
                SELECT source, title, link, event_ts, currency, impact, country
                FROM econ_events
                WHERE event_ts IS NOT NULL AND event_ts BETWEEN %s AND %s
                ORDER BY event_ts ASC
                LIMIT %s;
            """, (now_ts, horizon, int(EVENT_CFG["max_upcoming"])))
            rows = cur.fetchall()

    for (source, title, link, ts, currency, impact, country) in rows:
        out.append({
            "source": source,
            "title": title,
            "link": link,
            "ts": int(ts),
            "currency": (currency or None),
            "impact": _impact_norm(impact),
            "country": (country or None),
            "in_hours": round((int(ts) - now_ts) / 3600.0, 2),
        })

    if out:
        return out[: int(EVENT_CFG["max_upcoming"])]

    # fallback: show latest rows even if ts missing (for diagnostics/morning modal)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, event_ts, currency, impact, country
                FROM econ_events
                ORDER BY COALESCE(event_ts, 0) DESC, id DESC
                LIMIT 12;
            """)
            rows2 = cur.fetchall()

    unknown = []
    for (source, title, link, ts, currency, impact, country) in rows2:
        unknown.append({
            "source": source,
            "title": title,
            "link": link,
            "ts": int(ts) if ts else None,
            "currency": (currency or None),
            "impact": _impact_norm(impact),
            "country": (country or None),
            "in_hours": None
        })
    return unknown[:12]

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
    out = {"XAU": [], "US500": [], "WTI": []}

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
        add("WTI", "DTWEXBGS", w=(-0.4 if d > 0 else +0.2), note="USD headwind/tailwind", value=latest, delta=d)

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

def compute_bias(lookback_hours: int = 24, limit_rows: int = 1200) -> Dict[str, Any]:
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
    fred_drivers = {"XAU": [], "US500": [], "WTI": []}
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
            fred_drivers = {"XAU": [], "US500": [], "WTI": []}

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

        top3 = _top_drivers(contribs, topn=3)
        why_top5 = sorted(contribs, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]
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
            "run_token_required": bool(RUN_TOKENS),
            "run_token_hashes": RUN_TOKEN_HASHES,
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

    # Event mode flip
    p_ev = bool((prev.get("event", {}) or {}).get("event_mode", False))
    c_ev = bool((cur.get("event", {}) or {}).get("event_mode", False))
    if p_ev != c_ev:
        push(
            "event_mode_flip",
            f"EVENT MODE {'ON' if c_ev else 'OFF'}",
            asset=None,
            severity="WARN",
            extra={"prev": p_ev, "cur": c_ev, "reason": (cur.get("event", {}) or {}).get("reason", [])}
        )

    q2_drop_th = int(ALERT_CFG.get("q2_drop", 12))
    conflict_spike_th = float(ALERT_CFG.get("conflict_spike", 0.18))

    for a in ASSETS:
        pa = (p_assets.get(a, {}) or {})
        ca = (c_assets.get(a, {}) or {})

        pb = str(pa.get("bias", "NEUTRAL"))
        cb = str(ca.get("bias", "NEUTRAL"))
        if pb != cb:
            push(
                "bias_flip",
                f"{a} bias flip: {pb} → {cb}",
                asset=a,
                severity="WARN",
                extra={"prev": pb, "cur": cb, "score": ca.get("score"), "q2": ca.get("quality_v2"), "conflict": ca.get("conflict_index")}
            )

        pq2 = int(pa.get("quality_v2", 0))
        cq2 = int(ca.get("quality_v2", 0))
        if (pq2 - cq2) >= q2_drop_th:
            push(
                "q2_drop",
                f"{a} Q2 drop: {pq2} → {cq2} (Δ={pq2-cq2})",
                asset=a,
                severity="WARN",
                extra={"prev": pq2, "cur": cq2}
            )

        pc = float(pa.get("conflict_index", 0.0))
        cc = float(ca.get("conflict_index", 0.0))
        if (cc - pc) >= conflict_spike_th:
            push(
                "conflict_spike",
                f"{a} conflict spike: {pc:.3f} → {cc:.3f} (Δ={(cc-pc):.3f})",
                asset=a,
                severity="WARN",
                extra={"prev": pc, "cur": cc}
            )

    # Feeds degraded flip based on ratio
    p_meta = (prev.get("meta", {}) or {})
    c_meta = (cur.get("meta", {}) or {})
    p_fs = (p_meta.get("feeds_status", {}) or {})
    c_fs = (c_meta.get("feeds_status", {}) or {})
    if c_fs:
        pr = _feeds_ok_ratio(p_fs) if p_fs else 1.0
        cr = _feeds_ok_ratio(c_fs)
        thr = float(ALERT_CFG.get("feeds_degraded_ratio", 0.80))
        if pr >= thr and cr < thr:
            push(
                "feeds_degraded",
                f"FEEDS degraded: ok_ratio {pr:.2f} → {cr:.2f}",
                asset=None,
                severity="WARN",
                extra={"prev_ratio": pr, "cur_ratio": cr}
            )

    return alerts

# ============================================================
# PIPELINE RUN
# ============================================================

def pipeline_run():
    db_init()

    prev = load_bias()

    inserted_news = ingest_news_once(limit_per_feed=40)
    inserted_cal = ingest_calendar_once(limit_per_feed=250)

    payload = compute_bias(lookback_hours=24, limit_rows=1200)

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
        fail.append("Bias is NEUTRAL")
        fail_short.append("NEUTRAL")
        must.append("Bias must become BULLISH or BEARISH")
    elif bias == "NEUTRAL" and cfg.get("neutral_allow", False):
        maxd = float(cfg.get("neutral_flip_dist_max", 0.20))
        if to_opp > maxd:
            fail.append("Neutral is far from flip")
            fail_short.append("Neutral far")
            must.append(f"Move closer to flip (to_opposite ≤ {maxd})")

    qmin = int(cfg["quality_v2_min"])
    if q2 < qmin:
        fail.append(f"Quality too low (Q2 {q2} < {qmin})")
        fail_short.append("Q2 low")
        must.append(f"Q2 ≥ {qmin}")

    cmax = float(cfg["conflict_max"])
    if conflict > cmax:
        fail.append(f"Conflict too high ({conflict:.3f} > {cmax})")
        fail_short.append("Conflict high")
        must.append(f"Conflict ≤ {cmax}")

    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH") and to_opp < mind:
        fail.append(f"Too close to opposite flip (to_opposite {to_opp:.3f} < {mind})")
        fail_short.append("Flip close")
        must.append(f"to_opposite ≥ {mind}")

    if event_mode:
        if cfg.get("event_mode_block", True):
            oq = int(cfg.get("event_override_quality", 70))
            oc = float(cfg.get("event_override_conflict", 0.45))
            if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
                fail.append("Event mode is ON (macro risk)")
                fail_short.append("EVENT")
                must.append(f"Wait OR require Q2≥{oq} & Conflict≤{oc} & bias!=NEUTRAL")

    ok = (len(fail) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:3] if x.get("why")] or ["Insufficient matched evidence"]

    return {
        "ok": bool(ok),
        "label": "TRADE OK" if ok else "NO TRADE",
        "why": why_short[:3],
        "fail_reasons": fail[:6],
        "fail_short": fail_short[:3],
        "must_change": must[:4],
    }

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
        "trump_enabled": bool(TRUMP_ENABLED),
        "fred_enabled": bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"]) and requests is not None),
        "calendar_sources": list(CALENDAR_FEEDS),
        "run_token_required": bool(RUN_TOKENS),
        "run_token_hashes": RUN_TOKEN_HASHES,
    }
    if pretty:
        return PlainTextResponse(json.dumps(out, indent=2), media_type="application/json")
    return out

@app.get("/diag")
def diag():
    db_init()
    env = {
        "has_DATABASE_URL": bool(os.environ.get("DATABASE_URL", "").strip()),
        "PGSSLMODE": os.environ.get("PGSSLMODE", "prefer"),
        "FRED_enabled": bool(FRED_CFG["enabled"]),
        "requests_present": bool(requests is not None),
        "RUN_token_required": bool(RUN_TOKENS),
        "RUN_token_hashes": RUN_TOKEN_HASHES,
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
    return {"assets": ASSETS, "rules": RULES}

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
    if not RUN_TOKENS:
        return True
    t = (token or "").strip()
    if not t:
        return False
    # constant-ish time compare across list
    for x in RUN_TOKENS:
        if hashlib.sha256(t.encode()).digest() == hashlib.sha256(x.encode()).digest():
            return True
    return False

@app.get("/run")
def run_get(token: str = ""):
    if not _auth_run(token):
        return JSONResponse({"ok": False, "error": "unauthorized (token mismatch)", "expected_hash": RUN_TOKEN_HASHES}, status_code=401)
    try:
        payload = pipeline_run()
        return JSONResponse({"ok": True, "updated_utc": payload.get("updated_utc"), "meta": payload.get("meta", {})})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/run")
def run_post(token: str = ""):
    if not _auth_run(token):
        return JSONResponse({"ok": False, "error": "unauthorized (token mismatch)", "expected_hash": RUN_TOKEN_HASHES}, status_code=401)
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

# MyFXBook calendar page (standalone)
@app.get("/myfx_calendar", response_class=HTMLResponse)
def myfx_calendar():
    html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MyFXBook Calendar</title>
<style>
html,body{{height:100%;margin:0;background:#070a0f;color:#d7e2ff;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial}}
.wrap{{height:100%;}}
iframe{{border:0;width:100%;height:100%;}}
</style>
</head><body>
<div class="wrap">
<iframe src="{MYFXBOOK_IFRAME_SRC}" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
</div>
</body></html>"""
    return HTMLResponse(html)

# ============================================================
# UI
# ============================================================

def _pill_bias(b: str) -> str:
    if b == "BULLISH":
        return '<span class="pill bull">BULLISH</span>'
    if b == "BEARISH":
        return '<span class="pill bear">BEARISH</span>'
    return '<span class="pill neu">NEUTRAL</span>'

def _pill_gate(ok: bool) -> str:
    return '<span class="pill ok">TRADE OK</span>' if ok else '<span class="pill no">NO TRADE</span>'

def _pill_impact(imp: Optional[str]) -> str:
    imp = _impact_norm(imp)
    if imp == "HIGH":
        return '<span class="pill bear">HIGH</span>'
    if imp == "MED":
        return '<span class="pill warn">MED</span>'
    if imp == "LOW":
        return '<span class="pill neu">LOW</span>'
    return '<span class="pill neu">—</span>'

def _pick_next_event(now_ts: int, upcoming: List[Dict[str, Any]], prefer_ccy: str = "USD") -> Tuple[str, str]:
    """
    STRICT: Next event should be only prefer_ccy (default USD).
    If no prefer_ccy events exist -> show explicit message.
    """
    if not upcoming:
        return "No upcoming events in horizon", ""

    prefer_ccy = (prefer_ccy or "USD").upper()

    # Keep only prefer_ccy events (both timed and unknown-time)
    pref = [x for x in upcoming if str(x.get("currency") or "").upper() == prefer_ccy]
    if not pref:
        return f"No {prefer_ccy} events in horizon", ""

    # First: timed
    timed = [x for x in pref if x.get("ts")]
    if timed:
        timed.sort(key=lambda x: int(x.get("ts") or 0))
        pick = timed[0]
        ts = int(pick["ts"])
        imp = _impact_norm(pick.get("impact"))
        return (
            f"{_fmt_hhmm_utc(ts)} • {prefer_ccy} {imp or '—'} • {pick.get('title','')} ({_fmt_countdown(now_ts, ts)})",
            str(pick.get("source", "")),
        )

    # Else: unknown time
    u = pref[0]
    imp = _impact_norm(u.get("impact"))
    return (f"(time unknown) • {prefer_ccy} {imp or '—'} • {u.get('title','')}", str(u.get("source", "")))

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
    upcoming = (event.get("upcoming_events", []) or [])[:12]
    now_ts = int(time.time())

    reason_txt = _human_event_reason([str(x) for x in (event.get("reason", []) or [])])
    next_event_line, next_event_src = _pick_next_event(now_ts, upcoming, prefer_ccy="USD")

    trump = meta.get("trump", {}) or {}
    trump_flag = bool(trump.get("flag", False))
    trump_enabled = bool(trump.get("enabled", False))

    feeds_ok_ratio = _feeds_ok_ratio(feeds_status) if feeds_status else 1.0
    feeds_ok = feeds_ok_ratio >= float(ALERT_CFG.get("feeds_degraded_ratio", 0.80))

    fred_on, fred_why = _fred_status(meta.get("fred", {}) or {})
    token_required, token_why = _run_token_status()

    for sym in ASSETS:
        a = assets.get(sym, {}) or {}
        a["ui_gate"] = eval_trade_gate(a, event_mode, gate_profile)
        assets[sym] = a
    payload["assets"] = assets

    def _chip(label: str, value: str, cls: str, tip: str) -> str:
        return f'<span class="chip {cls}" title="{tip}"><span class="k">{label}</span> {value}</span>'

    ev_chip = _chip("EVENT", "ON" if event_mode else "OFF", "warn" if event_mode else "neu",
                    "EVENT MODE = macro risk window (recent major releases or upcoming events).")
    tr_chip = _chip("TRUMP", ("FLAG" if (trump_enabled and trump_flag) else ("ON" if trump_enabled else "OFF")),
                    "warn" if (trump_enabled and trump_flag) else "neu",
                    "TRUMP FLAG = Trump-related headlines detected in last 12h.")
    fd_chip = _chip("FEEDS", ("OK" if feeds_ok else "DEGRADED") + f" ({feeds_ok_ratio:.2f})",
                    "ok" if feeds_ok else "no",
                    "FEEDS OK ratio = share of RSS feeds successfully parsed.")
    fr_chip = _chip("FRED", ("ON" if fred_on else "OFF"), "neu", "FRED OFF reason: " + fred_why)
    rn_chip = _chip("RUN", ("TOKEN" if token_required else "OPEN"), "neu", "RUN = refresh pipeline. " + token_why)
    dg_chip = f'<a class="chip neu" href="/diag" target="_blank" rel="noopener" title="Diagnostics">DIAG</a>'

    def row(asset: str) -> str:
        a = assets.get(asset, {}) or {}
        bias = str(a.get("bias", "NEUTRAL"))
        gate = a.get("ui_gate", {}) or {}
        ok = bool(gate.get("ok", False))
        short = (" | ".join((gate.get("fail_short", []) or [])[:3]) if not ok else ((gate.get("why", []) or ["—"])[0]))
        return f"""
        <tr class="r">
          <td class="sym">{asset}</td>
          <td>{_pill_bias(bias)}</td>
          <td>{_pill_gate(ok)}</td>
          <td class="why">{short or "—"}</td>
          <td class="act"><button class="btn" onclick="openView('{asset}')">View</button></td>
        </tr>
        """

    js_payload = json.dumps(payload, ensure_ascii=False)
    tv_symbols = json.dumps(TV_TICKER_SYMBOLS, ensure_ascii=False)

    updated_short = updated.replace("T", " ").replace("+00:00", " UTC")
    if len(updated_short) > 22:
        updated_short = updated_short[:22].strip()

    TEMPLATE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <title>NEWS BIAS // TERMINAL</title>
  <style>
    :root{
      --bg:#070a0f; --panel:#0b111a; --line:rgba(255,255,255,.08);
      --text:#d7e2ff; --muted:#7b8aa7;
      --amber:#ffb000; --cyan:#00e5ff;
      --ok:#00ff6a; --warn:#ffb000; --no:#ff3b3b;
      --pillbg:rgba(255,255,255,.03);
      --btn:#0f1724; --btn2:#162238;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }
    html,body{height:100%; margin:0; background:var(--bg); color:var(--text);}
    body{padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);}
    a{color:var(--cyan); text-decoration:none;}
    a:hover{text-decoration:underline;}
    .wrap{max-width:1250px; margin:0 auto; padding:14px;}
    .hdr{border-bottom:1px solid var(--line); padding-bottom:12px; margin-bottom:12px;}
    .title{font-family:var(--mono); font-weight:900; letter-spacing:.8px; display:flex; justify-content:space-between; gap:10px; align-items:center; flex-wrap:wrap;}
    .title b{color:var(--amber);}
    .metaLine{font-family:var(--mono); color:var(--muted); font-size:12px; margin-top:8px; display:flex; gap:16px; flex-wrap:wrap; align-items:center;}
    .metaLine .k{color:var(--muted);}
    .metaLine .v{color:var(--text); font-weight:900;}
    .chips{display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; align-items:center;}
    .chip{font-family:var(--mono); font-size:12px; font-weight:900; padding:8px 10px; border-radius:14px; border:1px solid var(--line); background:var(--pillbg); display:inline-flex; gap:10px; align-items:center;}
    .chip .k{color:var(--muted); font-weight:900;}
    .chip.ok{color:var(--ok); border-color:rgba(0,255,106,.25);}
    .chip.no{color:var(--no); border-color:rgba(255,59,59,.25);}
    .chip.warn{color:var(--warn); border-color:rgba(255,176,0,.25);}
    .chip.neu{color:#b8c3da;}
    .pill{font-family:var(--mono); font-size:12px; font-weight:900; padding:6px 10px; border-radius:999px; border:1px solid var(--line); background:var(--pillbg);}
    .bull{color:var(--ok); border-color: rgba(0,255,106,.25);}
    .bear{color:var(--no); border-color: rgba(255,59,59,.25);}
    .neu{color:#b8c3da;}
    .ok{color:var(--ok); border-color: rgba(0,255,106,.25);}
    .no{color:var(--no); border-color: rgba(255,59,59,.25);}
    .warn{color:var(--warn); border-color: rgba(255,176,0,.25);}
    .btn{background:var(--btn); border:1px solid var(--line); color:var(--text); padding:9px 12px; border-radius:12px; cursor:pointer; font-family:var(--mono); font-weight:900;}
    .btn:hover{background:var(--btn2);}
    .btnrow{display:flex; gap:8px; flex-wrap:wrap; margin-top:12px;}
    .panel{background:var(--panel); border:1px solid var(--line); border-radius:16px; padding:12px; margin-top:12px;}

    /* Table stability */
    table{width:100%; border-collapse:collapse; font-family:var(--mono); table-layout:fixed;}
    th,td{border-top:1px solid var(--line); padding:12px 10px; font-size:12px; vertical-align:top;}
    th{color:var(--muted); font-weight:900;}
    .sym{color:var(--amber); font-weight:900;}
    .why{color:var(--text); overflow-wrap:anywhere; word-break:break-word;}
    .act{text-align:right; white-space:nowrap;}

    .muted{color:var(--muted);}
    .tvwrap{margin-top:12px; border:1px solid var(--line); border-radius:14px; overflow:hidden;}

    /* Blocks */
    .block{margin-top:12px;}
    .block:first-child{margin-top:0;}
    .blockTitle{
      font-family:var(--mono);
      font-size:12px;
      font-weight:900;
      color:var(--muted);
      letter-spacing:.6px;
      margin:0 0 8px 2px;
    }

    /* Tickers */
    .tickerstack{margin-top:12px; display:flex; flex-direction:column; gap:10px;}
    .ticklabel{font-family:var(--mono); font-size:12px; color:var(--muted); margin:0 0 6px 2px;}
    .tickerline{border:1px solid var(--line); border-radius:14px; padding:10px 0; overflow:hidden; background:rgba(255,255,255,.02);}
    .marquee{position:relative; overflow:hidden;}
    .marqueeInner{display:inline-flex; align-items:center; gap:36px; white-space:nowrap; will-change:transform; animation:scroll 30s linear infinite;}
    .marquee:hover .marqueeInner{animation-play-state:paused;}
    #marqueeNews{ animation-duration: 250s; }
    @keyframes scroll{ 0%{transform:translateX(0);} 100%{transform:translateX(-50%);} }
    .tick{font-family:var(--mono); font-size:12px; color:var(--text);}
    .tick .tag{color:var(--cyan); font-weight:900;}
    .tick b{color:var(--amber);}

    /* Modal */
    .modal{display:none; position:fixed; inset:0; background:rgba(0,0,0,.72); padding: calc(14px + env(safe-area-inset-top)) 14px calc(14px + env(safe-area-inset-bottom)); z-index:9998;}
    .modal .box{max-width:1180px; margin:0 auto; background:var(--panel); border:1px solid var(--line); border-radius:16px; max-height:86vh; overflow:auto; -webkit-overflow-scrolling:touch;}
    .modal .head{position:sticky; top:0; background:rgba(11,17,26,.92); backdrop-filter:blur(10px);
                 display:flex; justify-content:space-between; align-items:center; padding:12px; border-bottom:1px solid var(--line);}
    .modal .body{padding:12px;}
    .h2{font-family:var(--mono); font-weight:900; color:var(--muted); font-size:12px; margin-bottom:8px;}
    .list{font-family:var(--mono); font-size:12px;}
    .item{padding:10px 0; border-top:1px solid var(--line);}
    .item .t{font-weight:900;}
    .item .m{color:var(--muted); margin-top:4px;}
    .kz{color:var(--muted); font-family:var(--mono); font-size:12px; margin-top:10px; line-height:1.45;}

    .iframebox{width:100%; height:72vh; border:1px solid var(--line); border-radius:14px; overflow:hidden;}
    .iframebox iframe{width:100%; height:100%; border:0;}
    .iframebox.dark iframe{filter: invert(1) hue-rotate(180deg) contrast(0.92) brightness(0.95);}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hdr">

    <!-- HEADER -->
    <div class="block">
      <div class="title">
        <div><b>NEWS BIAS</b> // TERMINAL</div>
        <div class="muted" style="font-family:var(--mono); font-size:12px;">Profile: <b style="color:var(--amber);">__GATE_PROFILE__</b></div>
      </div>
    </div>

    <!-- META -->
    <div class="block">
      <div class="blockTitle">META</div>
      <div class="metaLine">
        <span class="k">Updated (UTC):</span> <span class="v">__UPDATED_SHORT__</span>
        <span class="k">Event reason:</span> <span class="v">__EVENT_REASON_H__</span>
      </div>
      <div class="metaLine" style="margin-top:8px;">
        <span class="k">Next event:</span>
        <span class="pill neu" style="display:inline-flex;gap:10px;align-items:center;">
          <span style="color:var(--text);font-weight:900;">__NEXT_EVENT__</span>
          <span class="muted" style="font-weight:900;">__NEXT_EVENT_SRC__</span>
        </span>

      </div>
    </div>

    <!-- SWITCHBOARD -->
    <div class="block">
      <div class="blockTitle">SWITCHBOARD</div>
      <div class="chips">
        __EV_CHIP__ __TR_CHIP__ __FEEDS_CHIP__ __FRED_CHIP__ __RUN_CHIP__ __DIAG_CHIP__
      </div>
    </div>

    <!-- ACTIONS -->
    <div class="block">
      <div class="blockTitle">ACTIONS</div>
      <div class="btnrow">
        <button class="btn" onclick="runNow()">R RUN</button>
        <button class="btn" onclick="openMorning()">M MORNING</button>
        <button class="btn" onclick="openMyfx()">E MYFX CAL</button>
        <button class="btn" onclick="clearSavedToken()">Clear saved token</button>
      </div>
    </div>

    <!-- MARKET TAPE -->
    <div class="block">
      <div class="blockTitle">MARKET TAPE</div>
      <div class="tvwrap">
        <div class="tradingview-widget-container">
          <div class="tradingview-widget-container__widget"></div>
          <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
          { "symbols": __TV_SYMBOLS__, "showSymbolLogo": true, "isTransparent": true, "displayMode": "adaptive",
            "colorTheme": "dark", "locale": "en" }
          </script>
        </div>
      </div>
    </div>

    <!-- TICKERS -->
    <div class="block">
      <div class="blockTitle">TICKERS</div>
      <div class="tickerstack">
        <div>
          <div class="ticklabel">HEADLINES (latest)</div>
          <div class="tickerline marquee"><div class="marqueeInner" id="marqueeNews"></div></div>
        </div>
        <div>
          <div class="ticklabel">STATUS (bias + trade gate)</div>
          <div class="tickerline marquee"><div class="marqueeInner" id="marqueeStatus"></div></div>
        </div>
      </div>
    </div>

  </div>

  <div class="panel">
    <div class="blockTitle">SUMMARY TABLE</div>
    <table>
      <colgroup>
        <col style="width:90px">
        <col style="width:140px">
        <col style="width:140px">
        <col>
        <col style="width:92px">
      </colgroup>
      <thead><tr><th>SYM</th><th>BIAS</th><th>TRADE</th><th>WHY (short)</th><th></th></tr></thead>
      <tbody>__ROW_XAU__ __ROW_US500__ __ROW_WTI__</tbody>
    </table>
    <div class="kz">Goal: fast read. If you need deep math (score/Q2/conflict/flip), open <b>View</b>.</div>
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
  const RUN_TOKEN_REQUIRED = !!(PAYLOAD && PAYLOAD.meta && PAYLOAD.meta.run_token_required);
  const EXPECTED_HASH = (PAYLOAD && PAYLOAD.meta && PAYLOAD.meta.run_token_hashes) ? PAYLOAD.meta.run_token_hashes : [];

  function $(id){ return document.getElementById(id); }
  function escapeHtml(s){
    s = (s===undefined || s===null) ? '' : String(s);
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
  }
  function showModal(title, html){
    $('mt').innerText = title;
    $('mb').innerHTML = html;
    $('modal').style.display = 'block';
  }
  function closeModal(){ $('modal').style.display = 'none'; }

  function clearSavedToken(){
    localStorage.removeItem('run_token');
    showModal('RUN TOKEN', '<div class="panel"><div class="h2">Cleared</div><div class="list"><div class="item"><div class="t">Saved token removed</div><div class="m">Next RUN will prompt again.</div></div></div></div>');
  }

  function getRunToken(){
    if(!RUN_TOKEN_REQUIRED) return '';
    var t = localStorage.getItem('run_token') || '';
    if(!t){
      const hint = (EXPECTED_HASH && EXPECTED_HASH.length) ? ('expected_hash=' + EXPECTED_HASH.join(',')) : '';
      t = prompt('RUN_TOKEN is required. Paste token. ' + hint) || '';
      t = (t || '').trim();
      if(t) localStorage.setItem('run_token', t);
    }
    return (t || '').trim();
  }

  async function runNow(){
    try{
      var token = getRunToken();
      var url = token ? ('/run?token=' + encodeURIComponent(token)) : '/run';
      var resp = await fetch(url, { method:'POST' });
      var js = await resp.json();
      if(resp.status === 401){
        showModal('RUN UNAUTHORIZED', ''
          + '<div class="panel">'
          + '<div class="h2">Token mismatch</div>'
          + '<div class="list">'
          + '<div class="item"><div class="t">Server expects hash</div><div class="m">' + escapeHtml((js.expected_hash||EXPECTED_HASH||[]).join(',')) + '</div></div>'
          + '<div class="item"><div class="t">Fix</div><div class="m">Click "Clear saved token", then RUN again and paste correct token.</div></div>'
          + '</div></div>');
        return;
      }
      if(js && js.ok === false && js.error){
        showModal('RUN ERROR', '<div class="panel"><div class="h2">Error</div><div class="list"><div class="item"><div class="t">' + escapeHtml(js.error) + '</div></div></div></div>');
        return;
      }
      setTimeout(function(){ location.reload(); }, 350);
    }catch(e){
      showModal('RUN ERROR', '<div class="panel"><div class="h2">Exception</div><div class="list"><div class="item"><div class="t">' + escapeHtml(String(e)) + '</div></div></div></div>');
    }
  }

  function buildMarqueeNews(){
    fetch('/latest?limit=30').then(r=>r.json()).then(function(js){
      var items = js.items || [];
      var parts = [];
      items.slice(0, 18).forEach(function(it){
        parts.push('<span class="tick"><span class="tag">NEWS</span> <b>' + escapeHtml(it.source||'') + '</b> • ' + escapeHtml(it.title||'') + '</span>');
      });
      if(!parts.length) parts.push('<span class="tick"><span class="tag">NEWS</span> no headlines</span>');
      var line = parts.join(' <span class="muted">•</span> ');
      $('marqueeNews').innerHTML = line + ' <span class="muted">•</span> ' + line;
    }).catch(function(){
      $('marqueeNews').innerHTML = '<span class="tick"><span class="tag">NEWS</span> error loading headlines</span>';
    });
  }

  function buildMarqueeStatus(){
    var a = (PAYLOAD && PAYLOAD.assets) ? PAYLOAD.assets : {};
    var parts = [];
    ['XAU','US500','WTI'].forEach(function(sym){
      var x = a[sym] || {};
      var b = x.bias || '—';
      var gate = x.ui_gate || {};
      var trade = gate.ok ? 'TRADE OK' : 'NO TRADE';
      var why = gate.ok ? ((gate.why||[])[0] || '') : ((gate.fail_short||[]).join(' | ') || 'Blocked');
      parts.push('<span class="tick"><span class="tag">' + sym + '</span> <b>' + escapeHtml(b) + '</b> • <b>' + escapeHtml(trade) + '</b>' + (why ? (' • ' + escapeHtml(why)) : '') + '</span>');
    });
    var line = parts.join(' <span class="muted">•</span> ');
    $('marqueeStatus').innerHTML = line + ' <span class="muted">•</span> ' + line;
  }

  function openView(sym){
    var a = (PAYLOAD && PAYLOAD.assets) ? (PAYLOAD.assets[sym] || {}) : {};
    var gate = a.ui_gate || {};
    var top = (a.top3_drivers || []);
    var why5 = (a.why_top5 || []);

    var head =
      '<div class="panel"><div class="h2">' + escapeHtml(sym) + '</div><div class="list">'
      + '<div class="item"><div class="t">Bias</div><div class="m">' + escapeHtml(a.bias || '—') + '</div></div>'
      + '<div class="item"><div class="t">Trade decision</div><div class="m"><b>' + escapeHtml(gate.label || (gate.ok ? 'TRADE OK' : 'NO TRADE')) + '</b></div></div>'
      + '</div></div>';

    var drivers =
      '<div class="panel"><div class="h2">Top drivers (why bias)</div><div class="list">'
      + (top.length ? top : [{why:'—'}]).map(function(x,i){
        return '<div class="item"><div class="t">' + (i+1) + '. ' + escapeHtml(x.why || '—') + '</div></div>';
      }).join('')
      + '</div></div>';

    var blocks =
      '<div class="panel"><div class="h2">If NO TRADE — blocks</div><div class="list">'
      + (((gate.fail_reasons || []).length ? (gate.fail_reasons || []) : ['—']).map(function(x){
        return '<div class="item"><div class="t">' + escapeHtml(x) + '</div></div>';
      }).join(''))
      + '</div>'
      + '<div class="kz"><b>What to wait for:</b><br>'
      + (((gate.must_change || []).length ? (gate.must_change || []).map(function(x){ return '→ ' + escapeHtml(x); }).join('<br>') : '—'))
      + '</div></div>';

    var src =
      '<div class="panel"><div class="h2">Sources (top 5 evidence, optional)</div><div class="list">'
      + (why5.length ? why5 : [{title:'—'}]).slice(0,5).map(function(x,i){
        var l = x.link || '';
        return '<div class="item">'
               + '<div class="t">' + (i+1) + '. ' + escapeHtml(x.why || '—') + '</div>'
               + '<div class="m">' + escapeHtml(x.title || '') + '</div>'
               + (l ? ('<div class="m"><a href="' + escapeHtml(l) + '" target="_blank" rel="noopener">Open source</a></div>') : '')
               + '</div>';
      }).join('')
      + '</div></div>';

    showModal('VIEW ' + sym, head + drivers + blocks + src);
  }

  function openMorning(){
    var ev = (PAYLOAD && PAYLOAD.event) ? PAYLOAD.event : {};
    var upcoming = (ev.upcoming_events || []).slice(0, 12);

    function evRow(x){
      const ts = x.ts ? x.ts : null;
      const when = ts ? new Date(ts*1000).toISOString().replace('T',' ').slice(0,16) + ' UTC' : '(time unknown)';
      const ccy = x.currency || '—';
      const imp = x.impact || '—';
      return '<div class="item"><div class="t">' + escapeHtml(when) + ' • ' + escapeHtml(ccy) + ' • ' + escapeHtml(imp) + '</div>'
             + '<div class="m">' + escapeHtml(x.title||'') + '</div>'
             + (x.link ? ('<div class="m"><a href="' + escapeHtml(x.link) + '" target="_blank" rel="noopener">Open</a></div>') : '')
             + '</div>';
    }

    var html =
      '<div class="panel"><div class="h2">Morning</div><div class="list">'
      + '<div class="item"><div class="t">Updated (UTC)</div><div class="m">' + escapeHtml(PAYLOAD.updated_utc || '') + '</div></div>'
      + '<div class="item"><div class="t">Event mode</div><div class="m">' + escapeHtml(ev.event_mode ? 'ON' : 'OFF') + ' • ' + escapeHtml((ev.reason||[]).join(' | ') || '—') + '</div></div>'
      + '</div></div>'
      + '<div class="panel"><div class="h2">Upcoming events</div><div class="list">'
      + (upcoming.length ? upcoming.map(evRow).join('') : '<div class="item"><div class="t">—</div></div>')
      + '</div></div>';

    showModal('M MORNING', html);
  }

  function openMyfx(){
    var html = ''
      + '<div class="panel">'
      + '  <div class="h2">MyFXBook Economic Calendar</div>'
      + '  <div class="btnrow">'
      + '    <button class="btn" onclick="toggleMyfxDark()">Toggle Dark</button>'
      + '    <span class="muted" style="font-family:var(--mono); font-size:12px;">Widget is white by default; dark mode uses CSS filter.</span>'
      + '  </div>'
      + '  <div class="iframebox dark" id="myfxBox"><iframe src="/myfx_calendar" loading="lazy"></iframe></div>'
      + '</div>';
    showModal('E MYFX CAL', html);
  }

  function toggleMyfxDark(){
    var box = document.getElementById('myfxBox');
    if(!box) return;
    if(box.classList.contains('dark')) box.classList.remove('dark');
    else box.classList.add('dark');
  }

  document.addEventListener('keydown', function(e){
    var k = String(e.key||'').toLowerCase();
    if(k === 'escape') closeModal();
    if(k === 'r') runNow();
    if(k === 'm') openMorning();
    if(k === 'e') openMyfx();
    if(k === '1') openView('XAU');
    if(k === '2') openView('US500');
    if(k === '3') openView('WTI');
  });

  $('modal').addEventListener('click', function(e){
    if(e.target && e.target.id === 'modal') closeModal();
  });

  buildMarqueeNews();
  buildMarqueeStatus();
  setInterval(buildMarqueeNews, 120000);
</script>
</body>
</html>
"""

    html = (TEMPLATE
        .replace("__UPDATED_SHORT__", str(updated_short))
        .replace("__GATE_PROFILE__", str(gate_profile))
        .replace("__EVENT_REASON_H__", str(reason_txt))
        .replace("__NEXT_EVENT__", str(next_event_line))
        .replace("__NEXT_EVENT_SRC__", (f"source={next_event_src}" if next_event_src else ""))
        .replace("__EV_CHIP__", ev_chip)
        .replace("__TR_CHIP__", tr_chip)
        .replace("__FEEDS_CHIP__", fd_chip)
        .replace("__FRED_CHIP__", fr_chip)
        .replace("__RUN_CHIP__", rn_chip)
        .replace("__DIAG_CHIP__", dg_chip)
        .replace("__ROW_XAU__", row("XAU"))
        .replace("__ROW_US500__", row("US500"))
        .replace("__ROW_WTI__", row("WTI"))
        .replace("__JS_PAYLOAD__", js_payload)
        .replace("__TV_SYMBOLS__", tv_symbols)
    )
    return HTMLResponse(html)
