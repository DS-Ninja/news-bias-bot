# app.py
# News Bias Bot (MVP++) — RSS + Trade Gate + iPhone-friendly UI + FRED macro drivers + Morning Plan
#
# Fixes in this version:
# - ✅ No hard dependency on "requests" (urllib fallback) -> Railway won't crash if requests missing
# - ✅ /diag endpoint for DB+env sanity checks
# - ✅ /dashboard wrapped with fail-safe HTML (never blank 500)
# - ✅ GET /run to avoid "Method Not Allowed" when opening in browser
# - ✅ published_parsed converted via calendar.timegm (UTC-safe)
# - ✅ FRED ingest throttled (skip if already fresh)
#
# Run:
#   uvicorn app:app --host 0.0.0.0 --port 8000

import os
import json
import time
import re
import hashlib
import math
import calendar
from datetime import datetime, timezone, date
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse

# --- Optional requests (fallback to urllib if missing)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

from urllib.request import urlopen, Request
from urllib.parse import urlencode

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

# Bias thresholds
BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

# Trump headlines unified
TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

# Event mode config
EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": int(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 6,
}

# FRED config
FRED_CFG = {
    "enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
    "api_key": os.environ.get("FRED_API_KEY", "").strip(),
    "window_days": int(os.environ.get("FRED_WINDOW_DAYS", "120")),
}
if not FRED_CFG["api_key"]:
    # if no key -> auto-disable (avoid misleading UI + avoid calls)
    FRED_CFG["enabled"] = False

FRED_SERIES = {
    "DGS10":    {"name": "US 10Y Nominal", "freq": "d"},
    "DFII10":   {"name": "US 10Y Real",    "freq": "d"},
    "T10YIE":   {"name": "10Y Breakeven",  "freq": "d"},
    "DTWEXBGS": {"name": "Broad USD",      "freq": "d"},
    "VIXCLS":   {"name": "VIX",            "freq": "d"},
    "BAA10Y":   {"name": "BAA-10Y Spread", "freq": "d"},
}

# ============================================================
# RSS FEEDS
# ============================================================

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

    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
}

CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR"}

# ============================================================
# SOURCE WEIGHTS
# ============================================================

SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.8,

    "FXSTREET_NEWS": 1.4,
    "FXSTREET_ANALYSIS": 1.2,

    "MARKETWATCH_TOP": 1.2,
    "MARKETWATCH_REALTIME": 1.3,

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

    "RSSAPP_1": 1.0,
    "RSSAPP_2": 1.0,

    "TRUMP_HEADLINES": 1.2,

    "FOREXFACTORY_CALENDAR": 0.0,
    "FRED": 1.0,
}

# ============================================================
# RULES (regex -> signed weight + explanation)
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
        return psycopg2.connect(db_url, connect_timeout=8)

    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    db = os.environ.get("PGDATABASE", "postgres")
    user = os.environ.get("PGUSER", "postgres")
    pwd = os.environ.get("PGPASSWORD", "")
    sslmode = os.environ.get("PGSSLMODE", "prefer")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, sslmode=sslmode, connect_timeout=8)

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
    _, payload_json = row
    try:
        return json.loads(payload_json)
    except Exception:
        return None

# ============================================================
# HELPERS
# ============================================================

def fingerprint(title: str, link: str) -> str:
    s = (title or "").strip() + "||" + (link or "").strip()
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def decay_weight(age_sec: int) -> float:
    if age_sec < 0:
        age_sec = 0
    return math.exp(-LAMBDA * float(age_sec))

def _fresh_bucket(age_sec: int) -> str:
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"

def match_rules(asset: str, title: str) -> List[Dict[str, Any]]:
    out = []
    t = (title or "").lower()
    for (pat, w, why) in RULES.get(asset, []):
        if re.search(pat, t, flags=re.I):
            out.append({"pattern": pat, "w": float(w), "why": why})
    return out

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

# ============================================================
# INGEST
# ============================================================

def ingest_once(limit_per_feed: int = 40) -> int:
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

                    published_parsed = e.get("published_parsed")
                    if published_parsed:
                        # UTC-safe
                        published_ts = int(calendar.timegm(published_parsed))
                    else:
                        published_ts = now

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
# EVENT MODE
# ============================================================

def _get_upcoming_events(now_ts: int) -> List[Dict[str, Any]]:
    url = RSS_FEEDS.get("FOREXFACTORY_CALENDAR")
    if not url:
        return []
    try:
        d = feedparser.parse(url)
        entries = getattr(d, "entries", []) or []
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    lookahead_sec = int(EVENT_CFG["lookahead_hours"] * 3600)
    horizon = now_ts + lookahead_sec

    for e in entries[:200]:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()

        ts = None
        published_parsed = e.get("published_parsed")
        if published_parsed:
            ts = int(calendar.timegm(published_parsed))

        if ts is not None:
            if now_ts <= ts <= horizon:
                out.append({"title": title, "link": link, "ts": int(ts), "in_hours": round((ts - now_ts) / 3600.0, 2)})
        else:
            if len([x for x in out if x.get("ts") is None]) < 2:
                out.append({"title": title, "link": link, "ts": None, "in_hours": None})

        if len(out) >= EVENT_CFG["max_upcoming"]:
            break

    out.sort(key=lambda x: (x["ts"] is None, x["ts"] or 0))
    return out

def _macro_recent_flag(rows: List[Tuple[str, str, str, int]], now_ts: int) -> bool:
    recent_sec = int(EVENT_CFG["recent_hours"] * 3600)
    macro_sources = {"FED", "BLS", "BEA"}
    for (source, _title, _link, ts) in rows:
        if source in macro_sources and (now_ts - int(ts)) <= recent_sec:
            return True
    return False

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
# HTTP (requests optional)
# ============================================================

def http_get_json(url: str, params: Dict[str, Any], timeout_sec: int = 12) -> Dict[str, Any]:
    if requests is not None:
        r = requests.get(url, params=params, timeout=timeout_sec)
        r.raise_for_status()
        return r.json()

    # urllib fallback
    qs = urlencode({k: str(v) for k, v in params.items() if v is not None})
    full = url + ("?" + qs if qs else "")
    req = Request(full, headers={"User-Agent": "news-bias-bot/1.0"})
    with urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    return json.loads(raw)

# ============================================================
# FRED
# ============================================================

def fred_fetch_observations(series_id: str, days: int = 120) -> List[Tuple[str, Optional[float]]]:
    if not (FRED_CFG["enabled"] and FRED_CFG["api_key"]):
        return []
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_CFG["api_key"],
        "file_type": "json",
        "sort_order": "desc",
        "limit": max(60, min(5000, days * 2)),
    }
    js = http_get_json(url, params=params, timeout_sec=12)
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

def _fred_is_fresh() -> bool:
    # Heuristic: if we have at least one series with today's row, consider fresh.
    today = date.today().isoformat()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM fred_series
                WHERE obs_date = %s::date
                LIMIT 1;
            """, (today,))
            return cur.fetchone() is not None

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

    # XAU
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

    # US500
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

    # WTI
    if len(usd) >= 6:
        latest, past = usd[0], usd[5]
        d = _pct_change(latest, past)
        add("WTI", "DTWEXBGS", w=(-0.4 if d > 0 else +0.2), note="USD headwind/tailwind", value=latest, delta=d)

    return out

# ============================================================
# SCORING / QUALITY
# ============================================================

def _flip_distances(score: float, th: float) -> Dict[str, float]:
    to_bullish = max(0.0, th - score)
    to_bearish = max(0.0, score + th)
    return {
        "to_bullish": round(to_bullish, 4),
        "to_bearish": round(to_bearish, 4),
        "note": "Δ needed in score units to reach +th (bullish) or -th (bearish).",
    }

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
    out = []
    for src, v in acc.items():
        out.append({"source": src, "net": round(v["net"], 4), "abs": round(v["abs"], 4), "count": int(cnt[src])})
    out.sort(key=lambda x: x["abs"], reverse=True)
    return out

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

    # --- FRED ingest + drivers (throttled)
    fred_inserted = 0
    fred_drivers = {"XAU": [], "US500": [], "WTI": []}
    if FRED_CFG["enabled"] and FRED_CFG["api_key"]:
        try:
            if not _fred_is_fresh():
                for sid in FRED_SERIES.keys():
                    try:
                        fred_inserted += fred_ingest_series(sid, days=FRED_CFG["window_days"])
                    except Exception:
                        pass
            fred_drivers = compute_fred_drivers()
        except Exception:
            fred_drivers = {"XAU": [], "US500": [], "WTI": []}

    # --- Event mode
    upcoming_events = _get_upcoming_events(now) if EVENT_CFG["enabled"] else []
    recent_macro = _macro_recent_flag(rows, now)
    event_mode = False
    if EVENT_CFG["enabled"]:
        event_mode = bool(recent_macro or any(x.get("ts") is not None for x in upcoming_events))

    # --- Trump flag
    trump = trump_flag_recent(rows, now, hours=12.0)

    assets_out: Dict[str, Any] = {}
    for asset in ASSETS:
        score = 0.0
        contribs: List[Dict[str, Any]] = []
        freshness = {"0-2h": 0, "2-8h": 0, "8-24h": 0}

        for (source, title, link, ts) in rows:
            age_sec = now - int(ts)
            if age_sec < 0:
                age_sec = 0
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

        why_top5 = sorted(contribs, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]
        evidence_count = len(contribs)
        src_div = len(set([w["source"] for w in contribs])) if contribs else 0

        # Quality v1
        strength = min(1.0, abs(score) / max(th, 1e-9))
        quality_v1 = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, src_div * 2.0)))

        # Consensus/conflict
        consensus_ratio, conflict_index, _abs_sum = _consensus_stats(contribs)

        # Quality v2
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
        flip = _flip_distances(score, th)
        cons_by_src = _consensus_by_source(contribs)

        assets_out[asset] = {
            "bias": bias,
            "score": round(score, 4),
            "threshold": th,

            "quality": int(quality_v1),
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
                "enabled": bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"])),
                "inserted_points_last_run": int(fred_inserted),
                "series": list(FRED_SERIES.keys()),
            },
            "trump": trump,
        },
        "event": {
            "enabled": bool(EVENT_CFG["enabled"]),
            "event_mode": bool(event_mode),
            "recent_macro": bool(recent_macro),
            "upcoming_events": upcoming_events,
            "lookahead_hours": float(EVENT_CFG["lookahead_hours"]),
            "recent_hours": float(EVENT_CFG["recent_hours"]),
            "ff_calendar_enabled": "FOREXFACTORY_CALENDAR" in RSS_FEEDS,
        }
    }
    return payload

def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=40)
    payload = compute_bias(lookback_hours=24, limit_rows=1200)
    payload["meta"]["inserted_last_run"] = int(inserted)
    payload["meta"]["feeds_status"] = feeds_health_live()
    save_bias(payload)
    return payload

# ============================================================
# TRADE GATE
# ============================================================

def eval_trade_gate(asset_obj: Dict[str, Any], event_mode: bool, profile: str) -> Dict[str, Any]:
    cfg = GATE_THRESHOLDS.get(profile, GATE_THRESHOLDS["STRICT"])

    bias = str(asset_obj.get("bias", "NEUTRAL"))
    score = float(asset_obj.get("score", 0.0))
    th = float(asset_obj.get("threshold", 1.0))
    q2 = int(asset_obj.get("quality_v2", 0))
    conflict = float(asset_obj.get("conflict_index", 1.0))
    flip = asset_obj.get("flip", {}) or {}
    to_bull = float(flip.get("to_bullish", max(0.0, th - score)))
    to_bear = float(flip.get("to_bearish", max(0.0, score + th)))

    if bias == "BULLISH":
        opp_dist = to_bear
        opp_label = "to_bearish"
    elif bias == "BEARISH":
        opp_dist = to_bull
        opp_label = "to_bullish"
    else:
        opp_dist = min(to_bull, to_bear)
        opp_label = "min(to_bullish,to_bearish)"

    fail_reasons: List[str] = []
    must_change: List[str] = []

    if bias == "NEUTRAL" and not cfg.get("neutral_allow", False):
        fail_reasons.append("Bias is NEUTRAL")
        must_change.append("bias must become BULLISH or BEARISH (score must cross ±threshold)")
    elif bias == "NEUTRAL" and cfg.get("neutral_allow", False):
        maxd = float(cfg.get("neutral_flip_dist_max", 0.20))
        if min(to_bull, to_bear) > maxd:
            fail_reasons.append("Bias is NEUTRAL (not near flip)")
            must_change.append(f"Δ to threshold must shrink: min(to_bullish,to_bearish) ≤ {maxd}")

    qmin = int(cfg["quality_v2_min"])
    if q2 < qmin:
        fail_reasons.append(f"Quality v2 too low ({q2} < {qmin})")
        must_change.append(f"quality_v2 must be ≥ {qmin}")

    cmax = float(cfg["conflict_max"])
    if conflict > cmax:
        fail_reasons.append(f"Conflict too high ({conflict} > {cmax})")
        must_change.append(f"conflict_index must be ≤ {cmax}")

    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH"):
        if opp_dist < mind:
            fail_reasons.append(f"Too close to opposite flip ({opp_label}={round(opp_dist,4)} < {mind})")
            must_change.append(f"{opp_label} must be ≥ {mind}")

    if event_mode and cfg.get("event_mode_block", True):
        oq = int(cfg.get("event_override_quality", 70))
        oc = float(cfg.get("event_override_conflict", 0.45))
        if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
            fail_reasons.append("Event mode ON (macro risk window)")
            must_change.append(
                f"Either wait until event_mode=OFF, or require quality_v2 ≥ {oq} and conflict_index ≤ {oc} (and bias != NEUTRAL)."
            )

    ok = (len(fail_reasons) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:3] if x.get("why")] or ["Insufficient matched evidence (rules)"]

    return {
        "ok": bool(ok),
        "label": "TRADE OK" if ok else "NO TRADE",
        "why": why_short[:3],
        "fail_reasons": fail_reasons[:4],
        "must_change": must_change[:4],
    }

# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP++)")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)

@app.get("/health")
def health():
    return {
        "ok": True,
        "gate_profile": GATE_PROFILE,
        "trump_enabled": bool(TRUMP_ENABLED),
        "fred_enabled": bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"])),
    }

@app.get("/diag")
def diag():
    # DB + counts + env flags
    out = {"db": {"ok": False}, "env": {}}
    try:
        db_init()
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM news_items;")
                news_cnt = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fred_series;")
                fred_cnt = int(cur.fetchone()[0])
                cur.execute("SELECT 1 FROM bias_state WHERE id=1;")
                has_bias = cur.fetchone() is not None
        out["db"] = {"ok": True, "news_items": news_cnt, "fred_points": fred_cnt, "has_bias_state": bool(has_bias)}
    except Exception as e:
        out["db"] = {"ok": False, "error": str(e)}

    out["env"] = {
        "has_DATABASE_URL": bool(os.environ.get("DATABASE_URL")),
        "PGHOST": os.environ.get("PGHOST", ""),
        "PGDATABASE": os.environ.get("PGDATABASE", ""),
        "PGUSER": os.environ.get("PGUSER", ""),
        "PGSSLMODE": os.environ.get("PGSSLMODE", "prefer"),
        "GATE_PROFILE": GATE_PROFILE,
        "TRUMP_ENABLED": str(int(TRUMP_ENABLED)),
        "FRED_ENABLED": str(int(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"]))),
        "HAS_REQUESTS": bool(requests is not None),
    }
    return JSONResponse(out)

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

@app.post("/run")
def run_now():
    return pipeline_run()

@app.get("/run", include_in_schema=False)
def run_now_get():
    # nice UX for browser
    return HTMLResponse("""
    <html><body style="font-family:Arial;padding:18px;">
      <h2>/run is POST</h2>
      <p>Use the dashboard button <b>Run now</b> or call:</p>
      <pre>curl -X POST https://YOUR_APP/run</pre>
      <p><a href="/dashboard">Back to dashboard</a></p>
    </body></html>
    """)

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

@app.get("/explain")
def explain(asset: str = "US500", limit: int = 60):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return {"error": "Unknown asset. Use XAU, US500, WTI."}

    db_init()
    now = int(time.time())
    cutoff = now - 24 * 3600

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT 1200;
            """, (cutoff,))
            rows = cur.fetchall()

    out = []
    for (source, title, link, ts) in rows:
        matches = match_rules(asset, title)
        if not matches:
            continue
        age = now - int(ts)
        if age < 0:
            age = 0
        w_src = float(SOURCE_WEIGHT.get(source, 1.0))
        w_time = decay_weight(age)
        for m in matches:
            contrib = float(m["w"]) * w_src * float(w_time)
            out.append({
                "source": source,
                "title": title,
                "link": link,
                "age_min": int(age / 60),
                "base_w": float(m["w"]),
                "src_w": float(w_src),
                "time_w": round(float(w_time), 4),
                "contrib": round(float(contrib), 4),
                "why": m["why"],
                "pattern": m["pattern"],
            })

    out_sorted = sorted(out, key=lambda x: abs(float(x["contrib"])), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}

@app.get("/feeds_health")
def feeds_health():
    return feeds_health_live()

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
    trump = meta.get("trump", {}) or {}

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
        "upcoming_events": (event.get("upcoming_events", []) or [])[:6],
        "trump": trump,
        "plan": {"XAU": pack("XAU"), "US500": pack("US500"), "WTI": pack("WTI")}
    }
    return JSONResponse(out)

# ============================================================
# UI (Dashboard) — FAIL-SAFE WRAPPER
# ============================================================

def _safe_html_error(title: str, err: str) -> HTMLResponse:
    return HTMLResponse(f"""
    <html><head><meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="font-family:Arial;padding:16px;">
      <h2>{title}</h2>
      <pre style="white-space:pre-wrap;background:#111;color:#eee;padding:12px;border-radius:10px;">{err}</pre>
      <p><a href="/diag">/diag</a> • <a href="/bias?pretty=1">Bias JSON</a> • <a href="/dashboard">Reload</a></p>
    </body></html>
    """)

def _pill(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="pill pill-bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="pill pill-bear">BEARISH</span>'
    return '<span class="pill pill-neutral">NEUTRAL</span>'

def _pill_gate(ok: bool) -> str:
    if ok:
        return '<span class="pill pill-ok">✅ TRADE OK</span>'
    return '<span class="pill pill-no">❌ NO TRADE</span>'

def _bar(v: int) -> str:
    vv = max(0, min(100, int(v)))
    return f"""
    <div class="bar"><div class="bar-fill" style="width:{vv}%"></div></div>
    <div class="bar-num">{vv}/100</div>
    """

def _tooltip(label: str, text: str) -> str:
    return f"""
    <span class="tipwrap">
      <span class="tipicon" tabindex="0" role="button" aria-label="{label}" data-tip="{text}">ⓘ</span>
    </span>
    """

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(compact: int = 0):
    try:
        db_init()
        payload = load_bias()
        if not payload:
            payload = pipeline_run()

        assets = payload.get("assets", {}) or {}
        updated = payload.get("updated_utc", "")
        meta = payload.get("meta", {}) or {}
        feeds_status = meta.get("feeds_status", {}) or {}
        gate_profile = str(meta.get("gate_profile", GATE_PROFILE))

        event = payload.get("event", {}) or {}
        event_mode = bool(event.get("event_mode", False))
        upcoming = event.get("upcoming_events", []) or []

        trump = meta.get("trump", {}) or {}
        trump_flag = bool(trump.get("flag", False))
        trump_enabled = bool(trump.get("enabled", False))

        # Pull last headlines
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source, title, link, published_ts
                    FROM news_items
                    ORDER BY published_ts DESC
                    LIMIT 220;
                """)
                rows = cur.fetchall()

        ev_html = '<div class="pill pill-warn">⚠️ EVENT MODE: ON</div>' if event_mode else '<div class="pill pill-ok">✅ EVENT MODE: OFF</div>'
        if trump_enabled:
            tr_badge = '<span class="pill pill-warn">TRUMP: ON</span>' if trump_flag else '<span class="pill pill-neutral">TRUMP: quiet</span>'
        else:
            tr_badge = '<span class="pill pill-neutral">TRUMP: disabled</span>'

        up_html = ""
        for x in upcoming[:6]:
            t = x.get("title", "")
            link = x.get("link", "")
            inh = x.get("in_hours", None)
            tag = f"in {inh}h" if inh is not None else "time unknown"
            up_html += f'<li><a href="{link}" target="_blank" rel="noopener">{t}</a> <span class="muted tiny">({tag})</span></li>'
        if not up_html:
            up_html = "<li class='muted'>—</li>"

        feeds_rows = ""
        for src in RSS_FEEDS.keys():
            st = feeds_status.get(src, {})
            ok = bool(st.get("ok", False))
            skipped = bool(st.get("skipped", False))
            entries = st.get("entries", 0)
            status = "OK" if ok else "ERR"
            if skipped:
                status = "SKIP"
            pill = '<span class="pill pill-ok">OK</span>' if status == "OK" else ('<span class="pill pill-neutral">SKIP</span>' if status == "SKIP" else '<span class="pill pill-no">ERR</span>')
            feeds_rows += f"<tr><td>{src}</td><td>{pill}</td><td class='muted'>{entries}</td></tr>"

        def render_asset(asset: str) -> str:
            a = assets.get(asset, {}) or {}
            bias = str(a.get("bias", "NEUTRAL"))
            score = float(a.get("score", 0.0))
            th = float(a.get("threshold", 1.0))

            q1 = int(a.get("quality", 0))
            q2 = int(a.get("quality_v2", 0))

            evc = int(a.get("evidence_count", 0))
            div = int(a.get("source_diversity", 0))

            consensus_ratio = float(a.get("consensus_ratio", 0.0))
            conflict_index = float(a.get("conflict_index", 1.0))

            freshness = a.get("freshness", {"0-2h": 0, "2-8h": 0, "8-24h": 0}) or {}
            flip = a.get("flip", {}) or {}
            cons_by_src = a.get("consensus_by_source", []) or []
            why_top5 = a.get("why_top5", []) or []
            top3 = a.get("top3_drivers", []) or []

            gate = eval_trade_gate(a, event_mode, gate_profile)

            tip_q1 = _tooltip("Quality v1", "v1 = strength vs threshold + evidence count + source diversity.")
            tip_q2 = _tooltip("Quality v2", "v2 = strength + evidence + diversity + freshness + consensus − conflict − (event penalty).")
            tip_conf = _tooltip("Conflict", "conflict_index = 1 - consensus_ratio. High conflict = unstable bias.")
            tip_cons = _tooltip("Consensus", "consensus_ratio = |net| / sum_abs. Higher = agreement in direction.")
            tip_flip = _tooltip("Flip", "Δ needed for score to reach bullish (+th) or bearish (-th) threshold.")
            tip_gate = _tooltip("Trade Gate", f"Gate = Bias + Quality v2 + Conflict + Event Mode + Flip guard. Profile: {gate_profile}.")

            gate_why = "".join([f"<li>{x}</li>" for x in (gate.get("why", []) or [])[:3]]) or "<li>—</li>"
            gate_need = "".join([f"<li>{x}</li>" for x in (gate.get('must_change', []) or [])[:3]]) or "<li>—</li>"

            td_html = ""
            for i, x in enumerate(top3[:3], start=1):
                td_html += f"""
                  <div class="td-row">
                    <div><b>{i}. {x.get('why','')}</b></div>
                    <div class="muted tiny">abs={x.get('abs_contrib_sum','')}</div>
                  </div>
                """
            if not td_html:
                td_html = '<div class="muted">—</div>'

            cs_rows = ""
            for x in cons_by_src[:8]:
                net = float(x.get("net", 0.0))
                net_cls = "pos" if net > 0 else ("neg" if net < 0 else "muted")
                cs_rows += f"""
                <tr>
                  <td>{x.get('source','')}</td>
                  <td class="{net_cls}">{x.get('net','')}</td>
                  <td class="muted">{x.get('abs','')}</td>
                  <td class="muted">{x.get('count','')}</td>
                </tr>
                """
            if not cs_rows:
                cs_rows = '<tr><td class="muted">—</td><td></td><td></td><td></td></tr>'

            why_html = ""
            for w in why_top5[:5]:
                why_html += f"""
                <li>
                  <div class="why-row">
                    <div><b>{w.get("why","")}</b></div>
                    <div class="why-meta">{w.get("source","")} • age={w.get("age_min","")}m • contrib={w.get("contrib","")}</div>
                  </div>
                  <div class="why-headline"><a href="{w.get("link","")}" target="_blank" rel="noopener">{w.get("title","")}</a></div>
                </li>
                """

            kw = {
                "XAU": ["gold", "xau", "fed", "fomc", "cpi", "inflation", "yields", "usd", "treasury", "safe-haven", "real"],
                "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "s&p", "nasdaq", "equities", "vix", "rates", "yields"],
                "WTI": ["oil", "crude", "wti", "opec", "inventory", "stocks", "pipeline", "sanctions", "outage", "spr", "output"],
            }[asset]
            shown = 0
            news_html = ""
            for (source, title, link, _ts) in rows:
                t = (title or "").lower()
                if not any(k in t for k in kw):
                    continue
                shown += 1
                if shown > (6 if compact else 10):
                    break
                news_html += f'<li><a href="{link}" target="_blank" rel="noopener">{title}</a> <span class="muted">[{source}]</span></li>'

            to_bull = float(flip.get("to_bullish", 0.0))
            to_bear = float(flip.get("to_bearish", 0.0))

            return f"""
            <section class="card" id="card-{asset}">
              <div class="card-head">
                <div class="head-left">
                  <div class="h2">
                    {asset} {_pill(bias)}
                    <span class="muted">score={round(score,3)} / th={round(th,3)}</span>
                    <span class="spacer"></span>
                    {_pill_gate(bool(gate.get("ok")))} {tip_gate}
                  </div>
                  <div class="sub muted tiny">
                    evidence={evc} • source_diversity={div} • {tip_cons} consensus={consensus_ratio} • {tip_conf} conflict={conflict_index}
                  </div>
                </div>
                <div class="actions">
                  <div class="btnrow">
                    <button class="btn" onclick="runNow('ALL')">Run now</button>
                    <button class="btn" onclick="showExplain('{asset}')">Explain</button>
                  </div>
                  <div class="tiny muted" id="status-{asset}" style="margin-top:6px;"></div>
                </div>
              </div>

              <div class="grid3">
                <div class="panel">
                  <div class="h3">Signal Quality</div>
                  <div class="qblock">
                    <div class="qrow"><div class="muted tiny">v1 {tip_q1}</div><div class="qval">{q1}/100</div></div>
                    {_bar(q1)}
                  </div>
                  <div class="qblock">
                    <div class="qrow"><div class="muted tiny">v2 {tip_q2}</div><div class="qval">{q2}/100</div></div>
                    {_bar(q2)}
                  </div>
                  <div class="muted tiny">freshness: 0-2h={freshness.get("0-2h",0)} • 2-8h={freshness.get("2-8h",0)} • 8-24h={freshness.get("8-24h",0)}</div>
                </div>

                <div class="panel">
                  <div class="h3">Trade Gate</div>
                  <div class="gatebox">
                    <div class="gatebadge">{_pill_gate(bool(gate.get("ok")))} </div>
                    <div class="muted tiny">WHY (top)</div>
                    <ul class="mini">{gate_why}</ul>
                    <div class="muted tiny" style="margin-top:10px;">To become OK:</div>
                    <ul class="mini">{gate_need}</ul>
                  </div>
                </div>

                <div class="panel">
                  <div class="h3">What would flip bias {tip_flip}</div>
                  <div class="flipgrid">
                    <div class="flipcard"><div class="muted tiny">to bullish</div><div class="flipnum">{to_bull}</div></div>
                    <div class="flipcard"><div class="muted tiny">to bearish</div><div class="flipnum">{to_bear}</div></div>
                  </div>
                  <div class="h3" style="margin-top:12px;">Top 3 drivers now</div>
                  <div class="td">{td_html}</div>
                </div>
              </div>

              <div class="grid2">
                <div class="panel compact-hide">
                  <div class="h3">Consensus by source</div>
                  <table class="ctable">
                    <thead><tr><th>Source</th><th>Net</th><th>Abs</th><th>n</th></tr></thead>
                    <tbody>{cs_rows}</tbody>
                  </table>
                </div>

                <div class="panel compact-hide">
                  <div class="h3">WHY (top 5)</div>
                  <ol class="why">{why_html or "<li>—</li>"}</ol>
                </div>

                <div class="panel">
                  <div class="h3">Latest relevant headlines</div>
                  <ul class="news">{news_html or "<li>—</li>"}</ul>
                </div>
              </div>
            </section>
            """

        compact_on = "1" if compact else "0"

        html = f"""
        <html data-compact="{compact_on}">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
          <meta name="color-scheme" content="dark light">
          <title>News Bias Dashboard</title>
          <style>
            :root {{
              --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
              --line:rgba(255,255,255,.08);
              --bull:#10b981; --bear:#ef4444; --neu:#64748b; --warn:#f59e0b;
              --ok:#22c55e; --no:#ef4444;
              --btn:#1b2636; --btn2:#223047;
              --link:#7dd3fc;
            }}
            html[data-theme="light"] {{
              --bg:#f7f8fb; --card:#ffffff; --muted:#5c6b7a; --text:#0b1220;
              --line:rgba(15,23,42,.12);
              --btn:#eef2f7; --btn2:#e5ecf5;
              --link:#0369a1;
              --ok:#16a34a; --no:#dc2626; --warn:#d97706;
            }}
            html, body {{ height:100%; }}
            body {{
              -webkit-text-size-adjust: 100%;
              font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
              background:var(--bg); color:var(--text); margin:0;
              padding: 14px;
            }}
            a {{ color:var(--link); text-decoration:none; }}
            a:hover {{ text-decoration:underline; }}
            .wrap {{ max-width: 1120px; margin: 0 auto; }}
            .top {{
              display:flex; justify-content:space-between; align-items:flex-end; gap:12px;
              position: sticky; top: 0; z-index: 50;
              background: color-mix(in srgb, var(--bg) 82%, transparent);
              backdrop-filter: blur(10px);
              padding: 12px 0;
              border-bottom: 1px solid var(--line);
            }}
            h1 {{ margin:0; font-size: 24px; }}
            .muted {{ color:var(--muted); }}
            .tiny {{ font-size:12px; }}
            .spacer {{ display:inline-block; width: 10px; }}
            .mini {{ position: sticky; top: 62px; z-index: 40; margin: 10px 0 14px 0; }}
            .mini-inner {{
              display:flex; gap:10px; align-items:center; justify-content:space-between;
              background: color-mix(in srgb, var(--card) 92%, transparent);
              border:1px solid var(--line);
              border-radius: 16px;
              padding: 10px 10px;
              backdrop-filter: blur(10px);
            }}
            .tabs {{ display:flex; gap:8px; flex-wrap:wrap; }}
            .tab {{
              display:inline-flex; gap:8px; align-items:center;
              padding:8px 10px; border-radius: 14px;
              border: 1px solid var(--line);
              background: var(--btn);
              cursor:pointer;
              font-weight:800;
            }}
            .tab:hover {{ background: var(--btn2); }}
            .tab small {{ font-weight:800; color:var(--muted); }}
            .toggles {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }}
            .card {{
              background:var(--card); border:1px solid var(--line);
              border-radius:18px; padding:14px; margin: 14px 0;
            }}
            .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
            .head-left {{ flex:1; min-width: 240px; }}
            .h2 {{ font-size:18px; font-weight:900; line-height: 1.25; display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
            .h3 {{ font-size:13px; margin: 0 0 10px 0; font-weight:900; }}
            .sub {{ margin-top:6px; }}
            .pill {{
              padding:4px 10px; border-radius:999px; font-size:12px; font-weight:900;
              border:1px solid var(--line);
            }}
            .pill-bull {{ background:rgba(16,185,129,.14); color:var(--bull); border-color: rgba(16,185,129,.30); }}
            .pill-bear {{ background:rgba(239,68,68,.14); color:var(--bear); border-color: rgba(239,68,68,.30); }}
            .pill-neutral {{ background:rgba(100,116,139,.14); color:#cbd5e1; border-color: rgba(100,116,139,.30); }}
            .pill-ok {{ background:rgba(34,197,94,.14); color:var(--ok); border-color: rgba(34,197,94,.30); }}
            .pill-no {{ background:rgba(239,68,68,.14); color:var(--no); border-color: rgba(239,68,68,.30); }}
            .pill-warn {{ background:rgba(245,158,11,.14); color:var(--warn); border-color: rgba(245,158,11,.30); }}
            .actions {{ min-width: 260px; text-align:right; }}
            .btnrow {{ display:flex; gap:8px; justify-content:flex-end; flex-wrap:wrap; }}
            .btn {{
              background:var(--btn);
              border:1px solid var(--line);
              color:var(--text);
              padding:10px 12px;
              border-radius:14px;
              cursor:pointer;
              font-weight:900;
              min-height: 40px;
            }}
            .btn:hover {{ background:var(--btn2); }}
            .grid3 {{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap:14px; margin-top: 14px; }}
            .grid2 {{ display:grid; grid-template-columns: 1.1fr 1.1fr 1.2fr; gap:12px; margin-top: 12px; }}
            .panel {{ background: rgba(255,255,255,.03); border:1px solid var(--line); border-radius:16px; padding:12px; }}
            .bar {{ height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; }}
            .bar-fill {{ height:10px; background: linear-gradient(90deg, rgba(34,197,94,.9), rgba(245,158,11,.9), rgba(239,68,68,.9)); }}
            .bar-num {{ margin-top:6px; font-weight:900; }}
            .qblock {{ margin-bottom: 10px; }}
            .qrow {{ display:flex; justify-content:space-between; align-items:baseline; gap:10px; }}
            .qval {{ font-weight:1000; }}
            .flipgrid {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px; }}
            .flipcard {{ background: rgba(255,255,255,.03); border:1px solid var(--line); border-radius:14px; padding:10px; }}
            .flipnum {{ font-weight:1000; font-size: 18px; }}
            .td-row {{ display:flex; justify-content:space-between; gap:10px; margin: 8px 0; }}
            .ctable {{ width:100%; border-collapse:collapse; margin-top: 10px; }}
            .ctable th, .ctable td {{ padding:8px 6px; border-top:1px solid var(--line); text-align:left; font-size: 13px; }}
            .pos {{ color: var(--ok); font-weight:900; }}
            .neg {{ color: var(--no); font-weight:900; }}
            .why, .news {{ margin:0; padding-left:18px; }}
            .why li {{ margin: 10px 0; }}
            .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
            .why-meta {{ color:var(--muted); font-size:12px; }}
            .tipwrap {{ position: relative; display:inline-block; }}
            .tipicon {{
              display:inline-flex; align-items:center; justify-content:center;
              width: 18px; height: 18px; border-radius: 999px;
              border:1px solid var(--line); color: var(--muted);
              font-size: 12px; font-weight: 1000;
              cursor: pointer; user-select: none;
            }}
            .tipbubble {{
              position:absolute; right:0; top: 22px;
              width: min(320px, 72vw);
              background: rgba(18,26,38,.98);
              border: 1px solid var(--line);
              border-radius: 14px;
              padding: 10px;
              font-size: 12px;
              color: var(--text);
              z-index: 20;
              display:none;
              box-shadow: 0 10px 30px rgba(0,0,0,.35);
            }}
            .tipbubble.show {{ display:block; }}
            html[data-compact="1"] .compact-hide {{ display:none !important; }}
            html[data-compact="1"] .grid2 {{ grid-template-columns: 1fr; }}
            html[data-compact="1"] .grid3 {{ grid-template-columns: 1fr; }}
            @media(max-width: 980px) {{
              .grid3 {{ grid-template-columns: 1fr; }}
              .grid2 {{ grid-template-columns: 1fr; }}
              .card-head {{ flex-direction: column; }}
              .actions {{ width: 100%; text-align:left; min-width: auto; }}
              .btnrow {{ justify-content:flex-start; }}
            }}
          </style>
        </head>
        <body>
          <div class="wrap">
            <div class="top">
              <div>
                <h1>News Bias Dashboard</h1>
                <div class="muted tiny">updated_utc: {updated} • gate_profile: <b>{gate_profile}</b></div>
                <div class="muted tiny" style="margin-top:6px; display:flex; gap:8px; flex-wrap:wrap;">
                  {ev_html}
                  {tr_badge}
                  <span class="pill pill-neutral">FRED: {"on" if (meta.get("fred",{}).get("enabled", False)) else "off"}</span>
                  <span class="pill pill-neutral"><a href="/diag" target="_blank">/diag</a></span>
                </div>
              </div>
              <div class="muted tiny">Tip: open <b>/run</b> in browser shows POST instruction.</div>
            </div>

            <div class="mini">
              <div class="mini-inner">
                <div class="tabs">
                  <button class="tab" onclick="jumpTo('card-XAU')">XAU <small id="mini-xau">—</small></button>
                  <button class="tab" onclick="jumpTo('card-US500')">US500 <small id="mini-us500">—</small></button>
                  <button class="tab" onclick="jumpTo('card-WTI')">WTI <small id="mini-wti">—</small></button>
                </div>
                <div class="toggles">
                  <button class="btn" onclick="toggleCompact()">Compact</button>
                  <button class="btn" onclick="toggleTheme()">Theme</button>
                  <button class="btn" onclick="runNow('ALL')">Run now</button>
                  <button class="btn" onclick="showJson()">JSON</button>
                  <button class="btn" onclick="showRules()">Rules</button>
                  <button class="btn" onclick="showMorning()">Morning</button>
                </div>
              </div>
            </div>

            <section class="card">
              <div class="card-head" style="align-items:center;">
                <div>
                  <div class="h2">Macro window</div>
                  <div class="muted tiny">event_mode={str(event_mode).lower()} • recent_macro={str(event.get("recent_macro", False)).lower()}</div>
                  <div class="muted tiny" style="margin-top:6px;">Upcoming (next):</div>
                  <ul class="news">{up_html}</ul>
                </div>
              </div>
            </section>

            {render_asset("XAU")}
            {render_asset("US500")}
            {render_asset("WTI")}

            <section class="card">
              <div class="h2">Feeds health (last run snapshot)</div>
              <div class="muted tiny" style="margin-top:6px;">
                Live debug: <a href="/feeds_health" target="_blank" rel="noopener">/feeds_health</a>
              </div>
              <table class="ctable" style="margin-top:10px;">
                <thead><tr><th>Feed</th><th>Status</th><th>Entries</th></tr></thead>
                <tbody>{feeds_rows}</tbody>
              </table>
            </section>

          </div>

          <div class="modal" id="modal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,.6); align-items:center; justify-content:center; padding:16px;">
            <div class="modal-box" style="width:min(1020px, 100%); max-height: 82vh; overflow:auto; background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px;">
              <div class="modal-head" style="display:flex; justify-content:space-between; align-items:center; gap:10px; position: sticky; top: 0; background: color-mix(in srgb, var(--card) 92%, transparent); padding-bottom: 10px;">
                <div class="h2" id="modal-title">Modal</div>
                <button class="btn" onclick="closeModal()">Close</button>
              </div>
              <div id="modal-body"></div>
            </div>
          </div>

          <script>
            const payloadMini = {json.dumps(assets, ensure_ascii=False)};

            (function initTheme(){{
              const saved = localStorage.getItem('theme');
              document.documentElement.setAttribute('data-theme', saved || 'dark');
            }})();
            (function initCompact(){{
              const c = localStorage.getItem('compact') || '{compact_on}';
              document.documentElement.setAttribute('data-compact', c);
            }})();
            (function initMini(){{
              function fmt(a) {{
                if(!a) return '—';
                const b = a.bias || 'NEUTRAL';
                const q = (a.quality_v2 ?? a.quality ?? 0);
                const s = (a.score ?? 0);
                return `${{b}} • q=${{q}} • s=${{s}}`;
              }}
              document.getElementById('mini-xau').innerText = fmt(payloadMini['XAU']);
              document.getElementById('mini-us500').innerText = fmt(payloadMini['US500']);
              document.getElementById('mini-wti').innerText = fmt(payloadMini['WTI']);
            }})();

            function openModal(title, bodyHtml) {{
              document.getElementById('modal-title').innerText = title;
              document.getElementById('modal-body').innerHTML = bodyHtml;
              document.getElementById('modal').style.display = 'flex';
            }}
            function closeModal() {{ document.getElementById('modal').style.display = 'none'; }}

            function escapeHtml(unsafe) {{
              return (unsafe || '').replaceAll('&', '&amp;')
                .replaceAll('<', '&lt;').replaceAll('>', '&gt;')
                .replaceAll('"', '&quot;').replaceAll("'", '&#039;');
            }}

            function jumpTo(id) {{
              const el = document.getElementById(id);
              if(el) el.scrollIntoView({{ behavior:'smooth', block:'start' }});
            }}

            function toggleTheme(){{
              const cur = document.documentElement.getAttribute('data-theme') || 'dark';
              const next = (cur === 'dark') ? 'light' : 'dark';
              document.documentElement.setAttribute('data-theme', next);
              localStorage.setItem('theme', next);
            }}

            function toggleCompact(){{
              const cur = document.documentElement.getAttribute('data-compact') || '0';
              const next = (cur === '1') ? '0' : '1';
              document.documentElement.setAttribute('data-compact', next);
              localStorage.setItem('compact', next);
            }}

            async function runNow(asset) {{
              const ids = ['XAU','US500','WTI'];
              ids.forEach(a => {{
                const el = document.getElementById('status-' + a);
                if(el) el.innerText = 'Running...';
              }});
              try {{
                const resp = await fetch('/run', {{ method:'POST' }});
                const data = await resp.json();
                const upd = data.updated_utc || '';
                ids.forEach(a => {{
                  const el = document.getElementById('status-' + a);
                  if(el) el.innerText = 'Updated: ' + upd;
                }});
                setTimeout(() => window.location.reload(), 450);
              }} catch(e) {{
                ids.forEach(a => {{
                  const el = document.getElementById('status-' + a);
                  if(el) el.innerText = 'Error: ' + e;
                }});
              }}
            }}

            async function showJson() {{
              try {{
                const resp = await fetch('/bias?pretty=1');
                const txt = await resp.text();
                openModal('JSON (pretty)', '<pre style="white-space:pre-wrap;">' + escapeHtml(txt) + '</pre>');
              }} catch(e) {{
                openModal('JSON', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
              }}
            }}

            async function showRules() {{
              try {{
                const resp = await fetch('/rules');
                const data = await resp.json();
                openModal('Rules', '<pre style="white-space:pre-wrap;">' + escapeHtml(JSON.stringify(data, null, 2)) + '</pre>');
              }} catch(e) {{
                openModal('Rules', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
              }}
            }}

            async function showMorning() {{
              try {{
                const resp = await fetch('/morning_plan');
                const data = await resp.json();
                openModal('Morning Plan', '<pre style="white-space:pre-wrap;">' + escapeHtml(JSON.stringify(data, null, 2)) + '</pre>');
              }} catch(e) {{
                openModal('Morning Plan', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
              }}
            }}

            async function showExplain(asset) {{
              try {{
                const resp = await fetch('/explain?asset=' + encodeURIComponent(asset) + '&limit=60');
                const data = await resp.json();
                if (data.error) {{
                  openModal('Explain ' + asset, '<div class="muted">' + escapeHtml(data.error) + '</div>');
                  return;
                }}
                const rows = (data.top_matches || []).map(x => {{
                  return `<tr>
                    <td style="padding:8px;border-top:1px solid rgba(255,255,255,.08);">
                      <b>${{escapeHtml(x.why || '')}}</b>
                      <div class="muted tiny">${{escapeHtml(x.source || '')}} • age=${{x.age_min}}m • contrib=${{x.contrib}}</div>
                      <div class="muted tiny">pattern: ${{escapeHtml(x.pattern || '')}}</div>
                    </td>
                    <td style="padding:8px;border-top:1px solid rgba(255,255,255,.08);">
                      <a href="${{x.link}}" target="_blank" rel="noopener">${{escapeHtml(x.title || '')}}</a>
                    </td>
                  </tr>`;
                }}).join('');
                openModal('Explain ' + asset, `
                  <div class="muted tiny">rules_count=${{data.rules_count}} • items=${{(data.top_matches||[]).length}}</div>
                  <div style="overflow:auto;margin-top:10px;">
                    <table style="width:100%;border-collapse:collapse;"><tbody>${{rows || '<tr><td class="muted">—</td></tr>'}}</tbody></table>
                  </div>
                `);
              }} catch(e) {{
                openModal('Explain ' + asset, '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
              }}
            }}

            // Tooltips
            function hideAllTips() {{ document.querySelectorAll('.tipbubble').forEach(x => x.remove()); }}
            function showTipFor(el) {{
              hideAllTips();
              const txt = el.getAttribute('data-tip') || '';
              const b = document.createElement('div');
              b.className = 'tipbubble show';
              b.innerText = txt;
              el.parentElement.appendChild(b);
              setTimeout(() => {{ try {{ b.remove(); }} catch(e) {{}} }}, 4200);
            }}
            document.addEventListener('click', (e) => {{
              const t = e.target;
              if (t && t.classList && t.classList.contains('tipicon')) {{
                e.preventDefault();
                showTipFor(t);
              }} else {{
                hideAllTips();
              }}
            }});
            document.getElementById('modal').addEventListener('click', (e) => {{ if(e.target && e.target.id === 'modal') closeModal(); }});
          </script>
        </body>
        </html>
        """
        return HTMLResponse(html)

    except Exception as e:
        return _safe_html_error("Dashboard error", str(e))
