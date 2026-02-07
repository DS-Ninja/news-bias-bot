# app.py
# NEWS BIAS // TERMINAL (Bloomberg-ish) — RSS + Postgres + Bias/Quality + Trade Gate + iPhone-friendly UI
# - Adds: /diag, GET /run with token, clearer flip units, event_mode reasons, ticker line + hotkeys, terminal table UI

import os
import json
import time
import re
import hashlib
import math
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
    "max_upcoming": 6,
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

# Token for /run GET (for GitHub cron)
RUN_TOKEN = os.environ.get("RUN_TOKEN", "").strip()

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

    # FF calendar mirror
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
}
CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR"}

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

    "FOREXFACTORY_CALENDAR": 0.0,
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
                    published_ts = int(time.mktime(published_parsed)) if published_parsed else now

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

def _macro_recent_flag(rows: List[Tuple[str, str, str, int]], now_ts: int) -> bool:
    recent_sec = int(EVENT_CFG["recent_hours"] * 3600)
    macro_sources = {"FED", "BLS", "BEA"}
    for (source, _title, _link, ts) in rows:
        if source in macro_sources and (now_ts - int(ts)) <= recent_sec:
            return True
    return False

def _get_upcoming_events(now_ts: int) -> List[Dict[str, Any]]:
    """
    FF mirror RSS часто не даёт правильного event timestamp.
    Поэтому:
    - пытаемся взять published_parsed (как есть)
    - если ts не попал в lookahead — всё равно покажем 1-2 элемента как "time unknown"
    """
    url = RSS_FEEDS.get("FOREXFACTORY_CALENDAR")
    if not url:
        return []

    try:
        d = feedparser.parse(url)
        entries = getattr(d, "entries", []) or []
    except Exception:
        return []

    lookahead_sec = int(EVENT_CFG["lookahead_hours"] * 3600)
    horizon = now_ts + lookahead_sec

    out: List[Dict[str, Any]] = []
    unknown: List[Dict[str, Any]] = []

    for e in entries[:250]:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        if not title:
            continue

        ts = None
        pp = e.get("published_parsed")
        if pp:
            try:
                ts = int(time.mktime(pp))
            except Exception:
                ts = None

        if ts is not None and (now_ts <= ts <= horizon):
            out.append({"title": title, "link": link, "ts": int(ts), "in_hours": round((ts - now_ts) / 3600.0, 2)})
        else:
            # keep a couple unknowns for UI
            if len(unknown) < 2:
                unknown.append({"title": title, "link": link, "ts": None, "in_hours": None})

        if len(out) >= EVENT_CFG["max_upcoming"]:
            break

    out.sort(key=lambda x: x["ts"] or 10**18)
    if not out:
        out = unknown[:2]
    return out[:EVENT_CFG["max_upcoming"]]

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
# FLIP METRICS (clearer)
# ============================================================

def flip_metrics(score: float, th: float, bias: str, median_abs: float) -> Dict[str, Any]:
    """
    Distances are in score-units.
    - to_bullish: ΔScore needed to reach +th
    - to_bearish: ΔScore needed to reach -th
    - to_neutral: ΔScore needed to get back inside (-th,+th) from current bias
    - to_opposite: ΔScore needed to flip to opposite side (cross other threshold)
    - approx_headlines_to_flip: translate to #median-headlines (rough, optional)
    """
    to_bull = max(0.0, th - score)
    to_bear = max(0.0, score + th)

    if bias == "BULLISH":
        to_neutral = max(0.0, score - th)
        to_opposite = score + th  # need to go down by this amount to reach -th
    elif bias == "BEARISH":
        to_neutral = max(0.0, -th - score)  # score is negative
        to_opposite = th - score            # need to go up by this amount to reach +th
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
        "note": "Units: score-sum (weighted evidence). Not price points. 'approx_headlines' uses median abs contribution.",
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

    event_reason = []
    if not EVENT_CFG["enabled"]:
        event_reason.append("disabled")
    else:
        if recent_macro:
            event_reason.append(f"recent_macro<= {EVENT_CFG['recent_hours']}h")
        if has_timed_event:
            event_reason.append(f"upcoming<= {EVENT_CFG['lookahead_hours']}h")
        if not event_reason:
            # Most common real situation: calendar has unknown timestamps
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

        why_top5 = sorted(contribs, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]
        top3 = _top_drivers(contribs, topn=3)
        cons_by_src = _consensus_by_source(contribs)
        med_abs = _median_abs_contrib(contribs)
        flip = flip_metrics(score, th, bias, med_abs)

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
                "enabled": bool(fred_on),
                "inserted_points_last_run": int(fred_inserted),
                "series": list(FRED_SERIES.keys()),
                "requests_present": bool(requests is not None),
            },
            "trump": trump,
        },
        "event": {
            "enabled": bool(EVENT_CFG["enabled"]),
            "event_mode": bool(event_mode),
            "reason": event_reason,
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
    }
    ok = True
    news_items = 0
    fred_points = 0
    has_bias_state = False
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM news_items;")
                news_items = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fred_series;")
                fred_points = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM bias_state;")
                has_bias_state = int(cur.fetchone()[0]) > 0
    except Exception:
        ok = False

    return {"db": {"ok": ok, "news_items": news_items, "fred_points": fred_points, "has_bias_state": has_bias_state}, "env": env}

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
    if not RUN_TOKEN:
        # If token not configured, allow (dev). In prod лучше поставить RUN_TOKEN.
        return True
    return bool(token and token == RUN_TOKEN)

@app.get("/run")
def run_get(token: str = ""):
    # For GitHub cron (GET). Use token.
    if not _auth_run(token):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        payload = pipeline_run()
        return JSONResponse({"ok": True, "updated_utc": payload.get("updated_utc"), "meta": payload.get("meta", {})})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/run")
def run_post(token: str = ""):
    # For manual "Run now" from UI. Also can use token if you want to lock it.
    if RUN_TOKEN and token and not _auth_run(token):
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

@app.get("/explain")
def explain(asset: str = "US500", limit: int = 80):
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
        age = max(0, now - int(ts))
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
        "plan": {"XAU": pack("XAU"), "US500": pack("US500"), "WTI": pack("WTI")}
    }
    return JSONResponse(out)

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
    event_reason = event.get("reason", []) or []
    upcoming = (event.get("upcoming_events", []) or [])[:6]

    trump = meta.get("trump", {}) or {}
    trump_flag = bool(trump.get("flag", False))
    trump_enabled = bool(trump.get("enabled", False))

    fred_on = bool(meta.get("fred", {}).get("enabled", False))
    feeds_ok = all(bool((feeds_status.get(k, {}) or {}).get("ok", False)) or bool((feeds_status.get(k, {}) or {}).get("skipped", False)) for k in RSS_FEEDS.keys())

    # next event line
    next_event = "—"
    if upcoming:
        u = upcoming[0]
        if u.get("ts"):
            dt = datetime.fromtimestamp(int(u["ts"]), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            next_event = f"{dt} • {u.get('title','')}"
        else:
            next_event = f"(time unknown) • {u.get('title','')}"

    # Build table rows
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
    # payload for JS
    js_payload = json.dumps(payload, ensure_ascii=False)

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
    .wrap{max-width:1200px; margin:0 auto; padding:14px;}
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
    .kz{color:var(--muted); font-family:var(--mono); font-size:12px; margin-top:8px;}
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
    .ticker{font-family:var(--mono); font-size:12px; color:var(--muted); display:flex; gap:8px; flex-wrap:wrap; align-items:center;}
    .ticker .tag{color:var(--cyan);}
    .modal{display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); padding: calc(14px + env(safe-area-inset-top)) 14px calc(14px + env(safe-area-inset-bottom));}
    .modal .box{max-width:1100px; margin:0 auto; background:var(--panel); border:1px solid var(--line); border-radius:16px; max-height:82vh; overflow:auto; -webkit-overflow-scrolling:touch;}
    .modal .head{position:sticky; top:0; background:rgba(11,17,26,.92); backdrop-filter:blur(10px);
                 display:flex; justify-content:space-between; align-items:center; padding:12px; border-bottom:1px solid var(--line);}
    .modal .body{padding:12px;}
    pre{white-space:pre-wrap; word-break:break-word; color:var(--text); font-family:var(--mono); font-size:12px;}
    @media(max-width: 780px){
      .why{max-width:none;}
      th:nth-child(11), td:nth-child(11) {display:none;}
    }
  </style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <div class="title"><b>NEWS BIAS</b> // TERMINAL</div>
    <div class="sub">updated_utc=__UPDATED__ • gate_profile=__GATE_PROFILE__ • event_mode=__EVENT_MODE__ • trump=__TRUMP__</div>
    <div class="ticker">
      <span class="tag">Next event:</span> __NEXT_EVENT__
      <span class="muted">• event_reason: __EVENT_REASON__</span>
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
      <button class="btn" onclick="openJson()">J JSON</button>
      <button class="btn" onclick="openAnalyst()">A ANALYST</button>
      <a class="btn" href="/bias?pretty=1" target="_blank" rel="noopener">Bias JSON</a>
    </div>
    <div class="hotkeys">Hotkeys: 1/2/3 = jump XAU/US500/WTI • R run • M morning • J json • A analyst • Esc close</div>
  </div>

  <div class="panel">
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
        __ROW_XAU__
        __ROW_US500__
        __ROW_WTI__
      </tbody>
    </table>

    <div class="kz">
      Flip units: score-sum (weighted evidence), not % / not market points.
      TO_NEU = ΔScore to return to Neutral band. TO_OPP = ΔScore to flip to opposite bias.
      ≈HEADLINES = TO_OPP / median(|contrib|) (rough intuition).
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

  function $(id){ return document.getElementById(id); }

  function showModal(title, html){
    $('mt').innerText = title;
    $('mb').innerHTML = html;
    $('modal').style.display = 'block';
  }
  function closeModal(){
    $('modal').style.display = 'none';
  }

  async function runNow(){
    try{
      const resp = await fetch('/run', { method:'POST' });
      const js = await resp.json();
      if(js.ok === false && js.error){
        showModal('RUN ERROR', '<pre>' + escapeHtml(JSON.stringify(js,null,2)) + '</pre>');
        return;
      }
      setTimeout(()=>location.reload(), 400);
    }catch(e){
      showModal('RUN ERROR', '<pre>' + escapeHtml(String(e)) + '</pre>');
    }
  }

  function escapeHtml(s){
    return (s||'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#039;');
  }

  function openView(asset){
    const a = (PAYLOAD.assets||{})[asset] || {};
    const ev = PAYLOAD.event || {};
    const meta = PAYLOAD.meta || {};

    const top3 = (a.top3_drivers||[]).map((x,i)=>`${i+1}. ${x.why} (abs=${x.abs_contrib_sum})`).join('\\n') || '—';
    const flip = a.flip || {};
    const why5 = (a.why_top5||[]).map((x,i)=>`${i+1}. ${x.why}\\n   src=${x.source} age=${x.age_min}m contrib=${x.contrib}\\n   ${x.title}\\n   ${x.link}\\n`).join('\\n') || '—';

    const html = `
      <div class="ticker"><span class="tag">${asset}</span> bias=${escapeHtml(a.bias||'')} score=${a.score} th=${a.threshold} q2=${a.quality_v2} conflict=${a.conflict_index}</div>
      <div class="panel" style="margin-top:10px;">
        <div class="sub">Flip</div>
        <pre>${escapeHtml(JSON.stringify(flip,null,2))}</pre>
      </div>
      <div class="panel" style="margin-top:10px;">
        <div class="sub">Top drivers</div>
        <pre>${escapeHtml(top3)}</pre>
      </div>
      <div class="panel" style="margin-top:10px;">
        <div class="sub">WHY top 5</div>
        <pre>${escapeHtml(why5)}</pre>
      </div>
      <div class="panel" style="margin-top:10px;">
        <div class="sub">Event</div>
        <pre>${escapeHtml(JSON.stringify(ev,null,2))}</pre>
      </div>
      <div class="panel" style="margin-top:10px;">
        <div class="sub">Meta</div>
        <pre>${escapeHtml(JSON.stringify({gate_profile: meta.gate_profile, fred: meta.fred, trump: meta.trump},null,2))}</pre>
      </div>
    `;
    showModal('VIEW ' + asset, html);
  }

  async function openMorning(){
    try{
      const resp = await fetch('/morning_plan');
      const js = await resp.json();
      showModal('MORNING PLAN', '<pre>' + escapeHtml(JSON.stringify(js,null,2)) + '</pre>');
    }catch(e){
      showModal('MORNING PLAN', '<pre>' + escapeHtml(String(e)) + '</pre>');
    }
  }

  async function openJson(){
    try{
      const resp = await fetch('/bias?pretty=1');
      const txt = await resp.text();
      showModal('BIAS JSON', '<pre>' + escapeHtml(txt) + '</pre>');
    }catch(e){
      showModal('BIAS JSON', '<pre>' + escapeHtml(String(e)) + '</pre>');
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
      xau: {bias:a.XAU?.bias, score:a.XAU?.score, q2:a.XAU?.quality_v2, conflict:a.XAU?.conflict_index, flip:a.XAU?.flip},
      us500: {bias:a.US500?.bias, score:a.US500?.score, q2:a.US500?.quality_v2, conflict:a.US500?.conflict_index, flip:a.US500?.flip},
      wti: {bias:a.WTI?.bias, score:a.WTI?.score, q2:a.WTI?.quality_v2, conflict:a.WTI?.conflict_index, flip:a.WTI?.flip},
    };
    showModal('ANALYST SNAPSHOT', '<pre>' + escapeHtml(JSON.stringify(s,null,2)) + '</pre>');
  }

  document.addEventListener('keydown', (e)=>{
    const k = (e.key||'').toLowerCase();
    if(k === 'escape') closeModal();
    if(k === 'r') runNow();
    if(k === 'm') openMorning();
    if(k === 'j') openJson();
    if(k === 'a') openAnalyst();
    if(k === '1') openView('XAU');
    if(k === '2') openView('US500');
    if(k === '3') openView('WTI');
  });

  $('modal').addEventListener('click', (e)=>{ if(e.target && e.target.id === 'modal') closeModal(); });
</script>
</body>
</html>
"""

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
        .replace("__ROW_XAU__", row("XAU"))
        .replace("__ROW_US500__", row("US500"))
        .replace("__ROW_WTI__", row("WTI"))
        .replace("__JS_PAYLOAD__", js_payload)
    )

    return HTMLResponse(html)

    
