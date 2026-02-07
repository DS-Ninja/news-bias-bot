# app.py
# News Bias Bot — Bloomberg-style Terminal Dashboard (2026-02-07)

import os
import json
import time
import re
import hashlib
import math
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import requests
import feedparser
import psycopg2
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

HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": int(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 6,
}

FRED_CFG = {
    "enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
    "api_key": os.environ.get("FRED_API_KEY", "").strip(),
    "window_days": int(os.environ.get("FRED_WINDOW_DAYS", "120")),
}
FRED_SERIES = {
    "DGS10":    {"name": "US 10Y Nominal"},
    "DFII10":   {"name": "US 10Y Real"},
    "T10YIE":   {"name": "10Y Breakeven"},
    "DTWEXBGS": {"name": "Broad USD"},
    "VIXCLS":   {"name": "VIX"},
    "BAA10Y":   {"name": "BAA-10Y Spread"},
}

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

SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0, "BLS": 3.0, "BEA": 2.8,
    "FXSTREET_NEWS": 1.4, "FXSTREET_ANALYSIS": 1.2,
    "MARKETWATCH_TOP": 1.2, "MARKETWATCH_REALTIME": 1.3,
    "OILPRICE": 1.2,
    "INV_STOCK_FUND": 1.0, "INV_COMMOD_TECH": 1.0, "INV_NEWS_11": 1.0, "INV_NEWS_95": 1.0,
    "INV_MKT_TECH": 1.0, "INV_MKT_FUND": 1.0, "INV_MKT_IDEAS": 0.9, "INV_FX_TECH": 0.95, "INV_FX_FUND": 0.95,
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
            );""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_published_ts ON news_items(published_ts DESC);")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bias_state (
                id SMALLINT PRIMARY KEY DEFAULT 1,
                updated_ts BIGINT NOT NULL,
                payload_json TEXT NOT NULL
            );""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS fred_series (
                series_id TEXT NOT NULL,
                obs_date  DATE NOT NULL,
                value     DOUBLE PRECISION,
                PRIMARY KEY(series_id, obs_date)
            );""")
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
            cur.execute("SELECT payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
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
                        published_ts = int(time.mktime(published_parsed))
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
            ts = int(time.mktime(published_parsed))

        if ts is not None:
            if now_ts <= ts <= horizon:
                out.append({
                    "title": title,
                    "link": link,
                    "ts": int(ts),
                    "in_hours": round((ts - now_ts) / 3600.0, 2),
                })
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
# SCORING / QUALITY
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
    out = []
    for src, v in acc.items():
        out.append({"source": src, "net": round(v["net"], 4), "abs": round(v["abs"], 4), "count": int(cnt[src])})
    out.sort(key=lambda x: x["abs"], reverse=True)
    return out

def flip_metrics(score: float, th: float) -> Dict[str, Any]:
    to_bullish = max(0.0, th - score)
    to_bearish = max(0.0, score + th)

    if score >= th:
        to_neutral = max(0.0, score - th)
        state = "BULLISH"
    elif score <= -th:
        to_neutral = max(0.0, (-th) - score)
        state = "BEARISH"
    else:
        to_neutral = 0.0
        state = "NEUTRAL"

    if state == "BULLISH":
        to_opposite = score + th
    elif state == "BEARISH":
        to_opposite = th - score
    else:
        to_opposite = min(to_bullish, to_bearish)

    return {
        "state": state,
        "to_neutral": round(float(to_neutral), 4),
        "to_opposite": round(float(max(0.0, to_opposite)), 4),
        "to_bullish": round(float(to_bullish), 4),
        "to_bearish": round(float(to_bearish), 4),
        "note": "Distances are in score-units (weighted evidence sum), not %/points.",
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

    fred_inserted = 0
    fred_drivers = {"XAU": [], "US500": [], "WTI": []}
    if FRED_CFG["enabled"] and FRED_CFG["api_key"]:
        for sid in FRED_SERIES.keys():
            try:
                fred_inserted += fred_ingest_series(sid, days=FRED_CFG["window_days"])
            except Exception:
                pass
        try:
            fred_drivers = compute_fred_drivers()
        except Exception:
            fred_drivers = {"XAU": [], "US500": [], "WTI": []}

    upcoming_events = _get_upcoming_events(now) if EVENT_CFG["enabled"] else []
    recent_macro = _macro_recent_flag(rows, now)
    event_mode = False
    if EVENT_CFG["enabled"]:
        event_mode = bool(recent_macro or any(x.get("ts") is not None for x in upcoming_events))

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

        strength = min(1.0, abs(score) / max(th, 1e-9))
        quality_v1 = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, src_div * 2.0)))

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
        flip = flip_metrics(score, th)
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
        fail_reasons.append("Bias=NEUTRAL")
        must_change.append("bias must cross ±threshold")
    elif bias == "NEUTRAL" and cfg.get("neutral_allow", False):
        maxd = float(cfg.get("neutral_flip_dist_max", 0.20))
        if min(to_bull, to_bear) > maxd:
            fail_reasons.append("Neutral (not near flip)")
            must_change.append(f"min(to_bull,to_bear) ≤ {maxd}")

    qmin = int(cfg["quality_v2_min"])
    if q2 < qmin:
        fail_reasons.append(f"Q2<{qmin}")
        must_change.append(f"quality_v2 ≥ {qmin}")

    cmax = float(cfg["conflict_max"])
    if conflict > cmax:
        fail_reasons.append(f"Conflict>{cmax}")
        must_change.append(f"conflict_index ≤ {cmax}")

    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH"):
        if opp_dist < mind:
            fail_reasons.append(f"OppFlip too close ({opp_label}<{mind})")
            must_change.append(f"{opp_label} ≥ {mind}")

    if event_mode:
        if cfg.get("event_mode_block", True):
            oq = int(cfg.get("event_override_quality", 70))
            oc = float(cfg.get("event_override_conflict", 0.45))
            if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
                fail_reasons.append("EVENT MODE")
                must_change.append(f"Need Q2≥{oq} & conflict≤{oc} & bias!=NEUTRAL OR wait event_mode=OFF")

    ok = (len(fail_reasons) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:2] if x.get("why")] or ["Insufficient matched evidence"]
    return {
        "ok": bool(ok),
        "label": "TRADE OK" if ok else "NO TRADE",
        "why": why_short[:2],
        "fail_reasons": fail_reasons[:3],
        "must_change": must_change[:4],
    }

# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot — Terminal UI")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)

@app.get("/health")
def health():
    return {"ok": True, "gate_profile": GATE_PROFILE, "trump_enabled": bool(TRUMP_ENABLED), "fred_enabled": bool(FRED_CFG["enabled"] and bool(FRED_CFG["api_key"]))}

@app.get("/diag", include_in_schema=False)
def diag():
    info = {"db": {"ok": False}, "env": {}}
    try:
        db_init()
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM news_items;")
                n_news = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fred_series;")
                n_fred = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM bias_state;")
                n_bias = int(cur.fetchone()[0])
        info["db"] = {"ok": True, "news_items": n_news, "fred_points": n_fred, "has_bias_state": bool(n_bias > 0)}
    except Exception as e:
        info["db"] = {"ok": False, "error": str(e)}
    info["env"] = {
        "has_DATABASE_URL": bool(os.environ.get("DATABASE_URL")),
        "PGHOST": os.environ.get("PGHOST", ""),
        "PGDATABASE": os.environ.get("PGDATABASE", ""),
        "PGUSER": os.environ.get("PGUSER", ""),
        "PGSSLMODE": os.environ.get("PGSSLMODE", "prefer"),
    }
    return JSONResponse(info)

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

@app.get("/run", include_in_schema=False)
def run_help():
    html = """
    <html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="font-family:system-ui,-apple-system,Segoe UI,Arial,sans-serif;padding:16px;line-height:1.45">
      <h2>/run requires POST</h2>
      <pre style="background:#f4f4f4;padding:12px;border-radius:10px;overflow:auto">curl -X POST https://YOUR_DOMAIN/run -H "Content-Type: application/json" -d '{}'</pre>
      <p>Or use the dashboard hotkey <b>R</b>.</p>
    </body></html>
    """
    return HTMLResponse(html)

@app.post("/run")
def run_now():
    return pipeline_run()

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
        "plan": {"XAU": pack("XAU"), "US500": pack("US500"), "WTI": pack("WTI")}
    }
    return JSONResponse(out)

# ============================================================
# Terminal UI helpers
# ============================================================

def _cls_bias(bias: str) -> str:
    return "b-bull" if bias == "BULLISH" else ("b-bear" if bias == "BEARISH" else "b-neu")

def _cls_gate(ok: bool) -> str:
    return "g-ok" if ok else "g-no"

def _fmt(x, nd=3):
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "—"

def _short(s: str, n: int = 46) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 1].rstrip() + "…"

def _kw(asset: str) -> List[str]:
    return {
        "XAU": ["gold", "xau", "fed", "fomc", "cpi", "inflation", "yields", "usd", "treasury", "safe-haven", "real"],
        "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "s&p", "nasdaq", "equities", "vix", "rates", "yields"],
        "WTI": ["oil", "crude", "wti", "opec", "inventory", "stocks", "pipeline", "sanctions", "outage", "spr", "output"],
    }[asset]

def _latest_relevant(rows: List[Tuple[str, str, str, int]], asset: str, n: int = 8) -> List[Tuple[str, str, str]]:
    keys = _kw(asset)
    out = []
    for (source, title, link, _ts) in rows:
        t = (title or "").lower()
        if any(k in t for k in keys):
            out.append((source, title, link))
            if len(out) >= n:
                break
    return out

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
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
    upcoming_n = len(upcoming[:6])

    trump = meta.get("trump", {}) or {}
    trump_flag = bool(trump.get("flag", False))
    trump_enabled = bool(trump.get("enabled", False))

    # Latest news rows for drilldown
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 300;
            """)
            rows = cur.fetchall()

    # Alert bar logic (fast + useful)
    feed_err = 0
    for src in RSS_FEEDS.keys():
        st = feeds_status.get(src, {})
        if st and (not st.get("ok", True)) and (not st.get("skipped", False)):
            feed_err += 1

    # Detect "Q2 drop" vs last saved in localStorage (client), but server can still raise flags:
    # We'll show only server-known alerts: EVENT, TRUMP, FEED_ERR.
    alert_items = []
    if event_mode:
        alert_items.append(f"EVENT MODE ON (upcoming={upcoming_n})")
    if trump_enabled and trump_flag:
        alert_items.append("TRUMP HEADLINES ACTIVE")
    if feed_err > 0:
        alert_items.append(f"FEED ERRORS: {feed_err}")

    alert_text = " • ".join(alert_items) if alert_items else "OK"

    def asset_row(asset: str) -> str:
        a = assets.get(asset, {}) or {}
        bias = str(a.get("bias", "NEUTRAL"))
        score = float(a.get("score", 0.0))
        th = float(a.get("threshold", 1.0))
        q2 = int(a.get("quality_v2", 0))
        conflict = float(a.get("conflict_index", 1.0))
        flip = a.get("flip", {}) or {}
        toN = float(flip.get("to_neutral", 0.0))
        toO = float(flip.get("to_opposite", 0.0))
        td = (a.get("top3_drivers", []) or [])
        top_driver = td[0].get("why", "—") if td else "—"

        gate = eval_trade_gate(a, event_mode, gate_profile)
        ok = bool(gate.get("ok"))

        # Compact reason for NO TRADE
        no_reason = ""
        if not ok:
            fr = (gate.get("fail_reasons", []) or [])
            no_reason = " | " + ", ".join(fr[:2]) if fr else " | blocked"

        return f"""
        <tr class="row" data-asset="{asset}">
          <td class="sym">{asset}</td>
          <td class="bias {_cls_bias(bias)}">{bias}</td>
          <td class="num">{_fmt(score,3)}</td>
          <td class="num">{_fmt(th,3)}</td>
          <td class="q2">{q2}</td>
          <td class="num">{_fmt(conflict,3)}</td>
          <td class="num">{_fmt(toN,3)}</td>
          <td class="num">{_fmt(toO,3)}</td>
          <td class="gate {_cls_gate(ok)}">{gate.get("label","")}{no_reason}</td>
          <td class="drv">{_short(top_driver, 54)}</td>
          <td class="act">
            <button class="kbtn" onclick="openPanel('{asset}')">View</button>
          </td>
        </tr>
        """

    def details_panel(asset: str) -> str:
        a = assets.get(asset, {}) or {}
        bias = str(a.get("bias", "NEUTRAL"))
        score = a.get("score", 0.0)
        th = a.get("threshold", 1.0)
        q2 = a.get("quality_v2", 0)
        conflict = a.get("conflict_index", 0.0)
        flip = a.get("flip", {}) or {}
        gate = eval_trade_gate(a, event_mode, gate_profile)

        why_top5 = a.get("why_top5", []) or []
        cons_by_src = a.get("consensus_by_source", []) or []
        news = _latest_relevant(rows, asset, n=10)

        why_html = ""
        for w in why_top5[:5]:
            why_html += f"""
              <li>
                <div class="w1"><span class="wtag">{w.get("source","")}</span>
                  <span class="wmeta">age={w.get("age_min","")}m • contrib={w.get("contrib","")}</span>
                </div>
                <div class="w2"><b>{w.get("why","")}</b></div>
                <div class="w3"><a href="{w.get("link","")}" target="_blank" rel="noopener">{_short(w.get("title",""), 120)}</a></div>
              </li>
            """
        if not why_html:
            why_html = "<li class='muted'>—</li>"

        cs_rows = ""
        for x in cons_by_src[:12]:
            net = float(x.get("net", 0.0))
            cls = "pos" if net > 0 else ("neg" if net < 0 else "muted")
            cs_rows += f"<tr><td>{x.get('source','')}</td><td class='{cls}'>{x.get('net','')}</td><td class='muted'>{x.get('abs','')}</td><td class='muted'>{x.get('count','')}</td></tr>"
        if not cs_rows:
            cs_rows = "<tr><td class='muted'>—</td><td></td><td></td><td></td></tr>"

        news_html = ""
        for (src, title, link) in news:
            news_html += f"<li><a href='{link}' target='_blank' rel='noopener'>{_short(title, 120)}</a> <span class='muted'>[{src}]</span></li>"
        if not news_html:
            news_html = "<li class='muted'>—</li>"

        must = "".join([f"<li>{x}</li>" for x in (gate.get("must_change", []) or [])[:4]]) or "<li class='muted'>—</li>"
        fails = "".join([f"<li>{x}</li>" for x in (gate.get("fail_reasons", []) or [])[:4]]) or "<li class='muted'>—</li>"

        return f"""
        <div class="panelbox" id="panel-{asset}" style="display:none;">
          <div class="phead">
            <div class="ptitle">{asset} / {bias} <span class="muted">score={score} th={th} q2={q2} conflict={conflict}</span></div>
            <div class="pactions">
              <button class="kbtn" onclick="runNow()">R</button>
              <button class="kbtn" onclick="showExplain('{asset}')">Explain</button>
              <button class="kbtn" onclick="closePanel()">Esc</button>
            </div>
          </div>

          <div class="pgrid">
            <div class="pcol">
              <div class="ph">Gate</div>
              <div class="gatebig {_cls_gate(bool(gate.get('ok')))}">{gate.get("label","")}</div>
              <div class="ph2">Fail reasons</div>
              <ul class="plist">{fails}</ul>
              <div class="ph2">To become OK</div>
              <ul class="plist">{must}</ul>
              <div class="ph2">Flip</div>
              <div class="muted tiny">ToNeutral={flip.get("to_neutral","—")} • ToOpposite={flip.get("to_opposite","—")} • Units=score-sum</div>
            </div>

            <div class="pcol">
              <details class="det" open>
                <summary>WHY (top 5)</summary>
                <ol class="why">{why_html}</ol>
              </details>

              <details class="det">
                <summary>Latest relevant headlines</summary>
                <ul class="news">{news_html}</ul>
              </details>
            </div>

            <div class="pcol">
              <details class="det" open>
                <summary>Consensus by source</summary>
                <table class="tbl">
                  <thead><tr><th>Source</th><th>Net</th><th>Abs</th><th>n</th></tr></thead>
                  <tbody>{cs_rows}</tbody>
                </table>
              </details>

              <details class="det">
                <summary>Flip units explained</summary>
                <div class="muted tiny">
                  <p><b>These numbers are NOT % and NOT market points.</b></p>
                  <p>They are <b>score-units</b> (sum of weighted evidence). Each headline match adds/subtracts:</p>
                  <pre class="code">contrib = rule_weight × source_weight × time_decay</pre>
                  <p><b>To Neutral</b>: distance back into [-th..+th]. <b>To Opposite</b>: distance to cross the opposite threshold.</p>
                </div>
              </details>
            </div>
          </div>
        </div>
        """

    # Upcoming events compact line
    up_line = "—"
    if upcoming_n:
        one = upcoming[0]
        tag = f"in {one.get('in_hours')}h" if one.get("in_hours") is not None else "time?"
        up_line = f"{one.get('title','')} ({tag})"

    html = f"""
    <html data-theme="term" data-analyst="0">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
      <meta name="color-scheme" content="dark light">
      <meta name="theme-color" content="#0b0e12">
      <title>News Bias — Terminal</title>
      <style>
        :root {{
          --bg:#0b0e12;
          --pane:#0f141b;
          --pane2:#101722;
          --line:rgba(255,255,255,.10);
          --muted:#8ea0b3;
          --text:#e7edf7;

          --bull:#00ff9a;
          --bear:#ff4d4d;
          --neu:#a0aec0;

          --ok:#00ff9a;
          --no:#ff4d4d;
          --warn:#ffd166;

          --accent:#ffb000; /* terminal accent */
          --link:#7dd3fc;

          --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
          --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
        }}

        html[data-theme="light"] {{
          --bg:#f7f8fb;
          --pane:#ffffff;
          --pane2:#ffffff;
          --line:rgba(15,23,42,.14);
          --muted:#5c6b7a;
          --text:#0b1220;
          --bull:#16a34a;
          --bear:#dc2626;
          --neu:#64748b;
          --ok:#16a34a;
          --no:#dc2626;
          --warn:#d97706;
          --accent:#b45309;
          --link:#0369a1;
        }}

        html, body {{ height:100%; }}
        body {{
          margin:0;
          background:var(--bg);
          color:var(--text);
          font-family: var(--mono);
          -webkit-text-size-adjust: 100%;
          padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
        }}

        a {{ color:var(--link); text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}

        .wrap {{
          max-width: 1280px;
          margin: 0 auto;
          padding: 10px 10px calc(18px + env(safe-area-inset-bottom));
        }}

        .hdr {{
          position: sticky; top: 0; z-index: 50;
          background: color-mix(in srgb, var(--bg) 78%, transparent);
          backdrop-filter: blur(10px);
          border-bottom: 1px solid var(--line);
          padding: 10px 0;
        }}

        .topline {{
          display:flex; gap:10px; align-items:center; justify-content:space-between; flex-wrap:wrap;
        }}

        .title {{
          font-family: var(--mono);
          font-weight: 900;
          letter-spacing: .2px;
          color: var(--accent);
        }}
        .sub {{
          font-family: var(--mono);
          color: var(--muted);
          font-size: 12px;
        }}

        .hot {{
          font-family: var(--mono);
          font-size: 12px;
          color: var(--muted);
          display:flex; gap:8px; align-items:center; flex-wrap:wrap;
          justify-content:flex-end;
        }}

        .chip {{
          border: 1px solid var(--line);
          background: var(--pane);
          padding: 6px 10px;
          border-radius: 10px;
          cursor:pointer;
          user-select:none;
        }}
        .chip:hover {{ background: var(--pane2); }}

        .alert {{
          margin-top: 10px;
          border: 1px solid var(--line);
          background: var(--pane);
          border-radius: 12px;
          padding: 10px 12px;
          display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap;
        }}
        .alert .left {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
        .badge {{
          font-family: var(--mono);
          font-weight: 900;
          padding: 4px 10px;
          border-radius: 999px;
          border: 1px solid var(--line);
        }}
        .b-ok {{ color: var(--ok); border-color: color-mix(in srgb, var(--ok) 30%, var(--line)); background: color-mix(in srgb, var(--ok) 10%, transparent); }}
        .b-warn {{ color: var(--warn); border-color: color-mix(in srgb, var(--warn) 30%, var(--line)); background: color-mix(in srgb, var(--warn) 10%, transparent); }}
        .b-no {{ color: var(--no); border-color: color-mix(in srgb, var(--no) 30%, var(--line)); background: color-mix(in srgb, var(--no) 10%, transparent); }}

        .tblwrap {{
          margin-top: 12px;
          border: 1px solid var(--line);
          border-radius: 14px;
          overflow: hidden;
          background: var(--pane);
        }}

        table {{
          width: 100%;
          border-collapse: collapse;
          font-family: var(--mono);
        }}
        thead th {{
          font-size: 12px;
          color: var(--muted);
          text-align: left;
          padding: 10px 10px;
          background: color-mix(in srgb, var(--pane) 92%, transparent);
          border-bottom: 1px solid var(--line);
          position: sticky; top: 86px; z-index: 30;
        }}
        tbody td {{
          padding: 10px 10px;
          border-top: 1px solid var(--line);
          font-size: 12.5px;
          vertical-align: top;
        }}
        tbody tr:hover {{
          background: color-mix(in srgb, var(--pane2) 70%, transparent);
        }}

        .sym {{ font-weight: 1000; color: var(--accent); }}
        .num {{ color: var(--text); }}
        .q2 {{ font-weight: 1000; }}
        .drv {{ color: var(--text); opacity: .92; }}
        .act {{ text-align: right; }}

        .bias {{ font-weight: 1000; }}
        .b-bull {{ color: var(--bull); }}
        .b-bear {{ color: var(--bear); }}
        .b-neu {{ color: var(--neu); }}

        .gate {{ font-weight: 1000; }}
        .g-ok {{ color: var(--ok); }}
        .g-no {{ color: var(--no); }}

        .kbtn {{
          font-family: var(--mono);
          font-weight: 1000;
          padding: 6px 10px;
          border-radius: 10px;
          border: 1px solid var(--line);
          background: var(--pane2);
          color: var(--text);
          cursor: pointer;
          min-height: 34px;
        }}
        .kbtn:hover {{ background: var(--pane); }}
        .muted {{ color: var(--muted); }}
        .tiny {{ font-size: 12px; }}

        /* Panel overlay */
        .overlay {{
          display:none;
          position: fixed;
          inset: 0;
          background: rgba(0,0,0,.65);
          z-index: 80;
          padding: calc(12px + env(safe-area-inset-top)) 10px calc(12px + env(safe-area-inset-bottom));
        }}
        .overlay.show {{ display:block; }}
        .panelbox {{
          max-width: 1280px;
          margin: 0 auto;
          border: 1px solid var(--line);
          border-radius: 14px;
          background: var(--pane);
          overflow: auto;
          max-height: 84vh;
          -webkit-overflow-scrolling: touch;
        }}
        .phead {{
          position: sticky; top: 0; z-index: 90;
          display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;
          padding: 12px 12px;
          background: color-mix(in srgb, var(--pane) 92%, transparent);
          border-bottom: 1px solid var(--line);
        }}
        .ptitle {{ font-weight: 1000; color: var(--accent); }}
        .pgrid {{
          display:grid; grid-template-columns: 1fr 1.3fr 1fr; gap: 12px;
          padding: 12px;
        }}
        .pcol {{
          border: 1px solid var(--line);
          border-radius: 12px;
          padding: 10px;
          background: var(--pane2);
        }}
        .ph {{ font-weight: 1000; color: var(--muted); margin-bottom: 8px; }}
        .ph2 {{ margin-top: 10px; font-weight: 1000; color: var(--muted); }}
        .plist {{ margin: 6px 0 0 18px; }}
        .gatebig {{ font-weight: 1000; font-size: 14px; }}

        details.det {{
          border: 1px solid var(--line);
          border-radius: 12px;
          padding: 8px 10px;
          background: color-mix(in srgb, var(--pane2) 70%, transparent);
          margin-top: 10px;
        }}
        details.det summary {{
          cursor:pointer;
          list-style:none;
          user-select:none;
          font-weight: 1000;
          color: var(--text);
        }}
        details.det summary::-webkit-details-marker {{ display:none; }}

        .why {{ margin: 8px 0 0 18px; }}
        .why li {{ margin: 8px 0; }}
        .wtag {{ color: var(--muted); border: 1px solid var(--line); padding: 2px 6px; border-radius: 999px; }}
        .wmeta {{ color: var(--muted); font-size: 12px; margin-left: 8px; }}
        .w2 {{ margin-top: 4px; }}
        .w3 {{ margin-top: 4px; color: var(--muted); }}

        .tbl th, .tbl td {{ padding: 8px 6px; border-top: 1px solid var(--line); font-size: 12px; text-align:left; }}
        .pos {{ color: var(--ok); font-weight:1000; }}
        .neg {{ color: var(--no); font-weight:1000; }}
        .news {{ margin: 8px 0 0 18px; }}

        .code {{
          background: rgba(255,255,255,.05);
          border:1px solid var(--line);
          border-radius:10px;
          padding: 10px;
          overflow:auto;
        }}

        /* iPhone friendliness: collapse columns */
        @media(max-width: 980px) {{
          thead th {{ position: static; }}
          .pgrid {{ grid-template-columns: 1fr; }}
        }}
        @media(max-width: 700px) {{
          /* show "terminal compact": hide some columns */
          .col-th, .col-ton, .col-too {{ display:none; }}
        }}
        @media(max-width: 520px) {{
          .col-conf, .col-drv {{ display:none; }}
        }}
      </style>
    </head>
    <body>
      <div class="hdr">
        <div class="wrap">
          <div class="topline">
            <div>
              <div class="title">NEWS BIAS // TERMINAL</div>
              <div class="sub">updated_utc={updated} • gate_profile={gate_profile} • event_mode={str(event_mode).lower()} • trump={("on" if trump_enabled else "off")}</div>
              <div class="sub">Next event: {up_line}</div>
            </div>
            <div class="hot">
              <span class="chip" onclick="jump('XAU')">1 XAU</span>
              <span class="chip" onclick="jump('US500')">2 US500</span>
              <span class="chip" onclick="jump('WTI')">3 WTI</span>
              <span class="chip" onclick="runNow()">R RUN</span>
              <span class="chip" onclick="openJSON()">J JSON</span>
              <span class="chip" onclick="openMorning()">M MORNING</span>
              <span class="chip" onclick="toggleTheme()">T THEME</span>
              <span class="chip" onclick="toggleAnalyst()">A ANALYST</span>
              <span class="chip" onclick="openLink('/diag')">/diag</span>
            </div>
          </div>

          <div class="alert">
            <div class="left">
              <span class="badge {('b-warn' if event_mode else 'b-ok')}">EVENT {('ON' if event_mode else 'OFF')}</span>
              <span class="badge {('b-warn' if (trump_enabled and trump_flag) else 'b-ok')}">TRUMP {('ACTIVE' if (trump_enabled and trump_flag) else ('QUIET' if trump_enabled else 'OFF'))}</span>
              <span class="badge {('b-no' if feed_err>0 else 'b-ok')}">FEEDS {('ERR='+str(feed_err) if feed_err>0 else 'OK')}</span>
              <span class="badge b-ok">STATUS {alert_text}</span>
            </div>
            <div class="right">
              <button class="kbtn" onclick="runNow()">RUN NOW</button>
              <button class="kbtn" onclick="openMorning()">MORNING</button>
            </div>
          </div>

          <div class="tblwrap" id="tbl">
            <table>
              <thead>
                <tr>
                  <th>SYM</th>
                  <th>BIAS</th>
                  <th>SCORE</th>
                  <th class="col-th">TH</th>
                  <th>Q2</th>
                  <th class="col-conf">CONFLICT</th>
                  <th class="col-ton">TO_N</th>
                  <th class="col-too">TO_O</th>
                  <th>GATE</th>
                  <th class="col-drv">TOP DRIVER</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {asset_row("XAU")}
                {asset_row("US500")}
                {asset_row("WTI")}
              </tbody>
            </table>
          </div>

          <div class="sub tiny muted" style="margin-top:10px;">
            Flip units: <b>score-sum</b> (weighted evidence). Not % / not market points. • Hotkeys: 1/2/3, R, J, M, T, A, Esc
          </div>
        </div>
      </div>

      <div class="wrap">
        <!-- Analyst mode panels render here but are hidden until A -->
        <div id="analyst" style="display:none; margin-top:12px;">
          <div class="tblwrap" style="padding:12px;">
            <div class="muted tiny">
              Analyst mode shows deep drilldowns via View button per asset. It is kept out of the main screen to stay Bloomberg-style.
            </div>
          </div>
        </div>
      </div>

      <!-- Overlay panels -->
      <div class="overlay" id="overlay" onclick="overlayClick(event)">
        <div class="panelbox" onclick="event.stopPropagation()">
          {details_panel("XAU")}
          {details_panel("US500")}
          {details_panel("WTI")}
        </div>
      </div>

      <!-- Modal -->
      <div class="overlay" id="modal" onclick="closeModal()">
        <div class="panelbox" onclick="event.stopPropagation()">
          <div class="phead">
            <div class="ptitle" id="mtitle">Modal</div>
            <div class="pactions">
              <button class="kbtn" onclick="closeModal()">Esc</button>
            </div>
          </div>
          <div style="padding:12px;" id="mbody"></div>
        </div>
      </div>

      <script>
        const THEME_KEY = 'term_theme';
        const ANALYST_KEY = 'term_analyst';

        function openLink(p){{
          window.open(p, '_blank', 'noopener');
        }}

        function setTheme(t){{
          document.documentElement.setAttribute('data-theme', t);
          localStorage.setItem(THEME_KEY, t);
        }}
        (function initTheme(){{
          const t = localStorage.getItem(THEME_KEY);
          if(t === 'light' || t === 'term') setTheme(t);
          else setTheme('term');
        }})();

        function toggleTheme(){{
          const cur = document.documentElement.getAttribute('data-theme') || 'term';
          setTheme(cur === 'term' ? 'light' : 'term');
        }}

        function toggleAnalyst(){{
          const cur = localStorage.getItem(ANALYST_KEY) === '1';
          const next = !cur;
          localStorage.setItem(ANALYST_KEY, next ? '1' : '0');
          document.getElementById('analyst').style.display = next ? 'block' : 'none';
        }}
        (function initAnalyst(){{
          const on = localStorage.getItem(ANALYST_KEY) === '1';
          document.getElementById('analyst').style.display = on ? 'block' : 'none';
        }})();

        function jump(a){{
          const tr = document.querySelector(`tr[data-asset="${{a}}"]`);
          if(tr) tr.scrollIntoView({{behavior:'smooth', block:'center'}});
        }}

        function overlayClick(e){{
          closePanel();
        }}

        function openPanel(a){{
          document.getElementById('overlay').classList.add('show');
          ['XAU','US500','WTI'].forEach(x => {{
            const p = document.getElementById('panel-' + x);
            if(p) p.style.display = (x === a) ? 'block' : 'none';
          }});
        }}
        function closePanel(){{
          document.getElementById('overlay').classList.remove('show');
          ['XAU','US500','WTI'].forEach(x => {{
            const p = document.getElementById('panel-' + x);
            if(p) p.style.display = 'none';
          }});
        }}

        function openModal(title, html){{
          document.getElementById('mtitle').innerText = title;
          document.getElementById('mbody').innerHTML = html;
          document.getElementById('modal').classList.add('show');
        }}
        function closeModal(){{
          document.getElementById('modal').classList.remove('show');
        }}

        async function runNow(){{
          try {{
            const r = await fetch('/run', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:'{{}}'}});
            await r.json();
            window.location.reload();
          }} catch(e) {{
            openModal('RUN error', `<div class="muted">` + String(e) + `</div>`);
          }}
        }}

        async function openJSON(){{
          try {{
            const r = await fetch('/bias?pretty=1');
            const t = await r.text();
            openModal('BIAS JSON', `<pre class="code" style="white-space:pre-wrap;">${{escapeHtml(t)}}</pre>`);
          }} catch(e) {{
            openModal('JSON error', `<div class="muted">` + String(e) + `</div>`);
          }}
        }}

        async function openMorning(){{
          try {{
            const r = await fetch('/morning_plan');
            const j = await r.json();
            openModal('MORNING PLAN', `<pre class="code" style="white-space:pre-wrap;">${{escapeHtml(JSON.stringify(j,null,2))}}</pre>`);
          }} catch(e) {{
            openModal('Morning error', `<div class="muted">` + String(e) + `</div>`);
          }}
        }}

        function escapeHtml(unsafe) {{
          return (unsafe || '').replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
        }}

        async function showExplain(asset){{
          try {{
            const r = await fetch('/explain?asset=' + encodeURIComponent(asset) + '&limit=60');
            const j = await r.json();
            if(j.error) {{
              openModal('Explain ' + asset, `<div class="muted">${{escapeHtml(j.error)}}</div>`);
              return;
            }}
            const rows = (j.top_matches || []).map(x => {{
              return `<tr>
                <td style="padding:8px;border-top:1px solid rgba(255,255,255,.10);">
                  <b>${{escapeHtml(x.why || '')}}</b>
                  <div class="muted tiny">${{escapeHtml(x.source||'')}} • age=${{x.age_min}}m • contrib=${{x.contrib}}</div>
                  <div class="muted tiny">pattern: ${{escapeHtml(x.pattern||'')}}</div>
                </td>
                <td style="padding:8px;border-top:1px solid rgba(255,255,255,.10);">
                  <a href="${{x.link}}" target="_blank" rel="noopener">${{escapeHtml(x.title||'')}}</a>
                </td>
              </tr>`;
            }}).join('');
            openModal('Explain ' + asset, `
              <div class="muted tiny">rules_count=${{j.rules_count}} • items=${{(j.top_matches||[]).length}}</div>
              <div style="overflow:auto; margin-top:10px;">
                <table class="tbl" style="width:100%;border-collapse:collapse;">
                  <tbody>${{rows || '<tr><td class="muted">—</td></tr>'}}</tbody>
                </table>
              </div>
            `);
          }} catch(e) {{
            openModal('Explain error', `<div class="muted">` + String(e) + `</div>`);
          }}
        }}

        // Hotkeys
        document.addEventListener('keydown', (e) => {{
          if(e.key === 'Escape') {{
            closePanel();
            closeModal();
          }}
          if(e.key === '1') jump('XAU');
          if(e.key === '2') jump('US500');
          if(e.key === '3') jump('WTI');
          if(e.key.toLowerCase() === 'r') runNow();
          if(e.key.toLowerCase() === 'j') openJSON();
          if(e.key.toLowerCase() === 'm') openMorning();
          if(e.key.toLowerCase() === 't') toggleTheme();
          if(e.key.toLowerCase() === 'a') toggleAnalyst();
        }});
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

# Explain endpoint (needed by terminal panels)
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
