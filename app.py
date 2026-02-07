# app.py
# News Bias Bot (MVP++) — RSS + Trade Gate + iPhone-friendly UI + FRED macro drivers + Morning Plan

import os
import json
import time
import re
import hashlib
import math
import calendar
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
    "DGS10":    {"name": "US 10Y Nominal", "freq": "d"},
    "DFII10":   {"name": "US 10Y Real",    "freq": "d"},
    "T10YIE":   {"name": "10Y Breakeven",  "freq": "d"},
    "DTWEXBGS": {"name": "Broad USD",      "freq": "d"},
    "VIXCLS":   {"name": "VIX",            "freq": "d"},
    "BAA10Y":   {"name": "BAA-10Y Spread", "freq": "d"},
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
    """
    Railway/managed PG часто требует SSL.
    Поддержка:
      - DATABASE_URL
      - PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
    + optional: PGSSLMODE (prefer/require/disable)
    """
    db_url = os.environ.get("DATABASE_URL", "").strip()
    sslmode = os.environ.get("PGSSLMODE", "prefer").strip()

    if db_url:
        # psycopg2 позволяет передать sslmode отдельно
        return psycopg2.connect(db_url, sslmode=sslmode)

    host = os.environ.get("PGHOST", "").strip()
    port = os.environ.get("PGPORT", "5432").strip()
    db = os.environ.get("PGDATABASE", "").strip()
    user = os.environ.get("PGUSER", "").strip()
    pwd = os.environ.get("PGPASSWORD", "")

    if not (host and db and user):
        raise RuntimeError("DB env is not set. Provide DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD.")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, sslmode=sslmode)

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
            res[src] = {
                "ok": True,
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": int(len(entries)),
            }
        except Exception as e:
            res[src] = {"ok": False, "error": str(e), "entries": 0}
    return res

def _error_html(title: str, err: Exception) -> HTMLResponse:
    msg = f"{type(err).__name__}: {str(err)}"
    html = f"""
    <html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <style>
      body{{font-family:system-ui,Segoe UI,Arial; padding:18px; max-width:900px; margin:0 auto;}}
      pre{{white-space:pre-wrap; background:#111827; color:#e5e7eb; padding:12px; border-radius:12px;}}
      a{{color:#2563eb}}
    </style>
    </head>
    <body>
      <h2>{title}</h2>
      <p>Это не “молчаливый 500”. Я вывел причину:</p>
      <pre>{msg}</pre>
      <p><b>Действия:</b></p>
      <ul>
        <li>Проверь <a href="/diag" target="_blank">/diag</a> (DB test)</li>
        <li>Проверь <a href="/health" target="_blank">/health</a></li>
        <li>Запусти пайплайн: <a href="/run" target="_blank">/run</a> (GET теперь тоже запускает)</li>
      </ul>
    </body></html>
    """
    return HTMLResponse(html, status_code=500)

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
                        # IMPORTANT: use UTC conversion, not local server TZ
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
                out.append({
                    "title": title,
                    "link": link,
                    "ts": int(ts),
                    "in_hours": round((ts - now_ts) / 3600.0, 2),
                })
        else:
            if len([x for x in out if x.get("ts") is None]) < 2:
                out.append({
                    "title": title,
                    "link": link,
                    "ts": None,
                    "in_hours": None,
                })

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
# SCORING / QUALITY helpers (unchanged)
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
            "top3_drivers": _top_drivers(contribs, topn=3),
            "flip": _flip_distances(score, th),
            "consensus_by_source": _consensus_by_source(contribs),
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
# TRADE GATE (unchanged)
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

    if event_mode:
        if cfg.get("event_mode_block", True):
            oq = int(cfg.get("event_override_quality", 70))
            oc = float(cfg.get("event_override_conflict", 0.45))
            if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
                fail_reasons.append("Event mode ON (macro risk window)")
                must_change.append(f"Either wait until event_mode=OFF, or require quality_v2 ≥ {oq} and conflict_index ≤ {oc} (and bias != NEUTRAL).")

    ok = (len(fail_reasons) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:3] if x.get("why")]
    if not why_short:
        why_short = ["Insufficient matched evidence (rules)"]

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
    """
    Быстрая диагностика: DB connect + таблицы + rowcounts
    """
    info = {
        "db": {},
        "env": {
            "has_DATABASE_URL": bool(os.environ.get("DATABASE_URL", "").strip()),
            "PGHOST": os.environ.get("PGHOST", ""),
            "PGDATABASE": os.environ.get("PGDATABASE", ""),
            "PGUSER": os.environ.get("PGUSER", ""),
            "PGSSLMODE": os.environ.get("PGSSLMODE", "prefer"),
        }
    }
    try:
        db_init()
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM news_items;")
                n_news = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fred_series;")
                n_fred = int(cur.fetchone()[0])
                cur.execute("SELECT updated_ts FROM bias_state WHERE id=1;")
                row = cur.fetchone()
        info["db"] = {
            "ok": True,
            "news_items": n_news,
            "fred_points": n_fred,
            "has_bias_state": bool(row),
        }
        return info
    except Exception as e:
        info["db"] = {"ok": False, "error": f"{type(e).__name__}: {str(e)}"}
        return JSONResponse(info, status_code=500)

@app.get("/rules")
def rules():
    return {"assets": ASSETS, "rules": RULES}

@app.get("/bias")
def bias(pretty: int = 0):
    try:
        db_init()
        state = load_bias()
        if not state:
            state = pipeline_run()
        if pretty:
            return PlainTextResponse(json.dumps(state, ensure_ascii=False, indent=2), media_type="application/json")
        return JSONResponse(state)
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)}"}, status_code=500)

@app.post("/run")
def run_now_post():
    try:
        return pipeline_run()
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)}"}, status_code=500)

# ✅ GET /run теперь тоже запускает (чтобы в браузере работало)
@app.get("/run")
def run_now_get():
    try:
        return pipeline_run()
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)}"}, status_code=500)

@app.get("/latest")
def latest(limit: int = 40):
    try:
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
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)}"}, status_code=500)

@app.get("/morning_plan")
def morning_plan():
    try:
        db_init()
        payload = load_bias()
        if not payload:
            payload = pipeline_run()

        assets = payload.get("assets", {}) or {}
        meta = payload.get("meta", {}) or {}
        event = payload.get("event", {}) or {}
        trump = meta.get("trump", {}) or {}

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
            "trump": trump,
            "plan": {
                "XAU": pack("XAU"),
                "US500": pack("US500"),
                "WTI": pack("WTI"),
            }
        }
        return JSONResponse(out)
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {str(e)}"}, status_code=500)

# ============================================================
# UI (Dashboard)
# ============================================================

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
        # ВАЖНО: dashboard теперь не падает "тихо", если DB/ingest сломан
        db_init()
        payload = load_bias()
        if not payload:
            payload = pipeline_run()

        # ---- дальше твой HTML (я НЕ менял логику UI, только обернул в try/except)
        # ---- (тут оставь свой HTML код как был ниже; чтобы ответ не был на 3000 строк,
        # ----  просто вставь твой текущий body/dashboard HTML блок без изменений)

        # ✅ Чтобы не засорять ответ повтором твоей огромной HTML-части,
        # ✅ ниже я возвращаю "короткую" версию заглушки.
        # ВАЖНО: в твоём проекте просто оставь твою текущую HTML-генерацию как была.
        return HTMLResponse(
            "<html><body style='font-family:system-ui;padding:18px'>"
            "<h2>Dashboard OK</h2>"
            "<p>Твой HTML блок ниже должен быть вставлен обратно. "
            "Главное — теперь есть try/except, и сервис не будет отдавать пустой 500.</p>"
            "<p><a href='/run'>Run now</a> • <a href='/diag'>Diag</a> • <a href='/bias?pretty=1'>Bias JSON</a></p>"
            "</body></html>"
        )

    except Exception as e:
        return _error_html("Dashboard error", e)
