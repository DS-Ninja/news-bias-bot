import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse

# ============================================================
# CONFIG
# ============================================================

ASSETS = ["XAU", "US500", "WTI"]

DB_DSN = os.environ.get("DB_DSN", "dbname=newsbias user=postgres password=postgres host=127.0.0.1 port=5432")

GATE_PROFILE = os.environ.get("GATE_PROFILE", "STRICT").strip().upper()
if GATE_PROFILE not in ("STRICT", "MODERATE"):
    GATE_PROFILE = "STRICT"

GATE_THRESHOLDS = {
    "STRICT": {
        "quality_v2_min": 55,
        "conflict_max": 0.55,
        "min_opp_flip_dist": 0.35,
        "neutral_allow": False,
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

# Bias thresholds (per-asset)
BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

# ============================================================
# RSS FEEDS
# ============================================================

RSS_FEEDS: Dict[str, str] = {
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/bls_latest.rss",
    "BEA": "https://www.bea.gov/rss/rss.xml",
    "OPEC": "https://www.opec.org/opec_web/en/press_room.rss",
    "EIA": "https://www.eia.gov/rss/press_release.xml",

    "OILPRICE": "https://oilprice.com/rss/main",

    "INVESTING_NEWS_25": "https://www.investing.com/rss/news_25.rss",
    "INVESTING_STOCK_FUND": "https://www.investing.com/rss/stock_Fundamental.rss",
    "INVESTING_COMMOD_TECH": "https://www.investing.com/rss/commodities_Technical.rss",
    "INVESTING_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INVESTING_NEWS_95": "https://www.investing.com/rss/news_95.rss",
    "INVESTING_MKT_TECH": "https://www.investing.com/rss/market_overview_Technical.rss",
    "INVESTING_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INVESTING_MKT_IDEAS": "https://www.investing.com/rss/market_overview_investing_ideas.rss",
    "INVESTING_FX_TECH": "https://www.investing.com/rss/forex_Technical.rss",
    "INVESTING_FX_FUND": "https://www.investing.com/rss/forex_Fundamental.rss",

    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "FXSTREET_STOCKS": "https://www.fxstreet.com/rss/stocks",

    "MARKETWATCH_TOP_STORIES": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REAL_TIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "MARKETWATCH_BREAKING": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
    "MARKETWATCH_MARKETPULSE": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",

    "DAILYFOREX_NEWS": "https://www.dailyforex.com/rss/forexnews.xml",
    "DAILYFOREX_TECH": "https://www.dailyforex.com/rss/technicalanalysis.xml",
    "DAILYFOREX_FUND": "https://www.dailyforex.com/rss/fundamentalanalysis.xml",

    # Optional
    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",
    "POLITICO_TRUMP": "https://rss.politico.com/donald-trump.xml",

    # Calendar (do not ingest as news)
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml?version=51c52bdf678435acb58756461c1e8226",
}

CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR"}  # don't store as news_items

SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.8,
    "EIA": 2.4,
    "OPEC": 2.2,

    "OILPRICE": 1.2,

    "INVESTING_NEWS_25": 1.0,
    "INVESTING_STOCK_FUND": 1.0,
    "INVESTING_COMMOD_TECH": 1.0,
    "INVESTING_NEWS_11": 1.0,
    "INVESTING_NEWS_95": 1.0,
    "INVESTING_MKT_TECH": 1.0,
    "INVESTING_MKT_FUND": 1.0,
    "INVESTING_MKT_IDEAS": 0.9,
    "INVESTING_FX_TECH": 0.9,
    "INVESTING_FX_FUND": 0.9,

    "FXSTREET_NEWS": 1.4,
    "FXSTREET_ANALYSIS": 1.2,
    "FXSTREET_STOCKS": 1.2,

    "MARKETWATCH_TOP_STORIES": 1.3,
    "MARKETWATCH_REAL_TIME": 1.3,
    "MARKETWATCH_BREAKING": 1.6,
    "MARKETWATCH_MARKETPULSE": 1.2,

    "DAILYFOREX_NEWS": 1.0,
    "DAILYFOREX_TECH": 1.0,
    "DAILYFOREX_FUND": 1.2,

    "RSSAPP_1": 1.0,
    "RSSAPP_2": 1.0,
    "POLITICO_TRUMP": 1.2,

    # calendar is not news, but used for risk
    "FOREXFACTORY_CALENDAR": 0.0,
}

# Optional: source allowlist per asset (if you want strict mapping).
# If empty dict => all sources are eligible for all assets.
# Example:
# ASSET_SOURCE_ALLOWLIST = {
#     "XAU": {"FED","BLS","BEA","FXSTREET_NEWS","MARKETWATCH_BREAKING","INVESTING_NEWS_25"},
#     "US500": {"FED","BLS","BEA","MARKETWATCH_TOP_STORIES","MARKETWATCH_BREAKING","INVESTING_NEWS_95"},
#     "WTI": {"EIA","OPEC","OILPRICE","MARKETWATCH_BREAKING","INVESTING_NEWS_11"},
# }
ASSET_SOURCE_ALLOWLIST: Dict[str, set] = {}

# ============================================================
# RULES: regex patterns -> signed weight + explanation
# ============================================================

RULES: Dict[str, List[Tuple[str, float, str]]] = {
    "XAU": [
        (r"\b(cpi|inflation|ppi|core inflation)\b", +0.7, "Inflation focus supports gold (hedge narrative)"),
        (r"\b(rate cut|cuts|dovish|easing)\b", +0.8, "Dovish rates supportive for gold"),
        (r"\b(rate hike|hikes|hawkish|tightening)\b", -0.8, "Hawkish rates weigh on gold"),
        (r"\b(real yields|yields rise|treasury yields)\b", -0.8, "Higher yields weigh on gold"),
        (r"\b(strong dollar|usd strengthens|dollar rallies)\b", -0.7, "Stronger USD weighs on gold"),
        (r"\b(geopolitical|safe[- ]haven|risk aversion|flight to safety)\b", +0.5, "Safe-haven demand supports gold"),
    ],
    "US500": [
        (r"\b(earnings beat|better[- ]than[- ]expected earnings|guidance raised)\b", +0.8, "Earnings upside supports equities"),
        (r"\b(earnings miss|guidance cut|profit warning)\b", -0.9, "Earnings downside pressures equities"),
        (r"\b(hawkish|rate hike|higher for longer)\b", -0.8, "Higher rates pressure equity multiples"),
        (r"\b(dovish|rate cut|easing)\b", +0.6, "Easing expectations support equities"),
        (r"\b(recession fears|hard landing)\b", -0.7, "Growth fear pressures equities"),
        (r"\b(risk on|rally broadens)\b", +0.4, "Risk-on supports equities"),
    ],
    "WTI": [
        (r"\b(opec|output cut|production cut|supply cut)\b", +0.9, "Supply cuts support crude"),
        (r"\b(sanctions|disruption|outage|pipeline|attack|geopolitical)\b", +0.8, "Supply risk supports crude"),
        (r"\b(inventories rise|inventory build|stocks build)\b", -0.8, "Inventory build pressures crude"),
        (r"\b(inventories draw|inventory draw|stocks draw)\b", +0.7, "Inventory draw supports crude"),
        (r"\b(demand weakens|recession fears|slowdown)\b", -0.6, "Demand concerns pressure crude"),
    ],
}

# ============================================================
# FRED (drivers)
# ============================================================

ENABLE_FRED = os.environ.get("ENABLE_FRED", "1").strip() == "1"
FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()  # required for FRED API

FRED_SERIES = {
    # Rates / yields
    "DGS10": "10Y Treasury (nominal)",
    "DFII10": "10Y TIPS (real yield)",
    # USD proxy
    "DTWEXBGS": "Trade Weighted USD Index (broad, goods)",
    # Vol / risk
    "VIXCLS": "VIX",
    # Equity proxy
    "SP500": "S&P 500 index",
    # Oil spot (WTI)
    "DCOILWTICO": "WTI spot",
    # FX proxy (if you want)
    "DEXJPUS": "USDJPY",
}

# How FRED impacts each asset (bias boost, not a replacement for RSS)
# sign convention: + => bullish for asset, - => bearish for asset
FRED_IMPACT = {
    "XAU": [
        ("DFII10", -1.00, "Real yields ↑ => Gold ↓"),
        ("DTWEXBGS", -0.70, "USD ↑ => Gold ↓"),
        ("VIXCLS", +0.40, "Risk-off (VIX ↑) => Gold ↑"),
    ],
    "US500": [
        ("DGS10", -0.60, "Yields ↑ => Equities ↓"),
        ("VIXCLS", -0.70, "VIX ↑ => Equities ↓"),
        ("DTWEXBGS", -0.15, "USD ↑ slightly negative for risk assets"),
    ],
    "WTI": [
        ("DTWEXBGS", -0.25, "USD ↑ => Oil ↓ (pricing effect)"),
        ("DCOILWTICO", +0.20, "Spot strength supports trend bias (light)"),
        ("VIXCLS", -0.15, "Risk-off mildly negative for crude"),
    ],
}

# How to score FRED snapshot -> numeric boost in score units
# We use daily change and 5d change as signals; clamp to avoid overpowering RSS.
FRED_SCALERS = {
    "DFII10": 0.80,      # real yields move is powerful for XAU
    "DGS10": 0.50,
    "DTWEXBGS": 0.40,
    "VIXCLS": 0.35,
    "SP500": 0.10,
    "DCOILWTICO": 0.20,
    "DEXJPUS": 0.10,
}

FRED_CLAMP = 0.45  # max abs boost per series

def _fred_get_series_latest(series_id: str, limit: int = 8) -> List[Tuple[str, float]]:
    """
    Returns list of (date_str, value) latest observations, newest last.
    """
    if not FRED_API_KEY:
        return []
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": str(limit),
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    obs = j.get("observations", []) or []
    out: List[Tuple[str, float]] = []
    for o in reversed(obs):  # oldest -> newest
        ds = str(o.get("date", ""))
        vs = str(o.get("value", ""))
        try:
            v = float(vs)
        except Exception:
            continue
        out.append((ds, v))
    return out

def _fred_snapshot() -> Dict[str, Any]:
    """
    Returns per-series: last, d1, d5. Best-effort.
    """
    snap: Dict[str, Any] = {"ok": False, "series": {}}
    if not ENABLE_FRED:
        snap["ok"] = False
        snap["error"] = "ENABLE_FRED=0"
        return snap
    if not FRED_API_KEY:
        snap["ok"] = False
        snap["error"] = "Missing FRED_API_KEY"
        return snap

    try:
        for sid in FRED_SERIES.keys():
            pts = _fred_get_series_latest(sid, limit=8)
            if len(pts) < 2:
                continue
            last_date, last = pts[-1]
            prev = pts[-2][1]
            d1 = last - prev
            d5 = (last - pts[-6][1]) if len(pts) >= 6 else (last - pts[0][1])
            snap["series"][sid] = {
                "name": FRED_SERIES.get(sid, sid),
                "last_date": last_date,
                "last": round(last, 6),
                "d1": round(d1, 6),
                "d5": round(d5, 6),
            }
        snap["ok"] = True
        return snap
    except Exception as e:
        snap["ok"] = False
        snap["error"] = str(e)
        return snap

def _fred_boost_for_asset(asset: str, snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert FRED snapshot to a bounded score boost in "score units".
    """
    out = {
        "ok": bool(snap.get("ok")),
        "boost": 0.0,
        "drivers": [],
        "note": "FRED boost is bounded and only nudges RSS bias; it does NOT override rules.",
    }
    if not snap.get("ok"):
        return out

    total = 0.0
    series_data = snap.get("series", {}) or {}

    for sid, polarity, why in FRED_IMPACT.get(asset, []):
        sd = series_data.get(sid)
        if not sd:
            continue

        # We use combined signal = 0.65*d1 + 0.35*(d5/5)
        d1 = float(sd.get("d1", 0.0))
        d5 = float(sd.get("d5", 0.0))
        sig = 0.65 * d1 + 0.35 * (d5 / 5.0)

        k = float(FRED_SCALERS.get(sid, 0.2))
        raw = polarity * sig * k

        # clamp
        raw = max(-FRED_CLAMP, min(FRED_CLAMP, raw))

        total += raw

        out["drivers"].append({
            "series": sid,
            "name": sd.get("name", sid),
            "last": sd.get("last"),
            "d1": d1,
            "d5": d5,
            "polarity": polarity,
            "k": k,
            "raw_boost": round(raw, 4),
            "why": why,
        })

    out["boost"] = round(total, 4)
    return out

# ============================================================
# EVENT / RISK MODE
# ============================================================

EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": int(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 6,
}

TRUMP_FLAG = {
    "enabled": True,
    "lookback_hours": float(os.environ.get("TRUMP_LOOKBACK_HOURS", "24")),
    "regex": r"\b(trump|donald trump|white house|oval office|tariff|trade war)\b",
    "sources_hint": {"POLITICO_TRUMP", "MARKETWATCH_BREAKING", "MARKETWATCH_REAL_TIME", "INVESTING_NEWS_25", "FXSTREET_NEWS"},
}

def _parse_event_time_best_effort(text: str) -> Optional[int]:
    s = (text or "").strip()
    # 1) YYYY-MM-DD HH:MM
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})\b", s)
    if m:
        y, mo, da, hh, mm = map(int, m.groups())
        dt = datetime(y, mo, da, hh, mm, tzinfo=timezone.utc)
        return int(dt.timestamp())
    return None

def _get_upcoming_events(now_ts: int) -> List[Dict[str, Any]]:
    url = RSS_FEEDS.get("FOREXFACTORY_CALENDAR")
    if not url:
        return []
    d = feedparser.parse(url)
    entries = getattr(d, "entries", []) or []
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
        else:
            ts = _parse_event_time_best_effort(title)

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
    macro_sources = {"FED", "BLS", "BEA", "EIA", "OPEC"}
    for (source, _title, _link, ts) in rows:
        if source in macro_sources and (now_ts - int(ts)) <= recent_sec:
            return True
    return False

def _trump_headlines_flag(rows: List[Tuple[str, str, str, int]], now_ts: int) -> Dict[str, Any]:
    if not TRUMP_FLAG["enabled"]:
        return {"enabled": False, "flag": False, "count": 0, "items": []}

    lookback_sec = int(float(TRUMP_FLAG["lookback_hours"]) * 3600)
    cutoff = now_ts - lookback_sec
    rx = re.compile(TRUMP_FLAG["regex"], flags=re.I)

    items = []
    for (source, title, link, ts) in rows:
        if int(ts) < cutoff:
            continue
        if rx.search(title or ""):
            items.append({
                "source": source,
                "title": title,
                "link": link,
                "published_ts": int(ts),
                "age_min": int((now_ts - int(ts)) / 60),
            })

    items.sort(key=lambda x: x["published_ts"], reverse=True)
    return {
        "enabled": True,
        "flag": len(items) > 0,
        "count": len(items),
        "items": items[:8],
        "regex": TRUMP_FLAG["regex"],
    }

# ============================================================
# DB
# ============================================================

def db_conn():
    return psycopg2.connect(DB_DSN)

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
        conn.commit()

def fingerprint(title: str, link: str) -> str:
    s = (title or "") + "||" + (link or "")
    return str(abs(hash(s)))

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

                d = feedparser.parse(url)
                entries = getattr(d, "entries", []) or []

                if int(getattr(d, "bozo", 0)) == 1 and len(entries) == 0:
                    continue

                for e in entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    published_parsed = e.get("published_parsed")
                    if published_parsed:
                        published_ts = int(time.mktime(published_parsed))
                    else:
                        published_ts = now

                    fp = fingerprint(title, link)
                    try:
                        cur.execute(
                            "INSERT INTO news_items(source,title,link,published_ts,fingerprint) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (fingerprint) DO NOTHING;",
                            (src, title, link, published_ts, fp),
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                    except Exception:
                        conn.rollback()
                        continue

        conn.commit()
    return inserted

def feeds_health_live() -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True, "feeds": {}}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            entries = getattr(d, "entries", []) or []
            out["feeds"][src] = {
                "ok": True,
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": len(entries),
            }
        except Exception as e:
            out["feeds"][src] = {"ok": False, "error": str(e), "entries": 0, "bozo": 1}
    return out

# ============================================================
# SCORING HELPERS
# ============================================================

def decay_weight(age_sec: int) -> float:
    if age_sec <= 0:
        return 1.0
    return float(pow(2.718281828, -LAMBDA * age_sec))

def _fresh_bucket(age_sec: int) -> str:
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"

def match_rules(asset: str, title: str) -> List[Dict[str, Any]]:
    title_l = (title or "").lower()
    out: List[Dict[str, Any]] = []
    for (pat, w, why) in RULES.get(asset, []):
        if re.search(pat, title_l, flags=re.I):
            out.append({"pattern": pat, "w": float(w), "why": why})
    return out

def _consensus_stats(contribs: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    net = sum(float(x.get("contrib", 0.0)) for x in contribs)
    abs_sum = sum(abs(float(x.get("contrib", 0.0))) for x in contribs)
    if abs_sum <= 1e-12:
        return 0.0, 1.0, 0.0
    consensus_ratio = abs(net) / abs_sum
    conflict_index = 1.0 - consensus_ratio
    return round(consensus_ratio, 4), round(conflict_index, 4), round(abs_sum, 4)

def _flip_distances(score: float, th: float) -> Dict[str, float]:
    to_bullish = max(0.0, th - score)
    to_bearish = max(0.0, score + th)
    return {
        "to_bullish": round(to_bullish, 4),
        "to_bearish": round(to_bearish, 4),
        "note": "Δ needed in score units to reach +th or -th",
    }

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

def _asset_sources_allowed(asset: str) -> Optional[set]:
    s = ASSET_SOURCE_ALLOWLIST.get(asset)
    if not s:
        return None
    return s

def compute_asset_pack(asset: str, rows: List[Tuple[str, str, str, int]], now: int, fred_snap: Dict[str, Any], risk_flags: Dict[str, Any]) -> Dict[str, Any]:
    th = float(BIAS_THRESH.get(asset, 1.0))
    allowed_sources = _asset_sources_allowed(asset)

    score = 0.0
    contribs: List[Dict[str, Any]] = []
    freshness = {"0-2h": 0, "2-8h": 0, "8-24h": 0}

    # RSS layer
    for (source, title, link, ts) in rows:
        if allowed_sources is not None and source not in allowed_sources:
            continue

        age_sec = max(0, now - int(ts))
        w_src = float(SOURCE_WEIGHT.get(source, 1.0))
        w_time = float(decay_weight(age_sec))

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
                "src_w": w_src,
                "time_w": round(w_time, 4),
                "contrib": round(contrib, 4),
                "why": m["why"],
                "pattern": m["pattern"],
            })

    # FRED macro layer (bounded boost)
    fred_pack = _fred_boost_for_asset(asset, fred_snap) if ENABLE_FRED else {"ok": False, "boost": 0.0, "drivers": []}
    score_total = score + float(fred_pack.get("boost", 0.0))

    # bias
    if score_total >= th:
        bias = "BULLISH"
    elif score_total <= -th:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    why_top5 = sorted(contribs, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]

    evidence_count = len(contribs)
    src_div = len(set([w["source"] for w in contribs])) if contribs else 0

    # quality v1
    strength = min(1.0, abs(score_total) / max(th, 1e-9))
    quality_v1 = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, src_div * 2.0)))

    # v2: consensus/conflict
    consensus_ratio, conflict_index, _abs_sum = _consensus_stats(contribs)

    strength_2 = min(1.2, abs(score_total) / max(th, 1e-9))
    ev_term = min(1.0, evidence_count / 18.0)
    div_term = min(1.0, src_div / 7.0)
    fresh_total = sum(int(v) for v in freshness.values())
    fresh02 = freshness["0-2h"]
    fresh28 = freshness["2-8h"]
    fresh_score = 0.0
    if fresh_total > 0:
        fresh_score = min(1.0, (fresh02 * 1.0 + fresh28 * 0.6) / max(1.0, fresh_total))

    # risk penalties (NOT direction triggers)
    risk_penalty = 0.0
    if bool(risk_flags.get("event_mode", False)):
        risk_penalty += 0.15
    if bool(risk_flags.get("trump_headlines", {}).get("flag", False)):
        risk_penalty += 0.10  # unified trump risk

    raw_v2 = (
        0.45 * min(1.0, strength_2) +
        0.20 * ev_term +
        0.10 * div_term +
        0.10 * fresh_score +
        0.15 * consensus_ratio -
        0.35 * conflict_index -
        risk_penalty
    )
    quality_v2 = int(max(0, min(100, round(raw_v2 * 100))))

    top3 = _top_drivers(contribs, topn=3)
    flip = _flip_distances(float(score_total), th)
    cons_by_src = _consensus_by_source(contribs)

    return {
        "bias": bias,
        "score": round(score_total, 4),
        "score_rss": round(score, 4),
        "score_fred_boost": round(float(fred_pack.get("boost", 0.0)), 4),
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
        "fred": fred_pack,
        "sources_allowlist": sorted(list(allowed_sources)) if allowed_sources is not None else None,
    }

# ============================================================
# BIAS COMPUTE + SAVE/LOAD
# ============================================================

def compute_bias(lookback_hours: int = 24, limit_rows: int = 1200) -> Dict[str, Any]:
    now = int(time.time())
    cutoff = now - lookback_hours * 3600

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT %s;
                """,
                (cutoff, limit_rows),
            )
            rows: List[Tuple[str, str, str, int]] = cur.fetchall()

    # risk/event flags (global, not directional triggers)
    upcoming_events = _get_upcoming_events(now) if EVENT_CFG["enabled"] else []
    recent_macro = _macro_recent_flag(rows, now)
    event_mode = bool(recent_macro or any(x.get("ts") is not None for x in upcoming_events))

    trump = _trump_headlines_flag(rows, now)

    risk_flags = {
        "event_mode": event_mode,
        "recent_macro": recent_macro,
        "upcoming_events": upcoming_events,
        "trump_headlines": trump,
    }

    fred_snap = _fred_snapshot() if ENABLE_FRED else {"ok": False, "error": "ENABLE_FRED=0", "series": {}}

    assets_out: Dict[str, Any] = {}
    for asset in ASSETS:
        assets_out[asset] = compute_asset_pack(asset, rows, now, fred_snap, risk_flags)

    payload: Dict[str, Any] = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
            "gate_profile": GATE_PROFILE,
            "fred_enabled": ENABLE_FRED,
        },
        "risk": {
            "event_mode": event_mode,
            "recent_macro": recent_macro,
            "upcoming_events": upcoming_events,
            "trump_headlines": trump,
        },
        "fred": fred_snap,
    }
    return payload

def save_bias(payload: Dict[str, Any]):
    now = int(time.time())
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bias_state(id, updated_ts, payload_json)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE SET updated_ts=EXCLUDED.updated_ts, payload_json=EXCLUDED.payload_json;
                """,
                (now, json.dumps(payload, ensure_ascii=False)),
            )
        conn.commit()

def load_bias() -> Optional[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT updated_ts, payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    if not row:
        return None
    _updated_ts, payload_json = row
    return json.loads(payload_json)

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

def eval_trade_gate(asset_obj: Dict[str, Any], risk_obj: Dict[str, Any], profile: str) -> Dict[str, Any]:
    cfg = GATE_THRESHOLDS[profile]

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

    # Bias gate
    if bias == "NEUTRAL" and not cfg.get("neutral_allow", False):
        fail_reasons.append("Bias is NEUTRAL")
        must_change.append("score must reach +th or -th (become BULLISH/BEARISH)")
    elif bias == "NEUTRAL" and cfg.get("neutral_allow", False):
        maxd = float(cfg.get("neutral_flip_dist_max", 0.20))
        if min(to_bull, to_bear) > maxd:
            fail_reasons.append("Bias is NEUTRAL (not near flip)")
            must_change.append(f"Δ to threshold must shrink: min(to_bullish,to_bearish) ≤ {maxd}")

    # Quality v2 gate
    qmin = int(cfg["quality_v2_min"])
    if q2 < qmin:
        fail_reasons.append(f"Quality v2 too low ({q2} < {qmin})")
        must_change.append(f"quality_v2 must be ≥ {qmin}")

    # Conflict gate
    cmax = float(cfg["conflict_max"])
    if conflict > cmax:
        fail_reasons.append(f"Conflict too high ({conflict} > {cmax})")
        must_change.append(f"conflict_index must be ≤ {cmax}")

    # Opp flip distance guard
    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH"):
        if opp_dist < mind:
            fail_reasons.append(f"Too close to opposite flip ({opp_label}={round(opp_dist,4)} < {mind})")
            must_change.append(f"{opp_label} must be ≥ {mind}")

    # Risk mode gate: event_mode and trump_headlines are NON-directional blockers/penalties
    event_mode = bool((risk_obj or {}).get("event_mode", False))
    trump_flag = bool(((risk_obj or {}).get("trump_headlines", {}) or {}).get("flag", False))

    if event_mode and cfg.get("event_mode_block", True):
        oq = int(cfg.get("event_override_quality", 70))
        oc = float(cfg.get("event_override_conflict", 0.45))
        if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
            fail_reasons.append("Event mode ON (macro risk window)")
            must_change.append(f"Need event_mode=OFF OR quality_v2 ≥ {oq} and conflict_index ≤ {oc} (and bias != NEUTRAL).")

    if trump_flag and profile == "STRICT":
        # unified trump flag blocks strict unless very clean
        oq = max(int(cfg.get("event_override_quality", 70)), 72)
        oc = min(float(cfg.get("event_override_conflict", 0.45)), 0.45)
        if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
            fail_reasons.append("Trump headlines risk ON (global)")
            must_change.append(f"Wait risk to cool OR require quality_v2 ≥ {oq} and conflict_index ≤ {oc} (and bias != NEUTRAL).")

    ok = (len(fail_reasons) == 0)

    td = asset_obj.get("top3_drivers", []) or []
    why_short = [x.get("why", "") for x in td[:3] if x.get("why")] or ["Insufficient matched evidence (rules)"]

    return {
        "ok": bool(ok),
        "label": "TRADE OK" if ok else "NO TRADE",
        "why": why_short[:3],
        "fail_reasons": fail_reasons[:5],
        "must_change": must_change[:5],
    }

# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP+)")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)

@app.get("/health")
def health():
    return {"ok": True, "gate_profile": GATE_PROFILE, "fred_enabled": ENABLE_FRED}

@app.get("/rules")
def rules():
    return {"assets": ASSETS, "rules": RULES}

@app.post("/run")
def run_now():
    return pipeline_run()

@app.get("/bias")
def bias(pretty: int = 0):
    db_init()
    state = load_bias()
    if not state:
        state = pipeline_run()
    if pretty:
        return PlainTextResponse(json.dumps(state, ensure_ascii=False, indent=2), media_type="application/json")
    return JSONResponse(state)

@app.get("/latest")
def latest(limit: int = 40):
    db_init()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source, title, link, published_ts FROM news_items ORDER BY published_ts DESC LIMIT %s;",
                (limit,),
            )
            rows = cur.fetchall()
    return {"items": [{"source": s, "title": t, "link": l, "published_ts": int(ts)} for (s, t, l, ts) in rows]}

@app.get("/explain")
def explain(asset: str = "US500", limit: int = 60):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return {"error": f"unknown asset: {asset}"}

    now = int(time.time())
    cutoff = now - 24 * 3600
    out: List[Dict[str, Any]] = []

    db_init()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT 1200;
                """,
                (cutoff,),
            )
            rows = cur.fetchall()

    allowed_sources = _asset_sources_allowed(asset)

    for (source, title, link, ts) in rows:
        if allowed_sources is not None and source not in allowed_sources:
            continue
        matches = match_rules(asset, title)
        if not matches:
            continue
        age_sec = max(0, now - int(ts))
        w_src = float(SOURCE_WEIGHT.get(source, 1.0))
        w_time = float(decay_weight(age_sec))
        for m in matches:
            contrib = float(m["w"]) * w_src * w_time
            out.append({
                "source": source,
                "title": title,
                "link": link,
                "age_min": int(age_sec / 60),
                "base_w": float(m["w"]),
                "src_w": float(w_src),
                "time_w": round(float(w_time), 4),
                "contrib": round(float(contrib), 4),
                "why": m["why"],
                "pattern": m["pattern"],
            })

    out_sorted = sorted(out, key=lambda x: abs(float(x["contrib"])), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, [])), "allowlist": sorted(list(allowed_sources)) if allowed_sources else None}

@app.get("/feeds_health")
def feeds_health():
    return feeds_health_live()

# ============================================================
# MORNING PLAN (report)
# ============================================================

def _pill(cls: str, text: str) -> str:
    return f'<span class="pill {cls}">{text}</span>'

def _pill_bias(bias: str) -> str:
    if bias == "BULLISH":
        return _pill("pill-bull", "BULLISH")
    if bias == "BEARISH":
        return _pill("pill-bear", "BEARISH")
    return _pill("pill-neutral", "NEUTRAL")

@app.get("/morning_plan", response_class=HTMLResponse)
def morning_plan():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {}) or {}
    risk = payload.get("risk", {}) or {}
    fred = payload.get("fred", {}) or {}
    updated = payload.get("updated_utc", "")

    gate_profile = (payload.get("meta", {}) or {}).get("gate_profile", GATE_PROFILE)

    trump = (risk.get("trump_headlines", {}) or {})
    trump_on = bool(trump.get("flag", False))
    event_on = bool(risk.get("event_mode", False))

    # compact cards
    rows_html = ""
    for a in ASSETS:
        ao = assets.get(a, {}) or {}
        bias = str(ao.get("bias", "NEUTRAL"))
        score = float(ao.get("score", 0.0))
        th = float(ao.get("threshold", 1.0))
        q2 = int(ao.get("quality_v2", 0))
        conf = float(ao.get("conflict_index", 1.0))
        gate = eval_trade_gate(ao, risk, gate_profile)

        drivers = (ao.get("top3_drivers", []) or [])[:3]
        d_html = "".join([f"<li>{x.get('why','')} <span class='muted'>abs={x.get('abs_contrib_sum','')}</span></li>" for x in drivers]) or "<li class='muted'>—</li>"

        fred_pack = ao.get("fred", {}) or {}
        fboost = float(fred_pack.get("boost", 0.0))
        fdrivers = (fred_pack.get("drivers", []) or [])[:3]
        fd_html = "".join([f"<li>{x.get('series')} ({x.get('raw_boost')}) — {x.get('why')}</li>" for x in fdrivers]) or "<li class='muted'>—</li>"

        rows_html += f"""
        <div class="card">
          <div class="h2">{a} {_pill_bias(bias)} <span class="muted">score={round(score,3)} / th={round(th,3)} • q2={q2} • conflict={round(conf,3)}</span></div>
          <div style="margin-top:8px;">{_pill("pill-ok" if gate.get("ok") else "pill-no", "✅ TRADE OK" if gate.get("ok") else "❌ NO TRADE")} <span class="muted">({gate_profile})</span></div>
          <div class="grid">
            <div class="box">
              <div class="h3">WHY (RSS drivers)</div>
              <ul class="mini">{d_html}</ul>
            </div>
            <div class="box">
              <div class="h3">Macro (FRED) boost</div>
              <div class="muted">boost: <b>{round(fboost,4)}</b></div>
              <ul class="mini">{fd_html}</ul>
            </div>
          </div>
          <div class="h3" style="margin-top:10px;">To become OK</div>
          <ul class="mini">{''.join([f"<li>{x}</li>" for x in (gate.get("must_change", []) or [])[:4]]) or "<li class='muted'>—</li>"}</ul>
        </div>
        """

    trump_items = "".join([
        f"<li><b>{x.get('source')}</b> <span class='muted'>age={x.get('age_min')}m</span> — <a href='{x.get('link')}' target='_blank' rel='noopener'>{x.get('title')}</a></li>"
        for x in (trump.get("items") or [])[:6]
    ]) or "<li class='muted'>—</li>"

    upcoming = "".join([
        f"<li><a href='{x.get('link')}' target='_blank' rel='noopener'>{x.get('title')}</a> <span class='muted'>({('in '+str(x.get('in_hours'))+'h') if x.get('in_hours') is not None else 'time unknown'})</span></li>"
        for x in (risk.get("upcoming_events") or [])[:6]
    ]) or "<li class='muted'>—</li>"

    fred_line = ""
    if fred.get("ok"):
        # show a few key series
        pick = ["DFII10","DGS10","DTWEXBGS","VIXCLS","DCOILWTICO"]
        parts = []
        for sid in pick:
            sd = (fred.get("series", {}) or {}).get(sid)
            if not sd:
                continue
            parts.append(f"{sid}: {sd.get('last')} (d1 {sd.get('d1')}, d5 {sd.get('d5')})")
        fred_line = " • ".join(parts) if parts else ""
    else:
        fred_line = f"FRED disabled/error: {fred.get('error','')}".strip()

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Morning Plan</title>
      <style>
        :root {{
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.08);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b; --warn:#f59e0b;
          --ok:#22c55e; --no:#ef4444;
          --link:#7dd3fc;
        }}
        body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;margin:0;padding:14px;}}
        a{{color:var(--link);text-decoration:none;}} a:hover{{text-decoration:underline;}}
        .wrap{{max-width:1100px;margin:0 auto;}}
        .top{{display:flex;justify-content:space-between;gap:12px;align-items:flex-end;}}
        .muted{{color:var(--muted);}} .h1{{font-size:22px;font-weight:900;margin:0;}}
        .card{{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px;margin:12px 0;}}
        .h2{{font-size:16px;font-weight:900;display:flex;gap:8px;flex-wrap:wrap;align-items:center;}}
        .h3{{font-size:13px;color:#cfe0ff;font-weight:900;margin:10px 0 8px;}}
        .pill{{padding:4px 10px;border-radius:999px;font-size:12px;font-weight:900;border:1px solid var(--line);}}
        .pill-bull{{background:rgba(16,185,129,.14);color:var(--bull);border-color:rgba(16,185,129,.30);}}
        .pill-bear{{background:rgba(239,68,68,.14);color:var(--bear);border-color:rgba(239,68,68,.30);}}
        .pill-neutral{{background:rgba(100,116,139,.14);color:#cbd5e1;border-color:rgba(100,116,139,.30);}}
        .pill-ok{{background:rgba(34,197,94,.14);color:var(--ok);border-color:rgba(34,197,94,.30);}}
        .pill-no{{background:rgba(239,68,68,.14);color:var(--no);border-color:rgba(239,68,68,.30);}}
        .pill-warn{{background:rgba(245,158,11,.14);color:var(--warn);border-color:rgba(245,158,11,.30);}}
        .grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:10px;}}
        .box{{background:rgba(255,255,255,.03);border:1px solid var(--line);border-radius:14px;padding:12px;}}
        .mini{{margin:0;padding-left:18px;}} .mini li{{margin:6px 0;}}
        @media(max-width: 880px){{.grid{{grid-template-columns:1fr;}}}}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <div>
            <div class="h1">Morning Plan</div>
            <div class="muted">updated_utc: {updated} • gate_profile: <b>{gate_profile}</b></div>
            <div class="muted" style="margin-top:6px;">
              risk: {('EVENT_MODE=ON' if event_on else 'EVENT_MODE=OFF')} • {('TRUMP_HEADLINES=ON' if trump_on else 'TRUMP_HEADLINES=OFF')}
            </div>
            <div class="muted" style="margin-top:6px;">FRED: {fred_line}</div>
          </div>
          <div class="muted" style="text-align:right;">
            <div><a href="/dashboard">Dashboard</a> • <a href="/bias?pretty=1">JSON</a></div>
            <div style="margin-top:6px;">Run: <a href="#" onclick="fetch('/run',{{method:'POST'}}).then(()=>location.reload());return false;">/run</a></div>
          </div>
        </div>

        <div class="card">
          <div class="h2">Risk flags</div>
          <div class="h3">Trump headlines (recent)</div>
          <ul class="mini">{trump_items}</ul>
          <div class="h3" style="margin-top:10px;">Upcoming events</div>
          <ul class="mini">{upcoming}</ul>
        </div>

        {rows_html}

      </div>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ============================================================
# DASHBOARD (keep your existing visual style if you want)
# Minimal: redirect users to /morning_plan for "report"
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    # If you want to keep your huge dashboard HTML, просто вставь сюда.
    # Сейчас — быстрый хаб.
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()
    updated = payload.get("updated_utc", "")
    return HTMLResponse(f"""
    <html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
    <title>News Bias</title></head>
    <body style="font-family:Arial;padding:16px;">
      <h2>News Bias</h2>
      <div>updated_utc: {updated}</div>
      <ul>
        <li><a href="/morning_plan">/morning_plan</a> (утренний репорт)</li>
        <li><a href="/bias?pretty=1">/bias</a></li>
        <li><a href="/latest">/latest</a></li>
        <li><a href="/feeds_health">/feeds_health</a></li>
      </ul>
      <form action="/run" method="post">
        <button type="submit">Run now</button>
      </form>
    </body></html>
    """)

if __name__ == "__main__":
    # Run via: uvicorn this_file_name:app --host 0.0.0.0 --port 8000
    pass
