import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse


# ============================================================
# CONFIG
# ============================================================

ASSETS = ["XAU", "US500", "WTI"]

# --- Gate profile: STRICT (default) or MODERATE
GATE_PROFILE = os.environ.get("GATE_PROFILE", "STRICT").strip().upper()
if GATE_PROFILE not in ("STRICT", "MODERATE"):
    GATE_PROFILE = "STRICT"

# Strict/Moderate gate thresholds
GATE_THRESHOLDS = {
    "STRICT": {
        "quality_v2_min": 55,
        "conflict_max": 0.55,
        "min_opp_flip_dist": 0.35,   # distance to opposite bias must be >= this
        "neutral_allow": False,      # neutral bias => NO TRADE (strict)
        "event_mode_block": True,
        "event_override_quality": 70,
        "event_override_conflict": 0.45,
    },
    "MODERATE": {
        "quality_v2_min": 42,
        "conflict_max": 0.70,
        "min_opp_flip_dist": 0.20,
        "neutral_allow": True,       # allow near-threshold neutrals
        "neutral_flip_dist_max": 0.20,
        "event_mode_block": False,   # not hard block, but still penalize
        "event_override_quality": 60,
        "event_override_conflict": 0.60,
    }
}

# ============================================================
# RSS FEEDS
# ============================================================

RSS_FEEDS: Dict[str, str] = {
    # Macro / Rates / USD drivers
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/bls_latest.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",

    # Energy / WTI
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
    "OILPRICE": "https://oilprice.com/rss/main",

    # Broad market headlines
    "INVESTING_NEWS_25": "https://www.investing.com/rss/news_25.rss",

    # Investing.com additional feeds (you requested)
    "INVESTING_STOCK_FUND": "https://www.investing.com/rss/stock_Fundamental.rss",
    "INVESTING_COMMOD_TECH": "https://www.investing.com/rss/commodities_Technical.rss",
    "INVESTING_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INVESTING_NEWS_95": "https://www.investing.com/rss/news_95.rss",
    "INVESTING_MKT_TECH": "https://www.investing.com/rss/market_overview_Technical.rss",
    "INVESTING_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INVESTING_MKT_IDEAS": "https://www.investing.com/rss/market_overview_investing_ideas.rss",
    "INVESTING_FX_TECH": "https://www.investing.com/rss/forex_Technical.rss",
    "INVESTING_FX_FUND": "https://www.investing.com/rss/forex_Fundamental.rss",

    # FXStreet
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "FXSTREET_STOCKS": "https://www.fxstreet.com/rss/stocks",

    # MarketWatch
    "MARKETWATCH_TOP_STORIES": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REAL_TIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "MARKETWATCH_BREAKING": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
    "MARKETWATCH_MARKETPULSE": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",

    # Financial Times (may be restricted)
    "FT_PRECIOUS_METALS": "https://www.ft.com/precious-metals?format=rss",

    # DailyForex
    "DAILYFOREX_NEWS": "https://www.dailyforex.com/rss/forexnews.xml",
    "DAILYFOREX_TECH": "https://www.dailyforex.com/rss/technicalanalysis.xml",
    "DAILYFOREX_FUND": "https://www.dailyforex.com/rss/fundamentalanalysis.xml",

    # Your extra RSS
    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",

    # ForexFactory calendar (faireconomy mirror)
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml?version=51c52bdf678435acb58756461c1e8226",

    # Trump / politics RSS (you asked)
    "POLITICO_TRUMP": "https://rss.politico.com/donald-trump.xml",
}

# ============================================================
# SOURCE WEIGHTS
# ============================================================

SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.5,
    "EIA": 3.0,

    "OILPRICE": 1.2,

    # Investing feeds
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

    "MARKETWATCH_TOP_STORIES": 1.6,
    "MARKETWATCH_REAL_TIME": 1.6,
    "MARKETWATCH_BREAKING": 1.8,
    "MARKETWATCH_MARKETPULSE": 1.5,

    "FT_PRECIOUS_METALS": 1.6,

    "DAILYFOREX_NEWS": 1.2,
    "DAILYFOREX_TECH": 1.0,
    "DAILYFOREX_FUND": 1.2,

    "RSSAPP_1": 1.0,
    "RSSAPP_2": 1.0,

    "FOREXFACTORY_CALENDAR": 2.0,
    "POLITICO_TRUMP": 1.2,
}

# ============================================================
# DECAY + THRESHOLDS
# ============================================================

HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

# ============================================================
# RULES: regex patterns -> signed weight + explanation
# ============================================================

RULES: Dict[str, List[Tuple[str, float, str]]] = {
    "US500": [
        (r"\b(upgrades?|upgrade|raises? (price )?target|rebound|stabilize|demand lifts|beats?|strong (results|earnings))\b",
         +1.0, "Equity positive tone"),
        (r"\b(plunges?|rout|slides?|downgrade|downgrades?|cuts? .* to (neutral|sell)|writedowns?|bill for .* pullback|warns?|wary)\b",
         -1.2, "Equity negative tone"),
        (r"\b(unemployment|jobs|payrolls|cpi|inflation|fomc|federal reserve|fed)\b",
         -0.2, "Macro event headline (direction unknown)"),
        (r"\b(ai capex|capex)\b",
         -0.1, "Capex / valuation uncertainty"),
    ],
    "XAU": [
        (r"\b(wall st|wall street|futures|s&p|spx|nasdaq|dow|treasur(y|ies)|yields?|vix|risk[- ]off)\b.*\b(rout|plunges?|slides?|sell[- ]off|wary)\b"
         r"|\b(rout|plunges?|slides?|sell[- ]off)\b.*\b(wall st|futures|s&p|nasdaq|treasur(y|ies)|yields?|vix|risk[- ]off)\b",
         +0.9, "Market-wide risk-off supports gold"),
        (r"\b(rebound|risk[- ]on|stocks to buy|buy after .* drop|strong earnings)\b",
         -0.7, "Risk-on pressures gold"),
        (r"\b(fomc statement|fed issues.*statement|federal open market committee|longer[- ]run goals)\b",
         +0.2, "Fed event risk (watch yields/USD)"),
        (r"\b(strong dollar|usd strengthens|yields rise|real yields)\b",
         -0.8, "Stronger USD / higher yields weighs on gold"),
        # Geopolitical / safe-haven quick add
        (r"\b(geopolitical|safe[- ]haven|risk aversion|flight to safety)\b",
         +0.5, "Safe-haven demand supports gold"),
    ],
    "WTI": [
        (r"\b(crude oil|tanker rates|winter storm|disruption|outage|pipeline|opec|output cut|sanctions)\b",
         +0.9, "Supply / flow supports oil"),
        (r"\b(inventory draw|stocks fall|draw)\b",
         +0.8, "Inventories draw supports oil"),
        (r"\b(inventory build|stocks rise|build)\b",
         -0.8, "Inventories build pressures oil"),
        (r"\b(natural gas|electricity|nuclear|coal)\b",
         0.0, "Not crude-direct"),
        (r"\b(demand weakens|recession fears|slowdown)\b",
         -0.6, "Demand concerns pressure oil"),
    ],
}

# ============================================================
# EVENT MODE (macro calendar)
# ============================================================

EVENT_CFG = {
    "enabled": True,
    "lookahead_hours": int(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
    "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")),
    "max_upcoming": 6,
}

# Very lightweight parsing for event timestamps (best effort)
# Supports patterns like: "Feb 06 2026 13:30", "2026-02-06 13:30", etc.
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
}


# ============================================================
# DB
# ============================================================

def db_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(url)


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
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bias_state (
                id SMALLINT PRIMARY KEY DEFAULT 1,
                updated_ts BIGINT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """)
            conn.commit()


def norm_text(s: str) -> str:
    return (s or "").strip().lower()


def fingerprint(title: str, link: str) -> str:
    t = norm_text(title)[:200]
    l = (link or "").strip()[:200]
    return f"{t}||{l}"


# ============================================================
# FEEDS HEALTH (live parse)
# ============================================================

def feeds_health_live() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            entries = getattr(d, "entries", []) or []
            out[src] = {
                "ok": True,
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": len(entries),
                "bozo_exception": str(getattr(d, "bozo_exception", ""))[:240] if getattr(d, "bozo", 0) else "",
            }
        except Exception as e:
            out[src] = {"ok": False, "error": str(e)}
    return out


# ============================================================
# INGEST
# ============================================================

def ingest_once(limit_per_feed: int = 40) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                d = feedparser.parse(url)
                entries = getattr(d, "entries", []) or []

                # If bozo==1 but entries exist — still ingest (FED/OILPRICE often do this).
                # If bozo==1 and entries==0 — skip as truly broken.
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
                        # Some feeds omit published; use now.
                        published_ts = now

                    fp = fingerprint(title, link)

                    try:
                        cur.execute(
                            """
                            INSERT INTO news_items (source, title, link, published_ts, fingerprint)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (fingerprint) DO NOTHING;
                            """,
                            (src, title, link, published_ts, fp)
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                    except Exception:
                        conn.rollback()
                        continue

        conn.commit()

    return inserted


# ============================================================
# SCORING HELPERS
# ============================================================

def decay_weight(age_sec: int) -> float:
    return float(pow(2.718281828, -LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    tl = (text or "").lower()
    out = []
    for pattern, w, why in RULES.get(asset, []):
        if re.search(pattern, tl, flags=re.IGNORECASE):
            out.append({"pattern": pattern, "w": float(w), "why": why})
    return out


def _fresh_bucket(age_sec: int) -> str:
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"


def _flip_distances(score: float, th: float) -> Dict[str, float]:
    # "how much score delta needed" to reach bullish threshold or bearish threshold
    to_bullish = max(0.0, th - score)         # need to increase score by this
    to_bearish = max(0.0, score + th)         # need to decrease score by this (towards -th)
    return {"to_bullish": round(to_bullish, 4), "to_bearish": round(to_bearish, 4),
            "note": "Δ needed in score units (same scale as 'score')"}


def _consensus_stats(contribs: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    # consensus_ratio = |net| / sum_abs ; conflict_index = 1 - consensus_ratio
    net = sum(float(x.get("contrib", 0.0)) for x in contribs)
    abs_sum = sum(abs(float(x.get("contrib", 0.0))) for x in contribs)
    if abs_sum <= 1e-12:
        return 0.0, 1.0, 0.0
    consensus_ratio = abs(net) / abs_sum
    conflict_index = 1.0 - consensus_ratio
    return round(consensus_ratio, 4), round(conflict_index, 4), round(abs_sum, 4)


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
        out.append({
            "source": src,
            "net": round(v["net"], 4),
            "abs": round(v["abs"], 4),
            "count": int(cnt[src]),
        })
    out.sort(key=lambda x: x["abs"], reverse=True)
    return out


def _top_drivers(contribs: List[Dict[str, Any]], topn: int = 3) -> List[Dict[str, Any]]:
    # group by "why" label and sum abs contributions
    acc: Dict[str, float] = {}
    for x in contribs:
        why = str(x.get("why", ""))
        c = float(x.get("contrib", 0.0))
        acc[why] = acc.get(why, 0.0) + abs(c)
    out = [{"why": k, "abs_contrib_sum": round(v, 4)} for k, v in acc.items()]
    out.sort(key=lambda x: x["abs_contrib_sum"], reverse=True)
    return out[:topn]


def _macro_recent_flag(rows: List[Tuple[str, str, str, int]], now_ts: int) -> bool:
    # If any macro source headline published within recent_hours
    recent_sec = int(EVENT_CFG["recent_hours"] * 3600)
    macro_sources = {"FED", "BLS", "BEA"}
    for (source, _title, _link, ts) in rows:
        if source in macro_sources and (now_ts - int(ts)) <= recent_sec:
            return True
    return False


def _parse_event_time_best_effort(text: str) -> Optional[int]:
    """
    Best-effort parse from title/summary. Returns unix ts (UTC) or None.
    Works for some feeds; if not parseable we still show title in upcoming.
    """
    s = (text or "").strip()

    # 1) YYYY-MM-DD HH:MM
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})\b", s)
    if m:
        y, mo, da, hh, mm = map(int, m.groups())
        dt = datetime(y, mo, da, hh, mm, tzinfo=timezone.utc)
        return int(dt.timestamp())

    # 2) "Feb 06 2026 13:30"
    m = re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),?\s+(20\d{2})\s+(\d{1,2}):(\d{2})\b", s, flags=re.I)
    if m:
        mon_s, da, y, hh, mm = m.groups()
        mo = MONTHS.get(mon_s.lower()[:3], 0)
        if mo:
            dt = datetime(int(y), mo, int(da), int(hh), int(mm), tzinfo=timezone.utc)
            return int(dt.timestamp())

    return None


def _get_upcoming_events(now_ts: int) -> List[Dict[str, Any]]:
    """
    Pull upcoming events from the ForexFactory calendar RSS feed in-memory (live parse).
    We DON'T store calendar items in DB; we just use it to drive "event mode".
    """
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

        # Prefer published_parsed if present
        ts = None
        published_parsed = e.get("published_parsed")
        if published_parsed:
            ts = int(time.mktime(published_parsed))
        else:
            # best-effort from title
            ts = _parse_event_time_best_effort(title)

        # Only include if time is known and within lookahead; otherwise keep a few unknown-time items
        if ts is not None:
            if now_ts <= ts <= horizon:
                out.append({
                    "title": title,
                    "link": link,
                    "ts": int(ts),
                    "in_hours": round((ts - now_ts) / 3600.0, 2),
                })
        else:
            # unknown time – include but mark
            # keep limited; we don't want to spam
            if len([x for x in out if x.get("ts") is None]) < 2:
                out.append({
                    "title": title,
                    "link": link,
                    "ts": None,
                    "in_hours": None,
                })

        if len(out) >= EVENT_CFG["max_upcoming"]:
            break

    # sort known-time first
    out.sort(key=lambda x: (x["ts"] is None, x["ts"] or 0))
    return out


# ============================================================
# BIAS COMPUTE
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
                (cutoff, limit_rows)
            )
            rows: List[Tuple[str, str, str, int]] = cur.fetchall()

    # Event mode signals
    upcoming_events = _get_upcoming_events(now) if EVENT_CFG["enabled"] else []
    recent_macro = _macro_recent_flag(rows, now)

    event_mode = False
    if EVENT_CFG["enabled"]:
        # Event mode ON if: recent macro prints OR upcoming calendar events exist soon
        event_mode = bool(recent_macro or any(x.get("ts") is not None for x in upcoming_events))

    assets_out: Dict[str, Any] = {}

    for asset in ASSETS:
        score = 0.0
        contribs: List[Dict[str, Any]] = []  # all matched items with contrib
        freshness = {"0-2h": 0, "2-8h": 0, "8-24h": 0}

        for (source, title, link, ts) in rows:
            age_sec = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age_sec)

            matches = match_rules(asset, title)
            if not matches:
                continue

            bucket = _fresh_bucket(age_sec)
            # Count evidence per match, but buckets per headline (avoid overcount)
            freshness[bucket] += 1

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

        # --- Quality v1 (legacy-ish)
        strength = min(1.0, abs(score) / max(th, 1e-9))
        quality_v1 = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, src_div * 2.0)))

        # --- Consensus / conflict / v2 quality
        consensus_ratio, conflict_index, abs_sum = _consensus_stats(contribs)

        # Quality v2 idea:
        # - reward strength vs threshold
        # - reward evidence, but diminishing returns
        # - reward diversity
        # - penalize conflict heavily
        # - penalize low freshness concentration (optional mild)
        strength_2 = min(1.2, abs(score) / max(th, 1e-9))  # allow >1 if far beyond threshold
        ev_term = min(1.0, evidence_count / 18.0)
        div_term = min(1.0, src_div / 7.0)

        # freshness concentration: prefer having at least some 0-2h and 2-8h evidence
        fresh_total = sum(int(v) for v in freshness.values())
        fresh02 = freshness["0-2h"]
        fresh28 = freshness["2-8h"]
        fresh_score = 0.0
        if fresh_total > 0:
            fresh_score = min(1.0, (fresh02 * 1.0 + fresh28 * 0.6) / max(1.0, fresh_total))

        # event penalty (when event_mode ON)
        event_penalty = 0.15 if event_mode else 0.0

        raw_v2 = (
            0.45 * min(1.0, strength_2) +
            0.20 * ev_term +
            0.10 * div_term +
            0.10 * fresh_score +
            0.15 * consensus_ratio -   # if consensus strong, good
            0.35 * conflict_index -     # conflict is bad
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


# ============================================================
# STATE
# ============================================================

def save_bias(payload: dict):
    now = int(time.time())
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bias_state (id, updated_ts, payload_json)
                VALUES (1, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET updated_ts = EXCLUDED.updated_ts, payload_json = EXCLUDED.payload_json;
                """,
                (now, json.dumps(payload, ensure_ascii=False))
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

def eval_trade_gate(asset_obj: Dict[str, Any], event_block: bool, profile: str) -> Dict[str, Any]:
    """
    Returns gate verdict + reasons + "what must change".
    Gate is per-asset:
      Bias + Quality v2 + Event Mode + Conflict threshold + distance to flip guard.
    """
    cfg = GATE_THRESHOLDS[profile]

    bias = str(asset_obj.get("bias", "NEUTRAL"))
    score = float(asset_obj.get("score", 0.0))
    th = float(asset_obj.get("threshold", 1.0))
    q2 = int(asset_obj.get("quality_v2", 0))
    conflict = float(asset_obj.get("conflict_index", 1.0))
    flip = asset_obj.get("flip", {}) or {}
    to_bull = float(flip.get("to_bullish", max(0.0, th - score)))
    to_bear = float(flip.get("to_bearish", max(0.0, score + th)))

    # Distance to opposite bias:
    # If bullish, distance to bearish is "to_bearish" + (something)? Actually to reach -th from current score needs delta=score+th => to_bear
    # If bearish, distance to bullish is th-score => to_bull
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
        must_change.append(f"score must move to reach threshold: min(to_bullish,to_bearish) < {cfg.get('neutral_flip_dist_max', 0.20) if cfg.get('neutral_allow') else '—'} (or become BULLISH/BEARISH)")
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

    # Opp flip distance guard (avoid standing on knife-edge)
    mind = float(cfg["min_opp_flip_dist"])
    if bias in ("BULLISH", "BEARISH"):
        if opp_dist < mind:
            fail_reasons.append(f"Too close to opposite flip ({opp_label}={round(opp_dist,4)} < {mind})")
            must_change.append(f"{opp_label} must be ≥ {mind}")

    # Event mode gate
    if event_block:
        if cfg.get("event_mode_block", True):
            # Hard block unless override conditions are met
            oq = int(cfg.get("event_override_quality", 70))
            oc = float(cfg.get("event_override_conflict", 0.45))
            if not (q2 >= oq and conflict <= oc and bias != "NEUTRAL"):
                fail_reasons.append("Event mode ON (macro risk window)")
                must_change.append(f"Either wait until event_mode=OFF, or require quality_v2 ≥ {oq} and conflict_index ≤ {oc} (and bias != NEUTRAL).")

    ok = (len(fail_reasons) == 0)

    # WHY (2–3) from top drivers
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

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/health")
def health():
    return {"ok": True, "gate_profile": GATE_PROFILE}


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


@app.get("/latest")
def latest(limit: int = 40):
    db_init()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT %s;
                """,
                (limit,)
            )
            rows = cur.fetchall()
    return {
        "items": [
            {"source": s, "title": t, "link": l, "published_ts": int(ts)}
            for (s, t, l, ts) in rows
        ]
    }


@app.get("/explain")
def explain(asset: str = "US500", limit: int = 60):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return {"error": f"asset must be one of {ASSETS}"}

    now = int(time.time())
    cutoff = now - 24 * 3600

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
                (cutoff,)
            )
            rows = cur.fetchall()

    out = []
    for (source, title, link, ts) in rows:
        matches = match_rules(asset, title)
        if not matches:
            continue
        age = now - int(ts)
        w_src = SOURCE_WEIGHT.get(source, 1.0)
        w_time = decay_weight(age)
        for m in matches:
            contrib = m["w"] * w_src * w_time
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
            })

    out_sorted = sorted(out, key=lambda x: abs(float(x["contrib"])), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}


@app.get("/feeds_health")
def feeds_health():
    return feeds_health_live()


# ============================================================
# DASHBOARD UI
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
    # Tap friendly: uses data-tip and CSS + JS to show on tap
    return f"""
    <span class="tipwrap">
      <span class="tipicon" tabindex="0" role="button" aria-label="{label}" data-tip="{text}">ⓘ</span>
    </span>
    """


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(compact: int = 0):
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {}) or {}
    updated = payload.get("updated_utc", "")
    feeds_status = (payload.get("meta", {}) or {}).get("feeds_status", {}) or {}
    gate_profile = (payload.get("meta", {}) or {}).get("gate_profile", GATE_PROFILE)

    ev = payload.get("event", {}) or {}
    event_mode = bool(ev.get("event_mode", False))
    upcoming = ev.get("upcoming_events", []) or []

    # Pull last headlines (raw) to display per-asset relevant list
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 120;
            """)
            rows = cur.fetchall()

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
        top3 = a.get("top3_drivers", []) or []
        cons_by_src = a.get("consensus_by_source", []) or []
        why_top5 = a.get("why_top5", []) or []

        gate = eval_trade_gate(a, event_mode, gate_profile)

        # WHY list
        why_html = ""
        for w in why_top5[:5]:
            why_html += f"""
            <li>
              <div class="why-row">
                <div class="why-title"><b>{w.get('why','')}</b></div>
                <div class="why-meta">contrib={w.get('contrib','')}, src={w.get('source','')}, age={w.get('age_min','')}m</div>
              </div>
              <div class="why-headline">{w.get('title','')}</div>
              <a class="tiny" href="{w.get('link','')}" target="_blank" rel="noopener">open</a>
            </li>
            """

        # relevant headlines
        kw = {
            "XAU": ["gold", "xau", "fed", "fomc", "cpi", "inflation", "yields", "usd", "risk", "treasury", "safe-haven"],
            "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "rout", "slides", "rebound", "s&p", "nasdaq", "equities"],
            "WTI": ["oil", "crude", "wti", "opec", "inventory", "stocks", "tanker", "pipeline", "storm", "spr", "output"],
        }[asset]

        news_html = ""
        shown = 0
        for (source, title, link, _ts) in rows:
            t = (title or "").lower()
            if not any(k in t for k in kw):
                continue
            shown += 1
            if shown > (6 if compact else 10):
                break
            news_html += f'<li><a href="{link}" target="_blank" rel="noopener">{title}</a> <span class="muted">[{source}]</span></li>'

        # top drivers
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

        # consensus by source table
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

        # flip
        to_bull = float(flip.get("to_bullish", 0.0))
        to_bear = float(flip.get("to_bearish", 0.0))

        # Gate WHY + must-change
        gate_why = "".join([f"<li>{x}</li>" for x in (gate.get("why", []) or [])[:3]]) or "<li>—</li>"
        gate_need = "".join([f"<li>{x}</li>" for x in (gate.get("must_change", []) or [])[:3]]) or "<li>—</li>"

        # Signal quality tooltips
        tip_q1 = _tooltip("Quality v1", "v1 = strength vs threshold + evidence count + source diversity (simple).")
        tip_q2 = _tooltip("Quality v2", "v2 = strength + evidence + diversity + freshness + consensus − conflict − (event penalty).")
        tip_conf = _tooltip("Conflict", "conflict_index = 1 - consensus_ratio. High conflict = sources fighting each other (unstable bias).")
        tip_cons = _tooltip("Consensus", "consensus_ratio = |net| / sum_abs. Higher = agreement in direction.")
        tip_flip = _tooltip("Flip", "Δ needed for score to reach bullish (+th) or bearish (-th) threshold.")

        # Trade gate tooltip
        tip_gate = _tooltip("Trade Gate", f"Gate = Bias + Quality v2 + Conflict + Event Mode + Flip guard. Profile: {gate_profile}.")

        # Card layout
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
              <div class="muted tiny" style="margin-bottom:6px;">Quick actions</div>
              <div class="btnrow">
                <button class="btn" onclick="runNow('{asset}')">Run now</button>
                <button class="btn" onclick="showJson()">JSON</button>
                <button class="btn" onclick="showExplain('{asset}')">Explain</button>
              </div>
              <div class="tiny muted" id="status-{asset}" style="margin-top:6px;"></div>
            </div>
          </div>

          <div class="grid3">
            <div class="panel">
              <div class="h3">Signal Quality</div>
              <div class="qblock">
                <div class="qrow">
                  <div class="muted tiny">v1 {tip_q1}</div>
                  <div class="qval">{q1}/100</div>
                </div>
                {_bar(q1)}
              </div>
              <div class="qblock">
                <div class="qrow">
                  <div class="muted tiny">v2 {tip_q2}</div>
                  <div class="qval">{q2}/100</div>
                </div>
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
                <div class="flipcard">
                  <div class="muted tiny">to bullish</div>
                  <div class="flipnum">{to_bull}</div>
                </div>
                <div class="flipcard">
                  <div class="muted tiny">to bearish</div>
                  <div class="flipnum">{to_bear}</div>
                </div>
              </div>
              <div class="h3" style="margin-top:12px;">Freshness buckets</div>
              <div class="flipgrid">
                <div class="flipcard"><div class="muted tiny">0-2h</div><div class="flipnum">{freshness.get("0-2h",0)}</div></div>
                <div class="flipcard"><div class="muted tiny">2-8h</div><div class="flipnum">{freshness.get("2-8h",0)}</div></div>
                <div class="flipcard"><div class="muted tiny">8-24h</div><div class="flipnum">{freshness.get("8-24h",0)}</div></div>
              </div>
            </div>
          </div>

          <div class="grid2">
            <div class="panel">
              <div class="h3">Top 3 drivers now</div>
              <div class="td">{td_html}</div>

              <div class="h3" style="margin-top:12px;">Consensus by source</div>
              <table class="ctable">
                <thead>
                  <tr><th>Source</th><th>Net</th><th>Abs</th><th>n</th></tr>
                </thead>
                <tbody>{cs_rows}</tbody>
              </table>
            </div>

            <div class="panel">
              <div class="h3">WHY (top 5)</div>
              <ol class="why">{why_html or "<li>—</li>"}</ol>

              <div class="h3" style="margin-top:12px;">Latest relevant headlines</div>
              <ul class="news">{news_html or "<li>—</li>"}</ul>
            </div>
          </div>
        </section>
        """

    # Feeds status table
    feeds_rows = ""
    for src in RSS_FEEDS.keys():
        st = feeds_status.get(src, {})
        ok = bool(st.get("ok", False))
        bozo = st.get("bozo", "")
        entries = st.get("entries", "")
        icon = "✅" if ok and int(bozo or 0) == 0 else ("⚠️" if ok else "❌")
        feeds_rows += f"""
        <tr>
          <td>{icon} {src}</td>
          <td class="muted">bozo={bozo}</td>
          <td class="muted">entries={entries}</td>
        </tr>
        """

    # Event panel
    ev_html = ""
    if event_mode:
        ev_html += '<div class="pill pill-warn">⚠️ EVENT MODE: ON</div>'
    else:
        ev_html += '<div class="pill pill-ok">✅ EVENT MODE: OFF</div>'

    ev_html += f"""
      <div class="muted tiny" style="margin-top:8px;">
        recent_macro={bool(ev.get("recent_macro", False))} • lookahead={ev.get("lookahead_hours","")}h
      </div>
    """

    up_html = ""
    for x in upcoming[:6]:
        t = x.get("title", "")
        link = x.get("link", "")
        inh = x.get("in_hours", None)
        tag = f"in {inh}h" if inh is not None else "time unknown"
        up_html += f'<li><a href="{link}" target="_blank" rel="noopener">{t}</a> <span class="muted tiny">({tag})</span></li>'
    if not up_html:
        up_html = "<li class='muted'>—</li>"

    compact_on = "true" if compact else "false"

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
      <title>News Bias Dashboard</title>
      <style>
        :root {{
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.08);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b;
          --btn:#1b2636; --btn2:#223047;
          --ok:#22c55e; --no:#ef4444; --warn:#f59e0b;
        }}
        html, body {{ height:100%; }}
        body {{
          font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
          background:var(--bg); color:var(--text); margin:0;
          padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
        }}
        .wrap {{ max-width: 1120px; margin: 18px auto; padding: 0 14px; }}
        .top {{
          display:flex; justify-content:space-between; align-items:flex-end; gap:12px;
          position: sticky; top: 0; background: rgba(11,15,23,.78); backdrop-filter: blur(8px);
          padding: 12px 0; z-index: 10; border-bottom: 1px solid var(--line);
        }}
        h1 {{ margin:0; font-size: 22px; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ font-size:12px; }}
        .spacer {{ display:inline-block; width: 10px; }}
        .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:14px; margin: 14px 0; }}
        .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
        .head-left {{ flex:1; min-width: 240px; }}
        .h2 {{ font-size:18px; font-weight:800; line-height: 1.25; display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
        .h3 {{ font-size:13px; margin: 0 0 10px 0; color:#cfe0ff; font-weight:800; }}
        .sub {{ margin-top:6px; }}
        .pill {{ padding:4px 10px; border-radius:999px; font-size:12px; font-weight:800; border:1px solid var(--line); }}
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
          font-weight:800;
          min-height: 40px;
        }}
        .btn:hover {{ background:var(--btn2); }}
        .grid2 {{ display:grid; grid-template-columns: 1fr 1fr; gap:14px; margin-top: 14px; }}
        .grid3 {{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap:14px; margin-top: 14px; }}
        .panel {{ background: rgba(255,255,255,.03); border:1px solid var(--line); border-radius:16px; padding:12px; }}
        @media(max-width: 980px) {{
          .grid3 {{ grid-template-columns: 1fr; }}
          .grid2 {{ grid-template-columns: 1fr; }}
          .card-head {{ flex-direction: column; }}
          .actions {{ width: 100%; text-align:left; min-width: auto; }}
          .btnrow {{ justify-content:flex-start; }}
        }}
        .why, .news {{ margin:0; padding-left:18px; }}
        .why li {{ margin: 10px 0; }}
        .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
        .why-meta {{ color:var(--muted); font-size:12px; }}
        .why-headline {{ margin-top:4px; }}
        a {{ color:#7dd3fc; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .bar {{ height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; }}
        .bar-fill {{ height:10px; background: linear-gradient(90deg, rgba(34,197,94,.9), rgba(245,158,11,.9), rgba(239,68,68,.9)); }}
        .bar-num {{ margin-top:6px; font-weight:800; }}
        .qblock {{ margin-bottom: 10px; }}
        .qrow {{ display:flex; justify-content:space-between; align-items:baseline; gap:10px; }}
        .qval {{ font-weight:900; }}
        .flipgrid {{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10px; }}
        .flipcard {{ background: rgba(255,255,255,.03); border:1px solid var(--line); border-radius:14px; padding:10px; }}
        .flipnum {{ font-weight:900; font-size: 18px; }}
        .td-row {{ display:flex; justify-content:space-between; gap:10px; margin: 8px 0; }}
        .ctable {{ width:100%; border-collapse:collapse; margin-top: 10px; }}
        .ctable th, .ctable td {{ padding:8px 6px; border-top:1px solid var(--line); text-align:left; font-size: 13px; }}
        .pos {{ color: var(--ok); font-weight:800; }}
        .neg {{ color: var(--no); font-weight:800; }}
        .mini {{ margin: 6px 0 0 0; padding-left: 18px; }}
        .mini li {{ margin: 6px 0; }}
        .modal {{
          position:fixed; inset:0; background:rgba(0,0,0,.6);
          display:none; align-items:center; justify-content:center; padding:16px;
        }}
        .modal.show {{ display:flex; }}
        .modal-box {{
          width:min(980px, 100%); max-height: 85vh; overflow:auto;
          background:var(--card); border:1px solid var(--line); border-radius:18px; padding:14px;
        }}
        .modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; position: sticky; top: 0; background: rgba(18,26,38,.95); padding-bottom: 10px; }}
        pre {{ background:rgba(255,255,255,.06); padding:12px; border-radius:12px; overflow:auto; }}
        .table {{ width:100%; border-collapse:collapse; }}
        .table td {{ border-top:1px solid var(--line); padding:8px 6px; }}

        /* Tooltips (tap/hover) */
        .tipwrap {{ position: relative; display:inline-block; }}
        .tipicon {{
          display:inline-flex; align-items:center; justify-content:center;
          width: 18px; height: 18px; border-radius: 999px;
          border:1px solid var(--line); color: var(--muted);
          font-size: 12px; font-weight: 900;
          cursor: pointer; user-select: none;
        }}
        .tipicon:focus {{ outline: 2px solid rgba(125,211,252,.35); outline-offset: 2px; }}
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
        .compact .news li:nth-child(n+7) {{ display:none; }}
      </style>
    </head>
    <body class="{ 'compact' if compact else '' }" data-compact="{compact_on}">
      <div class="wrap">
        <div class="top">
          <div>
            <h1>News Bias Dashboard</h1>
            <div class="muted tiny">updated_utc: {updated} • gate_profile: <b>{gate_profile}</b></div>
            <div class="muted tiny" style="margin-top:6px;">{ev_html}</div>
          </div>
          <div class="muted tiny" style="text-align:right;">
            Tip: open <b>/</b> → redirects here. Use your domain as the short link.<br/>
            <button class="btn" onclick="toggleCompact()" style="margin-top:8px;">Toggle Compact</button>
          </div>
        </div>

        <section class="card">
          <div class="h3">Macro calendar (next)</div>
          <ul class="news">{up_html}</ul>
          <div class="muted tiny" style="margin-top:10px;">Event mode affects Trade Gate (especially STRICT).</div>
        </section>

        {render_asset("XAU")}
        {render_asset("US500")}
        {render_asset("WTI")}

        <section class="card">
          <div class="h3">Feeds health (last run snapshot)</div>
          <table class="table"><tbody>{feeds_rows}</tbody></table>
          <div class="muted tiny" style="margin-top:10px;">
            Deep debug: <a href="/feeds_health" target="_blank" rel="noopener">/feeds_health</a> (live parse now)
          </div>
        </section>
      </div>

      <!-- MODAL -->
      <div class="modal" id="modal" onclick="modalBackdrop(event)">
        <div class="modal-box" onclick="event.stopPropagation()">
          <div class="modal-head">
            <div class="h2" id="modal-title">Modal</div>
            <button class="btn" onclick="closeModal()">Close</button>
          </div>
          <div id="modal-body" style="margin-top:10px;"></div>
        </div>
      </div>

      <script>
        // Compact mode (persist)
        const compactKey = 'nb_compact';
        function setCompact(on) {{
          document.body.classList.toggle('compact', !!on);
          document.body.dataset.compact = on ? 'true' : 'false';
          try {{ localStorage.setItem(compactKey, on ? '1' : '0'); }} catch(e) {{}}
        }}
        function getCompact() {{
          try {{ return (localStorage.getItem(compactKey) || '{1 if compact else 0}') === '1'; }} catch(e) {{ return {compact_on}; }}
        }}
        function toggleCompact() {{
          setCompact(!getCompact());
        }}
        setCompact(getCompact());

        function openModal(title, bodyHtml) {{
          document.getElementById('modal-title').innerText = title;
          document.getElementById('modal-body').innerHTML = bodyHtml;
          document.getElementById('modal').classList.add('show');
        }}
        function closeModal() {{
          document.getElementById('modal').classList.remove('show');
        }}
        function modalBackdrop(e) {{
          // clicking on backdrop closes
          closeModal();
        }}

        async function runNow(asset) {{
          const el = document.getElementById('status-' + asset);
          if (el) el.innerText = 'Running...';
          try {{
            const resp = await fetch('/run', {{ method:'POST' }});
            const data = await resp.json();
            if (el) el.innerText = 'Updated: ' + (data.updated_utc || '');
            setTimeout(() => window.location.reload(), 350);
          }} catch(e) {{
            if (el) el.innerText = 'Error: ' + e;
          }}
        }}

        async function showJson() {{
          try {{
            const resp = await fetch('/bias?pretty=1');
            const txt = await resp.text();
            openModal('JSON (pretty)', '<pre>' + escapeHtml(txt) + '</pre>');
          }} catch(e) {{
            openModal('JSON', '<div class="muted">Error: ' + e + '</div>');
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
                </td>
                <td style="padding:8px;border-top:1px solid rgba(255,255,255,.08);">
                  <a href="${{x.link}}" target="_blank" rel="noopener">${{escapeHtml(x.title || '')}}</a>
                </td>
              </tr>`;
            }}).join('');
            const html = `
              <div class="muted tiny">rules_count=${{data.rules_count}} • items=${{(data.top_matches||[]).length}}</div>
              <table style="width:100%;border-collapse:collapse;margin-top:10px;">
                <tbody>${{rows || '<tr><td class="muted">—</td></tr>'}}</tbody>
              </table>
            `;
            openModal('Explain ' + asset, html);
          }} catch(e) {{
            openModal('Explain ' + asset, '<div class="muted">Error: ' + e + '</div>');
          }}
        }}

        function escapeHtml(unsafe) {{
          return (unsafe || '').replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
        }}

        // Tooltips: tap/hover
        function hideAllTips() {{
          document.querySelectorAll('.tipbubble').forEach(x => x.remove());
        }}
        function showTipFor(el) {{
          hideAllTips();
          const txt = el.getAttribute('data-tip') || '';
          const b = document.createElement('div');
          b.className = 'tipbubble show';
          b.innerText = txt;
          el.parentElement.appendChild(b);
          // auto close after a while (mobile friendly)
          setTimeout(() => {{
            try {{ b.remove(); }} catch(e) {{}}
          }}, 4200);
        }}
        document.addEventListener('click', (e) => {{
          const t = e.target;
          if (t && t.classList && t.classList.contains('tipicon')) {{
            e.preventDefault();
            showTipFor(t);
          }} else {{
            // click outside closes tips
            hideAllTips();
          }}
        }});
      </script>
    </body>
    </html>
    """
    return html
