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

RSS_FEEDS = {
    # Macro / Rates / USD drivers
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/bls_latest.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",

    # Energy / WTI
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
    "OILPRICE": "https://oilprice.com/rss/main",

    # Broad market headlines
    "INVESTING": "https://www.investing.com/rss/news_25.rss",

    # FXStreet
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "FXSTREET_STOCKS": "https://www.fxstreet.com/rss/stocks",

    # MarketWatch (valid RSS feeds)
    "MARKETWATCH_TOP_STORIES": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REAL_TIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "MARKETWATCH_BREAKING": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
    "MARKETWATCH_MARKETPULSE": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",

    # Financial Times (may be restricted; ok to try)
    "FT_PRECIOUS_METALS": "https://www.ft.com/precious-metals?format=rss",

    # DailyForex (RSS)
    "DAILYFOREX_NEWS": "https://www.dailyforex.com/rss/forexnews.xml",
    "DAILYFOREX_TECH": "https://www.dailyforex.com/rss/technicalanalysis.xml",
    "DAILYFOREX_FUND": "https://www.dailyforex.com/rss/fundamentalanalysis.xml",

    # --- NEW: your feeds ---
    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",

    # ForexFactory calendar weekly feed (proxy feed)
    "FF_CALENDAR_WEEK": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml?version=51c52bdf678435acb58756461c1e8226&utm_source=chatgpt.com",

    # Trump / politics
    "POLITICO_TRUMP": "https://rss.politico.com/donald-trump.xml",
}

# impact weights by source (MVP heuristic)
SOURCE_WEIGHT = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.5,
    "EIA": 3.0,

    "INVESTING": 1.0,
    "OILPRICE": 1.2,

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

    # new feeds - keep neutral-ish until you calibrate
    "RSSAPP_1": 1.1,
    "RSSAPP_2": 1.1,
    "FF_CALENDAR_WEEK": 2.2,    # calendar is important for "event mode"
    "POLITICO_TRUMP": 1.3,
}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

# Bias thresholds (per-asset)
BIAS_THRESH = {
    "US500": 1.2,
    "XAU":   0.9,
    "WTI":   0.9,
}

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
         r"|\b(rout|plunges?|slides?|sell[- ]off)\b.*\b(wall st|wall street|futures|s&p|spx|nasdaq|dow|treasur(y|ies)|yields?|vix|risk[- ]off)\b",
         +0.9, "Market-wide risk-off supports gold"),
        (r"\b(rebound|risk[- ]on|stocks to buy|buy after .* drop|strong earnings)\b",
         -0.7, "Risk-on pressures gold"),
        (r"\b(fomc statement|fed issues.*statement|federal open market committee|longer[- ]run goals)\b",
         +0.2, "Fed event risk (watch yields/USD)"),
        (r"\b(strong dollar|usd strengthens|yields rise|real yields)\b",
         -0.8, "Stronger USD / higher yields weighs on gold"),
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
    ],
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

def ingest_once(limit_per_feed: int = 30) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                d = feedparser.parse(url)
                entries = getattr(d, "entries", []) or []

                # bozo==1 but entries exist -> still ingest
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
                        # Some feeds (calendar) may not have published dates reliably
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


def freshness_bucket(age_sec: int) -> str:
    h = age_sec / 3600.0
    if h <= 2.0:
        return "0-2h"
    if h <= 8.0:
        return "2-8h"
    return "8-24h"


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


# ============================================================
# EVENT MODE (ForexFactory calendar feed MVP parse)
# ============================================================

HIGH_IMPACT_MARKERS = [
    "high impact", "impact: high", "★★★", "high",
    "fomc", "cpi", "ppi", "nfp", "non-farm", "payroll",
    "unemployment", "jobs", "gdp", "retail sales",
    "ism", "pmi", "rate decision", "fed", "powell",
]

def extract_calendar_events(rows_24h: List[Tuple[str, str, str, int]],
                            lookahead_hours: float = 18.0,
                            recent_hours: float = 6.0) -> Dict[str, Any]:
    """
    MVP:
    - We treat FF_CALENDAR_WEEK items as "events".
    - We don't have structured timestamps, so we approximate by published_ts (ingest timestamp).
    - We'll flag "high impact" using text markers in title/summary (we only have title stored).
    """
    now = int(time.time())
    lookahead_sec = int(lookahead_hours * 3600)
    recent_sec = int(recent_hours * 3600)

    upcoming: List[Dict[str, Any]] = []
    recent_macro = False

    for (source, title, link, ts) in rows_24h:
        if source != "FF_CALENDAR_WEEK":
            continue

        t = (title or "").lower()
        is_high = any(m in t for m in HIGH_IMPACT_MARKERS)

        age = now - int(ts)

        # "recent" macro: within recent_hours (published)
        if is_high and age <= recent_sec:
            recent_macro = True

        # "upcoming" is hard without true event time; we approximate:
        # if item is fresh (<= lookahead) we show it as "upcoming-ish"
        if is_high and age <= lookahead_sec:
            upcoming.append({
                "title": title,
                "link": link,
                "age_min": int(age / 60),
                "impact": "HIGH",
            })

    event_mode = (len(upcoming) > 0) or recent_macro
    return {
        "enabled": True,
        "ff_calendar_enabled": True,
        "lookahead_hours": lookahead_hours,
        "recent_hours": recent_hours,
        "event_mode": bool(event_mode),
        "recent_macro": bool(recent_macro),
        "upcoming_events": upcoming[:12],
    }


# ============================================================
# COMPUTE BIAS + SIGNAL QUALITY V2
# ============================================================

def compute_bias(lookback_hours: int = 24, limit_rows: int = 1200):
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
            rows = cur.fetchall()

    assets_out: Dict[str, Any] = {}

    # event mode uses same rows
    event_info = extract_calendar_events(rows, lookahead_hours=18.0, recent_hours=6.0)

    for asset in ASSETS:
        total = 0.0
        matches_all: List[Dict[str, Any]] = []

        # for v2 analytics
        freshness_counts = {"0-2h": 0, "2-8h": 0, "8-24h": 0}
        by_source_net: Dict[str, float] = {}
        by_source_abs: Dict[str, float] = {}
        by_source_cnt: Dict[str, int] = {}
        by_driver_abs: Dict[str, float] = {}

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            # freshness bucket counts: count MATCHED items (not all items)
            b = freshness_bucket(age)
            freshness_counts[b] = freshness_counts.get(b, 0) + 1

            for m in matches:
                base_w = m["w"]
                contrib = base_w * w_src * w_time
                total += contrib

                matches_all.append({
                    "source": source,
                    "title": title,
                    "link": link,
                    "published_ts": int(ts),
                    "age_min": int(age / 60),
                    "base_w": base_w,
                    "src_w": w_src,
                    "time_w": round(w_time, 4),
                    "contrib": round(contrib, 4),
                    "why": m["why"],
                })

                # consensus by source
                by_source_net[source] = by_source_net.get(source, 0.0) + contrib
                by_source_abs[source] = by_source_abs.get(source, 0.0) + abs(contrib)
                by_source_cnt[source] = by_source_cnt.get(source, 0) + 1

                # top drivers now (by "why" label)
                why = m["why"]
                by_driver_abs[why] = by_driver_abs.get(why, 0.0) + abs(contrib)

        th = BIAS_THRESH.get(asset, 1.0)
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_top5 = sorted(matches_all, key=lambda x: abs(safe_float(x.get("contrib"))), reverse=True)[:5]

        # -------- Quality v1 (keep, but not the main anymore)
        evidence_count = len(matches_all)
        source_div = len(set([w["source"] for w in matches_all])) if matches_all else 0
        strength = min(1.0, abs(total) / max(th, 1e-9))
        quality_v1 = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, source_div * 2.0)))

        # -------- Signal Quality v2
        # Consensus ratio: (abs(net)) / (sum abs)
        sum_abs = sum(by_source_abs.values()) if by_source_abs else 0.0
        consensus_ratio = (abs(total) / sum_abs) if sum_abs > 1e-9 else 0.0

        # Conflict index: 1 - consensus_ratio (bounded)
        conflict_index = float(max(0.0, min(1.0, 1.0 - consensus_ratio)))

        # "What would flip the bias" distance in score units
        if bias == "NEUTRAL":
            to_bull = max(0.0, th - total)
            to_bear = max(0.0, th + total)
        elif bias == "BULLISH":
            # to bearish: need move from total down to -th => delta = total + th
            to_bull = 0.0
            to_bear = max(0.0, total + th)
        else:  # BEARISH
            to_bull = max(0.0, -th - total)  # total is negative
            to_bear = 0.0

        top3_drivers = sorted(
            [{"why": k, "abs_contrib_sum": round(v, 4)} for k, v in by_driver_abs.items()],
            key=lambda x: x["abs_contrib_sum"],
            reverse=True
        )[:3]

        consensus_by_source = sorted(
            [{
                "source": s,
                "net": round(by_source_net.get(s, 0.0), 4),
                "abs": round(by_source_abs.get(s, 0.0), 4),
                "count": int(by_source_cnt.get(s, 0)),
            } for s in by_source_net.keys()],
            key=lambda x: abs(x["net"]),
            reverse=True
        )[:10]

        # v2 quality score:
        # - reward strength vs threshold
        # - reward diversity
        # - penalize conflict
        # - reward freshness (more 0-2h and 2-8h)
        fresh_score = 0.0
        fresh_score += min(1.0, freshness_counts.get("0-2h", 0) / 6.0) * 0.45
        fresh_score += min(1.0, freshness_counts.get("2-8h", 0) / 10.0) * 0.35
        fresh_score += min(1.0, freshness_counts.get("8-24h", 0) / 18.0) * 0.20

        strength2 = min(1.0, abs(total) / max(th, 1e-9))
        diversity2 = min(1.0, source_div / 8.0)
        conflict_penalty = conflict_index  # 0..1

        quality_v2 = int(
            max(0, min(100,
                (strength2 * 45.0) +
                (diversity2 * 20.0) +
                (fresh_score * 20.0) +
                ((1.0 - conflict_penalty) * 15.0)
            ))
        )

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "threshold": th,

            "quality": quality_v1,
            "quality_v2": quality_v2,

            "evidence_count": evidence_count,
            "source_diversity": source_div,

            "consensus_ratio": round(consensus_ratio, 4),
            "conflict_index": round(conflict_index, 4),
            "freshness": freshness_counts,

            "top3_drivers": top3_drivers,
            "flip": {
                "to_bullish": round(to_bull, 4),
                "to_bearish": round(to_bear, 4),
                "note": "Δ needed in score units (same scale as 'score')",
            },
            "consensus_by_source": consensus_by_source,

            "why_top5": why_top5,
        }

        # Attach event mode (global) under each asset too (convenient for UI)
        assets_out[asset]["event"] = event_info

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
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
            cur.execute("SELECT payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    if not row:
        return None
    return json.loads(row[0])


def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=40)
    payload = compute_bias(lookback_hours=24, limit_rows=1200)

    payload["meta"]["inserted_last_run"] = inserted
    payload["meta"]["feeds_status"] = feeds_health_live()

    save_bias(payload)
    return payload


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/bias")
def bias(pretty: int = 0):
    db_init()
    state = load_bias()
    if not state:
        state = pipeline_run()

    if pretty:
        return PlainTextResponse(json.dumps(state, ensure_ascii=False, indent=2), media_type="application/json")

    return JSONResponse(state)


@app.get("/rules")
def rules():
    # dump rules for UI/debug
    out = {}
    for asset, items in RULES.items():
        out[asset] = [{"pattern": p, "weight": w, "why": why} for (p, w, why) in items]
    return {"assets": ASSETS, "rules": out}


@app.post("/run")
def run_now_post():
    return pipeline_run()


@app.get("/run", include_in_schema=False)
def run_now_get():
    # If user opens /run in browser -> do not show "black screen"
    pipeline_run()
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/latest")
def latest(limit: int = 30):
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
def explain(asset: str = "US500", limit: int = 50):
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
                "base_w": m["w"],
                "src_w": w_src,
                "time_w": round(w_time, 4),
                "contrib": round(contrib, 4),
                "why": m["why"],
                "pattern": m["pattern"],
            })

    out_sorted = sorted(out, key=lambda x: abs(safe_float(x.get("contrib"))), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}


@app.get("/feeds_health")
def feeds_health():
    return feeds_health_live()


# ============================================================
# DASHBOARD UI
# ============================================================

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="pill pill-bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="pill pill-bear">BEARISH</span>'
    return '<span class="pill pill-neutral">NEUTRAL</span>'


def _bar(value: int) -> str:
    v = max(0, min(100, int(value)))
    return f"""
    <div class="bar">
      <div class="bar-fill" style="width:{v}%;"></div>
    </div>
    <div class="bar-num">{v}/100</div>
    """


def _mini_kv(k: str, v: str) -> str:
    return f'<div class="kv"><span class="muted">{k}</span><span class="kvv">{v}</span></div>'


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    feeds_status = (payload.get("meta", {}) or {}).get("feeds_status", {})

    # Pull last headlines (raw)
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
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = safe_float(a.get("score", 0.0))
        th = safe_float(a.get("threshold", 1.0))

        q1 = int(a.get("quality", 0))
        q2 = int(a.get("quality_v2", 0))

        ev = int(a.get("evidence_count", 0))
        div = int(a.get("source_diversity", 0))
        consensus = safe_float(a.get("consensus_ratio", 0.0))
        conflict = safe_float(a.get("conflict_index", 0.0))
        fresh = a.get("freshness", {}) or {}
        top3 = a.get("top3_drivers", []) or []
        flip = a.get("flip", {}) or {}
        cbs = a.get("consensus_by_source", []) or []
        why = a.get("why_top5", []) or []
        event = a.get("event", {}) or {}
        event_mode = bool(event.get("event_mode", False))
        upcoming = event.get("upcoming_events", []) or []

        why_html = ""
        for w in why:
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

        # relevance filter
        kw = {
            "XAU": ["fed","fomc","cpi","inflation","yields","usd","risk","gold","treasury","real yields"],
            "US500": ["stocks","futures","earnings","downgrade","upgrade","rout","slides","rebound","sp","s&p","nasdaq","dow"],
            "WTI": ["oil","crude","opec","inventory","stocks","tanker","pipeline","storm","spr","sanctions"],
        }[asset]

        news_html = ""
        shown = 0
        for (source, title, link, _ts) in rows:
            t = (title or "").lower()
            if not any(k in t for k in kw):
                continue
            shown += 1
            if shown > 9:
                break
            news_html += f'<li><a href="{link}" target="_blank" rel="noopener">{title}</a> <span class="muted">[{source}]</span></li>'

        top3_html = "".join([
            f'<li><span class="muted">{x.get("why","")}</span><span class="kvv">{x.get("abs_contrib_sum","")}</span></li>'
            for x in top3
        ]) or "<li class='muted'>—</li>"

        flip_html = f"""
          {_mini_kv("to bullish", str(flip.get("to_bullish", "—")))}
          {_mini_kv("to bearish", str(flip.get("to_bearish", "—")))}
        """

        fresh_html = f"""
          {_mini_kv("0–2h", str(fresh.get("0-2h", 0)))}
          {_mini_kv("2–8h", str(fresh.get("2-8h", 0)))}
          {_mini_kv("8–24h", str(fresh.get("8-24h", 0)))}
        """

        cbs_rows = ""
        for x in cbs[:8]:
            cbs_rows += f"""
              <tr>
                <td>{x.get('source','')}</td>
                <td class="muted">net={x.get('net','')}</td>
                <td class="muted">abs={x.get('abs','')}</td>
                <td class="muted">n={x.get('count','')}</td>
              </tr>
            """
        if not cbs_rows:
            cbs_rows = "<tr><td class='muted'>—</td><td></td><td></td><td></td></tr>"

        # event mode badge
        em_badge = ""
        if event_mode:
            em_badge = '<span class="pill pill-warn">EVENT MODE</span>'

        upcoming_html = ""
        for e in upcoming[:6]:
            upcoming_html += f"""
              <li>
                <a href="{e.get('link','')}" target="_blank" rel="noopener">{e.get('title','')}</a>
                <span class="muted tiny"> • age={e.get('age_min','')}m • {e.get('impact','')}</span>
              </li>
            """
        if not upcoming_html:
            upcoming_html = "<li class='muted'>—</li>"

        return f"""
        <section class="card" id="card-{asset}">
          <div class="card-head">
            <div>
              <div class="h2">{asset} {_badge(bias)} {em_badge} <span class="muted">score={score} / th={th}</span></div>

              <div class="quality">
                <div class="q-label">Signal Quality v2</div>
                {_bar(q2)}
                <div class="muted tiny">q1={q1} • evidence={ev} • source_diversity={div}</div>
              </div>

              <div class="grid2" style="margin-top:10px;">
                <div class="box">
                  <div class="h3">Top 3 drivers now</div>
                  <ul class="klist">{top3_html}</ul>
                </div>

                <div class="box">
                  <div class="h3">What would flip the bias</div>
                  {flip_html}
                  <div class="muted tiny" style="margin-top:6px;">Δ in score units</div>
                </div>

                <div class="box">
                  <div class="h3">Consensus / Conflict</div>
                  {_mini_kv("consensus", str(consensus))}
                  {_mini_kv("conflict", str(conflict))}
                </div>

                <div class="box">
                  <div class="h3">Freshness buckets</div>
                  {fresh_html}
                </div>
              </div>
            </div>

            <div class="actions">
              <div class="muted tiny" style="margin-bottom:6px;">Quick actions</div>
              <div class="btnrow">
                <button class="btn" onclick="runNow('{asset}')">Run now</button>
                <button class="btn" onclick="showJson()">JSON</button>
                <button class="btn" onclick="showExplain('{asset}')">Explain</button>
                <button class="btn" onclick="showRules()">Rules</button>
              </div>
              <div class="tiny muted" id="status-{asset}" style="margin-top:6px;"></div>
            </div>
          </div>

          <div class="grid">
            <div>
              <div class="h3">WHY (top 5)</div>
              <ol class="why">{why_html or "<li>—</li>"}</ol>

              <div class="h3" style="margin-top:14px;">Consensus by source (top)</div>
              <table class="table">
                <thead>
                  <tr><th>source</th><th>net</th><th>abs</th><th>n</th></tr>
                </thead>
                <tbody>{cbs_rows}</tbody>
              </table>
            </div>

            <div>
              <div class="h3">Latest relevant headlines</div>
              <ul class="news">{news_html or "<li>—</li>"}</ul>

              <div class="h3" style="margin-top:14px;">Upcoming macro (FF calendar, MVP)</div>
              <ul class="news">{upcoming_html}</ul>
            </div>
          </div>
        </section>
        """

    # feeds health table (snapshot)
    feeds_rows = ""
    for src in RSS_FEEDS.keys():
        st = feeds_status.get(src, {})
        ok = st.get("ok", False)
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

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>News Bias Dashboard</title>
      <style>
        :root {{
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.08);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b; --warn:#f59e0b;
          --btn:#1b2636; --btn2:#223047;
        }}
        body {{ font-family: Arial, sans-serif; background:var(--bg); color:var(--text); margin:0; }}
        .wrap {{ max-width: 1120px; margin: 20px auto; padding: 0 14px; }}
        .top {{ display:flex; justify-content:space-between; align-items:flex-end; gap:12px; }}
        h1 {{ margin:0; font-size: 28px; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ font-size:12px; }}
        .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px; margin: 14px 0; }}
        .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
        .h2 {{ font-size:22px; font-weight:700; }}
        .h3 {{ font-size:14px; margin: 8px 0; color:#cfe0ff; }}
        .pill {{ padding:4px 10px; border-radius:999px; font-size:12px; font-weight:700; vertical-align:middle; }}
        .pill-bull {{ background:rgba(16,185,129,.18); color:var(--bull); border:1px solid rgba(16,185,129,.35); }}
        .pill-bear {{ background:rgba(239,68,68,.18); color:var(--bear); border:1px solid rgba(239,68,68,.35); }}
        .pill-neutral {{ background:rgba(100,116,139,.18); color:#cbd5e1; border:1px solid rgba(100,116,139,.35); }}
        .pill-warn {{ background:rgba(245,158,11,.18); color:var(--warn); border:1px solid rgba(245,158,11,.35); }}
        .grid {{ display:grid; grid-template-columns: 1.2fr 1fr; gap:16px; }}
        .grid2 {{ display:grid; grid-template-columns: 1fr 1fr; gap:12px; }}
        .box {{ border:1px solid var(--line); border-radius:14px; padding:10px; background:rgba(255,255,255,.02); }}
        @media(max-width: 980px) {{
          .grid {{ grid-template-columns: 1fr; }}
          .grid2 {{ grid-template-columns: 1fr; }}
          .card-head {{ flex-direction:column; }}
        }}
        .why, .news {{ margin:0; padding-left:18px; }}
        .why li {{ margin: 10px 0; }}
        .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
        .why-meta {{ color:var(--muted); font-size:12px; }}
        .why-headline {{ margin-top:4px; }}
        a {{ color:#7dd3fc; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .actions {{ min-width: 300px; text-align:right; }}
        .btnrow {{ display:flex; gap:8px; justify-content:flex-end; flex-wrap:wrap; }}
        .btn {{
          background:var(--btn);
          border:1px solid var(--line);
          color:var(--text);
          padding:8px 10px;
          border-radius:12px;
          cursor:pointer;
          font-weight:700;
        }}
        .btn:hover {{ background:var(--btn2); }}
        .quality {{ margin-top:8px; }}
        .q-label {{ font-weight:700; margin-bottom:6px; }}
        .bar {{ height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; }}
        .bar-fill {{ height:10px; background:linear-gradient(90deg, rgba(16,185,129,.9), rgba(239,68,68,.9)); }}
        .bar-num {{ margin-top:6px; font-weight:700; }}
        .table {{ width:100%; border-collapse:collapse; font-size: 13px; }}
        .table th {{ text-align:left; color:#cfe0ff; font-weight:700; padding:8px 6px; border-top:1px solid var(--line); }}
        .table td {{ border-top:1px solid var(--line); padding:8px 6px; }}
        .klist {{ list-style:none; margin:0; padding:0; }}
        .klist li {{ display:flex; justify-content:space-between; gap:12px; padding:6px 0; border-top:1px solid rgba(255,255,255,.06); }}
        .klist li:first-child {{ border-top:none; }}
        .kv {{ display:flex; justify-content:space-between; gap:12px; padding:6px 0; border-top:1px solid rgba(255,255,255,.06); }}
        .kv:first-child {{ border-top:none; }}
        .kvv {{ font-weight:700; }}
        .modal {{
          position:fixed; inset:0; background:rgba(0,0,0,.6);
          display:none; align-items:center; justify-content:center; padding:16px;
        }}
        .modal.show {{ display:flex; }}
        .modal-box {{
          width:min(1040px, 100%); max-height: 85vh; overflow:auto;
          background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px;
        }}
        pre {{
          background:rgba(255,255,255,.06);
          padding:12px; border-radius:12px; overflow:auto;
        }}
        .modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <div>
            <h1>News Bias Dashboard</h1>
            <div class="muted tiny">updated_utc: {updated}</div>
          </div>
          <div class="muted tiny">Tip: open <b>/</b> → redirects here. Use your domain as the short link.</div>
        </div>

        {render_asset("XAU")}
        {render_asset("US500")}
        {render_asset("WTI")}

        <section class="card">
          <div class="h3">Feeds health (last run snapshot)</div>
          <table class="table">
            <tbody>
              {feeds_rows}
            </tbody>
          </table>
          <div class="muted tiny" style="margin-top:10px;">
            Deep debug: <a href="/feeds_health" target="_blank" rel="noopener">/feeds_health</a> (live parse now)
          </div>
        </section>
      </div>

      <!-- MODAL -->
      <div class="modal" id="modal">
        <div class="modal-box">
          <div class="modal-head">
            <div class="h2" id="modal-title">Modal</div>
            <button class="btn" onclick="closeModal()">Close</button>
          </div>
          <div id="modal-body" style="margin-top:10px;"></div>
        </div>
      </div>

      <script>
        function openModal(title, bodyHtml) {{
          document.getElementById('modal-title').innerText = title;
          document.getElementById('modal-body').innerHTML = bodyHtml;
          document.getElementById('modal').classList.add('show');
        }}
        function closeModal() {{
          document.getElementById('modal').classList.remove('show');
        }}

        async function runNow(asset) {{
          const el = document.getElementById('status-' + asset);
          el.innerText = 'Running...';
          try {{
            const resp = await fetch('/run', {{ method:'POST' }});
            const data = await resp.json();
            el.innerText = 'Updated: ' + (data.updated_utc || '');
            setTimeout(() => window.location.reload(), 350);
          }} catch(e) {{
            el.innerText = 'Error: ' + e;
          }}
        }}

        async function showJson() {{
          try {{
            const resp = await fetch('/bias?pretty=1');
            const txt = await resp.text();
            openModal('JSON (pretty)', '<pre>' + escapeHtml(txt) + '</pre>');
          }} catch(e) {{
            openModal('JSON', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        async function showRules() {{
          try {{
            const resp = await fetch('/rules');
            const txt = await resp.text();
            openModal('Rules', '<pre>' + escapeHtml(txt) + '</pre>');
          }} catch(e) {{
            openModal('Rules', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
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
                <td style="padding:8px 6px;border-top:1px solid rgba(255,255,255,.08);">
                  <b>${{escapeHtml(x.why || '')}}</b>
                  <div class="muted tiny">${{escapeHtml(x.source || '')}} • age=${{x.age_min}}m • contrib=${{x.contrib}} • w=${{x.base_w}} • src_w=${{x.src_w}} • time_w=${{x.time_w}}</div>
                  <div class="muted tiny">pattern: ${{escapeHtml(x.pattern || '')}}</div>
                </td>
                <td style="padding:8px 6px;border-top:1px solid rgba(255,255,255,.08);">
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
            openModal('Explain ' + asset, '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        function escapeHtml(unsafe) {{
          return (unsafe || '').replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
        }}
      </script>
    </body>
    </html>
    """
    return html
