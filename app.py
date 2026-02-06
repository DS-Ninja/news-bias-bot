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

RSS_FEEDS: Dict[str, str] = {
    # Macro / Rates / USD drivers
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/bls_latest.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",

    # Energy / WTI
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",
    "OILPRICE": "https://oilprice.com/rss/main",

    # Broad market headlines
    "INVESTING_GENERAL": "https://www.investing.com/rss/news_25.rss",

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

    # --- NEW (Investing.com extra)
    "INVESTING_STOCK_FUNDAMENTAL": "https://www.investing.com/rss/stock_Fundamental.rss",
    "INVESTING_COMMOD_TECH": "https://www.investing.com/rss/commodities_Technical.rss",
    "INVESTING_NEWS_COMMODITIES": "https://www.investing.com/rss/news_11.rss",
    "INVESTING_NEWS_STOCKS": "https://www.investing.com/rss/news_95.rss",
    "INVESTING_MKT_OVERVIEW_TECH": "https://www.investing.com/rss/market_overview_Technical.rss",
    "INVESTING_MKT_OVERVIEW_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INVESTING_IDEAS": "https://www.investing.com/rss/market_overview_investing_ideas.rss",
    "INVESTING_FOREX_TECH": "https://www.investing.com/rss/forex_Technical.rss",
    "INVESTING_FOREX_FUND": "https://www.investing.com/rss/forex_Fundamental.rss",

    # Optional extra feeds you shared earlier (keep them here if you want them always ON)
    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",
    "TRUMP_POLITICO": "https://rss.politico.com/donald-trump.xml",

    # ForexFactory calendar (via faireconomy mirror)
    "FF_CALENDAR_WEEK": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml?version=51c52bdf678435acb58756461c1e8226",
}

# impact weights by source (heuristic)
SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.5,
    "EIA": 3.0,

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

    # Investing.com family
    "INVESTING_GENERAL": 1.0,
    "INVESTING_STOCK_FUNDAMENTAL": 1.1,
    "INVESTING_COMMOD_TECH": 1.05,
    "INVESTING_NEWS_COMMODITIES": 1.05,
    "INVESTING_NEWS_STOCKS": 1.05,
    "INVESTING_MKT_OVERVIEW_TECH": 1.05,
    "INVESTING_MKT_OVERVIEW_FUND": 1.05,
    "INVESTING_IDEAS": 0.95,
    "INVESTING_FOREX_TECH": 1.05,
    "INVESTING_FOREX_FUND": 1.05,

    # Others
    "RSSAPP_1": 1.0,
    "RSSAPP_2": 1.0,
    "TRUMP_POLITICO": 0.9,

    # Calendar feed (events, not headlines; we don't ingest as news)
    "FF_CALENDAR_WEEK": 0.0,
}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

# Bias thresholds (per-asset)
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_items_published_ts ON news_items(published_ts DESC);")
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
# FEEDS HEALTH
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

CALENDAR_FEEDS = {"FF_CALENDAR_WEEK"}  # do not ingest into news_items


def ingest_once(limit_per_feed: int = 30) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                if src in CALENDAR_FEEDS:
                    continue

                d = feedparser.parse(url)
                entries = getattr(d, "entries", []) or []

                # If bozo==1 but entries exist — still ingest (some feeds do this).
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
# SCORING + SIGNAL QUALITY V2
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
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"


def compute_asset_pack(asset: str, rows: List[Tuple[str, str, str, int]], now: int) -> Dict[str, Any]:
    th = BIAS_THRESH.get(asset, 1.0)

    total = 0.0
    why_all: List[Dict[str, Any]] = []
    buckets = {"0-2h": 0, "2-8h": 0, "8-24h": 0}

    # For drivers + consensus
    by_why_abs: Dict[str, float] = {}
    by_source_net: Dict[str, float] = {}
    by_source_abs: Dict[str, float] = {}
    by_source_cnt: Dict[str, int] = {}

    pos_abs = 0.0
    neg_abs = 0.0

    for (source, title, link, ts) in rows:
        age = now - int(ts)
        if age < 0:
            age = 0

        w_src = SOURCE_WEIGHT.get(source, 1.0)
        w_time = decay_weight(age)

        matches = match_rules(asset, title)
        if not matches:
            continue

        bucket = freshness_bucket(age)
        buckets[bucket] += 1

        for m in matches:
            base_w = float(m["w"])
            contrib = base_w * w_src * w_time
            total += contrib

            why = m["why"]

            why_all.append({
                "source": source,
                "title": title,
                "link": link,
                "published_ts": int(ts),
                "age_min": int(age / 60),
                "base_w": base_w,
                "src_w": w_src,
                "time_w": round(w_time, 4),
                "contrib": round(contrib, 4),
                "why": why,
            })

            by_why_abs[why] = by_why_abs.get(why, 0.0) + abs(contrib)

            by_source_net[source] = by_source_net.get(source, 0.0) + contrib
            by_source_abs[source] = by_source_abs.get(source, 0.0) + abs(contrib)
            by_source_cnt[source] = by_source_cnt.get(source, 0) + 1

            if contrib >= 0:
                pos_abs += abs(contrib)
            else:
                neg_abs += abs(contrib)

    if total >= th:
        bias = "BULLISH"
    elif total <= -th:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    why_top5 = sorted(why_all, key=lambda x: abs(float(x["contrib"])), reverse=True)[:5]

    # ---- Quality v1 (kept)
    evidence_count = len(why_all)
    source_div = len(set([w["source"] for w in why_all])) if why_all else 0
    strength = min(1.0, abs(total) / max(th, 1e-9))
    quality = int(min(100, (strength * 60.0) + min(30, evidence_count * 2.0) + min(10, source_div * 2.0)))

    # ---- Quality v2 (more strict)
    # components:
    #  - strength vs threshold (0..50)
    #  - evidence count (0..20)
    #  - source diversity (0..15)
    #  - freshness (0..10) (more 0-2h better)
    #  - conflict penalty (0..-25)
    b0 = buckets["0-2h"]; b1 = buckets["2-8h"]; b2 = buckets["8-24h"]
    fresh_score = min(10.0, (b0 * 3.0 + b1 * 1.5 + b2 * 0.5))
    strength2 = min(1.5, abs(total) / max(th, 1e-9))  # allow >1
    comp_strength = min(50.0, strength2 * 35.0)  # 0..~50

    comp_evidence = min(20.0, evidence_count * 1.2)
    comp_div = min(15.0, source_div * 2.0)

    # conflict index: how balanced are pos/neg contributions (near 1 = heavy conflict, near 0 = one-sided)
    denom = (pos_abs + neg_abs) if (pos_abs + neg_abs) > 1e-9 else 1.0
    conflict_index = float(1.0 - abs(pos_abs - neg_abs) / denom)  # 0..1
    conflict_penalty = min(25.0, conflict_index * 25.0)

    quality_v2 = int(max(0.0, min(100.0, comp_strength + comp_evidence + comp_div + fresh_score - conflict_penalty)))

    # consensus ratio: net direction vs total magnitude (0..1)
    consensus_ratio = float(abs(total) / max(denom, 1e-9))

    # top3 drivers by "why"
    top3_drivers = [{"why": k, "abs_contrib_sum": round(v, 4)} for k, v in sorted(by_why_abs.items(), key=lambda kv: kv[1], reverse=True)[:3]]

    # flip distance
    # If score=0.11 and th=0.9 -> to_bullish = 0.79 ; to_bearish = 1.01
    flip = {
        "to_bullish": round(max(0.0, th - total), 4),
        "to_bearish": round(max(0.0, th + total), 4),
        "note": "Δ needed in score units (same scale as 'score')"
    }

    # consensus by source
    consensus_by_source = []
    for s in sorted(by_source_abs.keys(), key=lambda x: by_source_abs[x], reverse=True):
        consensus_by_source.append({
            "source": s,
            "net": round(by_source_net.get(s, 0.0), 4),
            "abs": round(by_source_abs.get(s, 0.0), 4),
            "count": int(by_source_cnt.get(s, 0)),
        })

    return {
        "bias": bias,
        "score": round(total, 4),
        "threshold": th,

        "quality": int(quality),
        "quality_v2": int(quality_v2),

        "evidence_count": int(evidence_count),
        "source_diversity": int(source_div),

        "consensus_ratio": round(consensus_ratio, 4),
        "conflict_index": round(conflict_index, 4),
        "freshness": buckets,

        "top3_drivers": top3_drivers,
        "flip": flip,
        "consensus_by_source": consensus_by_source,

        "why_top5": why_top5,
    }


def parse_ff_calendar_this_week(limit: int = 80) -> Dict[str, Any]:
    """
    Reads the FF calendar RSS-like XML and returns a simplified list.
    We don't hard-rely on exact schema fields because providers vary.
    """
    url = RSS_FEEDS.get("FF_CALENDAR_WEEK")
    if not url:
        return {"ok": False, "error": "FF_CALENDAR_WEEK missing"}

    try:
        d = feedparser.parse(url)
        entries = getattr(d, "entries", []) or []
        out = []
        for e in entries[:limit]:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            published_parsed = e.get("published_parsed")
            published_ts = int(time.mktime(published_parsed)) if published_parsed else 0

            # Many versions put details into summary/description
            summary = (e.get("summary") or e.get("description") or "").strip()
            out.append({
                "title": title,
                "link": link,
                "published_ts": published_ts,
                "summary": summary[:500],
            })
        return {"ok": True, "count": len(out), "events": out}
    except Exception as e:
        return {"ok": False, "error": str(e)}


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
            rows = cur.fetchall()

    assets_out = {}
    for asset in ASSETS:
        assets_out[asset] = compute_asset_pack(asset, rows, now)

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


def build_event_mode(payload: Dict[str, Any], recent_hours: float = 6.0, lookahead_hours: float = 18.0) -> Dict[str, Any]:
    """
    event_mode = TRUE if:
      - any FED/BLS/BEA item ingested within recent_hours
      - OR FF calendar feed is available and has entries (informational)
    We keep it lightweight (no brittle parsing).
    """
    now = int(time.time())
    recent_cutoff = now - int(recent_hours * 3600)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s AND source IN ('FED','BLS','BEA')
                ORDER BY published_ts DESC
                LIMIT 50;
                """,
                (recent_cutoff,)
            )
            macro_rows = cur.fetchall()

    recent_macro = [{
        "source": s, "title": t, "link": l, "published_ts": int(ts),
        "age_min": int((now - int(ts)) / 60)
    } for (s, t, l, ts) in macro_rows]

    ff = parse_ff_calendar_this_week(limit=80)
    ff_enabled = bool(ff.get("ok", False))

    event_mode = (len(recent_macro) > 0)
    return {
        "enabled": True,
        "event_mode": bool(event_mode),
        "recent_macro": bool(len(recent_macro) > 0),
        "recent_macro_items": recent_macro[:12],
        "lookahead_hours": float(lookahead_hours),
        "recent_hours": float(recent_hours),
        "ff_calendar_enabled": ff_enabled,
        "ff_calendar": ff if ff_enabled else {"ok": False},
    }


def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=30)

    payload = compute_bias(lookback_hours=24, limit_rows=1200)

    payload["meta"]["inserted_last_run"] = int(inserted)
    payload["meta"]["feeds_status"] = feeds_health_live()

    # event mode block
    payload["meta"]["event"] = build_event_mode(payload, recent_hours=6.0, lookahead_hours=18.0)

    save_bias(payload)
    return payload


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Dashboard (MVP+)")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=302)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/rules")
def rules():
    # Helpful for UI/tooling
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
                LIMIT 1400;
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

    out_sorted = sorted(out, key=lambda x: abs(x["contrib"]), reverse=True)[:limit]
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


def _bar(value: int) -> str:
    v = max(0, min(100, int(value)))
    return f"""
    <div class="bar"><div class="bar-fill" style="width:{v}%;"></div></div>
    <div class="bar-num">{v}/100</div>
    """


def _mini_kv(label: str, value: str) -> str:
    return f"""
    <div class="kv">
      <div class="kv-k">{label}</div>
      <div class="kv-v">{value}</div>
    </div>
    """


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    feeds_status = (payload.get("meta", {}) or {}).get("feeds_status", {}) or {}
    event = (payload.get("meta", {}) or {}).get("event", {}) or {}

    # Pull last headlines (raw) for "Latest relevant"
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
        score = a.get("score", 0.0)
        th = a.get("threshold", 1.0)

        q1 = int(a.get("quality", 0))
        q2 = int(a.get("quality_v2", 0))

        ev = int(a.get("evidence_count", 0))
        div = int(a.get("source_diversity", 0))

        consensus_ratio = a.get("consensus_ratio", 0.0)
        conflict_index = a.get("conflict_index", 0.0)

        freshness = a.get("freshness", {}) or {}
        top3 = a.get("top3_drivers", []) or []
        flip = a.get("flip", {}) or {}
        by_source = a.get("consensus_by_source", []) or []
        why = a.get("why_top5", []) or []

        # WHY
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

        # Latest relevant headlines (simple keyword filter)
        kw = {
            "XAU": ["gold", "xau", "fed", "fomc", "cpi", "inflation", "yields", "usd", "risk", "treasury", "real yields"],
            "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "rout", "slides", "rebound", "s&p", "nasdaq", "equities"],
            "WTI": ["oil", "crude", "wti", "opec", "inventory", "stocks", "tanker", "pipeline", "storm", "spr", "output cut"],
        }[asset]

        news_html = ""
        shown = 0
        for (source, title, link, _ts) in rows:
            t = (title or "").lower()
            if not any(k in t for k in kw):
                continue
            shown += 1
            if shown > 10:
                break
            news_html += f'<li><a href="{link}" target="_blank" rel="noopener">{title}</a> <span class="muted">[{source}]</span></li>'

        # Actionable cards
        top3_html = "".join([f"<li><b>{x.get('why','')}</b> <span class='muted tiny'>abs={x.get('abs_contrib_sum','')}</span></li>" for x in top3]) or "<li>—</li>"
        flip_html = f"""
          <div class="kvgrid">
            {_mini_kv("to bullish", str(flip.get("to_bullish", "—")))}
            {_mini_kv("to bearish", str(flip.get("to_bearish", "—")))}
          </div>
          <div class="muted tiny" style="margin-top:6px;">{flip.get("note","")}</div>
        """

        freshness_html = f"""
          <div class="kvgrid">
            {_mini_kv("0-2h", str(freshness.get("0-2h", 0)))}
            {_mini_kv("2-8h", str(freshness.get("2-8h", 0)))}
            {_mini_kv("8-24h", str(freshness.get("8-24h", 0)))}
          </div>
        """

        # consensus by source table (top 7)
        rows_src = ""
        for r in by_source[:7]:
            net = float(r.get("net", 0.0))
            cls = "pos" if net > 0 else ("neg" if net < 0 else "muted")
            rows_src += f"""
              <tr>
                <td>{r.get("source","")}</td>
                <td class="{cls}">{r.get("net","")}</td>
                <td class="muted">{r.get("abs","")}</td>
                <td class="muted">{r.get("count","")}</td>
              </tr>
            """
        if not rows_src:
            rows_src = "<tr><td class='muted'>—</td><td></td><td></td><td></td></tr>"

        return f"""
        <section class="card" id="{asset}">
          <div class="card-head">
            <div>
              <div class="h2">{asset} {_pill(bias)} <span class="muted">score={score} / th={th}</span></div>

              <div class="grid2" style="margin-top:10px;">
                <div class="box">
                  <div class="h3">Signal Quality</div>
                  <div class="quality">
                    <div class="muted tiny">v1</div>
                    {_bar(q1)}
                    <div class="muted tiny" style="margin-top:6px;">evidence={ev} • source_diversity={div}</div>
                  </div>
                  <div class="quality" style="margin-top:12px;">
                    <div class="muted tiny">v2 (strict)</div>
                    {_bar(q2)}
                    <div class="muted tiny" style="margin-top:6px;">consensus={consensus_ratio} • conflict={conflict_index}</div>
                  </div>
                </div>

                <div class="box">
                  <div class="h3">What would flip bias</div>
                  {flip_html}
                  <div class="h3" style="margin-top:14px;">Freshness buckets</div>
                  {freshness_html}
                </div>

                <div class="box compact-hide">
                  <div class="h3">Top 3 drivers now</div>
                  <ol class="tinylist">{top3_html}</ol>
                  <div class="h3" style="margin-top:14px;">Consensus by source</div>
                  <div class="table-wrap">
                    <table class="table">
                      <thead>
                        <tr><th>Source</th><th>Net</th><th>Abs</th><th>n</th></tr>
                      </thead>
                      <tbody>{rows_src}</tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>

            <div class="actions">
              <div class="muted tiny" style="margin-bottom:6px;">Quick actions</div>
              <div class="btnrow">
                <button class="btn" onclick="runNow('{asset}')">Run now</button>
                <button class="btn" onclick="showJson()">JSON</button>
                <button class="btn" onclick="showExplain('{asset}')">Explain</button>
              </div>
              <div class="tiny muted" id="status-{asset}" style="margin-top:8px;"></div>
            </div>
          </div>

          <div class="grid">
            <div class="box">
              <div class="h3">WHY (top 5)</div>
              <ol class="why">{why_html or "<li>—</li>"}</ol>
            </div>

            <div class="box">
              <div class="h3">Latest relevant headlines</div>
              <ul class="news">{news_html or "<li>—</li>"}</ul>
            </div>
          </div>
        </section>
        """

    # Feeds snapshot table
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

    # Event mode card (compact-safe: show short line even in compact)
    ev_mode = bool(event.get("event_mode", False))
    ev_recent = bool(event.get("recent_macro", False))
    ff_ok = bool((event.get("ff_calendar", {}) or {}).get("ok", False))
    ev_badge = '<span class="pill pill-warn">EVENT MODE</span>' if ev_mode else '<span class="pill pill-neutral">Normal</span>'
    ev_lines = ""
    for it in (event.get("recent_macro_items") or [])[:8]:
        ev_lines += f"<li><b>{it.get('source','')}</b> <span class='muted tiny'>age={it.get('age_min','')}m</span> — <a href='{it.get('link','')}' target='_blank' rel='noopener'>{it.get('title','')}</a></li>"
    if not ev_lines:
        ev_lines = "<li class='muted'>—</li>"

    ff_info = ""
    if ff_ok:
        cnt = (event.get("ff_calendar") or {}).get("count", 0)
        ff_info = f"<div class='muted tiny'>FF calendar feed: ok • items={cnt}</div>"
    else:
        ff_info = "<div class='muted tiny'>FF calendar feed: not available (or blocked)</div>"

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
      <meta name="color-scheme" content="dark light">
      <meta name="theme-color" content="#0b0f17" media="(prefers-color-scheme: dark)">
      <meta name="theme-color" content="#f7f8fb" media="(prefers-color-scheme: light)">
      <meta name="apple-mobile-web-app-capable" content="yes">
      <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
      <meta name="apple-mobile-web-app-title" content="News Bias">
      <meta name="format-detection" content="telephone=no">
      <title>News Bias Dashboard</title>

      <style>
        :root{{
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.08);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b; --warn:#f59e0b;
          --btn:#1b2636; --btn2:#223047;
          --link:#7dd3fc;
        }}
        html[data-theme="light"]{{
          --bg:#f7f8fb; --card:#ffffff; --muted:#5c6b7a; --text:#0b1220;
          --line:rgba(15,23,42,.12);
          --btn:#eef2f7; --btn2:#e5ecf5;
          --link:#0369a1;
        }}
        body {{
          font-family: Arial, sans-serif;
          background:var(--bg);
          color:var(--text);
          margin:0;
          -webkit-text-size-adjust: 100%;
        }}
        a {{ color:var(--link); text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ font-size:12px; }}
        .wrap {{
          max-width: 1120px;
          margin: 14px auto;
          padding-left: calc(14px + env(safe-area-inset-left));
          padding-right: calc(14px + env(safe-area-inset-right));
        }}
        h1 {{ margin:0; font-size: 28px; }}
        .top {{ display:flex; justify-content:space-between; align-items:flex-end; gap:12px; }}

        /* Sticky mini header (tabs + controls) */
        .mini {{
          position: sticky;
          top: 0;
          z-index: 50;
          margin: 10px 0 14px 0;
          background: rgba(0,0,0,.0);
        }}
        .mini-inner {{
          display:flex; gap:10px; align-items:center; justify-content:space-between;
          background: color-mix(in srgb, var(--card) 92%, transparent);
          border:1px solid var(--line);
          border-radius: 16px;
          padding: 10px 10px;
          backdrop-filter: blur(8px);
        }}
        .tabs {{ display:flex; gap:8px; flex-wrap:wrap; }}
        .tab {{
          display:inline-flex; gap:8px; align-items:center;
          padding:8px 10px;
          border-radius: 14px;
          border: 1px solid var(--line);
          background: var(--btn);
          cursor:pointer;
          font-weight:700;
        }}
        .tab:hover {{ background: var(--btn2); }}
        .tab small {{ font-weight:700; color:var(--muted); }}
        .toggles {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }}

        .btn {{
          background:var(--btn);
          border:1px solid var(--line);
          color:var(--text);
          padding:10px 12px;
          border-radius:14px;
          cursor:pointer;
          font-weight:700;
        }}
        .btn:hover {{ background:var(--btn2); }}

        .card {{
          background:var(--card);
          border:1px solid var(--line);
          border-radius:16px;
          padding:14px;
          margin: 14px 0;
        }}
        .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
        .h2 {{ font-size:22px; font-weight:700; }}
        .h3 {{ font-size:14px; margin: 8px 0; color: color-mix(in srgb, var(--text) 85%, var(--muted)); }}
        .pill {{ padding:4px 10px; border-radius:999px; font-size:12px; font-weight:700; vertical-align:middle; }}
        .pill-bull {{ background:rgba(16,185,129,.18); color:var(--bull); border:1px solid rgba(16,185,129,.35); }}
        .pill-bear {{ background:rgba(239,68,68,.18); color:var(--bear); border:1px solid rgba(239,68,68,.35); }}
        .pill-neutral {{ background:rgba(100,116,139,.18); color:#cbd5e1; border:1px solid rgba(100,116,139,.35); }}
        .pill-warn {{ background:rgba(245,158,11,.18); color:var(--warn); border:1px solid rgba(245,158,11,.35); }}

        .grid {{ display:grid; grid-template-columns: 1.15fr 1fr; gap:16px; margin-top: 14px; }}
        .grid2 {{ display:grid; grid-template-columns: 1fr 1fr 1.2fr; gap:12px; margin-top: 10px; }}
        .box {{
          background: color-mix(in srgb, var(--bg) 18%, transparent);
          border: 1px solid var(--line);
          border-radius: 14px;
          padding: 12px;
        }}

        .why, .news {{ margin:0; padding-left:18px; }}
        .why li {{ margin: 10px 0; }}
        .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
        .why-meta {{ color:var(--muted); font-size:12px; }}
        .why-headline {{ margin-top:4px; }}

        .actions {{ min-width: 260px; text-align:right; }}
        .btnrow {{ display:flex; gap:8px; justify-content:flex-end; flex-wrap:wrap; }}

        .quality {{ margin-top:8px; }}
        .bar {{ height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; }}
        html[data-theme="light"] .bar {{ background: rgba(15,23,42,.08); }}
        .bar-fill {{ height:10px; background: linear-gradient(90deg, rgba(16,185,129,.95), rgba(239,68,68,.95)); }}
        .bar-num {{ margin-top:6px; font-weight:700; }}

        .kvgrid {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px; }}
        .kv {{
          border: 1px solid var(--line);
          border-radius: 14px;
          padding: 10px;
          background: var(--btn);
        }}
        .kv-k {{ font-size: 12px; color: var(--muted); font-weight:700; }}
        .kv-v {{ font-size: 16px; font-weight: 800; margin-top: 4px; }}

        .table-wrap {{
          overflow:auto;
          -webkit-overflow-scrolling: touch;
          border-radius:12px;
        }}
        table {{ width:100%; border-collapse: collapse; }}
        th, td {{
          border-top:1px solid var(--line);
          padding: 8px 6px;
          text-align:left;
          font-size: 13px;
        }}
        th {{ color: var(--muted); font-weight: 800; }}

        .pos {{ color: var(--bull); font-weight: 800; }}
        .neg {{ color: var(--bear); font-weight: 800; }}

        .tinylist {{ margin:0; padding-left:18px; }}
        .tinylist li {{ margin: 8px 0; }}

        /* Modal */
        .modal {{
          position:fixed; inset:0; background:rgba(0,0,0,.6);
          display:none; align-items:center; justify-content:center;
          padding: calc(16px + env(safe-area-inset-top)) 16px calc(16px + env(safe-area-inset-bottom));
        }}
        .modal.show {{ display:flex; }}
        .modal-box {{
          width:min(1020px, 100%);
          max-height: 82vh;
          overflow:auto;
          -webkit-overflow-scrolling: touch;
          background:var(--card);
          border:1px solid var(--line);
          border-radius:16px;
          padding:14px;
        }}
        .modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; }}
        pre {{
          background:rgba(255,255,255,.06);
          padding:12px; border-radius:12px; overflow:auto;
        }}
        html[data-theme="light"] pre {{ background: rgba(15,23,42,.06); }}

        /* Compact mode */
        html[data-compact="1"] .compact-hide {{ display:none !important; }}
        html[data-compact="1"] .grid2 {{ grid-template-columns: 1fr 1fr; }}
        html[data-compact="1"] .grid {{ grid-template-columns: 1fr; }}

        @media(max-width: 980px) {{
          .grid2 {{ grid-template-columns: 1fr 1fr; }}
        }}
        @media(max-width: 560px) {{
          h1{{ font-size: 22px; }}
          .actions{{ min-width: unset; text-align:left; }}
          .btnrow{{ justify-content:flex-start; }}
          .card{{ padding:12px; }}
          .h2{{ font-size:18px; }}
          .grid{{ grid-template-columns: 1fr; }}
          .grid2{{ grid-template-columns: 1fr; }}
          .mini-inner{{ flex-direction:column; align-items:stretch; }}
          .toggles{{ justify-content:flex-start; }}
        }}
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

        <!-- Sticky Mini Header (tabs + toggles) -->
        <div class="mini">
          <div class="mini-inner">
            <div class="tabs">
              <button class="tab" onclick="jumpTo('XAU')">XAU <small id="mini-xau">—</small></button>
              <button class="tab" onclick="jumpTo('US500')">US500 <small id="mini-us500">—</small></button>
              <button class="tab" onclick="jumpTo('WTI')">WTI <small id="mini-wti">—</small></button>
            </div>
            <div class="toggles">
              <button class="btn" onclick="toggleCompact()">Compact</button>
              <button class="btn" onclick="toggleTheme()">Theme</button>
              <button class="btn" onclick="shareLink()">Share</button>
              <button class="btn" onclick="runNow('ALL')">Run now</button>
              <button class="btn" onclick="showJson()">JSON</button>
              <button class="btn" onclick="showRules()">Rules</button>
            </div>
          </div>
        </div>

        <section class="card">
          <div class="card-head" style="align-items:center;">
            <div>
              <div class="h2">Macro “event mode” {ev_badge}</div>
              <div class="muted tiny">recent_macro={str(ev_recent).lower()} • ff_calendar={str(ff_ok).lower()}</div>
              {ff_info}
            </div>
            <div class="actions" style="text-align:right;">
              <button class="btn" onclick="showEvent()">Details</button>
            </div>
          </div>
        </section>

        {render_asset("XAU")}
        {render_asset("US500")}
        {render_asset("WTI")}

        <section class="card">
          <div class="h2">Feeds health (last run snapshot)</div>
          <div class="muted tiny">Deep debug: <a href="/feeds_health" target="_blank" rel="noopener">/feeds_health</a> (live parse)</div>
          <div class="table-wrap" style="margin-top:10px;">
            <table>
              <thead><tr><th>Feed</th><th>Status</th><th>Entries</th></tr></thead>
              <tbody>{feeds_rows}</tbody>
            </table>
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
        const payloadMini = {json.dumps(assets, ensure_ascii=False)};

        // Theme init
        (function initTheme(){{
          const saved = localStorage.getItem('theme');
          if(saved === 'light' || saved === 'dark') {{
            document.documentElement.setAttribute('data-theme', saved);
          }} else {{
            document.documentElement.setAttribute('data-theme', 'dark');
          }}
        }})();

        // Compact init
        (function initCompact(){{
          const c = localStorage.getItem('compact') || '0';
          document.documentElement.setAttribute('data-compact', c);
        }})();

        // Mini status init
        (function initMini(){{
          function fmt(a) {{
            if(!a) return '—';
            const b = a.bias || 'NEUTRAL';
            const q = a.quality_v2 ?? a.quality ?? 0;
            const s = a.score ?? 0;
            return `${{b}} • q=${{q}} • s=${{s}}`;
          }}
          document.getElementById('mini-xau').innerText = fmt(payloadMini['XAU']);
          document.getElementById('mini-us500').innerText = fmt(payloadMini['US500']);
          document.getElementById('mini-wti').innerText = fmt(payloadMini['WTI']);
        }})();

        function openModal(title, bodyHtml) {{
          document.getElementById('modal-title').innerText = title;
          document.getElementById('modal-body').innerHTML = bodyHtml;
          document.getElementById('modal').classList.add('show');
        }}
        function closeModal() {{
          document.getElementById('modal').classList.remove('show');
        }}

        function escapeHtml(unsafe) {{
          return (unsafe || '').replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
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

        async function shareLink(){{
          const url = window.location.origin + '/';
          try {{
            if(navigator.share) {{
              await navigator.share({{ title:'News Bias Dashboard', url }});
            }} else {{
              await navigator.clipboard.writeText(url);
              openModal('Share', '<div class="muted">Link copied:</div><pre>' + escapeHtml(url) + '</pre>');
            }}
          }} catch(e) {{
            openModal('Share', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        async function runNow(asset) {{
          const ids = ['XAU','US500','WTI'];
          if(asset === 'ALL') {{
            ids.forEach(a => {{
              const el = document.getElementById('status-' + a);
              if(el) el.innerText = 'Running...';
            }});
          }} else {{
            const el = document.getElementById('status-' + asset);
            if(el) el.innerText = 'Running...';
          }}
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
            openModal('JSON (pretty)', '<pre>' + escapeHtml(txt) + '</pre>');
          }} catch(e) {{
            openModal('JSON', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        async function showRules() {{
          try {{
            const resp = await fetch('/rules');
            const data = await resp.json();
            openModal('Rules', '<pre>' + escapeHtml(JSON.stringify(data, null, 2)) + '</pre>');
          }} catch(e) {{
            openModal('Rules', '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        async function showExplain(asset) {{
          try {{
            const resp = await fetch('/explain?asset=' + encodeURIComponent(asset) + '&limit=80');
            const data = await resp.json();
            if (data.error) {{
              openModal('Explain ' + asset, '<div class="muted">' + escapeHtml(data.error) + '</div>');
              return;
            }}
            const rows = (data.top_matches || []).map(x => {{
              return `<tr>
                <td style="padding:6px;border-top:1px solid rgba(255,255,255,.08);">
                  <b>${{escapeHtml(x.why || '')}}</b>
                  <div class="muted tiny">${{escapeHtml(x.source || '')}} • age=${{x.age_min}}m • contrib=${{x.contrib}}</div>
                  <div class="muted tiny">pattern: ${{escapeHtml(x.pattern || '')}}</div>
                </td>
                <td style="padding:6px;border-top:1px solid rgba(255,255,255,.08);">
                  <a href="${{x.link}}" target="_blank" rel="noopener">${{escapeHtml(x.title || '')}}</a>
                </td>
              </tr>`;
            }}).join('');
            const html = `
              <div class="muted tiny">rules_count=${{data.rules_count}} • items=${{(data.top_matches||[]).length}}</div>
              <div class="table-wrap" style="margin-top:10px;">
                <table style="width:100%;border-collapse:collapse;">
                  <tbody>${{rows || '<tr><td class="muted">—</td></tr>'}}</tbody>
                </table>
              </div>
            `;
            openModal('Explain ' + asset, html);
          }} catch(e) {{
            openModal('Explain ' + asset, '<div class="muted">Error: ' + escapeHtml(String(e)) + '</div>');
          }}
        }}

        function showEvent() {{
          const ev = {json.dumps(event, ensure_ascii=False)};
          openModal('Macro Event Mode', '<pre>' + escapeHtml(JSON.stringify(ev, null, 2)) + '</pre>');
        }}
      </script>
    </body>
    </html>
    """
    return html
