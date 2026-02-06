import os
import json
import time
import re
import math
import html
import urllib.request
import ssl
import xml.etree.ElementTree as ET
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
}

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
}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = math.log(2.0) / HALF_LIFE_SEC

BIAS_THRESH = {
    "US500": 1.2,
    "XAU":   0.9,
    "WTI":   0.9,
}

# Optional: ForexFactory calendar XML (event mode)
FF_CAL_ENABLED = os.environ.get("FF_CAL_ENABLED", "0").strip() == "1"
FF_CAL_URL = os.environ.get("FF_CAL_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.xml").strip()
FF_CAL_TIMEOUT_SEC = int(os.environ.get("FF_CAL_TIMEOUT_SEC", "8"))
EVENT_MODE_LOOKAHEAD_HOURS = float(os.environ.get("EVENT_MODE_LOOKAHEAD_HOURS", "18"))
EVENT_MODE_RECENT_HOURS = float(os.environ.get("EVENT_MODE_RECENT_HOURS", "6"))

# ============================================================
# RULES
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

# Pre-compile regex for speed + stability
RULES_RX: Dict[str, List[Tuple[re.Pattern, float, str]]] = {}
for a, rules in RULES.items():
    compiled = []
    for pat, w, why in rules:
        compiled.append((re.compile(pat, flags=re.IGNORECASE), float(w), why))
    RULES_RX[a] = compiled


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
            # Helpful indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_published_ts ON news_items(published_ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source);")
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
# EVENT MODE (ForexFactory calendar - optional)
# ============================================================

def _http_get_text(url: str, timeout_sec: int = 8) -> str:
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "NewsBiasBot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec, context=ctx) as r:
        return r.read().decode("utf-8", errors="replace")


def ff_calendar_fetch() -> List[Dict[str, Any]]:
    """
    Best-effort parse of ForexFactory XML.
    Returns events with fields: title, country, impact, timestamp_utc (unix), dt_utc_iso.
    If disabled/fails -> [].
    """
    if not FF_CAL_ENABLED:
        return []

    try:
        xml_text = _http_get_text(FF_CAL_URL, timeout_sec=FF_CAL_TIMEOUT_SEC)
        root = ET.fromstring(xml_text)
        out = []
        # Common structure: <event>...</event>
        for ev in root.findall(".//event"):
            title = (ev.findtext("title") or "").strip()
            country = (ev.findtext("country") or "").strip()
            impact = (ev.findtext("impact") or "").strip()  # often: Low/Medium/High
            # Many FF XML variants contain <timestamp> unix seconds OR date/time strings.
            ts_txt = (ev.findtext("timestamp") or "").strip()
            ts = None
            if ts_txt.isdigit():
                ts = int(ts_txt)
            else:
                # fallback: try date+time (very variant). If cannot parse -> skip.
                # We'll keep it simple: skip.
                ts = None

            if not title or not country or ts is None:
                continue

            out.append({
                "title": title,
                "country": country,
                "impact": impact,
                "timestamp_utc": ts,
                "dt_utc_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            })
        return out
    except Exception:
        return []


def event_mode_for_asset(asset: str, why_all: List[Dict[str, Any]], ff_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    event_mode is true if:
    - recent macro source match within EVENT_MODE_RECENT_HOURS for FED/BLS/BEA
    OR
    - upcoming high-impact event in lookahead window (FF calendar), relevant to the asset.
    """
    now = int(time.time())
    recent_cut = now - int(EVENT_MODE_RECENT_HOURS * 3600)

    # 1) recent macro sources
    recent_macro = any(
        (w.get("source") in ("FED", "BLS", "BEA")) and int(w.get("published_ts", 0)) >= recent_cut
        for w in why_all
    )

    # 2) upcoming FF events (optional)
    look_to = now + int(EVENT_MODE_LOOKAHEAD_HOURS * 3600)
    relevant_countries = {"US"}  # keep simple MVP; extend later

    # relevant by asset (you can refine)
    if asset == "WTI":
        # still US macro matters; later add OPEC meeting etc
        relevant_countries = {"US"}
    elif asset == "US500":
        relevant_countries = {"US"}
    elif asset == "XAU":
        relevant_countries = {"US"}

    upcoming = []
    for e in (ff_events or []):
        ts = int(e.get("timestamp_utc", 0) or 0)
        if ts <= 0:
            continue
        if now <= ts <= look_to and (e.get("country") in relevant_countries):
            # filter impacts: keep High/Medium by default
            imp = (e.get("impact") or "").lower()
            if "high" in imp or "medium" in imp:
                upcoming.append(e)

    upcoming = sorted(upcoming, key=lambda x: x["timestamp_utc"])[:6]

    return {
        "enabled": True,
        "event_mode": bool(recent_macro or upcoming),
        "recent_macro": bool(recent_macro),
        "upcoming_events": upcoming,
        "lookahead_hours": EVENT_MODE_LOOKAHEAD_HOURS,
        "recent_hours": EVENT_MODE_RECENT_HOURS,
        "ff_calendar_enabled": FF_CAL_ENABLED,
    }


# ============================================================
# SCORING
# ============================================================

def decay_weight(age_sec: int) -> float:
    return float(math.exp(-LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    tl = (text or "").lower()
    out = []
    for rx, w, why in RULES_RX.get(asset, []):
        if rx.search(tl):
            out.append({"w": float(w), "why": why})
    return out


def _fresh_bucket(age_sec: int) -> str:
    if age_sec <= 2 * 3600:
        return "0-2h"
    if age_sec <= 8 * 3600:
        return "2-8h"
    return "8-24h"


def compute_bias(lookback_hours: int = 24, limit_rows: int = 800) -> dict:
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

    ff_events = ff_calendar_fetch()  # optional

    assets_out: Dict[str, Any] = {}

    for asset in ASSETS:
        total = 0.0
        why_all: List[Dict[str, Any]] = []

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = float(SOURCE_WEIGHT.get(source, 1.0))
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            for m in matches:
                base_w = float(m["w"])
                contrib = base_w * w_src * w_time
                total += contrib

                why_all.append({
                    "source": source,
                    "title": title,
                    "link": link,
                    "published_ts": int(ts),
                    "age_min": int(age / 60),
                    "age_sec": int(age),
                    "bucket": _fresh_bucket(int(age)),
                    "base_w": base_w,
                    "src_w": w_src,
                    "time_w": round(w_time, 4),
                    "contrib": round(contrib, 4),
                    "why": m["why"],
                })

        th = float(BIAS_THRESH.get(asset, 1.0))
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_sorted = sorted(why_all, key=lambda x: abs(float(x["contrib"])), reverse=True)
        why_top5 = why_sorted[:5]

        # --- Metrics for "smart visual"
        evidence_count = len(why_all)
        src_div = len(set([w["source"] for w in why_all])) if why_all else 0
        sum_abs = sum(abs(float(w["contrib"])) for w in why_all) if why_all else 0.0
        consensus_ratio = (abs(total) / sum_abs) if sum_abs > 1e-9 else 0.0
        consensus_ratio = float(max(0.0, min(1.0, consensus_ratio)))
        conflict_index = float(1.0 - consensus_ratio)

        # freshness buckets for matched items
        buckets = {"0-2h": 0, "2-8h": 0, "8-24h": 0}
        for w in why_all:
            b = w.get("bucket")
            if b in buckets:
                buckets[b] += 1

        # top drivers (aggregate by 'why')
        drv: Dict[str, float] = {}
        for w in why_all:
            k = str(w.get("why", "") or "")
            drv[k] = drv.get(k, 0.0) + abs(float(w.get("contrib", 0.0)))
        top3_drivers = sorted(
            [{"why": k, "abs_contrib_sum": round(v, 4)} for k, v in drv.items()],
            key=lambda x: float(x["abs_contrib_sum"]),
            reverse=True
        )[:3]

        # consensus by sources
        by_src: Dict[str, Dict[str, Any]] = {}
        for w in why_all:
            s = w["source"]
            by_src.setdefault(s, {"source": s, "net": 0.0, "abs": 0.0, "count": 0})
            c = float(w["contrib"])
            by_src[s]["net"] += c
            by_src[s]["abs"] += abs(c)
            by_src[s]["count"] += 1

        src_list = list(by_src.values())
        for x in src_list:
            x["net"] = round(float(x["net"]), 4)
            x["abs"] = round(float(x["abs"]), 4)
        src_list.sort(key=lambda x: float(x["abs"]), reverse=True)

        # What would flip
        to_bull = round(max(0.0, th - float(total)), 4)
        to_bear = round(max(0.0, th + float(total)), 4)  # need score <= -th
        flip_info = {
            "to_bullish": to_bull,
            "to_bearish": to_bear,
            "note": "Δ needed in score units (same scale as 'score')",
        }

        # Quality v2 (0..100), intentionally "harder" and conflict-aware
        strength = min(1.0, abs(float(total)) / max(th, 1e-9))
        q_strength = strength * 45.0
        q_evidence = min(25.0, evidence_count * 1.6)
        q_div = min(15.0, src_div * 2.5)
        q_cons = consensus_ratio * 15.0
        q_penalty = conflict_index * 10.0  # conflict penalty
        quality_v2 = int(max(0.0, min(100.0, q_strength + q_evidence + q_div + q_cons - q_penalty)))

        # Event mode (optional)
        event_info = event_mode_for_asset(asset, why_all, ff_events)

        assets_out[asset] = {
            "bias": bias,
            "score": round(float(total), 4),
            "threshold": th,

            # v1 + v2
            "quality": int(max(0, min(100, int((strength * 60.0) + min(30, evidence_count * 2.0) + min(10, src_div * 2.0))))),
            "quality_v2": quality_v2,

            "evidence_count": evidence_count,
            "source_diversity": src_div,
            "consensus_ratio": round(consensus_ratio, 4),
            "conflict_index": round(conflict_index, 4),
            "freshness": buckets,

            "top3_drivers": top3_drivers,
            "flip": flip_info,
            "consensus_by_source": src_list[:8],

            "event": event_info,

            "why_top5": why_top5,
        }

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
            "ff_calendar_enabled": FF_CAL_ENABLED,
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


def pipeline_run() -> dict:
    db_init()
    inserted = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=24, limit_rows=800)

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
                LIMIT 800;
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
        w_src = float(SOURCE_WEIGHT.get(source, 1.0))
        w_time = decay_weight(age)

        for m in matches:
            contrib = float(m["w"]) * w_src * w_time
            out.append({
                "source": source,
                "title": title,
                "link": link,
                "age_min": int(age / 60),
                "base_w": float(m["w"]),
                "src_w": w_src,
                "time_w": round(w_time, 4),
                "contrib": round(contrib, 4),
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

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="pill pill-bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="pill pill-bear">BEARISH</span>'
    return '<span class="pill pill-neutral">NEUTRAL</span>'


def _bar(value: int, cls: str = "") -> str:
    v = max(0, min(100, int(value)))
    extra = f" {cls}" if cls else ""
    return f"""
    <div class="bar{extra}">
      <div class="bar-fill" style="width:{v}%;"></div>
    </div>
    <div class="bar-num">{v}/100</div>
    """


def _pill_small(text: str) -> str:
    return f'<span class="pill pill-small">{html.escape(text)}</span>'


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 120;
            """)
            rows = cur.fetchall()

    feeds_status = (payload.get("meta", {}) or {}).get("feeds_status", {})

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        th = a.get("threshold", 1.0)

        q1 = int(a.get("quality", 0))
        q2 = int(a.get("quality_v2", 0))
        ev = int(a.get("evidence_count", 0))
        div = int(a.get("source_diversity", 0))

        cons = float(a.get("consensus_ratio", 0.0))
        conflict = float(a.get("conflict_index", 0.0))
        fresh = a.get("freshness", {"0-2h": 0, "2-8h": 0, "8-24h": 0})
        top3 = a.get("top3_drivers", [])
        flip = a.get("flip", {"to_bullish": 0.0, "to_bearish": 0.0})
        by_src = a.get("consensus_by_source", [])
        event = a.get("event", {"event_mode": False, "recent_macro": False, "upcoming_events": []})
        why = a.get("why_top5", [])

        # WHY
        why_html = ""
        for w in why:
            why_html += f"""
            <li>
              <div class="why-row">
                <div class="why-title"><b>{html.escape(str(w.get('why','')))}</b></div>
                <div class="why-meta">contrib={html.escape(str(w.get('contrib','')))}, src={html.escape(str(w.get('source','')))}, age={html.escape(str(w.get('age_min','')))}m</div>
              </div>
              <div class="why-headline">{html.escape(str(w.get('title','')))}</div>
              <a class="tiny" href="{html.escape(str(w.get('link','')))}" target="_blank" rel="noopener">open</a>
            </li>
            """

        # Latest relevant headlines
        kw = {
            "XAU": ["fed","fomc","cpi","inflation","yields","usd","risk","gold"],
            "US500": ["stocks","futures","earnings","downgrade","upgrade","rout","slides","rebound","sp","s&p","nasdaq"],
            "WTI": ["oil","crude","opec","inventory","stocks","tanker","pipeline","storm","spr"],
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
            news_html += f'<li><a href="{html.escape(link)}" target="_blank" rel="noopener">{html.escape(title)}</a> <span class="muted">[{html.escape(source)}]</span></li>'

        # Actionable cards
        top3_html = ""
        if top3:
            for x in top3:
                top3_html += f"<li>{html.escape(str(x.get('why','')))} <span class='muted'>(abs={html.escape(str(x.get('abs_contrib_sum','')))} )</span></li>"
        else:
            top3_html = "<li class='muted'>—</li>"

        flip_html = f"""
          <div class="kpi-row">
            <div class="kpi"><div class="k">To BULLISH</div><div class="v">{html.escape(str(flip.get('to_bullish', 0.0)))}</div></div>
            <div class="kpi"><div class="k">To BEARISH</div><div class="v">{html.escape(str(flip.get('to_bearish', 0.0)))}</div></div>
          </div>
          <div class="muted tiny">Δ score needed to cross threshold</div>
        """

        src_html = ""
        if by_src:
            for s in by_src[:6]:
                net = float(s.get("net", 0.0))
                sign = "▲" if net > 0 else ("▼" if net < 0 else "•")
                src_html += f"""
                <li>
                  <span class="muted">[{html.escape(str(s.get('source','')))}]</span>
                  <b>{sign} net={html.escape(str(s.get('net','')))}</b>
                  <span class="muted">abs={html.escape(str(s.get('abs','')))} • n={html.escape(str(s.get('count','')))}</span>
                </li>
                """
        else:
            src_html = "<li class='muted'>—</li>"

        fresh_html = f"""
          <div class="kpi-row">
            <div class="kpi"><div class="k">0–2h</div><div class="v">{int(fresh.get("0-2h",0))}</div></div>
            <div class="kpi"><div class="k">2–8h</div><div class="v">{int(fresh.get("2-8h",0))}</div></div>
            <div class="kpi"><div class="k">8–24h</div><div class="v">{int(fresh.get("8-24h",0))}</div></div>
          </div>
        """

        event_pill = _pill_small("EVENT MODE") if event.get("event_mode") else _pill_small("NORMAL")
        upcoming = event.get("upcoming_events", []) or []
        up_html = ""
        if upcoming:
            for e in upcoming[:4]:
                up_html += f"<li>{html.escape(str(e.get('dt_utc_iso','')))} • {html.escape(str(e.get('impact','')))} • {html.escape(str(e.get('title','')))}</li>"
        else:
            up_html = "<li class='muted'>—</li>"

        return f"""
        <section class="card" id="card-{asset}">
          <div class="card-head">
            <div>
              <div class="h2">{asset} {_badge(bias)} <span class="muted">score={score} / th={th}</span></div>

              <div class="meta-line">
                {event_pill}
                <span class="muted tiny">evidence={ev} • source_diversity={div} • consensus={cons} • conflict={conflict}</span>
              </div>

              <div class="quality">
                <div class="q-label">Signal Quality v2</div>
                {_bar(q2, "bar-v2")}
                <div class="muted tiny">v1={q1}/100 • v2={q2}/100</div>
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

          <div class="grid">
            <div>
              <div class="h3">WHY (top 5)</div>
              <ol class="why">{why_html or "<li>—</li>"}</ol>

              <div class="h3" style="margin-top:14px;">Actionable cards</div>
              <div class="cards-mini">
                <div class="mini">
                  <div class="mini-h">Top 3 drivers now</div>
                  <ul class="mini-ul">{top3_html}</ul>
                </div>
                <div class="mini">
                  <div class="mini-h">What would flip bias</div>
                  {flip_html}
                </div>
                <div class="mini">
                  <div class="mini-h">Freshness buckets</div>
                  {fresh_html}
                </div>
                <div class="mini">
                  <div class="mini-h">Consensus by sources</div>
                  <ul class="mini-ul">{src_html}</ul>
                </div>
                <div class="mini">
                  <div class="mini-h">Macro event mode</div>
                  <div class="muted tiny">recent_macro={str(bool(event.get("recent_macro"))).lower()} • ff_cal_enabled={str(bool(event.get("ff_calendar_enabled"))).lower()}</div>
                  <ul class="mini-ul">{up_html}</ul>
                </div>
              </div>
            </div>

            <div>
              <div class="h3">Latest relevant headlines</div>
              <ul class="news">{news_html or "<li>—</li>"}</ul>
            </div>
          </div>
        </section>
        """

    feeds_rows = ""
    for src in RSS_FEEDS.keys():
        st = feeds_status.get(src, {})
        ok = bool(st.get("ok", False))
        bozo = st.get("bozo", "")
        entries = st.get("entries", "")
        icon = "✅" if ok and int(bozo or 0) == 0 else ("⚠️" if ok else "❌")
        feeds_rows += f"""
        <tr>
          <td>{icon} {html.escape(src)}</td>
          <td class="muted">bozo={html.escape(str(bozo))}</td>
          <td class="muted">entries={html.escape(str(entries))}</td>
        </tr>
        """

    html_page = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>News Bias Dashboard</title>
      <style>
        :root {{
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.08);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b;
          --btn:#1b2636; --btn2:#223047;
        }}
        body {{ font-family: Arial, sans-serif; background:var(--bg); color:var(--text); margin:0; }}
        .wrap {{ max-width: 1100px; margin: 20px auto; padding: 0 14px; }}
        .top {{ display:flex; justify-content:space-between; align-items:flex-end; gap:12px; }}
        h1 {{ margin:0; font-size: 28px; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ font-size:12px; }}
        .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px; margin: 14px 0; }}
        .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
        .h2 {{ font-size:22px; font-weight:700; }}
        .h3 {{ font-size:14px; margin: 8px 0; color:#cfe0ff; }}
        .pill {{ padding:4px 10px; border-radius:999px; font-size:12px; font-weight:700; vertical-align:middle; }}
        .pill-small {{ background:rgba(255,255,255,.06); border:1px solid var(--line); color:#cfe0ff; }}
        .pill-bull {{ background:rgba(16,185,129,.18); color:var(--bull); border:1px solid rgba(16,185,129,.35); }}
        .pill-bear {{ background:rgba(239,68,68,.18); color:var(--bear); border:1px solid rgba(239,68,68,.35); }}
        .pill-neutral {{ background:rgba(100,116,139,.18); color:#cbd5e1; border:1px solid rgba(100,116,139,.35); }}
        .meta-line {{ margin-top:6px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
        .grid {{ display:grid; grid-template-columns: 1.25fr 1fr; gap:16px; }}
        @media(max-width: 980px) {{ .grid {{ grid-template-columns: 1fr; }} .card-head {{ flex-direction:column; }} }}
        .why, .news {{ margin:0; padding-left:18px; }}
        .why li {{ margin: 10px 0; }}
        .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
        .why-meta {{ color:var(--muted); font-size:12px; }}
        .why-headline {{ margin-top:4px; }}
        a {{ color:#7dd3fc; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .actions {{ min-width: 260px; text-align:right; }}
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
        .quality {{ margin-top:10px; }}
        .q-label {{ font-weight:700; margin-bottom:6px; }}
        .bar {{ height:10px; background:rgba(255,255,255,.08); border-radius:999px; overflow:hidden; }}
        .bar-fill {{ height:10px; background:linear-gradient(90deg, rgba(16,185,129,.9), rgba(239,68,68,.9)); }}
        .bar.bar-v2 .bar-fill {{ background:linear-gradient(90deg, rgba(125,211,252,.9), rgba(16,185,129,.9)); }}
        .bar-num {{ margin-top:6px; font-weight:700; }}
        .table {{ width:100%; border-collapse:collapse; }}
        .table td {{ border-top:1px solid var(--line); padding:8px 6px; }}
        .modal {{
          position:fixed; inset:0; background:rgba(0,0,0,.6);
          display:none; align-items:center; justify-content:center; padding:16px;
        }}
        .modal.show {{ display:flex; }}
        .modal-box {{
          width:min(980px, 100%); max-height: 85vh; overflow:auto;
          background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px;
        }}
        pre {{
          background:rgba(255,255,255,.06);
          padding:12px; border-radius:12px; overflow:auto;
        }}
        .modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; }}

        .cards-mini {{
          display:grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap:10px;
          margin-top:8px;
        }}
        @media(max-width: 720px) {{
          .cards-mini {{ grid-template-columns: 1fr; }}
        }}
        .mini {{
          background:rgba(255,255,255,.04);
          border:1px solid var(--line);
          border-radius:14px;
          padding:10px;
        }}
        .mini-h {{ font-weight:800; margin-bottom:6px; }}
        .mini-ul {{ margin:0; padding-left:18px; }}
        .kpi-row {{ display:flex; gap:10px; margin-top:6px; flex-wrap:wrap; }}
        .kpi {{
          flex:1;
          min-width: 120px;
          background:rgba(255,255,255,.04);
          border:1px solid var(--line);
          border-radius:12px;
          padding:8px;
        }}
        .k {{ color:var(--muted); font-size:12px; }}
        .v {{ font-size:18px; font-weight:900; }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <div>
            <h1>News Bias Dashboard</h1>
            <div class="muted tiny">updated_utc: {html.escape(str(updated))}</div>
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
          <div class="muted tiny" style="margin-top:8px;">
            FF calendar: enabled={str(FF_CAL_ENABLED).lower()} • url={html.escape(FF_CAL_URL)}
          </div>
        </section>
      </div>

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
            if (!resp.ok) {{
              const t = await resp.text();
              el.innerText = 'HTTP ' + resp.status + ': ' + t;
              return;
            }}
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

        async function showExplain(asset) {{
          try {{
            const resp = await fetch('/explain?asset=' + encodeURIComponent(asset) + '&limit=50');
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
                </td>
                <td style="padding:6px;border-top:1px solid rgba(255,255,255,.08);">
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
    return html_page
