# ============================================================
# News Bias Bot (MVP++) — Smart Dashboard + Signal Quality + Drivers Map + History
# - RSS ingest to Postgres (dedupe)
# - Bias scoring (regex rules * source weight * time decay)
# - Signal Quality (0-100) using amplitude/concentration/freshness/conflict
# - Smart HTML dashboard:
#     * Summary strip (Bias + Score + Quality)
#     * Per-asset bars: Score vs Threshold, Quality, Conflict, Freshness
#     * WHY top5
#     * Drivers map (topics net pressure + counts + last seen)
#     * Relevant headlines list
# - Health endpoints:
#     * /feeds_health (bozo + entries + bozo_exception)
# - History:
#     * bias_history table (per run snapshot)
#     * /history (last N snapshots)
#
# ENV:
#   DATABASE_URL = postgres://...
# ============================================================

import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

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

    # MarketWatch (Dow Jones feeds)
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
# Each match adds (weight * source_weight * time_decay)
# ============================================================

RULES: Dict[str, List[Tuple[str, float, str]]] = {
    "US500": [
        (r"\b(upgrades?|upgrade|raises? (price )?target|rebound|stabilize|demand lifts|beats?|strong (results|earnings)|beats estimates|guidance raised)\b",
         +1.0, "Equity positive tone"),
        (r"\b(plunges?|rout|slides?|downgrade|downgrades?|cuts? .* to (neutral|sell)|writedowns?|bill for .* pullback|warns?|wary|miss(es|ed)? estimates|guidance cut)\b",
         -1.2, "Equity negative tone"),
        (r"\b(unemployment|jobs|payrolls|cpi|inflation|pce|ppi|fomc|federal reserve|fed|rates?)\b",
         -0.2, "Macro event headline (direction unknown)"),
        (r"\b(ai capex|capex|spending surge|investment surge)\b",
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
        (r"\b(strong dollar|usd strengthens|dollar jumps|yields rise|real yields)\b",
         -0.8, "Stronger USD / higher yields weighs on gold"),
    ],

    "WTI": [
        (r"\b(crude oil|tanker rates|winter storm|disruption|outage|pipeline|opec|output cut|sanctions|red sea|strait)\b",
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
# TOPIC DRIVERS (Drivers Map)
# We infer "topics" from headline text, then aggregate contributions by topic.
# ============================================================

TOPICS: Dict[str, List[Tuple[str, str]]] = {
    "US500": [
        ("FED/RATES", r"\b(fed|fomc|rates?|powell|yield|treasur(y|ies)|real yields)\b"),
        ("INFLATION", r"\b(cpi|pce|ppi|inflation)\b"),
        ("JOBS", r"\b(unemployment|jobs|payrolls|nfp)\b"),
        ("EARNINGS", r"\b(earnings|results|guidance|beats?|miss(es|ed)?)\b"),
        ("TECH/AI", r"\b(ai|chip|semiconductor|capex|cloud)\b"),
        ("RISK-OFF", r"\b(rout|sell[- ]off|wary|plunge|slides?)\b"),
        ("RISK-ON", r"\b(rebound|stabilize|rally|surge)\b"),
    ],
    "XAU": [
        ("USD", r"\b(usd|dollar)\b"),
        ("YIELDS", r"\b(yields?|treasur(y|ies)|real yields)\b"),
        ("FED", r"\b(fed|fomc|powell)\b"),
        ("INFLATION", r"\b(cpi|pce|ppi|inflation)\b"),
        ("RISK-OFF", r"\b(rout|sell[- ]off|wary|plunge|risk[- ]off|vix)\b"),
        ("RISK-ON", r"\b(rebound|risk[- ]on|rally)\b"),
        ("GOLD", r"\b(gold|bullion)\b"),
    ],
    "WTI": [
        ("OPEC", r"\b(opec|opec\+)\b"),
        ("SUPPLY", r"\b(disruption|outage|pipeline|sanctions|output cut|attack|strait|red sea)\b"),
        ("INVENTORIES", r"\b(inventory|stocks|draw|build)\b"),
        ("DEMAND", r"\b(demand|recession|slowdown)\b"),
        ("SHIPPING", r"\b(tanker|freight|rates)\b"),
        ("CRUDE", r"\b(crude|oil|wti|brent)\b"),
        ("GAS/POWER", r"\b(natural gas|electricity|nuclear|coal)\b"),
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
            # Optional but very useful: store history of each pipeline run
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bias_history (
                id BIGSERIAL PRIMARY KEY,
                run_ts BIGINT NOT NULL,
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
                bozo = int(getattr(d, "bozo", 0))

                # Important:
                # - Some feeds set bozo=1 due to minor issues (encoding, redirects) but still have entries.
                # - We only skip if it's bozo AND empty.
                if bozo and len(entries) == 0:
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
# SCORING + QUALITY
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


def infer_topics(asset: str, text: str) -> List[str]:
    tl = (text or "").lower()
    topics = []
    for name, pat in TOPICS.get(asset, []):
        if re.search(pat, tl, flags=re.IGNORECASE):
            topics.append(name)
    if not topics:
        topics = ["OTHER"]
    return topics


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def compute_quality(score: float, threshold: float, contribs: List[float], ages_min: List[int]) -> Dict[str, Any]:
    """
    Quality = 100 * (0.35*amp + 0.25*conc + 0.25*fresh - 0.35*conf)
    """
    eps = 1e-9

    # amplitude: how far beyond threshold (scaled)
    amp = clamp(abs(score) / (2.0 * max(threshold, eps)), 0.0, 1.0)

    # concentration: top5 abs share
    abs_all = [abs(c) for c in contribs]
    sum_abs = sum(abs_all) + eps
    top5_abs = sum(sorted(abs_all, reverse=True)[:5])
    conc = clamp(top5_abs / sum_abs, 0.0, 1.0)

    # conflict: balance between positive and negative absolute pressure
    pos_abs = sum(abs(c) for c in contribs if c > 0)
    neg_abs = sum(abs(c) for c in contribs if c < 0)
    conf = clamp((min(pos_abs, neg_abs) / (pos_abs + neg_abs + eps)) * 2.0, 0.0, 1.0)

    # freshness: share of abs contributions in last 120 minutes
    fresh_abs = 0.0
    for c, age_m in zip(abs_all, ages_min):
        if age_m <= 120:
            fresh_abs += c
    fresh = clamp(fresh_abs / sum_abs, 0.0, 1.0)

    q = 100.0 * (0.35 * amp + 0.25 * conc + 0.25 * fresh - 0.35 * conf)
    q = clamp(q, 0.0, 100.0)

    # label
    if q >= 65:
        label = "HIGH"
    elif q >= 35:
        label = "OK"
    else:
        label = "LOW"

    return {
        "quality": round(q, 1),
        "quality_label": label,
        "amp": round(amp, 3),
        "conc": round(conc, 3),
        "fresh": round(fresh, 3),
        "conflict": round(conf, 3),
        "pos_abs": round(pos_abs, 4),
        "neg_abs": round(neg_abs, 4),
        "sum_abs": round(sum_abs, 4),
    }


def compute_bias(lookback_hours: int = 24, limit_rows: int = 1000):
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
        total = 0.0
        why_all = []
        contribs = []
        ages_min = []

        # topic aggregation
        topic_map: Dict[str, Dict[str, Any]] = {}

        for (source, title, link, ts) in rows:
            age_sec = now - int(ts)
            age_m = int(age_sec / 60)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age_sec)

            matches = match_rules(asset, title)
            if not matches:
                continue

            # topic inference once per headline
            topics = infer_topics(asset, title)

            for m in matches:
                base_w = m["w"]
                contrib = base_w * w_src * w_time
                total += contrib

                contribs.append(contrib)
                ages_min.append(age_m)

                why_all.append({
                    "source": source,
                    "title": title,
                    "link": link,
                    "published_ts": int(ts),
                    "age_min": age_m,
                    "base_w": base_w,
                    "src_w": w_src,
                    "time_w": round(w_time, 4),
                    "contrib": round(contrib, 4),
                    "why": m["why"],
                    "topics": topics,
                })

                # aggregate by topics
                for tp in topics:
                    if tp not in topic_map:
                        topic_map[tp] = {"topic": tp, "net": 0.0, "abs": 0.0, "count": 0, "last_age_min": 10**9}
                    topic_map[tp]["net"] += contrib
                    topic_map[tp]["abs"] += abs(contrib)
                    topic_map[tp]["count"] += 1
                    topic_map[tp]["last_age_min"] = min(topic_map[tp]["last_age_min"], age_m)

        th = BIAS_THRESH.get(asset, 1.0)
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_top5 = sorted(why_all, key=lambda x: abs(x["contrib"]), reverse=True)[:5]

        # drivers top by absolute pressure
        drivers = list(topic_map.values())
        drivers_sorted = sorted(drivers, key=lambda x: x["abs"], reverse=True)[:8]
        # rounding
        for d in drivers_sorted:
            d["net"] = round(d["net"], 4)
            d["abs"] = round(d["abs"], 4)

        q = compute_quality(score=total, threshold=th, contribs=contribs, ages_min=ages_min)

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "threshold": th,
            "quality": q,
            "why_top5": why_top5,
            "drivers_top": drivers_sorted,
        }

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
# STATE + HISTORY
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
            # history snapshot (always append)
            cur.execute(
                """
                INSERT INTO bias_history (run_ts, payload_json)
                VALUES (%s, %s);
                """,
                (now, json.dumps(payload, ensure_ascii=False))
            )
        conn.commit()


def load_bias():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT updated_ts, payload_json FROM bias_state WHERE id=1;")
            row = cur.fetchone()
    if not row:
        return None
    updated_ts, payload_json = row
    return {"updated_ts": updated_ts, "payload": json.loads(payload_json)}


def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=24, limit_rows=1000)
    payload["meta"]["inserted_last_run"] = inserted
    save_bias(payload)
    return payload


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP++)")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/bias")
def bias():
    db_init()
    st = load_bias()
    if not st:
        return pipeline_run()
    return st["payload"]


@app.get("/run")
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


@app.get("/signal")
def signal():
    """
    Compact “trading-ready” JSON:
    bias/score/threshold/quality + drivers top + why top
    """
    db_init()
    st = load_bias()
    payload = st["payload"] if st else pipeline_run()

    out = {"updated_utc": payload.get("updated_utc"), "assets": {}}
    for a in ASSETS:
        A = payload.get("assets", {}).get(a, {})
        out["assets"][a] = {
            "bias": A.get("bias"),
            "score": A.get("score"),
            "threshold": A.get("threshold"),
            "quality": (A.get("quality") or {}),
            "drivers_top": A.get("drivers_top", []),
            "why_top5": A.get("why_top5", []),
        }
    return out


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

        age_sec = now - int(ts)
        age_m = int(age_sec / 60)
        w_src = SOURCE_WEIGHT.get(source, 1.0)
        w_time = decay_weight(age_sec)

        topics = infer_topics(asset, title)

        for m in matches:
            contrib = m["w"] * w_src * w_time
            out.append({
                "source": source,
                "title": title,
                "link": link,
                "age_min": age_m,
                "base_w": m["w"],
                "src_w": w_src,
                "time_w": round(w_time, 4),
                "contrib": round(contrib, 4),
                "why": m["why"],
                "topics": topics,
            })

    out_sorted = sorted(out, key=lambda x: abs(x["contrib"]), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}


@app.get("/history")
def history(limit: int = 30):
    """
    Last N pipeline runs (compact):
    returns run_ts + per-asset score/bias/quality
    """
    db_init()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_ts, payload_json
                FROM bias_history
                ORDER BY run_ts DESC
                LIMIT %s;
                """,
                (limit,)
            )
            rows = cur.fetchall()

    items = []
    for run_ts, payload_json in rows:
        p = json.loads(payload_json)
        row = {"run_ts": int(run_ts), "updated_utc": p.get("updated_utc"), "assets": {}}
        for a in ASSETS:
            A = p.get("assets", {}).get(a, {})
            row["assets"][a] = {
                "bias": A.get("bias"),
                "score": A.get("score"),
                "threshold": A.get("threshold"),
                "quality": (A.get("quality") or {}).get("quality"),
                "quality_label": (A.get("quality") or {}).get("quality_label"),
            }
        items.append(row)
    return {"items": items}


@app.get("/feeds_health")
def feeds_health():
    out = {}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            bozo = int(getattr(d, "bozo", 0))
            entries = len(getattr(d, "entries", []) or [])
            bozo_exc = getattr(d, "bozo_exception", None)
            out[src] = {
                "bozo": bozo,
                "entries": entries,
                "bozo_exception": str(bozo_exc) if bozo_exc else None,
            }
        except Exception as e:
            out[src] = {"error": str(e)}
    return out


# ============================================================
# DASHBOARD (SMART VISUAL)
# ============================================================

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="badge badge-bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="badge badge-bear">BEARISH</span>'
    return '<span class="badge badge-neu">NEUTRAL</span>'


def _qbadge(q_label: str, q: float) -> str:
    if q_label == "HIGH":
        cls = "q-high"
    elif q_label == "OK":
        cls = "q-ok"
    else:
        cls = "q-low"
    return f'<span class="qbadge {cls}">{q_label} · {q:.1f}</span>'


def _bar(label: str, value_0_1: float, cls: str = "", right_text: str = "") -> str:
    v = int(clamp(value_0_1, 0.0, 1.0) * 100)
    return f"""
    <div class="barrow">
      <div class="barlabel">{label}</div>
      <div class="barwrap">
        <div class="barfill {cls}" style="width:{v}%"></div>
      </div>
      <div class="barright">{right_text}</div>
    </div>
    """


def _score_bar(score: float, th: float) -> str:
    # Map |score| to 0..1 using 2*th scale
    denom = max(2.0 * th, 1e-9)
    v = clamp(abs(score) / denom, 0.0, 1.0)
    side = "bull" if score >= 0 else "bear"
    return _bar("Score vs Th", v, cls=f"bar-{side}", right_text=f"{score:.3f} / th={th:.2f}")


def _driver_row(d: Dict[str, Any]) -> str:
    net = float(d.get("net", 0.0))
    count = int(d.get("count", 0))
    last_m = int(d.get("last_age_min", 0))
    # Map net to bar width by |net| scaled (soft)
    v = clamp(abs(net) / 2.0, 0.0, 1.0)  # 2.0 is a heuristic
    cls = "bar-bull" if net >= 0 else "bar-bear"
    sign = "+" if net >= 0 else ""
    return f"""
    <div class="driver">
      <div class="driver-top">
        <div class="driver-name">{d.get("topic","")}</div>
        <div class="driver-meta">{sign}{net:.3f} · n={count} · last={last_m}m</div>
      </div>
      <div class="driver-bar">
        <div class="barfill {cls}" style="width:{int(v*100)}%"></div>
      </div>
    </div>
    """


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    st = load_bias()
    payload = st["payload"] if st else pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    inserted = payload.get("meta", {}).get("inserted_last_run", 0)

    # Pull last headlines (raw)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 80;
            """)
            rows = cur.fetchall()

    # keyword filters per asset (for relevant headlines list)
    kw = {
        "XAU": ["fed","fomc","cpi","inflation","yields","usd","risk","gold","treasury","dollar"],
        "US500": ["stocks","futures","earnings","downgrade","upgrade","rout","slides","rebound","sp","s&p","nasdaq","dow"],
        "WTI": ["oil","crude","opec","inventory","stocks","tanker","pipeline","storm","spr","brent","wti"],
    }

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = float(a.get("score", 0.0) or 0.0)
        th = float(a.get("threshold", BIAS_THRESH.get(asset, 1.0)))
        why = a.get("why_top5", []) or []
        drivers = a.get("drivers_top", []) or []

        q = (a.get("quality") or {})
        qv = float(q.get("quality", 0.0) or 0.0)
        qlabel = str(q.get("quality_label", "LOW"))

        amp = float(q.get("amp", 0.0) or 0.0)
        conc = float(q.get("conc", 0.0) or 0.0)
        fresh = float(q.get("fresh", 0.0) or 0.0)
        conf = float(q.get("conflict", 0.0) or 0.0)

        why_html = ""
        for w in why:
            why_html += f"""
            <li>
              <div class="why-title">{w.get('why','')}</div>
              <div class="why-meta">contrib={w.get('contrib','')} · src={w.get('source','')} · age={w.get('age_min','')}m</div>
              <div class="why-head">{w.get('title','')}</div>
            </li>
            """
        if not why_html:
            why_html = "<li>—</li>"

        # relevant headlines
        news_html = ""
        shown = 0
        for (source, title, link, ts) in rows:
            t = (title or "").lower()
            if not any(k in t for k in kw[asset]):
                continue
            shown += 1
            if shown > 12:
                break
            news_html += f'<li><a href="{link}" target="_blank" rel="noreferrer">{title}</a> <span class="src">[{source}]</span></li>'
        if not news_html:
            news_html = "<li>—</li>"

        # bars
        bars_html = (
            _score_bar(score, th) +
            _bar("Quality", qv / 100.0, cls="bar-q", right_text=_qbadge(qlabel, qv)) +
            _bar("Conflict", conf, cls="bar-conf", right_text=f"{conf:.2f}") +
            _bar("Freshness(2h)", fresh, cls="bar-fresh", right_text=f"{fresh:.2f}") +
            _bar("Concentration", conc, cls="bar-conc", right_text=f"{conc:.2f}") +
            _bar("Amplitude", amp, cls="bar-amp", right_text=f"{amp:.2f}")
        )

        # drivers
        drivers_html = ""
        for d in drivers:
            drivers_html += _driver_row(d)
        if not drivers_html:
            drivers_html = '<div class="muted">—</div>'

        return f"""
        <div class="card">
          <div class="cardhead">
            <div class="h2">{asset} {_badge(bias)} <span class="small">score={score:.4f} · th={th:.2f}</span></div>
            <div>{_qbadge(qlabel, qv)}</div>
          </div>

          <div class="grid2">
            <div>
              <div class="sectiontitle">Signal meter</div>
              {bars_html}
            </div>
            <div>
              <div class="sectiontitle">Drivers map (net pressure)</div>
              {drivers_html}
            </div>
          </div>

          <div class="grid2">
            <div>
              <div class="sectiontitle">WHY (top 5)</div>
              <ol class="why">{why_html}</ol>
            </div>
            <div>
              <div class="sectiontitle">Latest relevant headlines</div>
              <ul class="news">{news_html}</ul>
            </div>
          </div>

          <div class="footerhint">
            Debug: <a href="/explain?asset={asset}" target="_blank">/explain?asset={asset}</a>
          </div>
        </div>
        """

    # Summary strip
    def sum_cell(asset: str) -> str:
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = float(a.get("score", 0.0) or 0.0)
        q = (a.get("quality") or {})
        qv = float(q.get("quality", 0.0) or 0.0)
        qlabel = str(q.get("quality_label", "LOW"))
        return f"""
        <div class="sumcell">
          <div class="sumtop">{asset} {_badge(bias)}</div>
          <div class="sumrow">score <b>{score:.3f}</b></div>
          <div class="sumrow">quality {_qbadge(qlabel, qv)}</div>
        </div>
        """

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>News Bias Dashboard</title>
      <style>
        body {{
          font-family: Arial, sans-serif;
          background: #f7f7fb;
          max-width: 1100px;
          margin: 18px auto;
          padding: 0 12px;
          color: #111;
        }}
        a {{ color:#1b4dd6; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}
        .top {{
          display:flex; align-items:flex-end; justify-content:space-between;
          margin-bottom:10px;
        }}
        .title {{
          font-size: 22px;
          font-weight: 800;
          margin: 0;
        }}
        .meta {{
          color:#666; font-size: 12px;
        }}
        .summary {{
          display:grid;
          grid-template-columns: repeat(3, 1fr);
          gap: 10px;
          margin: 12px 0 16px 0;
        }}
        .sumcell {{
          background:#fff;
          border:1px solid #e6e6ef;
          border-radius: 14px;
          padding: 12px;
          box-shadow: 0 1px 0 rgba(0,0,0,0.03);
        }}
        .sumtop {{
          display:flex; gap:8px; align-items:center;
          font-weight: 700;
          margin-bottom: 6px;
        }}
        .sumrow {{ color:#333; font-size: 13px; margin: 3px 0; }}
        .card {{
          background:#fff;
          border:1px solid #e6e6ef;
          border-radius: 18px;
          padding: 14px;
          margin: 12px 0;
          box-shadow: 0 1px 0 rgba(0,0,0,0.03);
        }}
        .cardhead {{
          display:flex;
          align-items:center;
          justify-content:space-between;
          gap:10px;
          margin-bottom: 10px;
        }}
        .h2 {{
          font-size: 18px;
          font-weight: 800;
        }}
        .small {{
          font-size: 12px;
          color:#666;
          font-weight: 500;
          margin-left: 8px;
        }}
        .badge {{
          padding: 4px 10px;
          border-radius: 999px;
          color: #fff;
          font-size: 12px;
          font-weight: 800;
        }}
        .badge-bull {{ background:#0b6; }}
        .badge-bear {{ background:#d33; }}
        .badge-neu  {{ background:#666; }}
        .qbadge {{
          padding: 4px 10px;
          border-radius: 999px;
          color: #fff;
          font-size: 12px;
          font-weight: 800;
          display:inline-block;
        }}
        .q-high {{ background:#0b6; }}
        .q-ok   {{ background:#c08a00; }}
        .q-low  {{ background:#d33; }}

        .grid2 {{
          display:grid;
          grid-template-columns: 1fr 1fr;
          gap: 12px;
          margin-top: 10px;
        }}
        @media (max-width: 900px) {{
          .grid2 {{ grid-template-columns: 1fr; }}
          .summary {{ grid-template-columns: 1fr; }}
        }}

        .sectiontitle {{
          font-weight: 800;
          margin: 6px 0 8px 0;
          color:#222;
        }}

        .barrow {{
          display:grid;
          grid-template-columns: 110px 1fr 170px;
          gap: 8px;
          align-items:center;
          margin: 6px 0;
        }}
        .barlabel {{ color:#333; font-size: 12px; }}
        .barwrap {{
          height: 10px;
          background: #eef0f7;
          border-radius: 999px;
          overflow: hidden;
          border: 1px solid #e6e6ef;
        }}
        .barfill {{
          height: 100%;
          border-radius: 999px;
        }}
        .barright {{
          text-align:right;
          font-size: 12px;
          color:#444;
        }}

        .bar-bull {{ background:#0b6; }}
        .bar-bear {{ background:#d33; }}
        .bar-q    {{ background:#333; }}
        .bar-conf {{ background:#8a2be2; }}
        .bar-fresh{{ background:#1b4dd6; }}
        .bar-conc {{ background:#00a3a3; }}
        .bar-amp  {{ background:#444; }}

        .why {{
          margin: 0;
          padding-left: 18px;
        }}
        .why li {{
          margin: 8px 0;
          padding-bottom: 8px;
          border-bottom: 1px dashed #eee;
        }}
        .why-title {{ font-weight: 800; }}
        .why-meta {{ color:#888; font-size: 12px; margin: 2px 0; }}
        .why-head {{ color:#222; font-size: 13px; }}

        .news {{
          margin: 0;
          padding-left: 18px;
        }}
        .news li {{ margin: 6px 0; }}
        .src {{ color:#999; font-size: 12px; }}

        .driver {{
          margin: 8px 0;
          padding: 8px;
          border: 1px solid #eee;
          border-radius: 12px;
          background: #fafbff;
        }}
        .driver-top {{
          display:flex;
          justify-content:space-between;
          gap: 10px;
          align-items:baseline;
        }}
        .driver-name {{ font-weight: 900; }}
        .driver-meta {{ color:#666; font-size: 12px; white-space:nowrap; }}
        .driver-bar {{
          height: 10px;
          background: #eef0f7;
          border-radius: 999px;
          overflow: hidden;
          border: 1px solid #e6e6ef;
          margin-top: 6px;
        }}

        .footerhint {{
          color:#888;
          font-size: 12px;
          margin-top: 8px;
        }}
        .muted {{ color:#999; font-size: 13px; }}
      </style>
    </head>
    <body>
      <div class="top">
        <div>
          <div class="title">News Bias Dashboard</div>
          <div class="meta">updated_utc: {updated} · inserted_last_run: {inserted} · endpoints: <a href="/signal" target="_blank">/signal</a> · <a href="/feeds_health" target="_blank">/feeds_health</a> · <a href="/history" target="_blank">/history</a></div>
        </div>
        <div class="meta">
          <a href="/run" target="_blank">Run now</a>
        </div>
      </div>

      <div class="summary">
        {sum_cell("XAU")}
        {sum_cell("US500")}
        {sum_cell("WTI")}
      </div>

      {render_asset("XAU")}
      {render_asset("US500")}
      {render_asset("WTI")}

      <div class="meta" style="margin:14px 0 24px 0;">
        Tip: if FT or some feeds are blocked, ingestion will keep working (bozo+entries logic).
      </div>
    </body>
    </html>
    """
    return html
