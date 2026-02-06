import os
import json
import time
from datetime import datetime, timezone

import feedparser
import psycopg2
from fastapi import FastAPI

# -----------------------------
# RSS Sources (MVP)
# -----------------------------
RSS_FEEDS = {
    # Macro / Rates / USD drivers
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/bls_latest.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",

    # Energy / WTI
    "EIA": "https://www.eia.gov/rss/todayinenergy.xml",

    # Broad market headlines (noisy, but ok as extra layer)
    "INVESTING": "https://www.investing.com/rss/news_25.rss",
}

ASSETS = ["XAU", "US500", "WTI"]

# Simple keywords -> direction per asset
# direction: +1 bullish, -1 bearish, 0 neutral/unknown
RULES = {
    "XAU": [
        (["hawkish", "rate hike", "higher for longer", "yields rise", "strong dollar", "usd strengthens"], -1),
        (["dovish", "rate cut", "yields fall", "weaker dollar", "usd weakens", "risk-off", "geopolitical"], +1),
    ],
    "US500": [
        (["hawkish", "rate hike", "higher for longer", "yields rise"], -1),
        (["dovish", "rate cut", "yields fall", "soft landing"], +1),
        (["recession", "risk-off", "credit stress"], -1),
    ],
    "WTI": [
        (["opec cuts", "supply disruption", "output cut", "pipeline outage"], +1),
        (["inventory build", "stocks rise", "demand weak", "oversupply"], -1),
        (["inventory draw", "stocks fall"], +1),
    ],
}

# impact weights by source (MVP heuristic)
SOURCE_WEIGHT = {
    "FED": 3.0,
    "BLS": 3.0,
    "BEA": 2.5,
    "EIA": 3.0,
    "INVESTING": 1.0,
}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC


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
    # Simple stable key
    t = norm_text(title)[:200]
    l = (link or "").strip()[:200]
    return f"{t}||{l}"


def ingest_once(limit_per_feed: int = 30) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                d = feedparser.parse(url)
                for e in d.entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    if not title or not link:
                        continue

                    # published time
                    published_parsed = e.get("published_parsed")
                    if published_parsed:
                        published_ts = int(time.mktime(published_parsed))
                    else:
                        # fallback: treat as "now"
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
                        # keep MVP resilient
                        conn.rollback()
                        continue

        conn.commit()

    return inserted


def classify_direction(asset: str, text: str) -> int:
    t = norm_text(text)
    for kws, direction in RULES.get(asset, []):
        for kw in kws:
            if kw in t:
                return direction
    return 0


def decay_weight(age_sec: int) -> float:
    # exp(-lambda * age)
    return float(pow(2.718281828, -LAMBDA * max(0, age_sec)))


def compute_bias(lookback_hours: int = 24):
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
                LIMIT 500;
                """,
                (cutoff,)
            )
            rows = cur.fetchall()

    # Score per asset + keep "why" contributions
    result = {}
    for asset in ASSETS:
        score = 0.0
        why = []

        for (source, title, link, ts) in rows:
            dirn = classify_direction(asset, title)
            if dirn == 0:
                continue

            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)
            contrib = dirn * w_src * w_time

            score += contrib
            why.append({
                "source": source,
                "title": title,
                "link": link,
                "published_ts": int(ts),
                "age_min": int(age / 60),
                "direction": dirn,
                "contrib": round(contrib, 4),
            })

        # decide bias
        # threshold tuned for MVP; you will later calibrate
        T = 2.0
        if score > T:
            bias = "BULLISH"
        elif score < -T:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_sorted = sorted(why, key=lambda x: abs(x["contrib"]), reverse=True)[:5]

        result[asset] = {
            "bias": bias,
            "score": round(score, 4),
            "why_top5": why_sorted,
        }

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": result,
        "meta": {
            "lookback_hours": 24,
            "feeds": list(RSS_FEEDS.keys()),
        }
    }
    return payload


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
    payload = compute_bias(lookback_hours=24)
    payload["meta"]["inserted_last_run"] = inserted
    save_bias(payload)
    return payload


# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="News Bias Bot (MVP)")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/bias")
def bias():
    db_init()
    state = load_bias()
    if not state:
        # First time: compute instantly
        payload = pipeline_run()
        return payload
    return state["payload"]

@app.get("/run")
def run_now():
    # Manual trigger from browser
    payload = pipeline_run()
    return payload

@app.get("/latest")
def latest(limit: int = 20):
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

