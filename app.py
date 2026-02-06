import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

import feedparser
import psycopg2
from fastapi import FastAPI


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
        # Risk-on / equities positive
        (r"\b(upgrades?|upgrade|raises? (price )?target|rebound|stabilize|demand lifts|beats?|strong (results|earnings))\b",
         +1.0, "Equity positive tone"),
        # Risk-off / equities negative
        (r"\b(plunges?|rout|slides?|downgrade|downgrades?|cuts? .* to (neutral|sell)|writedowns?|bill for .* pullback|warns?|wary)\b",
         -1.2, "Equity negative tone"),
        # Macro / rates-sensitive (direction ambiguous → small negative as risk)
        (r"\b(unemployment|jobs|payrolls|cpi|inflation|fomc|federal reserve|fed)\b",
         -0.2, "Macro event headline (direction unknown)"),
        # Capex risk (AI capex scares equity multiples sometimes)
        (r"\b(ai capex|capex)\b",
         -0.1, "Capex / valuation uncertainty"),
    ],

    "XAU": [
        # Gold bullish: risk-off, stress
        (r"\b(wall st|wall street|futures|s&p|spx|nasdaq|dow|treasur(y|ies)|yields?|vix|risk[- ]off)\b.*\b(rout|plunges?|slides?|sell[- ]off|wary)\b"
        r"|\b(rout|plunges?|slides?|sell[- ]off)\b.*\b(wall st|futures|s&p|nasdaq|treasur(y|ies)|yields?|vix|risk[- ]off)\b",
        +0.9, "Market-wide risk-off supports gold"),

        # Gold bearish: risk-on / strong equities
        (r"\b(rebound|risk[- ]on|stocks to buy|buy after .* drop|strong earnings)\b",
         -0.7, "Risk-on pressures gold"),
        # Fed mention (small positive because event risk often adds bid; we'll refine later)
        (r"\b(fomc statement|fed issues.*statement|federal open market committee|longer[- ]run goals)\b",
         +0.2, "Fed event risk (watch yields/USD)"),
        # Strong USD/yields (if present in titles later)
        (r"\b(strong dollar|usd strengthens|yields rise|real yields)\b",
         -0.8, "Stronger USD / higher yields weighs on gold"),
    ],

    "WTI": [
        # Crude / supply / shipping / disruptions
        (r"\b(crude oil|tanker rates|winter storm|disruption|outage|pipeline|opec|output cut|sanctions)\b",
         +0.9, "Supply / flow supports oil"),
        # Inventories (you'll get better signals once we add weekly EIA petroleum status)
        (r"\b(inventory draw|stocks fall|draw)\b",
         +0.8, "Inventories draw supports oil"),
        (r"\b(inventory build|stocks rise|build)\b",
         -0.8, "Inventories build pressures oil"),
        # Gas/power items: neutral for crude
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
# INGEST
# ============================================================

def ingest_once(limit_per_feed: int = 30) -> int:
    inserted = 0
    now = int(time.time())

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                d = feedparser.parse(url)

                # If feedparser flags issues, skip feed (FT can be restricted)
                if getattr(d, "bozo", 0):
                    # Uncomment for debugging:
                    # print(f"[WARN] bad feed {src}: {getattr(d, 'bozo_exception', None)}")
                    continue

                for e in d.entries[:limit_per_feed]:
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
# SCORING
# ============================================================

def decay_weight(age_sec: int) -> float:
    # exp(-lambda * age)
    return float(pow(2.718281828, -LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    """Return all matches with their base weight and why."""
    t = (text or "")
    tl = t.lower()
    out = []
    for pattern, w, why in RULES.get(asset, []):
        if re.search(pattern, tl, flags=re.IGNORECASE):
            out.append({"pattern": pattern, "w": float(w), "why": why})
    return out


def compute_bias(lookback_hours: int = 24, limit_rows: int = 800):
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

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            for m in matches:
                base_w = m["w"]
                contrib = base_w * w_src * w_time
                total += contrib

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
                    "why": m["why"],
                })

        th = BIAS_THRESH.get(asset, 1.0)
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_top5 = sorted(why_all, key=lambda x: abs(x["contrib"]), reverse=True)[:5]

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "why_top5": why_top5,
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
    payload = compute_bias(lookback_hours=24, limit_rows=800)
    payload["meta"]["inserted_last_run"] = inserted
    save_bias(payload)
    return payload


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/bias")
def bias():
    db_init()
    state = load_bias()
    if not state:
        return pipeline_run()
    return state["payload"]


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


@app.get("/explain")
def explain(asset: str = "US500", limit: int = 50):
    """
    Debug endpoint:
    returns top contributing matched headlines for selected asset.
    """
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
            })

    out_sorted = sorted(out, key=lambda x: abs(x["contrib"]), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}

from fastapi.responses import HTMLResponse

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span style="padding:4px 10px;border-radius:10px;background:#0b6; color:#fff;">BULLISH</span>'
    if bias == "BEARISH":
        return '<span style="padding:4px 10px;border-radius:10px;background:#d33; color:#fff;">BEARISH</span>'
    return '<span style="padding:4px 10px;border-radius:10px;background:#666; color:#fff;">NEUTRAL</span>'

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    st = load_bias()
    if not st:
        payload = pipeline_run()
    else:
        payload = st["payload"]

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")

    # Pull last headlines (raw) and show limited
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 40;
            """)
            rows = cur.fetchall()

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {"bias":"NEUTRAL","score":0.0,"why_top5":[]})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        why = a.get("why_top5", [])

        why_html = ""
        for w in why:
            why_html += f"""
            <li>
              <b>{w.get('why','')}</b>
              <span style="color:#999;">(contrib={w.get('contrib','')}, src={w.get('source','')}, age={w.get('age_min','')}m)</span><br/>
              <span style="color:#333;">{w.get('title','')}</span>
            </li>
            """

        # simple relevance filter for headlines list
        kw = {
            "XAU": ["fed","fomc","cpi","inflation","yields","usd","risk","gold"],
            "US500": ["stocks","futures","earnings","downgrade","upgrade","rout","slides","rebound","sp","s&p"],
            "WTI": ["oil","crude","opec","inventory","stocks","tanker","pipeline","storm","spr"],
        }[asset]

        news_html = ""
        shown = 0
        for (source, title, link, ts) in rows:
            t = (title or "").lower()
            if not any(k in t for k in kw):
                continue
            shown += 1
            if shown > 10:
                break
            news_html += f'<li><a href="{link}" target="_blank">{title}</a> <span style="color:#999;">[{source}]</span></li>'

        return f"""
        <div style="border:1px solid #ddd;border-radius:14px;padding:14px;margin:10px 0;">
          <h2 style="margin:0 0 6px 0;">{asset} {_badge(bias)} <span style="color:#666;font-size:14px;">score={score}</span></h2>
          <div style="margin:10px 0;">
            <h3 style="margin:0 0 6px 0;">WHY (top 5)</h3>
            <ol style="margin:0;padding-left:18px;">{why_html or "<li>—</li>"}</ol>
          </div>
          <div style="margin:10px 0;">
            <h3 style="margin:0 0 6px 0;">Latest relevant headlines</h3>
            <ul style="margin:0;padding-left:18px;">{news_html or "<li>—</li>"}</ul>
          </div>
        </div>
        """

    html = f"""
    <html>
    <head><meta charset="utf-8"><title>News Bias Dashboard</title></head>
    <body style="font-family:Arial, sans-serif; max-width:980px; margin:24px auto; padding:0 12px;">
      <h1 style="margin:0 0 6px 0;">News Bias Dashboard</h1>
      <div style="color:#666;margin-bottom:14px;">updated_utc: {updated}</div>
      {render_asset("XAU")}
      {render_asset("US500")}
      {render_asset("WTI")}
      <div style="color:#999;margin-top:16px;font-size:12px;">Tip: open /explain?asset=XAU for deeper debug.</div>
    </body>
    </html>
    """
    return html

@app.get("/feeds_health")
def feeds_health():
    out = {}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            out[src] = {
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": len(getattr(d, "entries", []) or []),
            }
        except Exception as e:
            out[src] = {"error": str(e)}
    return out
