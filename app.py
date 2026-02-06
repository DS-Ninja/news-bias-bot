import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
from urllib.parse import quote_plus

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse


# ============================================================
# CONFIG
# ============================================================

ASSETS = ["XAU", "US500", "WTI"]
DEFAULT_LOOKBACK_H = 24

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

    # Financial Times (may be restricted; ok to try)
    "FT_PRECIOUS_METALS": "https://www.ft.com/precious-metals?format=rss",

    # DailyForex
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

# dashboard relevance keywords (fast heuristic filter)
DASH_KW = {
    "XAU": ["gold", "xau", "fed", "fomc", "cpi", "inflation", "yields", "usd", "dollar", "risk", "treasury", "real yields"],
    "US500": ["stocks", "equities", "futures", "earnings", "downgrade", "upgrade", "rout", "slides", "rebound", "s&p", "spx", "nasdaq", "dow"],
    "WTI": ["oil", "crude", "wti", "opec", "inventory", "stocks", "tanker", "pipeline", "storm", "spr", "outage", "sanctions"],
}


# ============================================================
# DB
# ============================================================

def db_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")

    # Ensure sslmode in URL if provider requires it (many do).
    # If you already have it in DATABASE_URL, this does nothing.
    if "sslmode=" not in url and "localhost" not in url and "127.0.0.1" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"

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
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bias_history (
                id BIGSERIAL PRIMARY KEY,
                run_ts BIGINT NOT NULL,
                asset TEXT NOT NULL,
                bias TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                quality INT NOT NULL,
                details_json TEXT NOT NULL
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bias_history_asset_ts ON bias_history(asset, run_ts);")
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

                # Skip only if it's "bozo AND empty"
                # (Some feeds set bozo but still have valid entries.)
                if getattr(d, "bozo", 0) and len(entries) == 0:
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
    # exp(-lambda * age)
    return float(pow(2.718281828, -LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    tl = (text or "").lower()
    out = []
    for pattern, w, why in RULES.get(asset, []):
        if re.search(pattern, tl, flags=re.IGNORECASE):
            out.append({"pattern": pattern, "w": float(w), "why": why})
    return out


def compute_quality(asset: str, total_score: float, th: float, why_all: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Quality is not "accuracy". It's a confidence heuristic:
    - magnitude vs threshold (signal strength)
    - number of contributing hits (coverage)
    - distinct source diversity
    - consensus (how one-sided the contributions are)
    - recency (freshness)
    """
    if not why_all:
        return {"quality": 0, "components": {"strength": 0, "coverage": 0, "diversity": 0, "consensus": 0, "recency": 0}}

    # strength: |score| / (2*th) clipped to 1
    strength = min(1.0, abs(total_score) / max(1e-9, (2.0 * th)))

    # coverage: log-ish on number of matches
    n = len(why_all)
    coverage = min(1.0, (n / 12.0))  # 12+ matched contributions = "full enough"

    # diversity: distinct sources ratio
    srcs = {w.get("source") for w in why_all if w.get("source")}
    diversity = min(1.0, len(srcs) / 6.0)  # 6 distinct sources = strong

    # consensus: if contributions are mostly same sign
    pos = sum(1 for w in why_all if w.get("contrib", 0) > 0)
    neg = sum(1 for w in why_all if w.get("contrib", 0) < 0)
    denom = max(1, pos + neg)
    imbalance = abs(pos - neg) / denom  # 0..1
    consensus = imbalance

    # recency: median age min mapped (0 min => 1.0, 8h => ~0.5, 24h => low)
    ages = sorted([int(w.get("age_min", 999999)) for w in why_all])
    med_age = ages[len(ages) // 2]
    # 0..1440
    recency = max(0.0, min(1.0, 1.0 - (med_age / 1440.0)))

    # weighted blend
    q = (
        0.34 * strength +
        0.18 * coverage +
        0.16 * diversity +
        0.20 * consensus +
        0.12 * recency
    )
    quality = int(round(100 * max(0.0, min(1.0, q))))

    return {
        "quality": quality,
        "components": {
            "strength": round(strength, 3),
            "coverage": round(coverage, 3),
            "diversity": round(diversity, 3),
            "consensus": round(consensus, 3),
            "recency": round(recency, 3),
            "n_hits": n,
            "n_sources": len(srcs),
            "median_age_min": med_age,
        }
    }


def compute_bias(lookback_hours: int = DEFAULT_LOOKBACK_H, limit_rows: int = 800):
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

        why_top = sorted(why_all, key=lambda x: abs(x["contrib"]), reverse=True)[:8]

        q = compute_quality(asset, total, th, why_all)

        # contradictions: show top opposite-sign items vs overall bias
        overall_sign = 0
        if bias == "BULLISH":
            overall_sign = +1
        elif bias == "BEARISH":
            overall_sign = -1

        contr = []
        if overall_sign != 0:
            for w in sorted(why_all, key=lambda x: abs(x["contrib"]), reverse=True):
                if overall_sign * float(w.get("contrib", 0)) < 0:
                    contr.append(w)
                if len(contr) >= 4:
                    break

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "threshold": th,
            "quality": q["quality"],
            "quality_components": q["components"],
            "why_top": why_top,
            "contradictions_top": contr,
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
        conn.commit()


def save_history(payload: dict):
    now = int(time.time())
    assets = payload.get("assets", {}) or {}

    with db_conn() as conn:
        with conn.cursor() as cur:
            for asset, a in assets.items():
                cur.execute(
                    """
                    INSERT INTO bias_history (run_ts, asset, bias, score, quality, details_json)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        now,
                        asset,
                        str(a.get("bias", "NEUTRAL")),
                        float(a.get("score", 0.0)),
                        int(a.get("quality", 0)),
                        json.dumps(a, ensure_ascii=False),
                    )
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


def get_history(asset: str, points: int = 32) -> List[Dict[str, Any]]:
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return []

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_ts, score, quality, bias
                FROM bias_history
                WHERE asset = %s
                ORDER BY run_ts DESC
                LIMIT %s;
                """,
                (asset, points)
            )
            rows = cur.fetchall()

    out = []
    for (run_ts, score, quality, bias) in reversed(rows):
        out.append({
            "run_ts": int(run_ts),
            "score": float(score),
            "quality": int(quality),
            "bias": str(bias),
        })
    return out


def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=DEFAULT_LOOKBACK_H, limit_rows=900)
    payload["meta"]["inserted_last_run"] = inserted
    save_bias(payload)
    save_history(payload)
    return payload


# ============================================================
# DASHBOARD HELPERS
# ============================================================

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="badge bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="badge bear">BEARISH</span>'
    return '<span class="badge neut">NEUTRAL</span>'


def _qbadge(q: int) -> str:
    # quality 0..100
    if q >= 75:
        cls = "qhi"
        txt = "HIGH"
    elif q >= 45:
        cls = "qmid"
        txt = "MED"
    else:
        cls = "qlo"
        txt = "LOW"
    return f'<span class="qbadge {cls}">Q: {q} ({txt})</span>'


def _fmt_age(mins: int) -> str:
    if mins < 60:
        return f"{mins}m"
    h = mins // 60
    m = mins % 60
    return f"{h}h {m}m"


def _sparkline(values: List[float], w: int = 140, h: int = 34) -> str:
    if not values or len(values) < 2:
        return '<svg width="140" height="34"></svg>'

    vmin = min(values)
    vmax = max(values)
    if abs(vmax - vmin) < 1e-9:
        vmax = vmin + 1.0

    pts = []
    for i, v in enumerate(values):
        x = int(round((i / (len(values) - 1)) * (w - 2))) + 1
        y = int(round((1.0 - (v - vmin) / (vmax - vmin)) * (h - 2))) + 1
        pts.append(f"{x},{y}")
    poly = " ".join(pts)
    return f"""
    <svg width="{w}" height="{h}" viewBox="0 0 {w} {h}">
      <polyline fill="none" stroke="#444" stroke-width="2" points="{poly}" />
    </svg>
    """


def _safe(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/", include_in_schema=False)
def root():
    # Short entry point
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/run")
def run_now():
    return pipeline_run()


@app.get("/bias")
def bias():
    db_init()
    state = load_bias()
    if not state:
        return pipeline_run()
    return state["payload"]


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


@app.get("/explain")
def explain(asset: str = "US500", limit: int = 60):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return {"error": f"asset must be one of {ASSETS}"}

    now = int(time.time())
    cutoff = now - DEFAULT_LOOKBACK_H * 3600

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, title, link, published_ts
                FROM news_items
                WHERE published_ts >= %s
                ORDER BY published_ts DESC
                LIMIT 900;
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


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(asset: str = "ALL", q: str = "", autorefresh: int = 60):
    db_init()

    st = load_bias()
    if not st:
        payload = pipeline_run()
    else:
        payload = st["payload"]

    assets = payload.get("assets", {}) or {}
    updated = payload.get("updated_utc", "")

    # latest headlines pool
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 120;
            """)
            rows = cur.fetchall()

    asset = (asset or "ALL").upper().strip()
    if asset not in ("ALL", *ASSETS):
        asset = "ALL"

    ql = (q or "").strip().lower()

    feeds_h = {}
    try:
        # quick health snapshot
        for src, url in RSS_FEEDS.items():
            d = feedparser.parse(url)
            feeds_h[src] = {"bozo": int(getattr(d, "bozo", 0)), "entries": len(getattr(d, "entries", []) or [])}
    except Exception:
        feeds_h = {}

    def render_asset_card(sym: str) -> str:
        a = assets.get(sym, {})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        th = a.get("threshold", BIAS_THRESH.get(sym, 1.0))
        quality = int(a.get("quality", 0))
        comps = a.get("quality_components", {}) or {}

        why = a.get("why_top", []) or []
        contr = a.get("contradictions_top", []) or []

        # sparkline from history
        hist = get_history(sym, points=32)
        spark = _sparkline([h["score"] for h in hist]) if hist else _sparkline([])

        # filter relevant headlines
        kw = DASH_KW.get(sym, [])
        news_html = ""
        shown = 0
        for (source, title, link, ts) in rows:
            t = (title or "").lower()

            if ql:
                if ql not in t and ql not in (source or "").lower():
                    continue
            else:
                if kw and not any(k in t for k in kw):
                    continue

            shown += 1
            if shown > 10:
                break
            age_min = int((int(time.time()) - int(ts)) / 60)
            news_html += (
                f'<li class="li">'
                f'<a href="{link}" target="_blank">{_safe(title)}</a> '
                f'<span class="muted">[{_safe(source)} • {_fmt_age(age_min)}]</span>'
                f'</li>'
            )

        # WHY list
        why_html = ""
        for w in why:
            why_html += f"""
            <li class="li">
              <div><b>{_safe(w.get('why',''))}</b> <span class="muted">contrib={w.get('contrib','')} • {_safe(w.get('source',''))} • age={_fmt_age(int(w.get('age_min',0)))}</span></div>
              <div class="small">{_safe(w.get('title',''))}</div>
            </li>
            """

        contr_html = ""
        for w in contr:
            contr_html += f"""
            <li class="li">
              <div><b>Opposite</b> <span class="muted">contrib={w.get('contrib','')} • {_safe(w.get('source',''))} • age={_fmt_age(int(w.get('age_min',0)))}</span></div>
              <div class="small">{_safe(w.get('title',''))}</div>
            </li>
            """

        # quality row
        qc = comps
        qc_html = f"""
        <div class="qrow">
          <div class="qcell"><span class="muted">Strength</span><br/><b>{qc.get('strength',0)}</b></div>
          <div class="qcell"><span class="muted">Coverage</span><br/><b>{qc.get('coverage',0)} ({qc.get('n_hits',0)} hits)</b></div>
          <div class="qcell"><span class="muted">Diversity</span><br/><b>{qc.get('diversity',0)} ({qc.get('n_sources',0)} src)</b></div>
          <div class="qcell"><span class="muted">Consensus</span><br/><b>{qc.get('consensus',0)}</b></div>
          <div class="qcell"><span class="muted">Recency</span><br/><b>{qc.get('recency',0)} (med {qc.get('median_age_min',0)}m)</b></div>
        </div>
        """

        return f"""
        <div class="card">
          <div class="cardhead">
            <div>
              <div class="h2">{sym} {_badge(bias)} {_qbadge(quality)} <span class="muted">score={score} • th={th}</span></div>
              <div class="muted small">history: {spark}</div>
            </div>
            <div class="actions">
              <a class="btn" href="/dashboard?asset={sym}">Open</a>
              <a class="btn" href="/explain?asset={sym}" target="_blank">Explain JSON</a>
            </div>
          </div>

          <div class="section">
            <div class="h3">Signal quality breakdown</div>
            {qc_html}
          </div>

          <div class="grid">
            <div class="section">
              <div class="h3">WHY (top drivers)</div>
              <ul class="ul">{why_html or "<li class='li muted'>—</li>"}</ul>
            </div>

            <div class="section">
              <div class="h3">Contradictions (top)</div>
              <ul class="ul">{contr_html or "<li class='li muted'>—</li>"}</ul>
            </div>
          </div>

          <div class="section">
            <div class="h3">Latest relevant headlines</div>
            <ul class="ul">{news_html or "<li class='li muted'>—</li>"}</ul>
          </div>
        </div>
        """

    # tabs
    tab = lambda sym, label: f'<a class="tab {"active" if asset==sym else ""}" href="/dashboard?asset={sym}&q={quote_plus(q)}">{label}</a>'
    tabs_html = (
        tab("ALL", "ALL") +
        tab("XAU", "XAU") +
        tab("US500", "US500") +
        tab("WTI", "WTI")
    )

    # feed health summary line
    bad = [k for k,v in (feeds_h or {}).items() if int(v.get("bozo",0)) == 1 and int(v.get("entries",0)) == 0]
    warn = [k for k,v in (feeds_h or {}).items() if int(v.get("bozo",0)) == 1 and int(v.get("entries",0)) > 0]

    feeds_line = ""
    if feeds_h:
        feeds_line = (
            f"<div class='muted small'>feeds: "
            f"<b>bad(empty+bozo)</b>={len(bad)} • <b>warn(bozo+entries)</b>={len(warn)} • total={len(feeds_h)} "
            f"(<a href='/feeds_health' target='_blank'>details</a>)"
            f"</div>"
        )

    # choose which cards render
    cards = ""
    if asset == "ALL":
        for sym in ASSETS:
            cards += render_asset_card(sym)
    else:
        cards += render_asset_card(asset)

    # auto refresh meta
    ar = max(0, min(int(autorefresh), 600))
    meta_refresh = f"<meta http-equiv='refresh' content='{ar}'>" if ar > 0 else ""

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>News Bias Dashboard</title>
      {meta_refresh}
      <style>
        body {{ font-family: Arial, sans-serif; background:#fafafa; margin:0; }}
        .wrap {{ max-width: 1080px; margin: 22px auto; padding: 0 12px; }}
        .top {{ display:flex; justify-content:space-between; align-items:flex-end; gap:12px; flex-wrap:wrap; }}
        .h1 {{ margin:0; font-size: 24px; }}
        .muted {{ color:#777; }}
        .small {{ font-size: 12px; }}
        .tabs {{ display:flex; gap:8px; margin:12px 0; flex-wrap:wrap; }}
        .tab {{ padding:8px 12px; border:1px solid #ddd; border-radius:12px; background:#fff; text-decoration:none; color:#222; }}
        .tab.active {{ border-color:#333; }}
        .card {{ background:#fff; border:1px solid #e3e3e3; border-radius:16px; padding:14px; margin: 12px 0; }}
        .cardhead {{ display:flex; justify-content:space-between; align-items:flex-start; gap:10px; }}
        .h2 {{ font-size: 18px; margin:0 0 6px 0; }}
        .h3 {{ font-size: 14px; margin:0 0 8px 0; }}
        .badge {{ padding:3px 10px; border-radius:999px; font-weight:700; font-size:12px; color:#fff; }}
        .bull {{ background:#0b6; }}
        .bear {{ background:#d33; }}
        .neut {{ background:#666; }}
        .qbadge {{ padding:3px 10px; border-radius:999px; font-weight:700; font-size:12px; color:#111; background:#eee; margin-left:6px; }}
        .qhi {{ background:#c9f7d7; }}
        .qmid {{ background:#fff2c6; }}
        .qlo {{ background:#ffd0d0; }}
        .actions {{ display:flex; gap:8px; }}
        .btn {{ padding:8px 10px; border:1px solid #ddd; border-radius:12px; background:#fff; text-decoration:none; color:#222; font-size:12px; }}
        .section {{ margin-top: 10px; }}
        .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:12px; }}
        @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        .ul {{ margin:0; padding-left:16px; }}
        .li {{ margin: 8px 0; }}
        .qrow {{ display:grid; grid-template-columns: repeat(5, 1fr); gap:10px; }}
        @media (max-width: 900px) {{ .qrow {{ grid-template-columns: repeat(2, 1fr); }} }}
        .qcell {{ border:1px solid #eee; border-radius:12px; padding:10px; background:#fcfcfc; }}
        .search {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
        input[type="text"] {{ padding:10px 12px; border:1px solid #ddd; border-radius:12px; width:260px; }}
        select {{ padding:10px 12px; border:1px solid #ddd; border-radius:12px; }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="top">
          <div>
            <div class="h1">News Bias Dashboard</div>
            <div class="muted small">updated_utc: {updated} • lookback={payload.get("meta",{}).get("lookback_hours",DEFAULT_LOOKBACK_H)}h • inserted_last_run={payload.get("meta",{}).get("inserted_last_run",0)}</div>
            {feeds_line}
          </div>

          <form class="search" method="get" action="/dashboard">
            <input type="hidden" name="asset" value="{asset}">
            <label class="muted small">Search:</label>
            <input type="text" name="q" value="{_safe(q)}" placeholder="title/source contains...">
            <label class="muted small">Auto-refresh:</label>
            <select name="autorefresh">
              <option value="0" {"selected" if ar==0 else ""}>OFF</option>
              <option value="30" {"selected" if ar==30 else ""}>30s</option>
              <option value="60" {"selected" if ar==60 else ""}>60s</option>
              <option value="120" {"selected" if ar==120 else ""}>120s</option>
            </select>
            <button class="btn" type="submit">Apply</button>
            <a class="btn" href="/run">Run now</a>
          </form>
        </div>

        <div class="tabs">{tabs_html}</div>

        {cards}

        <div class="muted small" style="margin:18px 0;">
          Tip: /explain?asset=XAU gives raw contributions. Root "/" redirects here.
        </div>
      </div>
    </body>
    </html>
    """
    return html
