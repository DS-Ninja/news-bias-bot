import os
import json
import time
import re
import math
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any
from urllib.parse import quote

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse


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

    # Financial Times (may be restricted; ok to try)
    "FT_PRECIOUS_METALS": "https://www.ft.com/precious-metals?format=rss",

    # DailyForex
    "DAILYFOREX_NEWS": "https://www.dailyforex.com/rss/forexnews.xml",
    "DAILYFOREX_TECH": "https://www.dailyforex.com/rss/technicalanalysis.xml",
    "DAILYFOREX_FUND": "https://www.dailyforex.com/rss/fundamentalanalysis.xml",

    # OPTIONAL: ForexFactory calendar XML (not RSS news; it's event calendar)
    # Uncomment if you want it in the same ingestion stream:
    # "FF_CALENDAR_XML": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
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

    # "FF_CALENDAR_XML": 2.0,
}

# Exponential decay (half-life ~ 8 hours)
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

BIAS_THRESH = {
    "US500": 1.2,
    "XAU":   0.9,
    "WTI":   0.9,
}

# Freshness buckets (seconds)
BUCKETS = [
    ("0–2h", 0, 2 * 3600),
    ("2–8h", 2 * 3600, 8 * 3600),
    ("8–24h", 8 * 3600, 24 * 3600),
]

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

# Macro "event mode" triggers (very rough MVP)
EVENT_KEYWORDS = re.compile(
    r"\b(cpi|inflation|ppi|nfp|nonfarm|payrolls|fomc|rate decision|fed|powell|bls|bea|gdp|unemployment)\b",
    re.IGNORECASE
)


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
                entries = getattr(d, "entries", []) or []

                # Skip totally broken feeds (bozo + no entries)
                if getattr(d, "bozo", 0) and len(entries) == 0:
                    continue

                for e in entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    if not title:
                        continue
                    if not link:
                        link = url  # fallback

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
# SCORING / METRICS
# ============================================================

def decay_weight(age_sec: int) -> float:
    return float(math.exp(-LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    tl = (text or "").lower()
    out = []
    for pattern, w, why in RULES.get(asset, []):
        if re.search(pattern, tl, flags=re.IGNORECASE):
            out.append({"pattern": pattern, "w": float(w), "why": why})
    return out


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _quality_v2(total_score: float, th: float, evidence_count: int, source_diversity: int, conflict_index: float) -> int:
    """
    MVP quality:
    - magnitude vs threshold
    - evidence count & diversity
    - penalize conflict
    """
    mag = abs(total_score) / max(th, 1e-9)  # 0..inf
    mag_score = _clamp(mag, 0.0, 3.0) / 3.0  # 0..1

    ev_score = _clamp(evidence_count / 25.0, 0.0, 1.0)  # 25+ saturates
    div_score = _clamp(source_diversity / 8.0, 0.0, 1.0)  # 8 sources saturates

    # conflict_index is 0..1 (1=high conflict)
    conflict_penalty = 1.0 - _clamp(conflict_index, 0.0, 1.0)

    q = (0.50 * mag_score + 0.25 * ev_score + 0.15 * div_score + 0.10 * conflict_penalty) * 100.0
    return int(round(_clamp(q, 0.0, 100.0)))


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

    # Precompute feed health snapshot (cheap)
    feeds_status = {}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            feeds_status[src] = {
                "ok": True,
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": len(getattr(d, "entries", []) or []),
            }
        except Exception as e:
            feeds_status[src] = {"ok": False, "error": str(e)}

    # Global event mode
    macro_recent = []
    for (source, title, link, ts) in rows[:200]:
        age = now - int(ts)
        if age <= 6 * 3600 and (source in ("FED", "BLS", "BEA") or EVENT_KEYWORDS.search(title or "")):
            macro_recent.append({"source": source, "title": title, "link": link, "age_min": int(age/60)})
    event_mode = True if len(macro_recent) > 0 else False

    for asset in ASSETS:
        total = 0.0
        why_all = []

        # consensus by source (signed sums)
        by_source = {}  # src -> sum(contrib)
        # freshness buckets (evidence count)
        bucket_counts = {b[0]: 0 for b in BUCKETS}

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            # bucket count (per headline matched, not per rule)
            for name, lo, hi in BUCKETS:
                if lo <= age < hi:
                    bucket_counts[name] += 1
                    break

            for m in matches:
                base_w = m["w"]
                contrib = base_w * w_src * w_time
                total += contrib
                by_source[source] = by_source.get(source, 0.0) + contrib

                why_all.append({
                    "source": source,
                    "title": title,
                    "link": link,
                    "published_ts": int(ts),
                    "age_min": int(age / 60),
                    "base_w": float(base_w),
                    "src_w": float(w_src),
                    "time_w": round(float(w_time), 4),
                    "contrib": round(float(contrib), 4),
                    "why": m["why"],
                })

        th = BIAS_THRESH.get(asset, 1.0)
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_sorted = sorted(why_all, key=lambda x: abs(x["contrib"]), reverse=True)
        why_top5 = why_sorted[:5]
        top3_drivers = why_sorted[:3]

        evidence_count = len(why_all)
        source_diversity = len(set([w["source"] for w in why_all])) if why_all else 0

        # conflict index: fraction of absolute contribution coming from opposite sign
        if total == 0:
            conflict_index = 0.0
        else:
            sign = 1.0 if total > 0 else -1.0
            abs_sum = sum(abs(w["contrib"]) for w in why_all) or 1e-9
            opp_sum = sum(abs(w["contrib"]) for w in why_all if (w["contrib"] * sign) < 0)
            conflict_index = float(_clamp(opp_sum / abs_sum, 0.0, 1.0))

        # consensus: sources for/against
        src_for = []
        src_against = []
        for s, v in sorted(by_source.items(), key=lambda kv: abs(kv[1]), reverse=True):
            if v >= 0:
                src_for.append({"source": s, "net": round(v, 4)})
            else:
                src_against.append({"source": s, "net": round(v, 4)})

        # flip distance
        if bias == "BULLISH":
            flip_to_neutral_need = round(max(0.0, total - th), 4)  # how much to drop to reach th
        elif bias == "BEARISH":
            flip_to_neutral_need = round(max(0.0, -th - total), 4)  # how much to rise to reach -th
        else:
            # for neutral: distance to either threshold (closest)
            flip_to_bull_need = round(max(0.0, th - total), 4)
            flip_to_bear_need = round(max(0.0, th + total), 4)  # need to go below -th => -(th) - total; magnitude is th+total
            flip_to_neutral_need = {"to_bull": flip_to_bull_need, "to_bear": flip_to_bear_need}

        quality = _quality_v2(
            total_score=float(total),
            th=float(th),
            evidence_count=int(evidence_count),
            source_diversity=int(source_diversity),
            conflict_index=float(conflict_index),
        )

        assets_out[asset] = {
            "bias": bias,
            "score": round(float(total), 4),
            "threshold": float(th),

            "quality": int(quality),
            "evidence_count": int(evidence_count),
            "source_diversity": int(source_diversity),

            "conflict_index": round(float(conflict_index), 4),
            "freshness_buckets": bucket_counts,

            "top3_drivers": top3_drivers,
            "why_top5": why_top5,

            "consensus": {
                "for": src_for[:6],
                "against": src_against[:6],
            },

            "flip": flip_to_neutral_need,
        }

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
            "event_mode": event_mode,
            "event_mode_hits": macro_recent[:8],
            "feeds_status": feeds_status,
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
    return {"updated_ts": int(updated_ts), "payload": json.loads(payload_json)}


def pipeline_run():
    db_init()
    inserted = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=24, limit_rows=800)
    payload["meta"]["inserted_last_run"] = int(inserted)
    save_bias(payload)
    return payload


# ============================================================
# UI helpers
# ============================================================

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span class="badge bull">BULLISH</span>'
    if bias == "BEARISH":
        return '<span class="badge bear">BEARISH</span>'
    return '<span class="badge neu">NEUTRAL</span>'


def _bar(pct: int) -> str:
    pct = int(_clamp(pct, 0, 100))
    return f"""
    <div class="bar-wrap" title="{pct}/100">
      <div class="bar-fill" style="width:{pct}%;"></div>
      <div class="bar-text">{pct}/100</div>
    </div>
    """


def _html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _json_pretty(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/", response_class=RedirectResponse)
def root():
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/bias")
def bias_json():
    db_init()
    st = load_bias()
    if not st:
        return pipeline_run()
    return st["payload"]


@app.get("/run")
def run_now_json():
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
                (int(limit),)
            )
            rows = cur.fetchall()

    return {
        "items": [
            {"source": s, "title": t, "link": l, "published_ts": int(ts)}
            for (s, t, l, ts) in rows
        ]
    }


@app.get("/explain")
def explain_json(asset: str = "US500", limit: int = 50):
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
                "base_w": float(m["w"]),
                "src_w": float(w_src),
                "time_w": round(float(w_time), 4),
                "contrib": round(float(contrib), 4),
                "why": m["why"],
            })

    out_sorted = sorted(out, key=lambda x: abs(x["contrib"]), reverse=True)[:int(limit)]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}


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


# ---------------------------
# UI endpoints (HTML viewers)
# ---------------------------

@app.get("/ui/json", response_class=HTMLResponse)
def ui_json():
    st = load_bias()
    payload = st["payload"] if st else pipeline_run()
    body = _html_escape(_json_pretty(payload))
    return f"""
    <html><head><meta charset="utf-8"><title>JSON View</title>
    <style>
      body{{font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono","Courier New", monospace; padding:16px;}}
      pre{{white-space:pre-wrap; word-break:break-word; background:#0b0f14; color:#e6edf3; padding:14px; border-radius:12px;}}
      a{{color:#4ea1ff;}}
    </style></head>
    <body>
      <div style="margin-bottom:10px;">
        <a href="/dashboard">← back</a>
      </div>
      <pre>{body}</pre>
    </body></html>
    """


@app.get("/ui/explain", response_class=HTMLResponse)
def ui_explain(asset: str = "XAU"):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return HTMLResponse(f"asset must be one of {ASSETS}", status_code=400)

    data = explain_json(asset=asset, limit=80)
    body = _html_escape(_json_pretty(data))
    return f"""
    <html><head><meta charset="utf-8"><title>Explain {asset}</title>
    <style>
      body{{font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono","Courier New", monospace; padding:16px;}}
      pre{{white-space:pre-wrap; word-break:break-word; background:#0b0f14; color:#e6edf3; padding:14px; border-radius:12px;}}
      a{{color:#4ea1ff;}}
    </style></head>
    <body>
      <div style="margin-bottom:10px;">
        <a href="/dashboard">← back</a>
      </div>
      <pre>{body}</pre>
    </body></html>
    """


# ---------------------------
# DASHBOARD
# ---------------------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    st = load_bias()
    payload = st["payload"] if st else pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    event_mode = payload.get("meta", {}).get("event_mode", False)
    event_hits = payload.get("meta", {}).get("event_mode_hits", []) or []
    feeds_status = payload.get("meta", {}).get("feeds_status", {}) or {}

    # Pull last headlines for "Latest relevant"
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 80;
            """)
            rows = cur.fetchall()

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        th = a.get("threshold", 1.0)

        quality = int(a.get("quality", 0))
        evidence_count = int(a.get("evidence_count", 0))
        source_diversity = int(a.get("source_diversity", 0))
        conflict_index = float(a.get("conflict_index", 0.0))

        buckets = a.get("freshness_buckets", {}) or {}
        top3 = a.get("top3_drivers", []) or []
        why5 = a.get("why_top5", []) or []

        consensus = a.get("consensus", {}) or {}
        con_for = consensus.get("for", []) or []
        con_against = consensus.get("against", []) or []

        flip = a.get("flip", None)

        # Latest relevant headlines: simple keywords
        kw = {
            "XAU": ["fed","fomc","cpi","inflation","yields","usd","risk","gold","safe haven"],
            "US500": ["stocks","futures","earnings","downgrade","upgrade","rout","slides","rebound","sp","s&p","nasdaq"],
            "WTI": ["oil","crude","opec","inventory","stocks","tanker","pipeline","storm","spr","brent"],
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
            news_html += f'<li><a href="{link}" target="_blank">{_html_escape(title)}</a> <span class="muted">[{_html_escape(source)}]</span></li>'

        # WHY list
        why_html = ""
        for w in why5:
            why_html += f"""
            <li>
              <b>{_html_escape(w.get('why',''))}</b>
              <span class="muted">(contrib={w.get('contrib','')}, src={_html_escape(w.get('source',''))}, age={w.get('age_min','')}m)</span><br/>
              <span>{_html_escape(w.get('title',''))}</span>
              &nbsp; <a href="{w.get('link','')}" target="_blank" class="small">open</a>
            </li>
            """

        # Top 3 drivers card
        top3_html = ""
        for i, w in enumerate(top3, 1):
            top3_html += f"""
            <div class="driver">
              <div class="driver-h">
                <div class="driver-n">#{i}</div>
                <div class="driver-why">{_html_escape(w.get('why',''))}</div>
                <div class="driver-meta">contrib={w.get('contrib','')} · {_html_escape(w.get('source',''))} · {w.get('age_min','')}m</div>
              </div>
              <div class="driver-t">{_html_escape(w.get('title',''))} <a href="{w.get('link','')}" target="_blank" class="small">open</a></div>
            </div>
            """

        # Flip text
        if isinstance(flip, dict):
            flip_html = f"""
              <div class="kv"><span class="muted">to BULL:</span> <b>{flip.get('to_bull')}</b></div>
              <div class="kv"><span class="muted">to BEAR:</span> <b>{flip.get('to_bear')}</b></div>
            """
        else:
            flip_html = f"""<div class="kv"><span class="muted">distance:</span> <b>{flip}</b></div>"""

        # Buckets
        buckets_html = ""
        for name, _, _ in BUCKETS:
            buckets_html += f"""<span class="chip">{name}: <b>{int(buckets.get(name, 0))}</b></span> """

        # Consensus
        def _cons_list(items):
            if not items:
                return "<div class='muted'>—</div>"
            out = ""
            for it in items:
                out += f"<div class='kv'><b>{_html_escape(it.get('source',''))}</b><span class='muted'>net={it.get('net')}</span></div>"
            return out

        return f"""
        <div class="card">
          <div class="card-h">
            <div class="title">{asset} {_badge(bias)} <span class="muted">score={score} / th={th}</span></div>
            <div class="actions">
              <button class="btn" onclick="runNow()">Run now</button>
              <a class="btn" href="/ui/json" target="_blank">JSON</a>
              <a class="btn" href="/ui/explain?asset={quote(asset)}" target="_blank">Explain</a>
            </div>
          </div>

          <div class="grid">
            <div class="subcard">
              <div class="subttl">Signal Quality v2</div>
              {_bar(quality)}
              <div class="muted small">evidence={evidence_count} · source_diversity={source_diversity} · conflict={round(conflict_index,3)}</div>
              <div style="margin-top:8px;">
                <div class="subttl">Freshness</div>
                <div>{buckets_html}</div>
              </div>
            </div>

            <div class="subcard">
              <div class="subttl">What would flip the bias</div>
              {flip_html}
              <div class="muted small" style="margin-top:6px;">(distance to cross threshold / change regime)</div>
            </div>

            <div class="subcard">
              <div class="subttl">Consensus (sources)</div>
              <div class="split">
                <div>
                  <div class="muted small">FOR</div>
                  {_cons_list(con_for)}
                </div>
                <div>
                  <div class="muted small">AGAINST</div>
                  {_cons_list(con_against)}
                </div>
              </div>
            </div>
          </div>

          <div class="subcard" style="margin-top:10px;">
            <div class="subttl">Top 3 drivers now</div>
            {top3_html or "<div class='muted'>—</div>"}
          </div>

          <div style="margin-top:10px;">
            <div class="subttl">WHY (top 5)</div>
            <ol class="why">{why_html or "<li>—</li>"}</ol>
          </div>

          <div style="margin-top:10px;">
            <div class="subttl">Latest relevant headlines</div>
            <ul class="news">{news_html or "<li class='muted'>—</li>"}</ul>
          </div>
        </div>
        """

    # Feeds health table
    rows_h = ""
    for src, stt in feeds_status.items():
        ok = stt.get("ok", True)
        bozo = stt.get("bozo", 0)
        entries = stt.get("entries", 0)
        icon = "✅" if ok and entries > 0 else ("⚠️" if ok else "❌")
        rows_h += f"""
          <tr>
            <td>{icon} {_html_escape(src)}</td>
            <td class="muted">bozo={_html_escape(str(bozo))}</td>
            <td class="muted">entries={_html_escape(str(entries))}</td>
          </tr>
        """

    # Event mode block
    hits_html = ""
    for h in event_hits:
        hits_html += f"<li><b>{_html_escape(h.get('source',''))}</b> ({h.get('age_min')}m): {_html_escape(h.get('title',''))}</li>"
    if not hits_html:
        hits_html = "<li class='muted'>—</li>"

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>News Bias Dashboard</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body{{font-family:Arial, sans-serif; max-width:1060px; margin:24px auto; padding:0 12px; background:#fff; color:#111;}}
        .muted{{color:#6b7280;}}
        .small{{font-size:12px;}}
        .card{{border:1px solid #e5e7eb; border-radius:14px; padding:14px; margin:12px 0; background:#fff;}}
        .card-h{{display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap;}}
        .title{{font-size:22px; font-weight:700;}}
        .actions{{display:flex; gap:8px; align-items:center;}}
        .btn{{border:1px solid #d1d5db; background:#f9fafb; padding:8px 10px; border-radius:10px; text-decoration:none; color:#111; cursor:pointer; font-size:14px;}}
        .btn:hover{{background:#f3f4f6;}}
        .badge{{padding:4px 10px;border-radius:999px; font-weight:700; font-size:14px;}}
        .bull{{background:#0b6; color:#fff;}}
        .bear{{background:#d33; color:#fff;}}
        .neu{{background:#6b7280; color:#fff;}}
        .grid{{display:grid; grid-template-columns: repeat(3, 1fr); gap:10px; margin-top:10px;}}
        @media (max-width: 900px){{ .grid{{grid-template-columns:1fr;}} }}
        .subcard{{border:1px solid #eef2f7; border-radius:12px; padding:12px; background:#fcfcfd;}}
        .subttl{{font-weight:700; margin-bottom:6px;}}
        .bar-wrap{{position:relative; height:16px; background:#eef2f7; border-radius:999px; overflow:hidden;}}
        .bar-fill{{height:100%; background:#f59e0b;}}
        .bar-text{{position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:12px; font-weight:700; color:#111;}}
        .chip{{display:inline-block; padding:4px 8px; border:1px solid #e5e7eb; border-radius:999px; margin:2px 4px 0 0; background:#fff; font-size:12px;}}
        .why{{margin:0; padding-left:18px;}}
        .why li{{margin-bottom:8px;}}
        .news{{margin:0; padding-left:18px;}}
        .driver{{border:1px solid #eef2f7; border-radius:12px; padding:10px; margin:8px 0; background:#fff;}}
        .driver-h{{display:grid; grid-template-columns: 48px 1fr; gap:8px; align-items:baseline;}}
        .driver-n{{font-weight:800;}}
        .driver-why{{font-weight:700;}}
        .driver-meta{{grid-column:2; font-size:12px; color:#6b7280;}}
        .driver-t{{margin-top:6px;}}
        .split{{display:grid; grid-template-columns:1fr 1fr; gap:10px;}}
        @media (max-width: 900px){{ .split{{grid-template-columns:1fr;}} }}
        .kv{{display:flex; justify-content:space-between; gap:10px; padding:2px 0;}}
        table{{width:100%; border-collapse:collapse;}}
        td{{padding:6px 8px; border-top:1px solid #eef2f7; font-size:14px;}}
        .topline{{display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;}}
        .pill{{padding:6px 10px; border-radius:999px; border:1px solid #e5e7eb; background:#fff;}}
        .ok{{color:#0b6; font-weight:800;}}
        .warn{{color:#f59e0b; font-weight:800;}}
      </style>
      <script>
        async function runNow() {{
          const btns = document.querySelectorAll('.btn');
          btns.forEach(b => b.disabled = true);
          try {{
            const r = await fetch('/run', {{cache:'no-store'}});
            if (!r.ok) throw new Error('run failed');
            // Reload dashboard after run
            window.location.reload();
          }} catch (e) {{
            alert('Run failed: ' + e);
          }} finally {{
            btns.forEach(b => b.disabled = false);
          }}
        }}
      </script>
    </head>
    <body>
      <div class="topline">
        <div>
          <h1 style="margin:0 0 4px 0;">News Bias Dashboard</h1>
          <div class="muted">updated_utc: {updated}</div>
          <div style="margin-top:6px;">
            <span class="pill">event_mode: <span class="{('warn' if event_mode else 'ok')}">{str(event_mode).upper()}</span></span>
            <span class="pill">Tip: open <b>/</b> → redirects here (use your domain as short link)</span>
          </div>
        </div>
        <div class="actions">
          <button class="btn" onclick="runNow()">Run now</button>
          <a class="btn" href="/ui/json" target="_blank">JSON</a>
        </div>
      </div>

      <div class="card">
        <div class="subttl">Macro “event mode” hits (last ~6h)</div>
        <ul style="margin:0; padding-left:18px;">{hits_html}</ul>
      </div>

      {render_asset("XAU")}
      {render_asset("US500")}
      {render_asset("WTI")}

      <div class="card">
        <div class="subttl">Feeds health (last run snapshot)</div>
        <table>
          <tr><td><b>Feed</b></td><td><b>Status</b></td><td><b>Entries</b></td></tr>
          {rows_h}
        </table>
        <div class="muted small" style="margin-top:8px;">
          Deep debug: <a href="/feeds_health" target="_blank">/feeds_health</a>
        </div>
      </div>

      <div class="muted small" style="margin:14px 0;">
        Debug: <a href="/ui/explain?asset=XAU" target="_blank">/ui/explain?asset=XAU</a> ·
        Raw API: <a href="/bias" target="_blank">/bias</a> · <a href="/run" target="_blank">/run</a>
      </div>
    </body>
    </html>
    """
    return html
