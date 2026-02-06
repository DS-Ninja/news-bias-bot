import os
import json
import time
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse


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

    # OPTIONAL (X / POTUS): нет нативного RSS, держи placeholder
    # "POTUS_X_RSS": "https://<your-rss-generator>/x.com/POTUS",
}

# impact weights by source (heuristic)
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

def _entry_published_ts(e: dict, now_ts: int) -> int:
    # Prefer published_parsed, but some feeds use updated_parsed
    pp = e.get("published_parsed")
    if pp:
        return int(time.mktime(pp))
    up = e.get("updated_parsed")
    if up:
        return int(time.mktime(up))
    return now_ts


def ingest_once(limit_per_feed: int = 30) -> Dict[str, Any]:
    inserted = 0
    now = int(time.time())

    per_feed = {}

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                try:
                    d = feedparser.parse(url)
                except Exception as ex:
                    per_feed[src] = {"ok": False, "error": str(ex), "entries": 0, "bozo": 1}
                    continue

                entries = getattr(d, "entries", []) or []
                bozo = int(getattr(d, "bozo", 0))

                # IMPORTANT:
                # bozo=1 can still be usable (you already saw: FED/BLS can be bozo=1 but entries exist)
                # We only skip when it's bozo AND empty.
                if bozo == 1 and len(entries) == 0:
                    per_feed[src] = {
                        "ok": False,
                        "error": str(getattr(d, "bozo_exception", "bozo + empty")),
                        "entries": 0,
                        "bozo": 1,
                    }
                    continue

                per_feed[src] = {"ok": True, "entries": len(entries), "bozo": bozo}

                for e in entries[:limit_per_feed]:
                    title = (e.get("title") or "").strip()
                    link = (e.get("link") or "").strip()
                    if not title or not link:
                        continue

                    published_ts = _entry_published_ts(e, now)
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

    return {"inserted": inserted, "feeds": per_feed}


# ============================================================
# SCORING
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


def _quality_score(abs_score: float, threshold: float, evidence_count: int, src_diversity: int) -> int:
    """
    Quality: 0..100
    - core: abs_score vs threshold (clipped)
    - bonus: more evidence and more sources (diversity)
    """
    if threshold <= 0:
        threshold = 1.0

    core = min(1.5, abs_score / threshold)  # allow >1 for strong signals, but clip
    core01 = min(1.0, core)                 # 0..1

    # evidence bonus: saturates quickly
    ev_bonus = min(0.20, evidence_count * 0.02)     # up to +0.20
    src_bonus = min(0.15, src_diversity * 0.03)     # up to +0.15

    q = (core01 + ev_bonus + src_bonus)
    q = max(0.0, min(1.0, q))
    return int(round(q * 100))


def compute_bias(lookback_hours: int = 24, limit_rows: int = 800) -> Dict[str, Any]:
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

        src_set = set()
        evidence_fp = set()  # unique news items contributing (by link/title)

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            src_set.add(source)
            evidence_fp.add(fingerprint(title, link))

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

        quality = _quality_score(
            abs_score=abs(total),
            threshold=th,
            evidence_count=len(evidence_fp),
            src_diversity=len(src_set),
        )

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "threshold": th,
            "quality": quality,                    # 0..100
            "evidence_count": len(evidence_fp),     # how many unique news items drove the score
            "source_diversity": len(src_set),
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


def pipeline_run() -> Dict[str, Any]:
    db_init()
    ingest_meta = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=24, limit_rows=800)
    payload["meta"]["inserted_last_run"] = ingest_meta.get("inserted", 0)
    payload["meta"]["feeds_status"] = ingest_meta.get("feeds", {})
    save_bias(payload)
    return payload


# ============================================================
# API
# ============================================================

app = FastAPI(title="News Bias Bot (MVP)")


@app.get("/", include_in_schema=False)
def root():
    # Short entrypoint -> dashboard
    return RedirectResponse(url="/dashboard", status_code=302)


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
    # Manual trigger from browser
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


@app.get("/feeds_health")
def feeds_health():
    out = {}
    for src, url in RSS_FEEDS.items():
        try:
            d = feedparser.parse(url)
            out[src] = {
                "bozo": int(getattr(d, "bozo", 0)),
                "entries": len(getattr(d, "entries", []) or []),
                "ok": True if len(getattr(d, "entries", []) or []) > 0 else False,
            }
        except Exception as e:
            out[src] = {"ok": False, "error": str(e)}
    return out


# ============================================================
# DASHBOARD
# ============================================================

def _badge(bias: str) -> str:
    if bias == "BULLISH":
        return '<span style="padding:4px 10px;border-radius:10px;background:#0b6; color:#fff;">BULLISH</span>'
    if bias == "BEARISH":
        return '<span style="padding:4px 10px;border-radius:10px;background:#d33; color:#fff;">BEARISH</span>'
    return '<span style="padding:4px 10px;border-radius:10px;background:#666; color:#fff;">NEUTRAL</span>'


def _bar(pct: int) -> str:
    pct = max(0, min(100, int(pct)))
    # neutral gray background, colored fill by pct thresholds
    fill = "#d33" if pct >= 70 else ("#f0a000" if pct >= 45 else "#666")
    return f"""
    <div style="width:220px;background:#eee;border-radius:999px;overflow:hidden;border:1px solid #ddd;display:inline-block;vertical-align:middle;">
      <div style="width:{pct}%;background:{fill};height:10px;"></div>
    </div>
    <span style="margin-left:8px;color:#555;">{pct}/100</span>
    """


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    st = load_bias()
    payload = st["payload"] if st else pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    feeds_status = payload.get("meta", {}).get("feeds_status", {})

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 70;
            """)
            rows = cur.fetchall()

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {"bias": "NEUTRAL", "score": 0.0, "why_top5": []})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        th = a.get("threshold", BIAS_THRESH.get(asset, 1.0))
        quality = int(a.get("quality", 0))
        evid = int(a.get("evidence_count", 0))
        div = int(a.get("source_diversity", 0))
        why = a.get("why_top5", [])

        why_html = ""
        for w in why:
            why_html += f"""
            <li style="margin-bottom:8px;">
              <div><b>{w.get('why','')}</b>
              <span style="color:#999;">(contrib={w.get('contrib','')}, src={w.get('source','')}, age={w.get('age_min','')}m)</span></div>
              <div style="color:#333;">{w.get('title','')}</div>
              <div style="margin-top:3px;"><a href="{w.get('link','')}" target="_blank" style="color:#2157f3;text-decoration:none;">open</a></div>
            </li>
            """

        kw = {
            "XAU": ["fed", "fomc", "cpi", "inflation", "yields", "usd", "risk", "gold", "treasury", "vix"],
            "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "rout", "slides", "rebound", "sp", "s&p", "nasdaq"],
            "WTI": ["oil", "crude", "opec", "inventory", "stocks", "tanker", "pipeline", "storm", "spr", "sanctions"],
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
            news_html += f'<li><a href="{link}" target="_blank" style="text-decoration:none;color:#222;">{title}</a> <span style="color:#999;">[{source}]</span></li>'

        return f"""
        <div style="border:1px solid #ddd;border-radius:14px;padding:14px;margin:12px 0;background:#fff;">
          <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;">
            <div>
              <h2 style="margin:0 0 6px 0;">{asset} {_badge(bias)}
                <span style="color:#666;font-size:14px;margin-left:10px;">score={score} / th={th}</span>
              </h2>
              <div style="margin:6px 0;color:#444;">
                <span style="margin-right:10px;"><b>Quality</b> { _bar(quality) }</span>
              </div>
              <div style="margin:6px 0;color:#666;font-size:13px;">
                evidence={evid} • source_diversity={div}
              </div>
            </div>
            <div style="min-width:260px;">
              <div style="border:1px solid #eee;border-radius:12px;padding:10px;background:#fafafa;">
                <div style="color:#555;font-size:12px;margin-bottom:6px;">Quick actions</div>
                <div style="display:flex;gap:8px;flex-wrap:wrap;">
                  <a href="/run" style="padding:6px 10px;border-radius:10px;background:#222;color:#fff;text-decoration:none;">Run now</a>
                  <a href="/bias" style="padding:6px 10px;border-radius:10px;background:#eee;color:#111;text-decoration:none;border:1px solid #ddd;">JSON</a>
                  <a href="/explain?asset={asset}" style="padding:6px 10px;border-radius:10px;background:#eee;color:#111;text-decoration:none;border:1px solid #ddd;">Explain</a>
                </div>
              </div>
            </div>
          </div>

          <div style="margin:12px 0;">
            <h3 style="margin:0 0 8px 0;">WHY (top 5)</h3>
            <ol style="margin:0;padding-left:18px;">{why_html or "<li>—</li>"}</ol>
          </div>

          <div style="margin:12px 0;">
            <h3 style="margin:0 0 8px 0;">Latest relevant headlines</h3>
            <ul style="margin:0;padding-left:18px;line-height:1.5;">{news_html or "<li>—</li>"}</ul>
          </div>
        </div>
        """

    # feed summary
    feed_rows = ""
    for k, v in (feeds_status or {}).items():
        ok = "✅" if v.get("ok") else "⚠️"
        feed_rows += f"<tr><td style='padding:6px 8px;border-bottom:1px solid #eee;'>{ok} {k}</td>" \
                     f"<td style='padding:6px 8px;border-bottom:1px solid #eee;color:#555;'>bozo={v.get('bozo')}</td>" \
                     f"<td style='padding:6px 8px;border-bottom:1px solid #eee;color:#555;'>entries={v.get('entries')}</td></tr>"

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <title>News Bias Dashboard</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
    </head>
    <body style="font-family:Arial, sans-serif; max-width:1060px; margin:24px auto; padding:0 12px; background:#f6f7f9;">
      <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:10px;flex-wrap:wrap;">
        <div>
          <h1 style="margin:0 0 6px 0;">News Bias Dashboard</h1>
          <div style="color:#666;margin-bottom:10px;">updated_utc: {updated}</div>
        </div>
        <div style="color:#666;font-size:12px;margin-bottom:10px;">
          Tip: open <b>/</b> (root) → redirects here. Use your domain as the short link.
        </div>
      </div>

      {render_asset("XAU")}
      {render_asset("US500")}
      {render_asset("WTI")}

      <div style="border:1px solid #ddd;border-radius:14px;padding:14px;margin:12px 0;background:#fff;">
        <h3 style="margin:0 0 8px 0;">Feeds health (last run snapshot)</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr>
              <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">Feed</th>
              <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">Status</th>
              <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee;">Entries</th>
            </tr>
          </thead>
          <tbody>{feed_rows or ""}</tbody>
        </table>
        <div style="color:#999;margin-top:10px;font-size:12px;">
          Deep debug: /feeds_health (live parse now)
        </div>
      </div>

      <div style="color:#999;margin-top:16px;font-size:12px;">
        Next upgrade: add “Signal Quality v2” (consensus, conflict, freshness buckets, macro-event tagging).
      </div>
    </body>
    </html>
    """
    return html
