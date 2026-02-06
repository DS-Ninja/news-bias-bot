import os
import json
import time
import re
import math
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import feedparser
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse

try:
    # no extra dependency required; urllib is stdlib
    from urllib.request import urlopen, Request
    from urllib.parse import urlencode
except Exception:
    urlopen = None


# ============================================================
# CONFIG
# ============================================================

ASSETS = ["XAU", "US500", "WTI"]

# --- Added feeds you provided + extra Investing feeds
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

    # Investing additional
    "INVESTING_STOCK_FUND": "https://www.investing.com/rss/stock_Fundamental.rss",
    "INVESTING_COMMOD_TECH": "https://www.investing.com/rss/commodities_Technical.rss",
    "INVESTING_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INVESTING_NEWS_95": "https://www.investing.com/rss/news_95.rss",
    "INVESTING_MKT_TECH": "https://www.investing.com/rss/market_overview_Technical.rss",
    "INVESTING_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INVESTING_IDEAS": "https://www.investing.com/rss/market_overview_investing_ideas.rss",
    "INVESTING_FX_TECH": "https://www.investing.com/rss/forex_Technical.rss",
    "INVESTING_FX_FUND": "https://www.investing.com/rss/forex_Fundamental.rss",

    # FXStreet
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "FXSTREET_STOCKS": "https://www.fxstreet.com/rss/stocks",

    # MarketWatch
    "MARKETWATCH_TOP": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_RT": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "MARKETWATCH_BREAKING": "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
    "MARKETWATCH_PULSE": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",

    # Financial Times
    "FT_PRECIOUS": "https://www.ft.com/precious-metals?format=rss",

    # DailyForex
    "DAILYFOREX_NEWS": "https://www.dailyforex.com/rss/forexnews.xml",
    "DAILYFOREX_TECH": "https://www.dailyforex.com/rss/technicalanalysis.xml",
    "DAILYFOREX_FUND": "https://www.dailyforex.com/rss/fundamentalanalysis.xml",

    # Your extra RSS
    "RSSAPP_1": "https://rss.app/feeds/X1lZYAmHwbEHR8OY.xml",
    "RSSAPP_2": "https://rss.app/feeds/BDVzmd6sW0mF8DJ6.xml",
    "POLITICO_TRUMP": "https://rss.politico.com/donald-trump.xml",
}

# ForexFactory calendar XML (weekly)
FOREXFACTORY_CAL_XML = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"

# Source weights (MVP; tune later)
SOURCE_WEIGHT: Dict[str, float] = {
    "FED": 3.0, "BLS": 3.0, "BEA": 2.5, "EIA": 3.0,

    "OILPRICE": 1.2,

    "INVESTING_NEWS_25": 1.0,
    "INVESTING_STOCK_FUND": 1.1,
    "INVESTING_COMMOD_TECH": 1.1,
    "INVESTING_NEWS_11": 1.0,
    "INVESTING_NEWS_95": 1.0,
    "INVESTING_MKT_TECH": 1.0,
    "INVESTING_MKT_FUND": 1.0,
    "INVESTING_IDEAS": 0.9,
    "INVESTING_FX_TECH": 1.0,
    "INVESTING_FX_FUND": 1.0,

    "FXSTREET_NEWS": 1.4, "FXSTREET_ANALYSIS": 1.2, "FXSTREET_STOCKS": 1.2,

    "MARKETWATCH_TOP": 1.6, "MARKETWATCH_RT": 1.6, "MARKETWATCH_BREAKING": 1.8, "MARKETWATCH_PULSE": 1.5,

    "FT_PRECIOUS": 1.6,

    "DAILYFOREX_NEWS": 1.2, "DAILYFOREX_TECH": 1.0, "DAILYFOREX_FUND": 1.2,

    "RSSAPP_1": 1.0, "RSSAPP_2": 1.0,
    "POLITICO_TRUMP": 1.1,
}

# Exponential decay: half-life 8h
HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9}

# Trade Gate tuning (you asked strict vs moderate)
# default: moderate
GATE_PROFILE_DEFAULT = os.environ.get("GATE_PROFILE", "moderate").lower().strip()
GATE_PROFILES = {
    "strict":   {"q_min": 75, "conf_max": 0.45, "delta_to_th_max": 0.25, "event_q_min": 82},
    "moderate": {"q_min": 60, "conf_max": 0.60, "delta_to_th_max": 0.45, "event_q_min": 70},
}

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
# UTIL
# ============================================================

def now_ts() -> int:
    return int(time.time())


def decay_weight(age_sec: int) -> float:
    return float(math.exp(-LAMBDA * max(0, age_sec)))


def match_rules(asset: str, text: str) -> List[Dict[str, Any]]:
    tl = (text or "").lower()
    out = []
    for pattern, w, why in RULES.get(asset, []):
        if re.search(pattern, tl, flags=re.IGNORECASE):
            out.append({"pattern": pattern, "w": float(w), "why": why})
    return out


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

def ingest_once(limit_per_feed: int = 30) -> int:
    inserted = 0
    now = now_ts()

    with db_conn() as conn:
        with conn.cursor() as cur:
            for src, url in RSS_FEEDS.items():
                d = feedparser.parse(url)
                entries = getattr(d, "entries", []) or []

                # If bozo==1 but entries exist — ingest anyway (FED/OILPRICE often do this).
                # If bozo==1 and entries==0 — truly broken, skip.
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
# EVENT MODE (ForexFactory XML)
# ============================================================

def _http_get_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 12) -> str:
    if urlopen is None:
        raise RuntimeError("urllib not available")
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")


def parse_ff_calendar_thisweek(max_items: int = 120) -> Dict[str, Any]:
    """
    Best-effort parser for ForexFactory weekly XML.
    We only need: upcoming USD High/Medium events within next N hours.
    """
    out = {"ok": False, "items": [], "error": ""}
    try:
        xml_txt = _http_get_text(FOREXFACTORY_CAL_XML, headers={"User-Agent": "Mozilla/5.0"})
        root = ET.fromstring(xml_txt)

        now = datetime.now(timezone.utc)
        items = []

        # The ff xml structure may vary; we try robust find.
        # Typical: <event> <title> <country> <impact> <date> <time> ...
        for ev in root.findall(".//event"):
            title = (ev.findtext("title") or "").strip()
            country = (ev.findtext("country") or "").strip()
            impact = (ev.findtext("impact") or "").strip()
            date_s = (ev.findtext("date") or "").strip()
            time_s = (ev.findtext("time") or "").strip()

            if not title:
                continue

            # Parse datetime best-effort
            dt_utc = None
            # FF sometimes uses "Jan 06" + "13:30" etc; timezone unclear.
            # We treat it as UTC best-effort; this is "event mode", not exact trading execution.
            try:
                # Try: "Feb 06" + "13:30"
                # Add current year
                year = now.year
                dt = datetime.strptime(f"{date_s} {year} {time_s}".strip(), "%b %d %Y %H:%M")
                dt_utc = dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass

            items.append({
                "title": title,
                "country": country,
                "impact": impact,
                "dt_utc": dt_utc.isoformat() if dt_utc else "",
            })

            if len(items) >= max_items:
                break

        out["ok"] = True
        out["items"] = items
        return out
    except Exception as e:
        out["error"] = str(e)
        return out


def event_mode_flag(ff: Dict[str, Any], hours_ahead: int = 12) -> Dict[str, Any]:
    """
    Returns per-asset event flags (simple v1):
    - if there is a USD high-impact event within next 12h -> event_mode = True for XAU/US500/WTI
    """
    now = datetime.now(timezone.utc)
    until = now.timestamp() + hours_ahead * 3600
    reasons = []

    if not ff.get("ok"):
        return {"event_mode": False, "reasons": ["FF calendar not available"]}

    for it in ff.get("items", []):
        if (it.get("country") or "").upper() not in ("USD", "US", "UNITED STATES"):
            continue
        impact = (it.get("impact") or "").lower()
        if "high" not in impact and "medium" not in impact:
            continue

        dt_s = it.get("dt_utc") or ""
        if not dt_s:
            continue
        try:
            dt = datetime.fromisoformat(dt_s.replace("Z", "+00:00"))
            if now.timestamp() <= dt.timestamp() <= until:
                reasons.append(f"{it.get('impact','').upper()} USD: {it.get('title','')}")
        except Exception:
            continue

    if reasons:
        return {"event_mode": True, "reasons": reasons[:5]}
    return {"event_mode": False, "reasons": []}


# ============================================================
# SCORING + QUALITY V2 + CONSENSUS + CONFLICT + FRESHNESS
# ============================================================

def compute_bias(lookback_hours: int = 24, limit_rows: int = 900) -> Dict[str, Any]:
    now = now_ts()
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

    ff = parse_ff_calendar_thisweek()
    evflag = event_mode_flag(ff, hours_ahead=12)

    assets_out: Dict[str, Any] = {}

    for asset in ASSETS:
        total = 0.0
        why_all: List[Dict[str, Any]] = []
        abs_sum = 0.0

        # for consensus/conflict
        by_source: Dict[str, float] = {}
        pos_abs = 0.0
        neg_abs = 0.0

        # freshness buckets
        b_0_2 = 0
        b_2_8 = 0
        b_8_24 = 0

        for (source, title, link, ts) in rows:
            age = now - int(ts)
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)

            matches = match_rules(asset, title)
            if not matches:
                continue

            # freshness counters (by matched headlines)
            age_h = age / 3600.0
            if age_h <= 2:
                b_0_2 += 1
            elif age_h <= 8:
                b_2_8 += 1
            else:
                b_8_24 += 1

            for m in matches:
                base_w = float(m["w"])
                contrib = base_w * w_src * w_time

                total += contrib
                abs_sum += abs(contrib)

                if contrib >= 0:
                    pos_abs += abs(contrib)
                else:
                    neg_abs += abs(contrib)

                by_source[source] = by_source.get(source, 0.0) + contrib

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

        th = float(BIAS_THRESH.get(asset, 1.0))
        if total >= th:
            bias = "BULLISH"
        elif total <= -th:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        why_sorted = sorted(why_all, key=lambda x: abs(float(x["contrib"])), reverse=True)
        why_top5 = why_sorted[:5]

        # --- Conflict index (0..1): opposite pressure vs total
        # If total >=0, conflict = neg_abs/(pos_abs+neg_abs)
        # If total <0, conflict = pos_abs/(pos_abs+neg_abs)
        denom = max(1e-9, (pos_abs + neg_abs))
        if total >= 0:
            conflict = float(neg_abs / denom)
        else:
            conflict = float(pos_abs / denom)
        conflict = max(0.0, min(1.0, conflict))

        # --- Consensus (sources for/against)
        src_for = sum(1 for s, v in by_source.items() if v * total > 0 and abs(v) > 1e-6)
        src_against = sum(1 for s, v in by_source.items() if v * total < 0 and abs(v) > 1e-6)

        # --- Delta to threshold (what to flip)
        # If NEUTRAL: need reach +th or -th (we compute nearest)
        if bias == "NEUTRAL":
            delta_to_bull = th - total
            delta_to_bear = th + total  # how far to go down to -th
            delta_to_th = min(delta_to_bull, delta_to_bear)
        else:
            delta_to_th = max(0.0, th - abs(total))
        delta_to_th = float(delta_to_th)

        # --- Quality v2 (0..100)
        # components:
        # 1) strength vs threshold (0..1)
        strength = min(1.0, abs(total) / max(th, 1e-9))
        # 2) evidence saturation (0..1)
        evidence = len(why_all)
        ev_sat = min(1.0, evidence / 20.0)
        # 3) source diversity (0..1)
        src_div = len(set([w["source"] for w in why_all])) if why_all else 0
        div_sat = min(1.0, src_div / 8.0)
        # 4) freshness (0..1)
        fresh = (min(10, b_0_2) * 1.0 + min(10, b_2_8) * 0.6 + min(10, b_8_24) * 0.2) / 10.0
        fresh = max(0.0, min(1.0, fresh))
        # 5) conflict penalty
        # high conflict reduces quality
        conf_pen = (1.0 - conflict)

        q = 100.0 * (0.40 * strength + 0.18 * ev_sat + 0.14 * div_sat + 0.18 * fresh + 0.10 * conf_pen)
        quality_v2 = int(max(0, min(100, round(q))))

        # --- Trade Gate (per asset)
        prof = GATE_PROFILES.get(GATE_PROFILE_DEFAULT, GATE_PROFILES["moderate"])
        q_min = int(prof["q_min"])
        conf_max = float(prof["conf_max"])
        delta_max = float(prof["delta_to_th_max"])
        event_q_min = int(prof["event_q_min"])

        event_mode = bool(evflag.get("event_mode", False))
        gate_ok = True
        gate_reasons_no = []
        gate_reasons_yes = []

        if bias == "NEUTRAL":
            gate_ok = False
            gate_reasons_no.append("Bias NEUTRAL")
        else:
            gate_reasons_yes.append(f"Bias {bias}")

        if quality_v2 < q_min:
            gate_ok = False
            gate_reasons_no.append(f"Quality_v2<{q_min}")
        else:
            gate_reasons_yes.append(f"Quality_v2≥{q_min}")

        if conflict > conf_max:
            gate_ok = False
            gate_reasons_no.append(f"Conflict>{conf_max}")
        else:
            gate_reasons_yes.append(f"Conflict≤{conf_max}")

        if delta_to_th > delta_max:
            gate_ok = False
            gate_reasons_no.append(f"ΔtoTH>{delta_max}")
        else:
            gate_reasons_yes.append(f"ΔtoTH≤{delta_max}")

        if event_mode and quality_v2 < event_q_min:
            gate_ok = False
            gate_reasons_no.append(f"EventMode and Quality<{event_q_min}")

        # "What would flip to OK" targets
        flip_conditions = {
            "need_conflict_below": conf_max,
            "need_quality_at_least": max(q_min, event_q_min if event_mode else q_min),
            "need_delta_to_th_below": delta_max,
            "need_bias_non_neutral": True,
        }

        # Top drivers now: take top 3 contributions
        top3 = why_sorted[:3]

        assets_out[asset] = {
            "bias": bias,
            "score": round(total, 4),
            "threshold": th,
            "delta_to_th": round(delta_to_th, 4),

            "quality_v2": quality_v2,
            "evidence_count": int(evidence),
            "source_diversity": int(src_div),

            "consensus": {"sources_for": int(src_for), "sources_against": int(src_against)},
            "conflict_index": round(conflict, 4),

            "freshness": {"h0_2": int(b_0_2), "h2_8": int(b_2_8), "h8_24": int(b_8_24)},

            "trade_gate": {
                "ok": bool(gate_ok),
                "profile": GATE_PROFILE_DEFAULT,
                "why_yes": gate_reasons_yes[:3],
                "why_no": gate_reasons_no[:3],
                "flip_conditions": flip_conditions,
            },

            "top3_drivers": top3,
            "why_top5": why_top5,
        }

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "meta": {
            "lookback_hours": lookback_hours,
            "feeds": list(RSS_FEEDS.keys()),
            "feeds_status": feeds_health_live(),
            "event_mode": evflag,
            "ff_calendar_ok": bool(ff.get("ok")),
        }
    }
    return payload


# ============================================================
# STATE
# ============================================================

def save_bias(payload: dict):
    now = now_ts()
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
    inserted = ingest_once(limit_per_feed=30)
    payload = compute_bias(lookback_hours=24, limit_rows=900)
    payload["meta"]["inserted_last_run"] = inserted
    save_bias(payload)
    return payload


# ============================================================
# DRIVERS (FRED) - cached
# ============================================================

DRIVERS_CACHE: Dict[str, Any] = {"ts": 0, "payload": None}
DRIVERS_TTL_SEC = 120  # 2 min

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    # USD / FX
    "DEXUSEU": "EURUSD (USD per EUR)",
    "DEXJPUS": "USDJPY (JPY per USD)",
    "DTWEXBGS": "Broad USD index (proxy)",
    # rates / real yields / inflation
    "DGS10": "US 10Y nominal yield",
    "DFII10": "US 10Y real yield (TIPS)",
    "T10YIE": "US 10Y breakeven inflation",
    # risk
    "VIXCLS": "VIX",
    "SP500": "S&P 500",
    # oil
    "DCOILWTICO": "WTI spot",
}

def fred_latest(series_id: str) -> Dict[str, Any]:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        return {"ok": False, "error": "FRED_API_KEY not set"}

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 2,
    }
    url = FRED_BASE + "?" + urlencode(params)
    try:
        txt = _http_get_text(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(txt)
        obs = (data.get("observations") or [])
        # pick first non "." value
        for o in obs:
            v = o.get("value")
            if v is None or v == ".":
                continue
            return {"ok": True, "series_id": series_id, "date": o.get("date"), "value": v}
        return {"ok": False, "error": "no valid observations"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def drivers_payload() -> Dict[str, Any]:
    # cached
    now = now_ts()
    if DRIVERS_CACHE["payload"] and (now - int(DRIVERS_CACHE["ts"])) < DRIVERS_TTL_SEC:
        return DRIVERS_CACHE["payload"]

    snap = {}
    for sid, name in FRED_SERIES.items():
        snap[sid] = {"name": name, **fred_latest(sid)}

    payload = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "series": snap,
        "mapping": {
            "GOLD": ["DTWEXBGS", "DEXUSEU", "DFII10", "DGS10", "VIXCLS", "SP500"],
            "US500": ["SP500", "VIXCLS", "DGS10", "DFII10", "DTWEXBGS", "DEXUSEU"],
            "WTI": ["DCOILWTICO", "DTWEXBGS", "DGS10", "VIXCLS", "SP500"],
        }
    }
    DRIVERS_CACHE["ts"] = now
    DRIVERS_CACHE["payload"] = payload
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
        "items": [{"source": s, "title": t, "link": l, "published_ts": int(ts)} for (s, t, l, ts) in rows]
    }


@app.get("/explain")
def explain(asset: str = "US500", limit: int = 50):
    asset = asset.upper().strip()
    if asset not in ASSETS:
        return {"error": f"asset must be one of {ASSETS}"}

    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    # reuse already computed detailed matches via re-run light computation on recent rows
    now = now_ts()
    cutoff = now - 24 * 3600

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
                "pattern": m["pattern"],
            })

    out_sorted = sorted(out, key=lambda x: abs(float(x["contrib"])), reverse=True)[:limit]
    return {"asset": asset, "top_matches": out_sorted, "rules_count": len(RULES.get(asset, []))}


@app.get("/feeds_health")
def feeds_health():
    return feeds_health_live()


@app.get("/drivers")
def drivers(pretty: int = 0):
    p = drivers_payload()
    if pretty:
        return PlainTextResponse(json.dumps(p, ensure_ascii=False, indent=2), media_type="application/json")
    return JSONResponse(p)


# ============================================================
# DASHBOARD UI (iPhone-friendly, theme+compact)
# ============================================================

def _pill(kind: str, text: str) -> str:
    cls = "pill"
    if kind == "bull":
        cls += " pill-bull"
    elif kind == "bear":
        cls += " pill-bear"
    elif kind == "ok":
        cls += " pill-ok"
    elif kind == "no":
        cls += " pill-no"
    else:
        cls += " pill-neutral"
    return f'<span class="{cls}">{text}</span>'


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(theme: str = "dark", compact: int = 0):
    db_init()
    payload = load_bias()
    if not payload:
        payload = pipeline_run()

    assets = payload.get("assets", {})
    updated = payload.get("updated_utc", "")
    meta = payload.get("meta", {}) or {}
    feeds_status = meta.get("feeds_status", {}) or {}
    ev = meta.get("event_mode", {}) or {}

    # latest headlines
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, title, link, published_ts
                FROM news_items
                ORDER BY published_ts DESC
                LIMIT 120;
            """)
            rows = cur.fetchall()

    # rules overview
    rules_overview = ""
    for a in ASSETS:
        rules_overview += f"<li><b>{a}</b>: {len(RULES.get(a, []))} rules</li>"

    # feeds table
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

    dark = (theme or "dark").lower().strip() != "light"
    compact_cls = "compact" if int(compact or 0) == 1 else ""

    def render_asset(asset: str) -> str:
        a = assets.get(asset, {})
        bias = a.get("bias", "NEUTRAL")
        score = a.get("score", 0.0)
        th = a.get("threshold", 1.0)
        delta = a.get("delta_to_th", 0.0)

        q2 = int(a.get("quality_v2", 0))
        conflict = float(a.get("conflict_index", 0.0))
        cons = a.get("consensus", {}) or {}
        fresh = a.get("freshness", {}) or {}

        gate = a.get("trade_gate", {}) or {}
        gate_ok = bool(gate.get("ok", False))
        gate_prof = gate.get("profile", "")
        why_yes = gate.get("why_yes", []) or []
        why_no = gate.get("why_no", []) or []
        flip = gate.get("flip_conditions", {}) or {}

        top3 = a.get("top3_drivers", []) or []
        why_top5 = a.get("why_top5", []) or []

        if bias == "BULLISH":
            btag = _pill("bull", "BULLISH")
        elif bias == "BEARISH":
            btag = _pill("bear", "BEARISH")
        else:
            btag = _pill("neutral", "NEUTRAL")

        gtag = _pill("ok", "✅ TRADE OK") if gate_ok else _pill("no", "❌ NO TRADE")

        # WHY (2-3)
        why_short = (why_yes if gate_ok else why_no)[:3]
        why_short_html = "".join([f"<li>{x}</li>" for x in why_short]) or "<li>—</li>"

        # What would flip
        flip_html = f"""
          <div class="muted tiny">
            Flip-to-OK conditions:
            conflict&lt;{flip.get("need_conflict_below")} • quality≥{flip.get("need_quality_at_least")} • ΔtoTH&lt;{flip.get("need_delta_to_th_below")}
          </div>
        """

        # drivers top3 from RSS scoring
        top3_html = ""
        for w in top3:
            top3_html += f"""
            <li>
              <div class="why-row">
                <div class="why-title"><b>{w.get('why','')}</b></div>
                <div class="why-meta">contrib={w.get('contrib','')}, {w.get('source','')}, age={w.get('age_min','')}m</div>
              </div>
              <div class="why-headline">{w.get('title','')}</div>
            </li>
            """
        if not top3_html:
            top3_html = "<li>—</li>"

        # why top5 detailed
        why_html = ""
        for w in why_top5:
            why_html += f"""
            <li>
              <div class="why-row">
                <div class="why-title"><b>{w.get('why','')}</b></div>
                <div class="why-meta">contrib={w.get('contrib','')}, {w.get('source','')}, age={w.get('age_min','')}m</div>
              </div>
              <div class="why-headline">{w.get('title','')}</div>
              <a class="tiny" href="{w.get('link','')}" target="_blank" rel="noopener">open</a>
            </li>
            """
        if not why_html:
            why_html = "<li>—</li>"

        # latest relevant headlines
        kw = {
            "XAU": ["fed", "fomc", "cpi", "inflation", "yields", "usd", "risk", "gold", "treasury", "dollar"],
            "US500": ["stocks", "futures", "earnings", "downgrade", "upgrade", "rout", "slides", "rebound", "s&p", "nasdaq", "dow"],
            "WTI": ["oil", "crude", "opec", "inventory", "stocks", "tanker", "pipeline", "storm", "spr", "sanctions"],
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
        if not news_html:
            news_html = "<li>—</li>"

        # small metrics
        metrics = f"""
        <div class="metrics">
          <div class="m">{_pill("neutral", f"Qv2 {q2}/100")}</div>
          <div class="m">{_pill("neutral", f"Conflict {round(conflict,2)}")}</div>
          <div class="m">{_pill("neutral", f"Consensus +{cons.get('sources_for',0)} / -{cons.get('sources_against',0)}")}</div>
          <div class="m">{_pill("neutral", f"Fresh 0–2h:{fresh.get('h0_2',0)} 2–8h:{fresh.get('h2_8',0)} 8–24h:{fresh.get('h8_24',0)}")}</div>
        </div>
        """

        return f"""
        <section class="card" id="card-{asset}">
          <div class="card-head">
            <div class="left">
              <div class="h2">{asset} {btag} <span class="muted">score={score} / th={th} / Δ={delta}</span></div>
              <div class="gate">{gtag} <span class="muted tiny">profile={gate_prof}</span></div>
              {metrics}
              <div class="block">
                <div class="h3">WHY gate (2–3)</div>
                <ul class="bul">{why_short_html}</ul>
                {flip_html}
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
              <div class="h3">Top 3 drivers now (RSS)</div>
              <ol class="why">{top3_html}</ol>
              <div class="h3" style="margin-top:12px;">WHY (top 5)</div>
              <ol class="why">{why_html}</ol>
            </div>
            <div>
              <div class="h3">Latest relevant headlines</div>
              <ul class="news">{news_html}</ul>
            </div>
          </div>
        </section>
        """

    ev_html = ""
    if ev.get("event_mode"):
        rs = ev.get("reasons", []) or []
        ev_html = "<ul class='bul'>" + "".join([f"<li>{r}</li>" for r in rs]) + "</ul>"
    else:
        ev_html = "<div class='muted tiny'>No USD High/Medium events detected in next 12h (best-effort).</div>"

    # Theme variables
    if dark:
        css_vars = """
          --bg:#0b0f17; --card:#121a26; --muted:#93a4b8; --text:#e9f1ff;
          --line:rgba(255,255,255,.10);
          --bull:#10b981; --bear:#ef4444; --neu:#64748b;
          --btn:#1b2636; --btn2:#223047;
          --ok:#22c55e; --no:#ef4444;
        """
    else:
        css_vars = """
          --bg:#f6f7fb; --card:#ffffff; --muted:#52657a; --text:#0b1220;
          --line:rgba(2,6,23,.12);
          --bull:#0f766e; --bear:#b91c1c; --neu:#475569;
          --btn:#eef2ff; --btn2:#e0e7ff;
          --ok:#16a34a; --no:#dc2626;
        """

    html = f"""
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
      <title>News Bias Dashboard</title>
      <style>
        :root {{
          {css_vars}
        }}
        body {{
          font-family: Arial, sans-serif;
          background:var(--bg);
          color:var(--text);
          margin:0;
          padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
        }}
        .wrap {{ max-width: 1080px; margin: 18px auto; padding: 0 14px; }}
        .top {{
          display:flex; justify-content:space-between; align-items:flex-end; gap:12px;
          position: sticky; top: 0; z-index: 50;
          background: linear-gradient(to bottom, rgba(0,0,0,0.18), rgba(0,0,0,0.0));
          backdrop-filter: blur(6px);
          padding: 12px 0;
        }}
        h1 {{ margin:0; font-size: 24px; }}
        .muted {{ color:var(--muted); }}
        .tiny {{ font-size:12px; }}
        .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px; margin: 12px 0; }}
        .card-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; }}
        .h2 {{ font-size:20px; font-weight:800; }}
        .h3 {{ font-size:14px; margin: 8px 0; }}
        .pill {{ padding:4px 10px; border-radius:999px; font-size:12px; font-weight:800; display:inline-block; }}
        .pill-bull {{ background:rgba(16,185,129,.14); color:var(--bull); border:1px solid rgba(16,185,129,.28); }}
        .pill-bear {{ background:rgba(239,68,68,.14); color:var(--bear); border:1px solid rgba(239,68,68,.28); }}
        .pill-neutral {{ background:rgba(100,116,139,.14); color:var(--neu); border:1px solid rgba(100,116,139,.28); }}
        .pill-ok {{ background:rgba(34,197,94,.14); color:var(--ok); border:1px solid rgba(34,197,94,.28); }}
        .pill-no {{ background:rgba(239,68,68,.14); color:var(--no); border:1px solid rgba(239,68,68,.28); }}

        .grid {{ display:grid; grid-template-columns: 1.15fr 1fr; gap:16px; }}
        @media(max-width: 920px) {{
          .grid {{ grid-template-columns: 1fr; }}
          .card-head {{ flex-direction:column; }}
          .actions {{ width:100%; text-align:left; }}
          .btnrow {{ justify-content:flex-start; }}
        }}

        .why, .news {{ margin:0; padding-left:18px; }}
        .why li {{ margin: 10px 0; }}
        .why-row {{ display:flex; justify-content:space-between; gap:10px; }}
        .why-meta {{ color:var(--muted); font-size:12px; white-space:nowrap; }}
        .why-headline {{ margin-top:4px; }}
        a {{ color:#0284c7; text-decoration:none; }}
        a:hover {{ text-decoration:underline; }}

        .actions {{ min-width: 280px; text-align:right; }}
        .btnrow {{ display:flex; gap:8px; justify-content:flex-end; flex-wrap:wrap; }}
        .btn {{
          background:var(--btn);
          border:1px solid var(--line);
          color:var(--text);
          padding:10px 12px;
          border-radius:12px;
          cursor:pointer;
          font-weight:800;
        }}
        .btn:hover {{ background:var(--btn2); }}

        .gate {{ margin-top:8px; display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
        .metrics {{ margin-top:10px; display:flex; gap:8px; flex-wrap:wrap; }}
        .block {{ margin-top:10px; padding-top:10px; border-top:1px solid var(--line); }}
        .bul {{ margin:0; padding-left:18px; }}
        .table {{ width:100%; border-collapse:collapse; }}
        .table td {{ border-top:1px solid var(--line); padding:8px 6px; }}

        .modal {{
          position:fixed; inset:0; background:rgba(0,0,0,.55);
          display:none; align-items:center; justify-content:center; padding:16px;
        }}
        .modal.show {{ display:flex; }}
        .modal-box {{
          width:min(980px, 100%); max-height: 85vh; overflow:auto;
          background:var(--card); border:1px solid var(--line); border-radius:16px; padding:14px;
        }}
        pre {{
          background:rgba(127,127,127,.12);
          padding:12px; border-radius:12px; overflow:auto;
        }}
        .modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; }}

        /* compact mode */
        .compact .card {{ padding: 12px; }}
        .compact .why li {{ margin: 7px 0; }}
        .compact .btn {{ padding: 9px 10px; border-radius: 10px; }}
      </style>
    </head>
    <body class="{compact_cls}">
      <div class="wrap">
        <div class="top">
          <div>
            <h1>News Bias Dashboard</h1>
            <div class="muted tiny">updated_utc: {updated}</div>
            <div class="muted tiny">Event mode: {"ON" if ev.get("event_mode") else "OFF"}</div>
          </div>
          <div class="muted tiny" style="text-align:right;">
            <div>Short link: use your domain → <b>/</b></div>
            <div>
              <a href="/dashboard?theme={'light' if dark else 'dark'}&compact={1 if compact==0 else 0}">toggle theme/compact</a>
              • <a href="/drivers?pretty=1" target="_blank" rel="noopener">drivers JSON</a>
            </div>
          </div>
        </div>

        <section class="card">
          <div class="h3">Macro event mode (next 12h, best-effort)</div>
          {ev_html}
          <div class="muted tiny" style="margin-top:8px;">
            Source: ForexFactory weekly XML feed (parsed best-effort).
          </div>
        </section>

        {render_asset("XAU")}
        {render_asset("US500")}
        {render_asset("WTI")}

        <section class="card">
          <div class="h3">Rules overview</div>
          <ul class="bul">{rules_overview}</ul>
        </section>

        <section class="card">
          <div class="h3">Feeds health (last run snapshot)</div>
          <table class="table"><tbody>{feeds_rows}</tbody></table>
          <div class="muted tiny" style="margin-top:10px;">
            Deep debug: <a href="/feeds_health" target="_blank" rel="noopener">/feeds_health</a>
            • Bias JSON: <a href="/bias?pretty=1" target="_blank" rel="noopener">/bias?pretty=1</a>
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
            const txt = await resp.text();
            // try parse JSON
            let data = null;
            try {{ data = JSON.parse(txt); }} catch(e) {{}}
            el.innerText = data ? ('Updated: ' + (data.updated_utc || '')) : 'Updated (non-json)';
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
            const resp = await fetch('/explain?asset=' + encodeURIComponent(asset) + '&limit=80');
            const txt = await resp.text();
            let data = null;
            try {{ data = JSON.parse(txt); }} catch(e) {{}}
            if (!data) {{
              openModal('Explain ' + asset, '<pre>' + escapeHtml(txt) + '</pre>');
              return;
            }}
            if (data.error) {{
              openModal('Explain ' + asset, '<div class="muted">' + escapeHtml(data.error) + '</div>');
              return;
            }}
            const rows = (data.top_matches || []).map(x => {{
              return `<tr>
                <td style="padding:6px;border-top:1px solid rgba(127,127,127,.20);">
                  <b>${{escapeHtml(x.why || '')}}</b>
                  <div class="muted tiny">${{escapeHtml(x.source || '')}} • age=${{x.age_min}}m • contrib=${{x.contrib}}</div>
                  <div class="muted tiny">pattern: ${{escapeHtml(x.pattern || '')}}</div>
                </td>
                <td style="padding:6px;border-top:1px solid rgba(127,127,127,.20);">
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
