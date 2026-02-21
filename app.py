# app.py - NEWS BIAS TERMINAL v4 - Bloomberg Clean Style
# Assets: XAU, US500, WTI, ETH, BTC, EUR
# Improvements: BTC/EUR rules, more RSS feeds, improved scoring, slower ticker

import os, json, time, re, hashlib, math, hmac, calendar as pycalendar, threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
import feedparser, psycopg2, socket
from psycopg2 import extras
from concurrent.futures import ThreadPoolExecutor, as_completed

socket.setdefaulttimeout(float(os.environ.get("HTTP_TIMEOUT", "12")))

try:
    from psycopg2.pool import ThreadedConnectionPool
except: ThreadedConnectionPool = None

try: import requests
except: requests = None

# === AUTO-SCHEDULER ===
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    _APSCHEDULER_AVAILABLE = True
except ImportError:
    _APSCHEDULER_AVAILABLE = False

from fastapi import FastAPI, Header
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

# ── NEW: BTC and EUR added ──
ASSETS = ["XAU", "US500", "WTI", "ETH", "BTC", "EUR"]
GATE_PROFILE = os.environ.get("GATE_PROFILE", "STRICT").strip().upper()
if GATE_PROFILE not in ("STRICT", "MODERATE"): GATE_PROFILE = "STRICT"

GATE_THRESHOLDS = {
    "STRICT": {"quality_v2_min": 55, "conflict_max": 0.55, "min_opp_flip_dist": 0.35, "neutral_allow": False,
               "event_mode_block": True, "event_override_quality": 70, "event_override_conflict": 0.45},
    "MODERATE": {"quality_v2_min": 42, "conflict_max": 0.70, "min_opp_flip_dist": 0.20, "neutral_allow": True,
                 "event_mode_block": False, "event_override_quality": 60, "event_override_conflict": 0.60}
}

HALF_LIFE_SEC = 8 * 3600
LAMBDA = 0.69314718056 / HALF_LIFE_SEC

# ── IMPROVED: separate thresholds, BTC/EUR added ──
BIAS_THRESH = {"US500": 1.2, "XAU": 0.9, "WTI": 0.9, "ETH": 1.0, "BTC": 1.0, "EUR": 0.7}

TRUMP_ENABLED = os.environ.get("TRUMP_ENABLED", "1").strip() == "1"
TRUMP_PAT = re.compile(r"\b(trump|donald trump|white house)\b", re.I)

EVENT_CFG = {"enabled": True, "lookahead_hours": float(os.environ.get("EVENT_LOOKAHEAD_HOURS", "18")),
             "recent_hours": float(os.environ.get("EVENT_RECENT_HOURS", "6")), "max_upcoming": 12}

FRED_CFG = {"enabled": os.environ.get("FRED_ENABLED", "1").strip() == "1",
            "api_key": os.environ.get("FRED_API_KEY", "").strip(), "window_days": 120}
FRED_SERIES = {"DGS10": {}, "DFII10": {}, "T10YIE": {}, "DTWEXBGS": {}, "VIXCLS": {}, "BAA10Y": {}}

RUN_MAX_SEC = float(os.environ.get("RUN_MAX_SEC", "10"))
RUN_LOCK_TIMEOUT_SEC = 25
RSS_LIMIT, CAL_LIMIT = 18, 120
FEEDS_HEALTH_TTL_SEC, CAL_INGEST_TTL_SEC = 90, 300
RSS_PARSE_WORKERS = 10          # ++ increased workers for more feeds
FRED_ON_RUN = os.environ.get("FRED_ON_RUN", "1").strip() == "1"

AUTO_REFRESH_MINUTES = int(os.environ.get("AUTO_REFRESH_MINUTES", "15"))

_PIPELINE_LOCK = threading.Lock()
_FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS = None, 0
_CONN_FROM_POOL = set()

def get_run_tokens():
    rt = os.environ.get("RUN_TOKEN", "").strip()
    rts = os.environ.get("RUN_TOKENS", "").strip()
    toks = [rt] if rt else []
    if rts: toks.extend([x.strip() for x in rts.split(",") if x.strip()])
    return list(dict.fromkeys(toks))

MYFXBOOK_IFRAME = "https://widget.myfxbook.com/widget/calendar.html?lang=en&impacts=0,1,2,3&symbols=USD"

# ── NEW: BTC and EUR added to ticker tape, slower display ──
TV_SYMBOLS = [
    {"proName": "ICMARKETS:XAUUSD",  "title": "XAU"},
    {"proName": "ICMARKETS:US500",   "title": "US500"},
    {"proName": "ICMARKETS:XTIUSD",  "title": "WTI"},
    {"proName": "COINBASE:ETHUSD",   "title": "ETH"},
    {"proName": "COINBASE:BTCUSD",   "title": "BTC"},
    {"proName": "ICMARKETS:EURUSD",  "title": "EUR"},
    {"proName": "ICMARKETS:USDJPY",  "title": "JPY"},
]

RSS_FEEDS = {
    # === EXISTING FEEDS ===
    "FED": "https://www.federalreserve.gov/feeds/press_all.xml",
    "BLS": "https://www.bls.gov/feed/news_release.rss",
    "BEA": "https://apps.bea.gov/rss/rss.xml",
    "OILPRICE": "https://oilprice.com/rss/main",
    "FXSTREET_NEWS": "https://www.fxstreet.com/rss/news",
    "FXSTREET_ANALYSIS": "https://www.fxstreet.com/rss/analysis",
    "MARKETWATCH_TOP": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MARKETWATCH_REALTIME": "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    "INV_NEWS_11": "https://www.investing.com/rss/news_11.rss",
    "INV_MKT_FUND": "https://www.investing.com/rss/market_overview_Fundamental.rss",
    "INV_COMMOD": "https://www.investing.com/rss/commodities_Technical.rss",
    "TRUMP_HEADLINES": "https://rss.politico.com/donald-trump.xml",
    "FOREXFACTORY_CALENDAR": "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    "MYFX_CAL": "https://www.myfxbook.com/rss/forex-economic-calendar-events",
    "MYFX_NEWS": "https://www.myfxbook.com/rss/latest-forex-news",
    "NASDAQ_STOCKS": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
    "WSJ_MARKETS": "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",

    # === GOLD / PRECIOUS METALS ===
    "KITCO_NEWS": "https://www.kitco.com/rss/news.xml",
    "KITCO_GOLD": "https://www.kitco.com/rss/gold.xml",
    "MINING_COM": "https://www.mining.com/feed/",
    "GOLD_PRICE_ORG": "https://goldprice.org/rss",
    "BULLION_VAULT": "https://www.bullionvault.com/gold-news/rss",
    "SPROTT_GOLD": "https://www.sprott.com/rss/insights/",
    "GOLD_SWISS": "https://www.gold.org/rss",

    # === STOCKS / EQUITIES ===
    "CNBC_TOP": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "CNBC_STOCKS": "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "CNBC_EARNINGS": "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "YAHOO_FIN": "https://finance.yahoo.com/news/rssindex",
    "SEEKINGALPHA": "https://seekingalpha.com/market_currents.xml",
    "SEEKINGALPHA_TOP": "https://seekingalpha.com/feed.xml",
    "BENZINGA": "https://www.benzinga.com/feed",
    "BENZINGA_MARKETS": "https://www.benzinga.com/markets/feed",
    "ZACKS_COMMENTARY": "https://www.zacks.com/commentary/rss",
    "MOTLEY_FOOL": "https://www.fool.com/feeds/index.aspx",
    "THESTREET": "https://www.thestreet.com/rss",
    "BARRONS": "https://feeds.content.dowjones.io/public/rss/barrons",
    "IBD": "https://www.investors.com/feed/",
    "STOCKTWITS": "https://api.stocktwits.com/api/2/streams/trending.json",

    # === OIL / ENERGY ===
    "OILPRICE_ENERGY": "https://oilprice.com/rss/energy",
    "OILPRICE_OIL": "https://oilprice.com/rss/oil",
    "OILPRICE_GAS": "https://oilprice.com/rss/natural-gas",
    "RIGZONE": "https://www.rigzone.com/news/rss/rigzone_news.aspx",
    "EIA_TODAY": "https://www.eia.gov/rss/todayinenergy.xml",
    "WORLDOIL": "https://www.worldoil.com/rss/news",
    "OFFSHORE_MAG": "https://www.offshore-mag.com/rss",
    "ENERGY_VOICE": "https://www.energyvoice.com/feed/",
    "ARGUS_MEDIA": "https://www.argusmedia.com/en/rss-feeds/news",
    "PLATTS": "https://www.spglobal.com/commodityinsights/en/rss-feed/oil",
    "OIL_GAS_JOURNAL": "https://www.ogj.com/rss",

    # === FOREX / CURRENCIES ===
    "FOREXLIVE": "https://www.forexlive.com/feed/news",
    "DAILYFX_NEWS": "https://www.dailyfx.com/feeds/market-news",
    "DAILYFX_TOP": "https://www.dailyfx.com/feeds/top-stories",
    "FX_EMPIRE": "https://www.fxempire.com/news/feed",
    "ACTIONFOREX": "https://www.actionforex.com/feed/",
    "EARNFOREX": "https://www.earnforex.com/rss/news/",
    "FXLEADERS": "https://www.fxleaders.com/feed/",
    "BABYPIPS": "https://www.babypips.com/feed",

    # === CENTRAL BANKS ===
    "FED_SPEECHES": "https://www.federalreserve.gov/feeds/speeches.xml",
    "FED_TESTIMONY": "https://www.federalreserve.gov/feeds/testimony.xml",
    "ECB_PRESS": "https://www.ecb.europa.eu/rss/press.html",
    "BOE_NEWS": "https://www.bankofengland.co.uk/rss/news",
    "BOJ_ANNOUNCE": "https://www.boj.or.jp/en/announcements/release_2024/rel240101a.htm/rss.xml",
    "SNB_NEWS": "https://www.snb.ch/en/mmr/reference/rss_snb/source/rss_snb.en.xml",
    "RBA_MEDIA": "https://www.rba.gov.au/rss/rss-cb-media-releases.xml",
    "BOC_ANNOUNCE": "https://www.bankofcanada.ca/feed/",

    # === MACRO / ECONOMIC ===
    "TRADINGECONOMICS": "https://tradingeconomics.com/rss/news.aspx",
    "ECONODAY": "https://www.econoday.com/rss/",
    "BRIEFING": "https://www.briefing.com/rss",
    "TREASURY_PRESS": "https://home.treasury.gov/news/press-releases/rss.xml",
    "IMF_NEWS": "https://www.imf.org/en/News/rss",
    "WORLDBANK": "https://www.worldbank.org/en/news/rss.xml",
    "BIS_PRESS": "https://www.bis.org/doclist/press.rss",

    # === GENERAL FINANCIAL NEWS ===
    "REUTERS_BIZ": "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "REUTERS_MARKETS": "https://www.reutersagency.com/feed/?best-topics=markets&post_type=best",
    "AP_BUSINESS": "https://rsshub.app/apnews/topics/business",
    "BBC_BUSINESS": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "FT_MARKETS": "https://www.ft.com/markets?format=rss",
    "GUARDIAN_BIZ": "https://www.theguardian.com/uk/business/rss",
    "ECONOMIST": "https://www.economist.com/finance-and-economics/rss.xml",
    "FORTUNE": "https://fortune.com/feed/",
    "FORBES_MARKETS": "https://www.forbes.com/markets/feed/",

    # === GEOPOLITICS ===
    "REUTERS_WORLD": "https://www.reutersagency.com/feed/?best-topics=world&post_type=best",
    "AP_WORLD": "https://rsshub.app/apnews/topics/world-news",
    "BBC_WORLD": "https://feeds.bbci.co.uk/news/world/rss.xml",
    "ALJAZEERA": "https://www.aljazeera.com/xml/rss/all.xml",
    "POLITICO_ECON": "https://rss.politico.com/economy.xml",
    "POLITICO_FIN": "https://rss.politico.com/finance.xml",

    # === COMMODITIES GENERAL ===
    "INV_COMMODITIES": "https://www.investing.com/rss/commodities.rss",
    "COMMODITY_NEWS": "https://www.commodities-now.com/rss",
    "METAL_BULLETIN": "https://www.metalbulletin.com/rss",
    "FASTMARKETS": "https://www.fastmarkets.com/rss",
    "MINING_WEEKLY": "https://www.miningweekly.com/rss",

    # === CRYPTO (ETH + BTC) ===
    "COINDESK": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "COINTELEGRAPH": "https://cointelegraph.com/rss",
    "DECRYPT": "https://decrypt.co/feed",
    "THEBLOCK": "https://www.theblock.co/rss.xml",
    "CRYPTOSLATE": "https://cryptoslate.com/feed/",
    "BEINCRYPTO": "https://beincrypto.com/feed/",
    # ── NEW BTC-specific feeds ──
    "BITCOIN_MAGAZINE": "https://bitcoinmagazine.com/.rss/full/",
    "BITCOIN_COM_NEWS": "https://news.bitcoin.com/feed/",
    "WATCHER_GURU": "https://watcher.guru/news/feed",
    "CRYPTOBRIEFING": "https://cryptobriefing.com/feed/",
    "AMBCRYPTO": "https://ambcrypto.com/feed/",
    "NEWSBTC": "https://www.newsbtc.com/feed/",
    "BITCOINIST": "https://bitcoinist.com/feed/",
    "UTODAY_CRYPTO": "https://u.today/rss",

    # === ADDITIONAL INVESTING.COM ===
    "INV_STOCK_NEWS": "https://www.investing.com/rss/stock_stock_news.rss",
    "INV_FOREX": "https://www.investing.com/rss/forex.rss",
    "INV_CENTRAL_BANKS": "https://www.investing.com/rss/news_287.rss",
    "INV_ECONOMY": "https://www.investing.com/rss/news_14.rss",
    "INV_STOCK_MARKETS": "https://www.investing.com/rss/news_25.rss",

    # ── NEW EUR/Forex-specific feeds ──
    "EUR_USD_FXSTREET": "https://www.fxstreet.com/rss/rates-charts/eurusd",
    "EURACTIV_ECON": "https://www.euractiv.com/section/economy-jobs/feed/",
    "ECB_BLOG": "https://www.ecb.europa.eu/pub/blog/rss.html",
    "ECB_RESEARCH": "https://www.ecb.europa.eu/pub/research/rss.html",
    "EUROSTAT_NEWS": "https://ec.europa.eu/eurostat/en/news/rss",
    "BLOOMBERG_EUR": "https://feeds.bloomberg.com/economics/news.rss",
    "INV_EUR": "https://www.investing.com/rss/news_285.rss",
    "FOREXLIVE_EUR": "https://www.forexlive.com/feed/eurusd",
}

CALENDAR_FEEDS = {"FOREXFACTORY_CALENDAR", "MYFX_CAL"}

SOURCE_WEIGHT = {
    # === HIGH PRIORITY (Official / Institutional) ===
    "FED": 3.0, "FED_SPEECHES": 2.8, "FED_TESTIMONY": 2.8,
    "BLS": 3.0, "BEA": 2.8, "TREASURY_PRESS": 2.5,
    "ECB_PRESS": 2.5, "ECB_BLOG": 2.4, "ECB_RESEARCH": 2.2,
    "BOE_NEWS": 2.4, "BOJ_ANNOUNCE": 2.3,
    "SNB_NEWS": 2.2, "RBA_MEDIA": 2.2, "BOC_ANNOUNCE": 2.2,
    "IMF_NEWS": 2.0, "WORLDBANK": 1.8, "BIS_PRESS": 2.0,
    "EUROSTAT_NEWS": 2.0,

    # === GOLD SPECIALIZED ===
    "KITCO_NEWS": 2.0, "KITCO_GOLD": 2.2,
    "MINING_COM": 1.6, "GOLD_PRICE_ORG": 1.5,
    "BULLION_VAULT": 1.5, "SPROTT_GOLD": 1.4, "GOLD_SWISS": 1.6,

    # === OIL SPECIALIZED ===
    "OILPRICE": 1.8, "OILPRICE_ENERGY": 1.8, "OILPRICE_OIL": 2.0, "OILPRICE_GAS": 1.5,
    "RIGZONE": 1.7, "EIA_TODAY": 2.2, "WORLDOIL": 1.5,
    "OFFSHORE_MAG": 1.3, "ENERGY_VOICE": 1.4,
    "ARGUS_MEDIA": 1.8, "PLATTS": 2.0, "OIL_GAS_JOURNAL": 1.6,

    # === MAJOR FINANCIAL MEDIA ===
    "REUTERS_BIZ": 2.0, "REUTERS_MARKETS": 2.0, "REUTERS_WORLD": 1.5,
    "WSJ_MARKETS": 2.0, "BARRONS": 1.8, "FT_MARKETS": 1.9,
    "CNBC_TOP": 1.6, "CNBC_STOCKS": 1.7, "CNBC_EARNINGS": 1.8,
    "MARKETWATCH_TOP": 1.5, "MARKETWATCH_REALTIME": 1.6,
    "BBC_BUSINESS": 1.4, "ECONOMIST": 1.6, "FORTUNE": 1.3, "FORBES_MARKETS": 1.3,
    "AP_BUSINESS": 1.5, "AP_WORLD": 1.3, "BBC_WORLD": 1.3, "BLOOMBERG_EUR": 1.9,

    # === FOREX SPECIALIZED ===
    "FXSTREET_NEWS": 1.5, "FXSTREET_ANALYSIS": 1.4, "EUR_USD_FXSTREET": 1.8,
    "FOREXLIVE": 1.6, "FOREXLIVE_EUR": 1.8,
    "DAILYFX_NEWS": 1.5, "DAILYFX_TOP": 1.5,
    "FX_EMPIRE": 1.3, "ACTIONFOREX": 1.2, "EARNFOREX": 1.1,
    "FXLEADERS": 1.2, "BABYPIPS": 1.0,
    "INV_EUR": 1.4, "EURACTIV_ECON": 1.5,

    # === STOCK ANALYSIS ===
    "SEEKINGALPHA": 1.5, "SEEKINGALPHA_TOP": 1.4,
    "BENZINGA": 1.4, "BENZINGA_MARKETS": 1.5,
    "ZACKS_COMMENTARY": 1.4, "MOTLEY_FOOL": 1.2,
    "THESTREET": 1.3, "IBD": 1.5, "YAHOO_FIN": 1.3,
    "NASDAQ_STOCKS": 1.4,

    # === INVESTING.COM ===
    "INV_NEWS_11": 1.2, "INV_MKT_FUND": 1.3, "INV_COMMOD": 1.3,
    "INV_COMMODITIES": 1.3, "INV_STOCK_NEWS": 1.2, "INV_FOREX": 1.2,
    "INV_CENTRAL_BANKS": 1.5, "INV_ECONOMY": 1.4, "INV_STOCK_MARKETS": 1.3,

    # === MACRO / ECONOMIC ===
    "TRADINGECONOMICS": 1.6, "ECONODAY": 1.4, "BRIEFING": 1.3,

    # === GEOPOLITICS ===
    "POLITICO_ECON": 1.4, "POLITICO_FIN": 1.4, "TRUMP_HEADLINES": 1.3,
    "ALJAZEERA": 1.2, "GUARDIAN_BIZ": 1.2,

    # === COMMODITIES GENERAL ===
    "COMMODITY_NEWS": 1.2, "METAL_BULLETIN": 1.4, "FASTMARKETS": 1.3, "MINING_WEEKLY": 1.3,

    # === CRYPTO ===
    "COINDESK": 1.8, "COINTELEGRAPH": 1.6,
    "DECRYPT": 1.5, "THEBLOCK": 1.7, "CRYPTOSLATE": 1.3, "BEINCRYPTO": 1.2,
    # ── NEW BTC sources ──
    "BITCOIN_MAGAZINE": 2.0,   # premier BTC publication
    "BITCOIN_COM_NEWS": 1.5,
    "WATCHER_GURU": 1.2,
    "CRYPTOBRIEFING": 1.4,
    "AMBCRYPTO": 1.2,
    "NEWSBTC": 1.4,
    "BITCOINIST": 1.3,
    "UTODAY_CRYPTO": 1.2,

    # === CALENDAR (no bias weight) ===
    "FOREXFACTORY_CALENDAR": 0.0, "MYFX_CAL": 0.0,
    "MYFX_NEWS": 1.2,

    # === FRED ===
    "FRED": 1.5,
}

# ── IMPROVED & EXTENDED RULES ──
RULES = {
    "XAU": [
        (r"\b(fed|fomc|powell|hawkish|rate hike|rates higher|tightening)\b", -0.7, "Hawkish Fed weighs on gold"),
        (r"\b(rate cut|dovish|easing|pause|hold rates)\b", +0.6, "Dovish Fed supports gold"),
        (r"\b(cpi|inflation|pce|core inflation)\b", +0.25, "Inflation data supports gold"),
        (r"\b(strong dollar|usd strengthens|yields rise|real yields|dxy)\b", -0.8, "Strong USD weighs on gold"),
        (r"\b(geopolitical|safe[- ]haven|risk aversion|war|conflict|crisis)\b", +0.6, "Safe-haven demand rises"),
        (r"\b(risk-on|stocks rally|equity rally)\b", -0.2, "Risk-on mood pressures gold"),
        (r"\b(recession|slowdown|contraction)\b", +0.4, "Recession fears support gold"),
        (r"\b(central bank buying|gold reserve|cbr gold|pboc gold)\b", +0.8, "Central bank gold buying"),
        (r"\b(dollar weakens|usd falls|dxy drop)\b", +0.7, "Weak dollar lifts gold"),
        (r"\b(deflation|disinflation)\b", -0.3, "Disinflation weighs on gold"),
        (r"\b(all[- ]time high|record high|ath)\b.*\bgold\b", +0.5, "Gold at record high — momentum bullish"),
        (r"\b(etf inflow|gold etf|gld)\b", +0.5, "Gold ETF inflows bullish"),
    ],
    "US500": [
        (r"\b(earnings beat|guidance raised|revenue beat|profit up)\b", +0.6, "Strong earnings support stocks"),
        (r"\b(earnings miss|guidance cut|revenue miss|profit warning)\b", -0.7, "Weak earnings pressure stocks"),
        (r"\b(yields rise|rates higher|hawkish|tightening)\b", -0.6, "Rising yields pressure stocks"),
        (r"\b(rate cut|dovish|easing|pause)\b", +0.5, "Fed easing supports stocks"),
        (r"\b(risk-off|selloff|crash|correction)\b", -0.7, "Risk-off mood in markets"),
        (r"\b(rally|rebound|risk-on|bull market)\b", +0.3, "Risk-on momentum building"),
        (r"\b(recession|contraction|gdp miss)\b", -0.6, "Recession fears hit stocks"),
        (r"\b(strong jobs|nfp beat|jobless claims low)\b", +0.4, "Strong jobs boost stocks"),
        (r"\b(jobs miss|unemployment rise|layoffs)\b", -0.4, "Weak jobs pressure stocks"),
        (r"\b(buyback|stock buyback|repurchase)\b", +0.3, "Buybacks support prices"),
        (r"\b(vix spike|vix surge|volatility)\b", -0.5, "Volatility spike — risk-off"),
        (r"\b(ipo|m&a|merger|acquisition)\b", +0.2, "M&A activity signals confidence"),
        (r"\b(ai|artificial intelligence|tech rally|nvidia|semiconductor)\b", +0.3, "AI/tech sector momentum"),
        (r"\b(tariff|trade war|sanction)\b", -0.5, "Trade tensions weigh on stocks"),
    ],
    "WTI": [
        (r"\b(crude|oil|wti|brent)\b", +0.1, "Oil market focus"),
        (r"\b(opec|opec\+|output cut|production cut|quota cut)\b", +0.8, "OPEC cuts support prices"),
        (r"\b(opec increase|output increase|production increase)\b", -0.6, "OPEC output rise pressures oil"),
        (r"\b(inventory build|inventories rise|api build|eia build|crude build)\b", -0.8, "Inventory build pressures oil"),
        (r"\b(inventory draw|inventories fall|api draw|eia draw|crude draw)\b", +0.8, "Inventory draw supports oil"),
        (r"\b(disruption|outage|sanctions|pipeline attack|force majeure)\b", +0.7, "Supply disruption risk"),
        (r"\b(demand weak|recession|slowdown|china slowdown)\b", -0.6, "Demand concerns weigh on oil"),
        (r"\b(china demand|china growth|asia demand)\b", +0.5, "Asia demand supports oil"),
        (r"\b(iran|russia|venezuela|libya)\b.*\b(oil|crude|export|sanction)\b", +0.5, "Geopolitical supply risk"),
        (r"\b(hurricane|storm|gulf mexico)\b", +0.4, "Weather supply disruption"),
        (r"\b(shale|permian|us production|us output|rig count)\b", -0.3, "US production growth weighs"),
        (r"\b(strategic reserve|spr|strategic petroleum)\b", -0.4, "SPR release weighs on oil"),
        (r"\b(travel demand|fuel demand|gasoline demand|jet fuel)\b", +0.4, "Travel/fuel demand lifts oil"),
    ],
    "ETH": [
        (r"\b(ethereum|eth|ether)\b", +0.1, "Ethereum in focus"),
        (r"\b(defi|dex|decentralized finance|tvl|uniswap|aave|curve)\b", +0.5, "DeFi activity supports ETH"),
        (r"\b(sec|regulation|crackdown|ban|lawsuit)\b", -0.7, "Regulatory pressure on crypto"),
        (r"\b(etf|spot etf|institutional|grayscale|blackrock)\b", +0.8, "Institutional demand for ETH"),
        (r"\b(upgrade|eip|staking|validator|pectra|dencun)\b", +0.6, "Network upgrade supports ETH"),
        (r"\b(hack|exploit|bridge attack|rug pull|drain)\b", -0.9, "Security incident hurts sentiment"),
        (r"\b(risk-off|selloff|crash|recession)\b", -0.6, "Risk-off mood hits crypto"),
        (r"\b(risk-on|rally|bull|bull market)\b", +0.5, "Risk-on mood lifts crypto"),
        (r"\b(bitcoin|btc)\b", +0.2, "BTC move correlates with ETH"),
        (r"\b(layer.?2|l2|rollup|arbitrum|optimism|base|zksync)\b", +0.4, "L2 growth benefits ETH"),
        (r"\b(rate cut|dovish|easing|liquidity|m2)\b", +0.5, "Easier money lifts crypto"),
        (r"\b(hawkish|rate hike|tightening|quantitative tightening)\b", -0.4, "Tighter policy weighs on crypto"),
        (r"\b(gas fee|network fee|burn|eip.?1559)\b", +0.3, "ETH burn/fee mechanism bullish"),
        (r"\b(nft|web3|metaverse)\b", +0.2, "NFT/Web3 activity benefits ETH"),
    ],
    # ── NEW: BTC rules ──
    "BTC": [
        (r"\b(bitcoin|btc)\b", +0.1, "Bitcoin in focus"),
        (r"\b(halving|halvening|block reward)\b", +1.0, "Bitcoin halving — supply shock bullish"),
        (r"\b(etf|spot etf|bitcoin etf|blackrock|fidelity etf)\b", +0.9, "BTC ETF demand bullish"),
        (r"\b(etf outflow|etf selling)\b", -0.7, "BTC ETF outflows bearish"),
        (r"\b(institutional|whale|treasury|microstrategy|saylor)\b", +0.6, "Institutional BTC buying"),
        (r"\b(sec|regulation|crackdown|ban|lawsuit)\b", -0.7, "Regulatory pressure on BTC"),
        (r"\b(mining|hashrate|difficulty|miner)\b", +0.3, "Mining health supports BTC"),
        (r"\b(miner sell|miner capitulation)\b", -0.5, "Miner selling pressure"),
        (r"\b(hack|exchange hack|theft|stolen|lost keys)\b", -0.8, "Security incident weighs on BTC"),
        (r"\b(risk-off|selloff|crash|recession)\b", -0.6, "Risk-off mood hits BTC"),
        (r"\b(risk-on|rally|bull|bull market)\b", +0.5, "Risk-on mood lifts BTC"),
        (r"\b(rate cut|dovish|easing|liquidity)\b", +0.6, "Easier money lifts BTC"),
        (r"\b(hawkish|rate hike|tightening)\b", -0.5, "Tighter policy weighs on BTC"),
        (r"\b(all[- ]time high|ath|record high)\b.*\b(bitcoin|btc|crypto)\b", +0.7, "BTC at ATH — momentum bullish"),
        (r"\b(lightning network|taproot|ordinals|rune)\b", +0.3, "BTC network development"),
        (r"\b(cbdc|digital dollar|digital yuan)\b", -0.2, "CBDC competition concerns"),
        (r"\b(us strategic reserve|btc reserve|government bitcoin)\b", +0.9, "Government BTC reserve demand"),
        (r"\b(dollar index|dxy|dollar strength)\b", -0.4, "Strong dollar weighs on BTC"),
        (r"\b(dollar weak|usd falls|inflation hedge)\b", +0.5, "Weak dollar lifts BTC"),
        (r"\b(network congestion|fees spike)\b", -0.2, "High fees signal but can indicate demand"),
        (r"\b(m2|money supply|quantitative easing|money print)\b", +0.6, "Monetary expansion bullish for BTC"),
    ],
    # ── NEW: EUR rules (EURUSD direction) ──
    "EUR": [
        (r"\b(ecb|lagarde|european central bank)\b", +0.1, "ECB in focus"),
        (r"\b(ecb hike|ecb hawkish|rate hike europe|ecb tightening)\b", +0.7, "Hawkish ECB supports EUR"),
        (r"\b(ecb cut|ecb dovish|ecb easing|ecb pause|lower rates europe)\b", -0.6, "Dovish ECB weighs on EUR"),
        (r"\b(eu gdp|eurozone gdp|euro gdp|eu growth)\b", +0.4, "Strong EU growth supports EUR"),
        (r"\b(eu recession|eurozone recession|eu contraction)\b", -0.7, "EU recession weighs on EUR"),
        (r"\b(eu inflation|eurozone cpi|hicp|eu cpi)\b", +0.3, "EU inflation supports ECB hawkishness"),
        (r"\b(eu unemployment|eurozone jobs)\b", -0.2, "EU jobs data"),
        (r"\b(germany|german|bund|ifo|zew)\b", +0.2, "German data affects EUR"),
        (r"\b(german recession|german contraction|germany weak)\b", -0.5, "German weakness weighs on EUR"),
        (r"\b(eu debt crisis|sovereign debt|italy spread|greece)\b", -0.6, "EU debt risk weighs on EUR"),
        (r"\b(fed hike|fed hawkish|dollar rally|usd strength)\b", -0.7, "Fed hawkishness weighs on EUR"),
        (r"\b(fed cut|fed dovish|dollar weak|usd falls)\b", +0.7, "Fed dovishness lifts EUR"),
        (r"\b(eu energy|gas prices europe|energy crisis europe)\b", -0.4, "EU energy crisis weighs on EUR"),
        (r"\b(eu trade surplus|eu current account)\b", +0.3, "EU trade surplus supports EUR"),
        (r"\b(parity|eur.?usd parity|below parity)\b", -0.5, "EUR/USD parity threat bearish"),
        (r"\b(risk-off|geopolitical|war|ukraine|russia)\b", -0.4, "Risk-off / European geopolitics weigh"),
        (r"\b(risk-on|global growth|emerging market)\b", +0.3, "Risk-on supports EUR"),
        (r"\b(eu fiscal|eu stimulus|recovery fund|eu bond)\b", +0.4, "EU fiscal support bullish"),
        (r"\b(eu election|political risk europe|populist)\b", -0.3, "EU political risk weighs"),
    ],
}

# ============ DB ============
_DB_POOL = None
_DB_READY = False

def _make_dsn():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if db_url: return db_url
    return f"host={os.environ.get('PGHOST','localhost')} port={os.environ.get('PGPORT','5432')} dbname={os.environ.get('PGDATABASE','postgres')} user={os.environ.get('PGUSER','postgres')} password={os.environ.get('PGPASSWORD','')} connect_timeout=5"

def _get_pool():
    global _DB_POOL
    if _DB_POOL: return _DB_POOL
    if not ThreadedConnectionPool: return None
    try: _DB_POOL = ThreadedConnectionPool(1, 10, _make_dsn())
    except: pass
    return _DB_POOL

def db_conn():
    pool = _get_pool()
    if pool:
        try:
            c = pool.getconn()
            _CONN_FROM_POOL.add(id(c))
            return c
        except: pass
    return psycopg2.connect(_make_dsn())

def db_put(conn):
    if not conn: return
    pool, cid = _get_pool(), id(conn)
    if pool and cid in _CONN_FROM_POOL:
        _CONN_FROM_POOL.discard(cid)
        try: pool.putconn(conn); return
        except: pass
    try: conn.close()
    except: pass

def db_init():
    global _DB_READY
    if _DB_READY: return
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS news_items(id BIGSERIAL PRIMARY KEY,source TEXT,title TEXT,link TEXT,published_ts BIGINT,fingerprint TEXT UNIQUE);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_news_ts ON news_items(published_ts DESC);")
                cur.execute("CREATE TABLE IF NOT EXISTS bias_state(id SMALLINT PRIMARY KEY DEFAULT 1,updated_ts BIGINT,payload_json TEXT);")
                cur.execute("CREATE TABLE IF NOT EXISTS fred_series(series_id TEXT,obs_date DATE,value DOUBLE PRECISION,PRIMARY KEY(series_id,obs_date));")
                cur.execute("CREATE TABLE IF NOT EXISTS econ_events(id BIGSERIAL PRIMARY KEY,source TEXT,title TEXT,currency TEXT,impact TEXT,event_ts BIGINT,link TEXT,fingerprint TEXT UNIQUE);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_econ_ts ON econ_events(event_ts DESC);")
                cur.execute("CREATE TABLE IF NOT EXISTS kv_state(k TEXT PRIMARY KEY,v TEXT,updated_ts BIGINT);")
                # NEW: bias history for sparklines and signal log
                cur.execute("""CREATE TABLE IF NOT EXISTS bias_history(
                    id BIGSERIAL PRIMARY KEY,
                    asset TEXT NOT NULL,
                    bias TEXT,
                    score DOUBLE PRECISION,
                    quality INTEGER,
                    gate_ok BOOLEAN,
                    recorded_ts BIGINT NOT NULL
                );""")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_bh_asset_ts ON bias_history(asset, recorded_ts DESC);")
                # NEW: external metrics (Fear&Greed, on-chain, COT)
                cur.execute("""CREATE TABLE IF NOT EXISTS ext_metrics(
                    key TEXT PRIMARY KEY,
                    value_json TEXT,
                    fetched_ts BIGINT
                );""")
        _DB_READY = True
    finally: db_put(conn)

def kv_get(k):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM kv_state WHERE k=%s;", (k,))
            r = cur.fetchone()
            return r[0] if r else None
    except: return None
    finally: db_put(conn)

def kv_set(k, v):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO kv_state(k,v,updated_ts) VALUES(%s,%s,%s) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_ts=EXCLUDED.updated_ts;", (k, str(v), int(time.time())))
    finally: db_put(conn)

def save_bias(p):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO bias_state(id,updated_ts,payload_json) VALUES(1,%s,%s) ON CONFLICT(id) DO UPDATE SET updated_ts=EXCLUDED.updated_ts,payload_json=EXCLUDED.payload_json;", (int(time.time()), json.dumps(p)))
    finally: db_put(conn)

def load_bias():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT payload_json FROM bias_state WHERE id=1;")
            r = cur.fetchone()
            return json.loads(r[0]) if r else None
    except: return None
    finally: db_put(conn)

# ============ HELPERS ============
def fingerprint(t, l): return hashlib.sha1(f"{t}||{l}".encode()).hexdigest()

# ── IMPROVED: smoother decay with configurable half-life ──
def decay_weight(age_sec: float) -> float:
    """Exponential decay. 0-2h stays near 1.0, 8h ~= 0.5, 24h ~= 0.1"""
    return math.exp(-LAMBDA * max(0.0, age_sec))

def _fresh_bucket(age):
    return "0-2h" if age <= 7200 else ("2-8h" if age <= 28800 else "8-24h")

def match_rules(asset, title):
    return [{"w": w, "why": why} for pat, w, why in RULES.get(asset, []) if re.search(pat, title or "", re.I)]

def _ts_from_entry(e, now):
    for k in ["published_parsed", "updated_parsed"]:
        if e.get(k):
            try: return int(pycalendar.timegm(e[k]))
            except: pass
    return now

def _impact_norm(x):
    if not x: return None
    s = str(x).upper()
    return "HIGH" if "HI" in s else ("MED" if "ME" in s else ("LOW" if "LO" in s else None))

def _fmt_time(ts):
    if not ts: return ""
    d = ts - int(time.time())
    if d <= 0: return "now"
    if d < 3600: return f"{d//60}m"
    if d < 86400: return f"{d//3600}h {(d%3600)//60}m"
    return f"{d//86400}d"

_USD_KW = re.compile(r"\b(USD|Fed|FOMC|Powell|NFP|Payroll|CPI|PPI|GDP|Retail Sales|Jobless|Unemployment|ISM|PMI|Treasury|Michigan|JOLTS|ADP|Empire State|Durable Goods|Housing|Consumer Confidence|Initial Claims|Core PCE|Factory Orders|Trade Balance)\b", re.I)
_CCY_PAT = re.compile(r"\b(USD|EUR|GBP|JPY|CHF|AUD|CAD|NZD|CNY)\b", re.I)

def _detect_ccy(title):
    if not title: return None
    m = _CCY_PAT.search(title)
    if m: return m.group(1).upper()
    if _USD_KW.search(title): return "USD"
    for pat, ccy in [(r"\b(US|U\.S\.|American)\b", "USD"), (r"\b(Euro|ECB)\b", "EUR"), (r"\b(UK|BoE|British)\b", "GBP"), (r"\b(Japan|BoJ)\b", "JPY")]:
        if re.search(pat, title, re.I): return ccy
    return None

def feeds_health_live(force=False):
    global _FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS
    now = int(time.time())
    if not force and _FEEDS_HEALTH_CACHE and (now - _FEEDS_HEALTH_TS) <= FEEDS_HEALTH_TTL_SEC:
        return _FEEDS_HEALTH_CACHE
    res = {}
    for src, url in RSS_FEEDS.items():
        try:
            if src == "TRUMP_HEADLINES" and not TRUMP_ENABLED:
                res[src] = {"ok": True, "skipped": True}; continue
            d = feedparser.parse(url)
            res[src] = {"ok": True, "entries": len(d.entries or [])}
        except Exception as e:
            res[src] = {"ok": False, "error": str(e)}
    _FEEDS_HEALTH_CACHE, _FEEDS_HEALTH_TS = res, now
    return res

# ============ INGEST ============
def _parse_feed(src, url, now, limit):
    if src in CALENDAR_FEEDS or (src == "TRUMP_HEADLINES" and not TRUMP_ENABLED): return []
    try: d = feedparser.parse(url)
    except: return []
    out = []
    for e in (d.entries or [])[:limit]:
        t, l = (e.get("title") or "").strip(), (e.get("link") or "").strip()
        if t and l: out.append((src, t, l, _ts_from_entry(e, now), fingerprint(t, l)))
    return out

def ingest_news():
    now = int(time.time())
    tasks = [(s, u) for s, u in RSS_FEEDS.items() if s not in CALENDAR_FEEDS and not (s == "TRUMP_HEADLINES" and not TRUMP_ENABLED)]
    batch = []
    with ThreadPoolExecutor(max_workers=RSS_PARSE_WORKERS) as ex:
        for f in as_completed([ex.submit(_parse_feed, s, u, now, RSS_LIMIT) for s, u in tasks], timeout=RUN_MAX_SEC):
            try: batch.extend(f.result(timeout=0))
            except: pass
    if not batch: return 0
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, "INSERT INTO news_items(source,title,link,published_ts,fingerprint) VALUES %s ON CONFLICT DO NOTHING;", batch)
        return len(batch)
    finally: db_put(conn)

def ingest_calendar():
    for src in CALENDAR_FEEDS:
        url = RSS_FEEDS.get(src)
        if not url: continue
        try: d = feedparser.parse(url)
        except: continue
        conn = db_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for e in (d.entries or [])[:CAL_LIMIT]:
                        t = (e.get("title") or "").strip()
                        if not t: continue
                        l = (e.get("link") or "").strip()
                        ts = _ts_from_entry(e, int(time.time()))
                        ccy = _detect_ccy(t)
                        imp = _impact_norm(e.get("impact") or e.get("importance"))
                        fp = fingerprint(f"{src}||{t}", l or t)
                        try: cur.execute("INSERT INTO econ_events(source,title,currency,impact,event_ts,link,fingerprint) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;", (src, t, ccy, imp, ts, l, fp))
                        except: pass
        finally: db_put(conn)

def _get_upcoming(now):
    horizon = now + int(EVENT_CFG["lookahead_hours"] * 3600)
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT title, event_ts, currency, impact FROM econ_events 
                          WHERE event_ts BETWEEN %s AND %s AND currency='USD' AND impact IN ('HIGH','MED')
                          ORDER BY event_ts LIMIT 10;""", (now, horizon))
            rows = cur.fetchall()
            if not rows:
                cur.execute("""SELECT title, event_ts, currency, impact FROM econ_events 
                              WHERE event_ts BETWEEN %s AND %s ORDER BY event_ts LIMIT 5;""", (now, horizon))
                rows = cur.fetchall()
            return [{"title": t, "ts": ts, "currency": _detect_ccy(t) if not c else c, "impact": _impact_norm(i)} for t, ts, c, i in rows]
    finally: db_put(conn)

def _event_risk(upcoming, recent_macro):
    if recent_macro: return 0.7
    if not upcoming: return 0.1
    impacts = [x.get("impact") for x in upcoming if x.get("currency") == "USD"]
    if "HIGH" in impacts: return 0.9
    if "MED" in impacts: return 0.5
    return 0.2

# ============ FRED ============
def fred_ingest(sid, days=120):
    if not (FRED_CFG["enabled"] and FRED_CFG["api_key"] and requests): return 0
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                         params={"series_id": sid, "api_key": FRED_CFG["api_key"], "file_type": "json", "sort_order": "desc", "limit": days*2}, timeout=12)
        obs = r.json().get("observations", [])
    except: return 0
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for o in obs:
                    d, v = o.get("date"), o.get("value")
                    if d and v not in (None, ".", ""):
                        try: cur.execute("INSERT INTO fred_series VALUES(%s,%s,%s) ON CONFLICT DO UPDATE SET value=EXCLUDED.value;", (sid, d, float(v)))
                        except: pass
        return len(obs)
    finally: db_put(conn)

def fred_last(sid, n=90):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM fred_series WHERE series_id=%s AND value IS NOT NULL ORDER BY obs_date DESC LIMIT %s;", (sid, n))
            return [float(x[0]) for x in cur.fetchall()]
    finally: db_put(conn)

def compute_fred_drivers():
    out = {"XAU": [], "US500": [], "WTI": [], "ETH": [], "BTC": [], "EUR": []}
    dfii = fred_last("DFII10", 90)
    dgs  = fred_last("DGS10", 90)
    usd  = fred_last("DTWEXBGS", 120)
    vix  = fred_last("VIXCLS", 120)

    if len(dfii) >= 6:
        d = dfii[0] - dfii[5]
        msg = "Real yields rising" if d > 0 else "Real yields falling"
        out["XAU"].append({"w": -1.2 if d > 0 else 1.0, "why": msg})
        # BTC also inversely correlated to real yields
        out["BTC"].append({"w": -0.8 if d > 0 else 0.7, "why": msg})

    if len(dgs) >= 6:
        d = dgs[0] - dgs[5]
        msg = "Bond yields rising" if d > 0 else "Bond yields falling"
        out["US500"].append({"w": -0.9 if d > 0 else 0.5, "why": msg})
        # EUR: higher US yields strengthen USD, weigh on EUR
        out["EUR"].append({"w": -0.6 if d > 0 else 0.5, "why": msg})

    if len(usd) >= 6:
        d = (usd[0] - usd[5]) / max(abs(usd[5]), 1)
        msg = "Dollar strengthening" if d > 0 else "Dollar weakening"
        out["XAU"].append({"w": -0.9 if d > 0 else 0.7, "why": msg})
        out["EUR"].append({"w": -0.9 if d > 0 else 0.8, "why": msg})
        out["BTC"].append({"w": -0.5 if d > 0 else 0.4, "why": msg})

    if len(vix) >= 6:
        d = (vix[0] - vix[5]) / max(abs(vix[5]), 1)
        msg = "Volatility spiking" if d > 0 else "Volatility calming"
        out["US500"].append({"w": -1.0 if d > 0 else 0.4, "why": msg})
        out["ETH"].append({"w": -0.6 if d > 0 else 0.3, "why": msg})
        out["BTC"].append({"w": -0.6 if d > 0 else 0.3, "why": msg})

    return out

# ============ BIAS HISTORY ============
def save_bias_history(assets_out, gate_results):
    """Record each asset's bias snapshot for sparklines."""
    now = int(time.time())
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for asset, a in assets_out.items():
                    gate_ok = gate_results.get(asset, {}).get("ok", False)
                    cur.execute(
                        "INSERT INTO bias_history(asset,bias,score,quality,gate_ok,recorded_ts) VALUES(%s,%s,%s,%s,%s,%s);",
                        (asset, a["bias"], a["score"], a["quality"], gate_ok, now)
                    )
                # Prune older than 7 days
                cur.execute("DELETE FROM bias_history WHERE recorded_ts < %s;", (now - 7*86400,))
    except: pass
    finally: db_put(conn)

def load_bias_history(asset, hours=24):
    """Returns list of {ts, bias, score, quality, gate_ok} newest-first."""
    cutoff = int(time.time()) - hours * 3600
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT recorded_ts,bias,score,quality,gate_ok FROM bias_history WHERE asset=%s AND recorded_ts>=%s ORDER BY recorded_ts DESC LIMIT 96;",
                (asset, cutoff)
            )
            return [{"ts": r[0], "bias": r[1], "score": round(r[2],3), "quality": r[3], "gate_ok": r[4]} for r in cur.fetchall()]
    except: return []
    finally: db_put(conn)

# ============ EXTERNAL METRICS ============
EXT_METRICS_TTL = 3600  # refresh every hour

def _ext_get(key):
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value_json, fetched_ts FROM ext_metrics WHERE key=%s;", (key,))
            r = cur.fetchone()
            if r and (int(time.time()) - r[1]) < EXT_METRICS_TTL:
                return json.loads(r[0])
    except: pass
    finally: db_put(conn)
    return None

def _ext_set(key, value):
    conn = db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ext_metrics(key,value_json,fetched_ts) VALUES(%s,%s,%s) ON CONFLICT(key) DO UPDATE SET value_json=EXCLUDED.value_json,fetched_ts=EXCLUDED.fetched_ts;",
                    (key, json.dumps(value), int(time.time()))
                )
    except: pass
    finally: db_put(conn)

def fetch_fear_greed():
    """Fetch CNN Fear & Greed + Crypto Fear & Greed indices. Returns dict."""
    cached = _ext_get("fear_greed")
    if cached: return cached
    result = {}
    if not requests: return result
    # Crypto F&G (alternative.me — free, no key)
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
        d = r.json().get("data", [{}])[0]
        result["crypto_fg"] = {"value": int(d.get("value", 50)), "label": d.get("value_classification", "Neutral")}
    except: pass
    # CNN Fear & Greed via scrape-free endpoint
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata", timeout=8)
        score = r.json().get("fear_and_greed", {}).get("score", None)
        if score is not None:
            result["stock_fg"] = {"value": round(float(score)), "label": _fg_label(float(score))}
    except: pass
    if result: _ext_set("fear_greed", result)
    return result

def _fg_label(v):
    if v >= 75: return "Extreme Greed"
    if v >= 55: return "Greed"
    if v >= 45: return "Neutral"
    if v >= 25: return "Fear"
    return "Extreme Fear"

def _fg_to_weight(v, asset):
    """Convert F&G 0-100 to a bias contribution for the asset."""
    # Centre at 50: greed is bullish for risk assets, bearish for gold
    centered = (v - 50) / 50.0  # -1 .. +1
    if asset in ("ETH", "BTC", "US500"):
        return centered * 0.6
    if asset == "XAU":
        return -centered * 0.5   # fear = gold bullish
    return 0.0

# ============ COT REPORT (CFTC) ============
# CFTC publishes Commitments of Traders every Friday
# We parse the legacy CSV — no API key needed
# Columns we care about: NonComm_Long, NonComm_Short (speculative positioning)

COT_MARKETS = {
    # CFTC market code → our asset
    "088691": "XAU",    # Gold futures (COMEX)
    "067651": "WTI",    # Crude Oil, Light Sweet (NYMEX)
    "13874A": "EUR",    # Euro FX (CME)
    "133741": "BTC",    # Bitcoin (CME)
    "099741": "ETH",    # Ether (CME)
    "13874+": "US500",  # E-mini S&P 500 (CME) — proxy
}
COT_CSV_URL = "https://www.cftc.gov/dea/newcot/FinFutWk.txt"
COT_TTL = 86400 * 2  # refresh every 2 days (published weekly)

def fetch_cot():
    """Download CFTC COT report. Uses header row to find correct columns.
    Returns {asset: {net_long, pct_long, signal, weight, source}}
    """
    cached = _ext_get("cot")
    if cached:
        return cached
    if not requests:
        return {}
    result = {}
    try:
        r = requests.get(COT_CSV_URL, timeout=25,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        lines = r.text.splitlines()
        if not lines:
            raise ValueError("empty")

        # Parse header to find column indices dynamically
        header = [h.strip().strip('"').lower() for h in lines[0].split(",")]
        def col(name):
            for i, h in enumerate(header):
                if name.lower() in h:
                    return i
            return None

        # Try multiple known column name variants
        long_col  = col("noncomm_positions_long_all") or col("noncomm long") or col("non-commercial long") or 8
        short_col = col("noncomm_positions_short_all") or col("noncomm short") or col("non-commercial short") or 9
        code_col  = col("cftc_contract_market_code") or col("market code") or 2
        name_col  = col("market_and_exchange_names") or col("market name") or 0

        for line in lines[1:]:
            if not line.strip():
                continue
            parts = [p.strip().strip('"') for p in line.split(",")]
            if len(parts) < max(long_col, short_col, code_col) + 1:
                continue
            mkt_code = parts[code_col].strip()
            asset = COT_MARKETS.get(mkt_code)
            # Also match by name substring if code doesn't match
            if not asset:
                mkt_name = parts[name_col].upper() if name_col < len(parts) else ""
                for name_frag, a in [("GOLD", "XAU"), ("CRUDE OIL", "WTI"), ("EURO FX", "EUR"),
                                      ("BITCOIN", "BTC"), ("ETHER", "ETH"), ("E-MINI S&P", "US500")]:
                    if name_frag in mkt_name:
                        asset = a
                        break
            if not asset or asset in result:
                continue
            try:
                nl = float(parts[long_col].replace(",", ""))
                ns = float(parts[short_col].replace(",", ""))
                net   = nl - ns
                total = nl + ns or 1.0
                pct   = round(nl / total * 100, 1)
                result[asset] = _cot_interpret(int(net), pct, "CFTC")
            except (ValueError, IndexError):
                continue
    except Exception:
        pass

    if not result:
        result = _fetch_cot_fallback()
    if result:
        _ext_set("cot", result)
    return result

def _cot_interpret(net_long, pct_long, source="CFTC"):
    if pct_long >= 72:
        signal, weight = "Specs heavily long — crowded", +0.55
    elif pct_long >= 62:
        signal, weight = "Specs net long", +0.35
    elif pct_long <= 28:
        signal, weight = "Specs heavily short — crowded", -0.55
    elif pct_long <= 38:
        signal, weight = "Specs net short", -0.35
    else:
        signal, weight = "Specs neutral", 0.0
    return {"net_long": net_long, "pct_long": pct_long,
            "signal": signal, "weight": weight, "source": source}

def _fetch_cot_fallback():
    """Try CFTC Socrata open data API — Gold only, more reliable JSON."""
    result = {}
    if not requests:
        return result
    endpoints = [
        # Gold futures (Socrata dataset)
        ("https://publicreporting.cftc.gov/resource/jun7-fc8e.json?$limit=1&$order=report_date_as_mm_dd_yyyy+DESC&$where=cftc_contract_market_code=%27088691%27",
         "XAU"),
        # Crude Oil
        ("https://publicreporting.cftc.gov/resource/jun7-fc8e.json?$limit=1&$order=report_date_as_mm_dd_yyyy+DESC&$where=cftc_contract_market_code=%27067651%27",
         "WTI"),
        # Euro FX
        ("https://publicreporting.cftc.gov/resource/jun7-fc8e.json?$limit=1&$order=report_date_as_mm_dd_yyyy+DESC&$where=cftc_contract_market_code=%2713874A%27",
         "EUR"),
    ]
    for url, asset in endpoints:
        try:
            r = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            data = r.json()
            if data:
                rec = data[0]
                nl = float(rec.get("noncomm_positions_long_all", 0))
                ns = float(rec.get("noncomm_positions_short_all", 0))
                if nl + ns > 0:
                    result[asset] = _cot_interpret(int(nl - ns), round(nl / (nl + ns) * 100, 1), "CFTC-API")
        except Exception:
            continue
    return result

def cot_to_weight(asset, cot_data):
    """Return (weight, label) from COT data for a given asset."""
    d = cot_data.get(asset)
    if not d:
        return 0.0, None
    return d.get("weight", 0.0), f"COT: {d['signal']} ({d['pct_long']}% long)"

# ============ OPTIONS MARKET (CBOE Put/Call + Implied Vol) ============
OPTIONS_TTL = 3600  # refresh hourly

def fetch_options_data():
    """Fetch CBOE equity put/call ratio and VIX term structure.
    Returns {
        'equity_pcr': float,        # <0.7 greedy, >1.0 fearful
        'index_pcr': float,
        'total_pcr': float,
        'vix': float,               # current VIX
        'vix_signal': str,
        'pcr_signal': str,
        'pcr_weight_us500': float,
        'pcr_weight_xau': float,
    }
    """
    cached = _ext_get("options")
    if cached:
        return cached
    if not requests:
        return {}
    result = {}
    # ── 1. CBOE Put/Call Ratio (free, public endpoint) ──
    try:
        r = requests.get(
            "https://cdn.cboe.com/api/global/us_indices/pcr_totals/total_put_call_ratio.json",
            timeout=10, headers={"User-Agent": "Mozilla/5.0"}
        )
        d = r.json()
        # Fields: equity, index, total
        result["equity_pcr"] = float(d.get("equity", 0))
        result["index_pcr"]  = float(d.get("index",  0))
        result["total_pcr"]  = float(d.get("total",  0))
    except:
        pass

    # ── 2. VIX current level from CBOE ──
    try:
        r = requests.get(
            "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_VIX.json",
            timeout=10, headers={"User-Agent": "Mozilla/5.0"}
        )
        quotes = r.json().get("data", [])
        if quotes:
            # Last entry: [datetime, open, high, low, close, volume]
            result["vix"] = float(quotes[-1][4])  # close
    except:
        pass

    # ── 3. GVZ (Gold Volatility Index) via FRED if available ──
    try:
        gvz_vals = fred_last("VIXCLS", 5)  # fallback to VIX
        if gvz_vals:
            result["vix_fred"] = round(gvz_vals[0], 2)
    except:
        pass

    # ── Interpret signals ──
    pcr = result.get("equity_pcr", 0) or result.get("total_pcr", 0)
    vix = result.get("vix", 0) or result.get("vix_fred", 0)

    if pcr > 0:
        if pcr >= 1.1:
            result["pcr_signal"] = f"Extreme fear (P/C={pcr:.2f}) — contrarian bullish"
            result["pcr_weight_us500"] = +0.45  # contrarian: put heavy = bottom near
            result["pcr_weight_xau"]   = +0.3
        elif pcr >= 0.90:
            result["pcr_signal"] = f"Elevated put buying (P/C={pcr:.2f})"
            result["pcr_weight_us500"] = +0.2
            result["pcr_weight_xau"]   = +0.15
        elif pcr <= 0.55:
            result["pcr_signal"] = f"Extreme complacency (P/C={pcr:.2f}) — contrarian bearish"
            result["pcr_weight_us500"] = -0.4
            result["pcr_weight_xau"]   = -0.2
        elif pcr <= 0.70:
            result["pcr_signal"] = f"Low put/call ratio (P/C={pcr:.2f}) — mild caution"
            result["pcr_weight_us500"] = -0.15
            result["pcr_weight_xau"]   = -0.1
        else:
            result["pcr_signal"] = f"Neutral put/call ratio (P/C={pcr:.2f})"
            result["pcr_weight_us500"] = 0.0
            result["pcr_weight_xau"]   = 0.0

    if vix > 0:
        if vix >= 30:
            result["vix_signal"] = f"VIX spike {vix:.1f} — extreme fear, XAU bid"
            result["vix_weight_us500"] = -0.5
            result["vix_weight_xau"]   = +0.5
        elif vix >= 20:
            result["vix_signal"] = f"VIX elevated {vix:.1f} — risk-off"
            result["vix_weight_us500"] = -0.25
            result["vix_weight_xau"]   = +0.25
        elif vix <= 13:
            result["vix_signal"] = f"VIX low {vix:.1f} — complacency risk"
            result["vix_weight_us500"] = -0.1
            result["vix_weight_xau"]   = -0.15
        else:
            result["vix_signal"] = f"VIX normal {vix:.1f}"
            result["vix_weight_us500"] = 0.0
            result["vix_weight_xau"]   = 0.0

    if result:
        _ext_set("options", result)
    return result

# ============ MOMENTUM & VELOCITY ============
def compute_momentum(rows, asset, now):
    """
    Compare weighted score in the last 1h vs 1-4h window.
    Returns (momentum_score, velocity_score, velocity_why).
    momentum_score: positive = accelerating bullish, negative = accelerating bearish
    velocity: items/hour in last 2h vs baseline
    """
    # Split rows into time buckets
    s1h = s4h = 0.0
    c1h = c4h = 0
    for source, title, link, ts in rows:
        age = max(0.0, float(now - ts))
        if age > 14400: continue  # only 4h window
        w_src = SOURCE_WEIGHT.get(source, 1.0)
        w_time = decay_weight(age)
        for m in match_rules(asset, title):
            contrib = m["w"] * w_src * w_time
            if age <= 3600:
                s1h += contrib; c1h += 1
            else:
                s4h += contrib; c4h += 1

    # Momentum: is recent 1h stronger than the 1-4h average?
    avg_4h = s4h / max(c4h, 1)
    momentum = s1h - avg_4h  # positive = accelerating

    # Velocity: news items per hour in last 2h vs baseline 2-8h
    c2h   = sum(1 for s,t,l,ts in rows if (now-ts) <= 7200 and match_rules(asset, t))
    c8h   = sum(1 for s,t,l,ts in rows if 7200 < (now-ts) <= 28800 and match_rules(asset, t))
    rate2h = c2h / 2.0
    rate8h = c8h / 6.0
    velocity = rate2h - rate8h  # items/h above baseline

    vel_why = None
    if velocity > 1.5:
        vel_why = f"News velocity spiking (+{velocity:.1f}/h) — strong signal"
    elif velocity > 0.5:
        vel_why = f"Increasing news flow (+{velocity:.1f}/h)"

    return momentum, velocity, vel_why

# ============ CROSS-ASSET CORRELATION ADJUSTMENTS ============
# Correlation pairs: if asset A has strong signal, nudge correlated asset B
CORR_PAIRS = [
    ("BTC", "ETH",  +0.7),   # BTC bull → ETH bull
    ("ETH", "BTC",  +0.5),
    ("XAU", "BTC",  +0.25),  # inflation hedge correlation
    ("WTI", "XAU",  +0.15),  # commodity correlation
    ("US500", "ETH", +0.3),  # risk-on correlation
    ("US500", "BTC", +0.3),
]

def compute_corr_adjustments(scores_map):
    """Returns {asset: corr_adj} based on other assets' scores."""
    adj = {a: 0.0 for a in ASSETS}
    for src, dst, strength in CORR_PAIRS:
        src_score = scores_map.get(src, 0.0)
        src_th = BIAS_THRESH.get(src, 1.0)
        if abs(src_score) >= src_th * 0.6:  # only when src has meaningful signal
            normalized = src_score / max(abs(src_score), src_th)
            adj[dst] += normalized * strength
    return adj

# ============ IMPROVED COMPUTE BIAS ============
def compute_bias():
    now = int(time.time())
    cutoff = now - 86400
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,link,published_ts FROM news_items WHERE published_ts>=%s ORDER BY published_ts DESC LIMIT 1500;", (cutoff,))
            rows = cur.fetchall()
    finally: db_put(conn)

    fred_ok = FRED_CFG["enabled"] and FRED_CFG["api_key"] and requests
    fred_drivers = compute_fred_drivers() if fred_ok else {a: [] for a in ASSETS}

    upcoming = _get_upcoming(now)
    macro_sources = {"FED", "BLS", "BEA"}
    recent_macro = any(s in macro_sources and (now - ts) <= EVENT_CFG["recent_hours"] * 3600 for s, _, _, ts in rows)
    event_mode = EVENT_CFG["enabled"] and (recent_macro or any(x.get("ts") and x.get("currency") == "USD" and x.get("impact") in ("HIGH", "MED") for x in upcoming))
    event_risk = _event_risk(upcoming, recent_macro)

    # Fetch external metrics (non-blocking — cached)
    fg_data  = fetch_fear_greed()
    cot_data = fetch_cot()
    opt_data = fetch_options_data()

    # ── Pass 1: compute raw scores for cross-asset correlation ──
    raw_scores = {}
    for asset in ASSETS:
        s = 0.0
        for source, title, link, ts in rows:
            age = max(0.0, float(now - ts))
            w_src = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)
            for m in match_rules(asset, title):
                s += m["w"] * w_src * w_time
        for d in fred_drivers.get(asset, []):
            s += d["w"]
        raw_scores[asset] = s

    corr_adj = compute_corr_adjustments(raw_scores)

    assets_out = {}
    for asset in ASSETS:
        score = 0.0
        contribs, drivers_why = [], []
        freshness = {"0-2h": 0, "2-8h": 0, "8-24h": 0}
        matched_sources = set()

        for source, title, link, ts in rows:
            age = max(0.0, float(now - ts))
            w_src  = SOURCE_WEIGHT.get(source, 1.0)
            w_time = decay_weight(age)
            matches = match_rules(asset, title)
            for m in matches:
                contrib = m["w"] * w_src * w_time
                score  += contrib
                freshness[_fresh_bucket(age)] += 1
                matched_sources.add(source)
                contribs.append({"contrib": contrib, "why": m["why"]})
                if m["why"] not in drivers_why:
                    drivers_why.append(m["why"])

        for d in fred_drivers.get(asset, []):
            score += d["w"]
            contribs.append({"contrib": d["w"], "why": d["why"]})
            if d["why"] not in drivers_why:
                drivers_why.append(d["why"])

        # ── Correlation adjustment ──
        cadj = corr_adj.get(asset, 0.0)
        if abs(cadj) > 0.05:
            score += cadj
            contribs.append({"contrib": cadj, "why": "Cross-asset correlation signal"})

        # ── Fear & Greed contribution ──
        fg_contrib_label = None
        if asset in ("ETH", "BTC"):
            fg = fg_data.get("crypto_fg")
            if fg:
                fw = _fg_to_weight(fg["value"], asset)
                if abs(fw) > 0.05:
                    score += fw
                    label = f"Crypto F&G: {fg['value']} ({fg['label']})"
                    contribs.append({"contrib": fw, "why": label})
                    fg_contrib_label = label
        elif asset in ("US500", "XAU"):
            fg = fg_data.get("stock_fg")
            if fg:
                fw = _fg_to_weight(fg["value"], asset)
                if abs(fw) > 0.05:
                    score += fw
                    label = f"Mkt sentiment: {fg['value']} ({fg['label']})"
                    contribs.append({"contrib": fw, "why": label})
                    fg_contrib_label = label

        # ── Momentum & Velocity ──
        momentum, velocity, vel_why = compute_momentum(rows, asset, now)
        momentum_contrib = 0.0
        if abs(momentum) > 0.1:
            momentum_contrib = momentum * 0.4  # dampened
            score += momentum_contrib
            mom_label = "Bullish momentum accelerating" if momentum > 0 else "Bearish momentum accelerating"
            contribs.append({"contrib": momentum_contrib, "why": mom_label})
        if vel_why:
            vel_boost = velocity * 0.1 * (1 if score >= 0 else -1)
            score += vel_boost
            contribs.append({"contrib": vel_boost, "why": vel_why})

        # ── COT Report (speculative positioning) ──
        cot_weight, cot_label = cot_to_weight(asset, cot_data)
        cot_snap = cot_data.get(asset, {})
        if cot_label and abs(cot_weight) > 0.05:
            score += cot_weight
            contribs.append({"contrib": cot_weight, "why": cot_label})

        # ── Options Market (Put/Call + VIX) ──
        opt_labels = []
        if asset == "US500":
            pcr_w = opt_data.get("pcr_weight_us500", 0.0)
            vix_w = opt_data.get("vix_weight_us500", 0.0)
            if abs(pcr_w) > 0.05:
                score += pcr_w
                lbl = opt_data.get("pcr_signal", "Options signal")
                contribs.append({"contrib": pcr_w, "why": lbl})
                opt_labels.append(lbl)
            if abs(vix_w) > 0.05:
                score += vix_w
                lbl = opt_data.get("vix_signal", "VIX signal")
                contribs.append({"contrib": vix_w, "why": lbl})
                opt_labels.append(lbl)
        elif asset == "XAU":
            pcr_w = opt_data.get("pcr_weight_xau", 0.0)
            vix_w = opt_data.get("vix_weight_xau", 0.0)
            if abs(pcr_w) > 0.05:
                score += pcr_w
                lbl = opt_data.get("pcr_signal", "Options signal")
                contribs.append({"contrib": pcr_w, "why": lbl})
                opt_labels.append(lbl)
            if abs(vix_w) > 0.05:
                score += vix_w
                lbl = opt_data.get("vix_signal", "VIX signal")
                contribs.append({"contrib": vix_w, "why": lbl})
                opt_labels.append(lbl)
        elif asset in ("ETH", "BTC"):
            # Crypto: VIX spike = risk-off hits crypto, low VIX = risk-on lifts crypto
            vix_w = opt_data.get("vix_weight_us500", 0.0) * 0.7  # dampened
            if abs(vix_w) > 0.05:
                score += vix_w
                lbl = opt_data.get("vix_signal", "VIX signal")
                contribs.append({"contrib": vix_w, "why": lbl})
                opt_labels.append(lbl)

        th = BIAS_THRESH.get(asset, 1.0)
        bias = "BULLISH" if score >= th else ("BEARISH" if score <= -th else "NEUTRAL")

        # ── Aggregate contribs by label first (same rule on 100 articles → 1 entry) ──
        label_map: dict = {}
        for c in contribs:
            txt = c["why"]
            label_map[txt] = label_map.get(txt, 0.0) + c["contrib"]

        unique_contribs = sorted(
            [{"why": k, "contrib": v} for k, v in label_map.items()],
            key=lambda x: abs(x["contrib"]), reverse=True
        )

        # Conflict on unique signals (not raw RSS hits — much more accurate)
        u_net     = sum(x["contrib"] for x in unique_contribs)
        u_abs_sum = sum(abs(x["contrib"]) for x in unique_contribs) or 1.0
        conflict  = 1.0 - abs(u_net) / u_abs_sum

        src_div = len(matched_sources)
        fresh_total = sum(freshness.values()) or 1
        fresh_score = (freshness["0-2h"] * 1.0 + freshness["2-8h"] * 0.5 + freshness["8-24h"] * 0.1) / fresh_total

        strength   = min(1.0, abs(score) / max(th, 1.0))
        evidence_v = min(1.0, len(contribs) / 25.0)
        diversity  = min(1.0, src_div / 10.0)
        agreement  = 1.0 - conflict
        vel_bonus  = min(0.1, velocity * 0.02)
        # Extra quality bonus when multiple independent data sources agree
        ext_sources_agree = sum([
            1 if abs(cot_weight) > 0.1 and (cot_weight > 0) == (score > 0) else 0,
            1 if opt_labels and (opt_data.get("pcr_weight_us500",0) > 0) == (score > 0) else 0,
            1 if fg_contrib_label else 0,
        ])
        ext_bonus = min(0.08, ext_sources_agree * 0.03)

        raw = (
            0.30 * strength
          + 0.17 * evidence_v
          + 0.12 * diversity
          + 0.12 * fresh_score
          + 0.12 * agreement
          + 0.05 * min(1.0, abs(momentum))
          + vel_bonus
          + ext_bonus
          - 0.20 * conflict
          - (0.15 * event_risk if event_mode else 0.0)
        )
        quality = int(max(0, min(100, round(raw * 100))))

        # ── Aggregate contribs by label (same rule on 100 articles → single entry) ──
        label_map: dict = {}
        for c in contribs:
            txt = c["why"]
            if txt in label_map:
                label_map[txt] += c["contrib"]
            else:
                label_map[txt] = c["contrib"]
        # Rebuild as unique contrib list sorted by absolute weight
        unique_contribs = sorted(
            [{"why": k, "contrib": v} for k, v in label_map.items()],
            key=lambda x: abs(x["contrib"]), reverse=True
        )
        why_bias, why_opposite = [], []
        for d in unique_contribs:
            if (d["contrib"] > 0) == (score > 0):
                if len(why_bias) < 3: why_bias.append(d["why"])
            else:
                if len(why_opposite) < 2: why_opposite.append(d["why"])
            if len(why_bias) >= 3 and len(why_opposite) >= 2: break

        assets_out[asset] = {
            "bias": bias,
            "score": round(score, 3),
            "threshold": th,
            "quality": quality,
            "conflict": round(conflict, 2),
            "evidence_count": len(label_map),   # unique signals, not raw RSS hits
            "evidence_raw": len(contribs),       # raw count for debug
            "source_diversity": src_div,
            "why_bias": why_bias or ["Market signals unclear"],
            "why_opposite": why_opposite,
            "freshness": freshness,
            "event_mode": event_mode,
            "momentum": round(momentum, 3),
            "velocity": round(velocity, 2),
            "corr_adj": round(cadj, 3),
            "fear_greed": fg_contrib_label,
            "cot": {"pct_long": cot_snap.get("pct_long"), "signal": cot_snap.get("signal"), "net_long": cot_snap.get("net_long")} if cot_snap else None,
            "options": {"pcr": opt_data.get("equity_pcr") or opt_data.get("total_pcr"), "vix": opt_data.get("vix") or opt_data.get("vix_fred"), "pcr_signal": opt_data.get("pcr_signal"), "vix_signal": opt_data.get("vix_signal")} if opt_data else None,
        }

    # ── Save history snapshot for sparklines ──
    gate_results = {asset: eval_gate(assets_out[asset], {
        "event_mode": event_mode,
        "upcoming": upcoming
    }) for asset in ASSETS}
    save_bias_history(assets_out, gate_results)

    return {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "assets": assets_out,
        "event": {"event_mode": event_mode, "event_risk": round(event_risk, 2), "upcoming": upcoming[:5]},
        "meta": {"profile": GATE_PROFILE},
        "ext": {"fear_greed": fg_data},
    }

# ============ TRADE GATE ============
def eval_gate(a, event):
    cfg = GATE_THRESHOLDS[GATE_PROFILE]
    bias, quality, conflict = a.get("bias", "NEUTRAL"), a.get("quality", 0), a.get("conflict", 1)
    event_mode = event.get("event_mode", False)
    upcoming = event.get("upcoming", [])

    blockers, unlock = [], []

    if bias == "NEUTRAL":
        blockers.append("No clear market direction")
        unlock.append("Wait for clearer signal")

    if quality < cfg["quality_v2_min"]:
        blockers.append("Signal not strong enough")
        unlock.append("Wait for more confirming news")

    if conflict > cfg["conflict_max"]:
        blockers.append("Sources disagree on direction")
        unlock.append("Wait for consensus")

    if event_mode and cfg["event_mode_block"]:
        next_ev = next((e for e in upcoming if e.get("currency") == "USD" and e.get("impact") in ("HIGH", "MED")), None)
        if next_ev:
            blockers.append(f"Macro event coming ({next_ev.get('title', 'USD event')[:30]})")
            unlock.append("Wait for event to pass")
        elif not (quality >= cfg["event_override_quality"] and conflict <= cfg["event_override_conflict"]):
            blockers.append("Macro event window active")
            unlock.append("Wait for window to close")

    return {"ok": len(blockers) == 0, "blockers": blockers, "unlock": unlock}

# ============ PIPELINE ============
def pipeline_run():
    if not _PIPELINE_LOCK.acquire(timeout=RUN_LOCK_TIMEOUT_SEC):
        db_init()
        return load_bias() or {}
    try:
        db_init()
        ingest_news()
        last_cal = int(kv_get("cal_ts") or 0)
        if (int(time.time()) - last_cal) >= CAL_INGEST_TTL_SEC:
            ingest_calendar()
            kv_set("cal_ts", str(int(time.time())))
        if FRED_ON_RUN:
            for sid in FRED_SERIES: fred_ingest(sid, 120)
        payload = compute_bias()
        payload["meta"]["feeds_status"] = feeds_health_live()
        save_bias(payload)
        return payload
    finally: _PIPELINE_LOCK.release()

def _scheduled_pipeline():
    try:
        print(f"[scheduler] Running pipeline at {datetime.now(timezone.utc).isoformat()}")
        pipeline_run()
        print(f"[scheduler] Done.")
    except Exception as e:
        print(f"[scheduler] Error: {e}")

# ============ API ============
app = FastAPI(title="News Bias Terminal")

@app.on_event("startup")
def startup_event():
    db_init()
    if _APSCHEDULER_AVAILABLE:
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            _scheduled_pipeline,
            trigger="interval",
            minutes=AUTO_REFRESH_MINUTES,
            id="pipeline_refresh",
            replace_existing=True,
            max_instances=1,
        )
        scheduler.start()
        print(f"[scheduler] Started — pipeline runs every {AUTO_REFRESH_MINUTES} minutes.")
    else:
        print("[scheduler] APScheduler not installed — auto-refresh disabled.")

@app.get("/", include_in_schema=False)
def root(): return RedirectResponse("/dashboard")

@app.get("/health")
def health():
    return {"ok": True, "profile": GATE_PROFILE, "auto_refresh_minutes": AUTO_REFRESH_MINUTES,
            "scheduler": "apscheduler" if _APSCHEDULER_AVAILABLE else "disabled", "assets": ASSETS}

@app.get("/api/data")
def api_data():
    db_init()
    return load_bias() or pipeline_run()

@app.post("/api/refresh")
def api_refresh(token: str = "", x_run_token: Optional[str] = Header(None)):
    t = (token or x_run_token or "").strip()
    toks = get_run_tokens()
    if toks and not any(hmac.compare_digest(t, a) for a in toks):
        return JSONResponse({"ok": False}, 401)
    return {"ok": True, "updated": pipeline_run().get("updated_utc")}

@app.get("/latest")
def latest(limit: int = 20):
    db_init()
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source,title,published_ts FROM news_items ORDER BY published_ts DESC LIMIT %s;", (min(50, limit),))
            return {"items": [{"source": s, "title": t, "ts": ts} for s, t, ts in cur.fetchall()]}
    finally: db_put(conn)

@app.get("/api/history/{asset}")
def api_history(asset: str, hours: int = 24):
    """Return bias history for sparkline rendering."""
    db_init()
    if asset not in ASSETS:
        return JSONResponse({"error": "unknown asset"}, 400)
    return {"asset": asset, "history": load_bias_history(asset, hours)}

@app.get("/api/ext-status")
def api_ext_status():
    """Return status of external data sources: COT, Options, F&G."""
    db_init()
    return {
        "cot":     _ext_get("cot"),
        "options": _ext_get("options"),
        "fear_greed": _ext_get("fear_greed"),
    }

@app.get("/feeds")
def feeds():
    db_init()
    st = feeds_health_live(True)
    return {"total": len(st), "ok": sum(1 for v in st.values() if v.get("ok")), "feeds": st}

@app.get("/myfx_calendar", response_class=HTMLResponse)
def myfx_cal():
    return HTMLResponse(f'<!doctype html><html><head><meta charset="utf-8"><style>html,body{{height:100%;margin:0;background:#000}}</style></head><body><iframe src="{MYFXBOOK_IFRAME}" style="width:100%;height:100%;border:0"></iframe></body></html>')


# ============ DASHBOARD ============
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    db_init()
    payload = load_bias() or pipeline_run()
    assets  = payload.get("assets", {})
    event   = payload.get("event", {})
    meta    = payload.get("meta", {})

    now_ts     = int(time.time())
    event_mode = event.get("event_mode", False)
    upcoming   = event.get("upcoming", [])[:5]

    next_ev = next((e for e in upcoming if e.get("currency") == "USD" and e.get("impact") in ("HIGH", "MED")), None)
    if not next_ev and upcoming: next_ev = upcoming[0]

    feeds_st    = meta.get("feeds_status", {})
    feeds_ok    = sum(1 for v in feeds_st.values() if v.get("ok"))
    feeds_total = len(feeds_st) or len(RSS_FEEDS)

    asset_data = []
    for sym in ASSETS:
        a = assets.get(sym, {})
        gate = eval_gate(a, event)
        a["gate"] = gate
        asset_data.append((sym, a))

    # ── asset icon mapping ──
    ASSET_ICONS = {"XAU": "◈", "US500": "▦", "WTI": "⬡", "ETH": "⟠", "BTC": "₿", "EUR": "€"}

    def asset_row(sym, a):
        bias  = a.get("bias", "NEUTRAL")
        gate  = a.get("gate", {})
        ok    = gate.get("ok", False)
        blockers  = gate.get("blockers", [])
        why_bias  = a.get("why_bias", [])
        score     = a.get("score", 0.0)
        th        = a.get("threshold", 1.0)
        momentum  = a.get("momentum", 0.0)
        quality   = a.get("quality", 0)

        bias_icon  = "▲" if bias == "BULLISH" else ("▼" if bias == "BEARISH" else "●")
        bias_color = "#00d4aa" if bias == "BULLISH" else ("#ff5f56" if bias == "BEARISH" else "#666")
        trade_text = "TRADE" if ok else "WAIT"
        trade_color = "#00d4aa" if ok else "#ffbd2e"
        trade_bg    = "#00d4aa18" if ok else "#ffbd2e18"
        reason = blockers[0] if blockers else (why_bias[0] if why_bias else "Analyzing...")
        icon   = ASSET_ICONS.get(sym, "●")
        border_accent = bias_color if bias != "NEUTRAL" else "#30363d"

        # Strength bar: 0-100% fill based on score vs threshold
        strength_pct = min(100, int(abs(score) / max(th, 0.1) * 100))
        bar_color = bias_color

        # Momentum arrow
        mom_html = ""
        if abs(momentum) > 0.08:
            mom_arrow = "↑" if momentum > 0 else "↓"
            mom_color = "#00d4aa" if momentum > 0 else "#ff5f56"
            mom_html = f'<span style="color:{mom_color};font-size:9px;margin-left:4px">{mom_arrow}</span>'

        return f'''
        <div class="asset-card" onclick="openView('{sym}')" style="border-top:2px solid {border_accent}">
            <div class="card-top">
                <span class="card-icon">{icon}</span>
                <span class="card-sym">{sym}{mom_html}</span>
                <span class="card-bias" style="color:{bias_color}">{bias} {bias_icon}</span>
            </div>
            <div class="card-reason">{reason[:55]}</div>
            <div class="strength-bar">
                <div class="strength-fill" style="width:{strength_pct}%;background:{bar_color}"></div>
            </div>
            <div class="card-trade" style="background:{trade_bg};color:{trade_color};border:1px solid {trade_color}40">{trade_text}</div>
        </div>'''

    if next_ev:
        ev_ccy    = next_ev.get("currency") or "—"
        ev_title  = (next_ev.get("title") or "Event")[:40]
        ev_impact = next_ev.get("impact") or ""
        ev_time   = _fmt_time(next_ev.get("ts"))
        impact_color = "#ff5f56" if ev_impact == "HIGH" else ("#ffbd2e" if ev_impact == "MED" else "#888")
        next_ev_html = f'<span class="ev-ccy">{ev_ccy}</span> {ev_title} <span class="ev-impact" style="color:{impact_color}">{ev_impact}</span> <span class="ev-time">in {ev_time}</span>'
    else:
        next_ev_html = '<span class="ev-none">No major USD events upcoming</span>'

    js_payload  = json.dumps(payload).replace("</", "<\\/")
    tv_json     = json.dumps(TV_SYMBOLS)
    updated     = payload.get("updated_utc", "")[:19].replace("T", " ")

    return HTMLResponse(f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>NEWS BIAS TERMINAL</title>
<style>
:root {{
    --bg: #0d1117;
    --surface: #161b22;
    --surface2: #1c2128;
    --border: #30363d;
    --text: #c9d1d9;
    --text-muted: #8b949e;
    --orange: #ff9500;
    --green: #00d4aa;
    --red: #ff5f56;
    --yellow: #ffbd2e;
}}
* {{ margin:0; padding:0; box-sizing:border-box; -webkit-tap-highlight-color: transparent; }}
html {{ height: 100%; }}
body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'SF Mono', 'Consolas', 'Monaco', monospace;
    font-size: 13px;
    line-height: 1.4;
    min-height: 100vh;
    /* Prevent iOS bounce */
    overscroll-behavior: none;
}}

/* ── HEADER ── */
.header {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0 12px;
    height: 44px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: sticky;
    top: 0;
    z-index: 50;
}}
.logo {{ color: var(--orange); font-weight: 700; font-size: 11px; letter-spacing: 1px; line-height: 1; }}
.logo small {{ display: block; color: var(--text-muted); font-size: 8px; font-weight: 400; letter-spacing: 0.5px; margin-top: 2px; }}
.header-right {{ display: flex; align-items: center; gap: 6px; }}
.status {{ display: flex; align-items: center; gap: 4px; font-size: 9px; color: var(--text-muted); }}
.live-dot {{
    width: 5px; height: 5px;
    background: var(--green);
    border-radius: 50%;
    animation: pulse 2s infinite;
    flex-shrink: 0;
}}
@keyframes pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:0.4; }} }}

.hdr-btn {{
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text-muted);
    padding: 5px 9px;
    border-radius: 6px;
    font-size: 9px;
    font-weight: 700;
    cursor: pointer;
    font-family: inherit;
    letter-spacing: 0.3px;
    /* larger tap target on mobile */
    min-height: 30px;
    display: flex; align-items: center;
}}
.hdr-btn:active {{ background: var(--surface2); color: var(--orange); border-color: var(--orange); }}

/* ── TV WIDGET ── */
.tv-wrap {{ border-bottom: 1px solid var(--border); background: #000; }}

/* ── NEWS TICKER ── */
.news-ticker {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 5px 0;
    overflow: hidden;
    font-size: 10px;
    display: flex;
    align-items: center;
}}
.ticker-label {{
    color: var(--orange); font-size: 8px; font-weight: 700;
    padding: 0 10px; flex-shrink: 0;
    letter-spacing: 0.5px;
}}
.ticker-outer {{ overflow: hidden; flex: 1; }}
.ticker-scroll {{ display: inline-block; white-space: nowrap; animation: ticker 300s linear infinite; }}
.ticker-scroll:active {{ animation-play-state: paused; }}
@keyframes ticker {{ 0% {{ transform: translateX(0); }} 100% {{ transform: translateX(-50%); }} }}
.ticker-item {{ display: inline; margin-right: 36px; color: var(--text-muted); }}
.ticker-item b {{ color: var(--orange); margin-right: 5px; }}

/* ── NEXT EVENT BAR ── */
.next-event {{
    background: var(--surface2);
    border-bottom: 1px solid var(--border);
    padding: 7px 12px;
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 10px;
    min-height: 34px;
}}
.ev-label {{
    color: var(--orange); font-size: 8px; font-weight: 700;
    letter-spacing: 0.5px; flex-shrink: 0;
}}
.ev-body {{ flex: 1; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; color: var(--text); }}
.ev-ccy {{
    background: #1f6feb33; color: #58a6ff;
    padding: 1px 5px; border-radius: 3px;
    font-size: 9px; font-weight: 700; margin-right: 5px;
}}
.ev-impact {{ font-size: 8px; font-weight: 700; margin-left: 4px; }}
.ev-time {{ color: var(--text-muted); margin-left: 4px; font-size: 9px; }}
.ev-none {{ color: var(--text-muted); font-style: italic; font-size: 10px; }}

/* ── ASSET GRID (2 columns on mobile) ── */
.assets {{ padding: 10px 10px 0; }}
.section-header {{
    color: var(--text-muted); font-size: 8px; font-weight: 700;
    letter-spacing: 1px; margin-bottom: 8px; padding: 0 2px;
}}
.asset-grid {{
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 8px;
}}

/* ── ASSET CARD ── */
.asset-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 10px 10px;
    cursor: pointer;
    transition: border-color 0.15s, transform 0.1s;
    display: flex;
    flex-direction: column;
    gap: 8px;
    -webkit-user-select: none;
    user-select: none;
    /* border-top set inline for bias color accent */
}}
.asset-card:active {{ transform: scale(0.97); background: var(--surface2); }}

.card-top {{
    display: flex;
    align-items: center;
    gap: 6px;
}}
.card-icon {{
    font-size: 15px; opacity: 0.65; flex-shrink: 0;
    width: 20px; text-align: center;
}}
.card-sym {{
    font-size: 13px; font-weight: 700; color: var(--text); flex: 1;
    letter-spacing: 0.3px;
}}
.card-bias {{
    font-size: 9px; font-weight: 700; text-align: right;
    white-space: nowrap; letter-spacing: 0.3px;
}}
.card-reason {{
    font-size: 9px; color: var(--text-muted);
    line-height: 1.4;
    min-height: 26px;
    /* allow 2 lines on mobile */
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
}}
.card-trade {{
    align-self: flex-start;
    padding: 4px 10px;
    border-radius: 5px;
    font-size: 9px; font-weight: 700;
    letter-spacing: 0.5px;
    text-align: center;
    min-width: 52px;
}}

/* ── STRENGTH BAR ── */
.strength-bar {{
    height: 3px;
    background: var(--border);
    border-radius: 2px;
    overflow: hidden;
    margin: 0 0 2px;
}}
.strength-fill {{
    height: 100%;
    border-radius: 2px;
    transition: width 0.5s ease;
    opacity: 0.8;
}}

/* ── COUNTDOWN BAR ── */
.next-refresh {{
    margin: 8px 10px 0;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 12px;
    font-size: 9px;
    color: var(--text-muted);
    text-align: center;
}}
.next-refresh span {{ color: var(--green); font-weight: 700; }}

/* ── FOOTER ── */
.spacer {{ height: 64px; }}
.footer {{
    background: var(--surface);
    border-top: 1px solid var(--border);
    padding: 8px 12px;
    padding-bottom: calc(8px + env(safe-area-inset-bottom));
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: fixed;
    bottom: 0; left: 0; right: 0;
    z-index: 50;
}}
.footer-btns {{ display: flex; gap: 6px; }}
.btn {{
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 14px;
    border-radius: 8px;
    font-size: 10px;
    font-weight: 700;
    cursor: pointer;
    font-family: inherit;
    min-height: 36px;
    display: flex; align-items: center; gap: 4px;
}}
.btn:active {{ border-color: var(--orange); color: var(--orange); }}
.footer-info {{ color: var(--text-muted); font-size: 8px; text-align: right; line-height: 1.6; }}

/* ── MODAL ── */
.modal {{
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.88);
    z-index: 100;
    overflow-y: auto;
    -webkit-overflow-scrolling: touch;
}}
.modal.open {{ display: flex; justify-content: center; align-items: flex-end; }}
@media (min-height: 700px) {{
    .modal.open {{ align-items: center; padding: 20px; }}
}}
.modal-box {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px 16px 0 0;
    width: 100%;
    max-width: 540px;
    max-height: 88vh;
    overflow-y: auto;
    -webkit-overflow-scrolling: touch;
    padding-bottom: env(safe-area-inset-bottom);
}}
@media (min-height: 700px) {{
    .modal-box {{ border-radius: 16px; max-height: 82vh; }}
}}
/* drag handle */
.modal-handle {{
    width: 36px; height: 4px;
    background: var(--border);
    border-radius: 2px;
    margin: 10px auto 0;
}}
.modal-header {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 16px 12px;
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0;
    background: var(--surface);
    z-index: 1;
}}
.modal-title {{ font-size: 15px; font-weight: 700; color: var(--orange); }}
.modal-close {{
    background: var(--bg); border: 1px solid var(--border);
    color: var(--text-muted); font-size: 14px; cursor: pointer;
    padding: 4px 10px; border-radius: 6px;
    min-width: 32px; min-height: 32px;
    display: flex; align-items: center; justify-content: center;
}}
.modal-close:active {{ color: var(--text); }}
.modal-body {{ padding: 16px; }}
.view-section {{ margin-bottom: 18px; }}
.view-label {{
    font-size: 9px; font-weight: 700; color: var(--text-muted);
    letter-spacing: 0.8px; margin-bottom: 8px; text-transform: uppercase;
}}
.view-item {{
    padding: 10px 12px; background: var(--bg);
    border-radius: 6px; margin-bottom: 6px;
    font-size: 12px; border-left: 3px solid var(--border);
    line-height: 1.4;
}}
.view-item.why {{ border-left-color: var(--green); color: var(--green); }}
.view-item.block {{ border-left-color: var(--red); color: var(--red); }}
.view-item.unlock {{ border-left-color: var(--yellow); color: var(--yellow); }}
.view-item.opposite {{ border-left-color: #555; color: var(--text-muted); }}

/* stats row in modal */
.stats-row {{
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px;
    margin-bottom: 16px;
}}
.stat-box {{
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 8px 4px;
    text-align: center;
}}
.stat-val {{ font-size: 14px; font-weight: 700; color: var(--text); }}
.stat-lbl {{ font-size: 8px; color: var(--text-muted); margin-top: 2px; letter-spacing: 0.3px; }}

.feed-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 5px; }}
.feed-item {{
    background: var(--bg); padding: 7px 8px; border-radius: 5px;
    font-size: 9px; display: flex; justify-content: space-between; align-items: center;
    gap: 4px;
}}
.feed-name {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.feed-ok {{ color: var(--green); flex-shrink: 0; }}
.feed-bad {{ color: var(--red); flex-shrink: 0; }}

/* ── DESKTOP overrides ── */
@media (min-width: 640px) {{
    .header {{ padding: 0 16px; height: 48px; }}
    .logo {{ font-size: 12px; }}
    .logo small {{ display: none; }}
    .assets {{ padding: 12px 16px 0; }}
    .asset-grid {{ grid-template-columns: repeat(3, 1fr); gap: 10px; }}
    .asset-card {{ padding: 14px 12px 12px; gap: 10px; }}
    .card-sym {{ font-size: 14px; }}
    .card-reason {{ font-size: 10px; -webkit-line-clamp: 1; }}
    .section-header {{ font-size: 9px; }}
    .next-refresh {{ margin: 10px 16px 0; }}
    .spacer {{ height: 54px; }}
    .footer {{ padding: 8px 16px; }}
    .footer-info {{ font-size: 9px; }}
}}

@media (min-width: 900px) {{
    .asset-grid {{ grid-template-columns: repeat(3, 1fr); max-width: 900px; margin: 0 auto; }}
    .assets {{ max-width: 900px; margin: 0 auto; }}
    .next-refresh {{ max-width: 900px; margin: 10px auto 0; }}
}}
</style>
</head>
<body>

<div class="header">
    <div class="logo">NEWS BIAS <span style="color:var(--text-muted)">// TERMINAL</span>
        <small id="clock">{updated[:10]} {updated[11:16]} UTC</small>
    </div>
    <div class="header-right">
        <div class="status"><div class="live-dot"></div><span>LIVE</span></div>
        <button class="hdr-btn" onclick="manualRefresh()" id="refreshBtn">↻</button>
        <button class="hdr-btn" onclick="openFeeds()">{feeds_ok}/{feeds_total}</button>
    </div>
</div>

<!-- TradingView ticker tape — compact, slower scroll via CSS override -->
<div class="tv-wrap">
    <div class="tradingview-widget-container">
        <div class="tradingview-widget-container__widget"></div>
        <script src="https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js" async>
        {{"symbols":{tv_json},"showSymbolLogo":true,"isTransparent":true,"displayMode":"compact","colorTheme":"dark","speed":20}}
        </script>
    </div>
</div>

<div class="news-ticker">
    <span class="ticker-label">▶ NEWS</span>
    <span class="ticker-outer"><div class="ticker-scroll" id="newsTicker">Loading headlines...</div></span>
</div>

<div class="next-event">
    <span class="ev-label">NEXT EVENT</span>
    {next_ev_html}
</div>

<div class="assets">
    <div class="section-header">TRADE SIGNALS</div>
    <div class="asset-grid">
    {''.join(asset_row(sym, a) for sym, a in asset_data)}
    </div>
</div>

<div class="next-refresh">
    Auto-refresh every {AUTO_REFRESH_MINUTES} min &nbsp;|&nbsp; Next in: <span id="countdown">—</span>
</div>

<div class="spacer"></div>

<div class="footer">
    <div class="footer-btns">
        <button class="btn" onclick="openCalendar()">📅 CALENDAR</button>
        <button class="btn" onclick="openFeeds()">📡 FEEDS</button>
    </div>
    <div class="footer-info">
        <span>Profile: {GATE_PROFILE}</span>
        <span>Keys: 1-6 view • Esc close</span>
    </div>
</div>

<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
    <div class="modal-box">
        <div class="modal-handle"></div>
        <div class="modal-header">
            <div class="modal-title" id="modalTitle">View</div>
            <button class="modal-close" onclick="closeModal()">✕</button>
        </div>
        <div class="modal-body" id="modalBody"></div>
    </div>
</div>

<script>
const P = {js_payload};
const ASSETS = {json.dumps(ASSETS)};
const ASSET_ICONS = {json.dumps({sym: {"XAU":"◈","US500":"▦","WTI":"⬡","ETH":"⟠","BTC":"₿","EUR":"€"}.get(sym,"●") for sym in ASSETS})};
const CFG = {json.dumps(GATE_THRESHOLDS[GATE_PROFILE])};
const AUTO_REFRESH_MS = {AUTO_REFRESH_MINUTES} * 60 * 1000;

let _nextRefreshAt = Date.now() + AUTO_REFRESH_MS;

function updateCountdown() {{
    const remaining = Math.max(0, Math.round((_nextRefreshAt - Date.now()) / 1000));
    const m = Math.floor(remaining / 60);
    const s = remaining % 60;
    document.getElementById('countdown').textContent = m + 'm ' + String(s).padStart(2,'0') + 's';
}}
setInterval(updateCountdown, 1000);
updateCountdown();

// Full page reload every AUTO_REFRESH_MINUTES
setInterval(() => window.location.reload(), AUTO_REFRESH_MS);

// Lightweight clock + data sync every 30s
setInterval(async () => {{
    try {{
        const r = await fetch('/api/data');
        const d = await r.json();
        if (d.updated_utc) document.getElementById('clock').textContent = d.updated_utc.slice(0,19).replace('T',' ') + ' UTC';
    }} catch {{}}
}}, 30000);

async function manualRefresh() {{
    const btn = document.getElementById('refreshBtn');
    btn.textContent = '↻ ...';
    try {{
        await fetch('/api/data');
        _nextRefreshAt = Date.now() + AUTO_REFRESH_MS;
        window.location.reload();
    }} catch {{ btn.textContent = '↻ REFRESH'; }}
}}

async function loadNews() {{
    try {{
        const r = await fetch('/latest?limit=20');
        const d = await r.json();
        const items = (d.items || []).map(i => `<span class="ticker-item"><b>${{esc(i.source)}}</b>${{esc(i.title)}}</span>`).join('');
        document.getElementById('newsTicker').innerHTML = items + items; // doubled for seamless loop
    }} catch {{}}
}}
loadNews();
setInterval(loadNews, 120000);

function esc(s) {{ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}

function openModal(title, html) {{
    document.getElementById('modalTitle').textContent = title;
    document.getElementById('modalBody').innerHTML = html;
    document.getElementById('modal').classList.add('open');
}}
function closeModal() {{ document.getElementById('modal').classList.remove('open'); }}

async function openView(sym) {{
    const a = P.assets?.[sym] || {{}};
    const ev = P.event || {{}};
    const ext = P.ext || {{}};
    const bias = a.bias || 'NEUTRAL';
    const whyBias = a.why_bias || [];
    const whyOpp = a.why_opposite || [];
    const blockers = [], unlock = [];
    const quality = a.quality || 0;
    const conflict = a.conflict || 1;
    const evCount = a.evidence_count || 0;
    const momentum = a.momentum || 0;
    const velocity = a.velocity || 0;
    const eventMode = ev.event_mode;
    const upcoming = ev.upcoming || [];

    if (bias === 'NEUTRAL') {{ blockers.push('No clear market direction'); unlock.push('Wait for clearer signal'); }}
    if (quality < CFG.quality_v2_min) {{ blockers.push('Signal not strong enough'); unlock.push('Wait for more confirming news'); }}
    if (conflict > CFG.conflict_max) {{ blockers.push('Sources disagree on direction'); unlock.push('Wait for consensus'); }}
    if (eventMode && CFG.event_mode_block) {{
        const nextUSD = upcoming.find(e => e.currency === 'USD' && ['HIGH','MED'].includes(e.impact));
        if (nextUSD) {{
            blockers.push('Macro event coming (' + (nextUSD.title || 'USD event').slice(0,25) + ')');
            unlock.push('Wait for event to pass');
        }} else if (!(quality >= CFG.event_override_quality && conflict <= CFG.event_override_conflict)) {{
            blockers.push('Macro event window active');
            unlock.push('Wait for window to close');
        }}
    }}

    const ok = blockers.length === 0;
    const biasColor = bias === 'BULLISH' ? '#00d4aa' : (bias === 'BEARISH' ? '#ff5f56' : '#888');
    const tradeColor = ok ? '#00d4aa' : '#ffbd2e';
    const icon = ASSET_ICONS[sym] || '●';
    const momArrow = momentum > 0.08 ? ' ↑' : (momentum < -0.08 ? ' ↓' : '');
    const momColor = momentum > 0.08 ? '#00d4aa' : (momentum < -0.08 ? '#ff5f56' : 'transparent');

    // F&G bar
    let fgHtml = '';
    const fgCrypto = ext.fear_greed?.crypto_fg;
    const fgStock  = ext.fear_greed?.stock_fg;
    const fgSrc = ['ETH','BTC'].includes(sym) ? fgCrypto : (['US500','XAU'].includes(sym) ? fgStock : null);
    if (fgSrc) {{
        const fv = fgSrc.value;
        const fc = fv >= 60 ? '#00d4aa' : (fv <= 40 ? '#ff5f56' : '#ffbd2e');
        const lbl = ['ETH','BTC'].includes(sym) ? 'Crypto F&G' : 'Mkt F&G';
        fgHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 12px;background:var(--bg);border-radius:6px;margin-bottom:6px;font-size:11px">
            <span style="color:var(--text-muted);min-width:60px">${{lbl}}</span>
            <span style="color:${{fc}};font-weight:700;min-width:26px">${{fv}}</span>
            <span style="color:${{fc}};flex:1">${{fgSrc.label}}</span>
            <div style="width:60px;height:4px;background:var(--border);border-radius:2px;overflow:hidden;flex-shrink:0">
                <div style="width:${{fv}}%;height:100%;background:${{fc}};border-radius:2px"></div>
            </div>
        </div>`;
    }}

    // COT panel
    const cotD = a.cot;
    let cotHtml = '';
    if (cotD && cotD.pct_long != null) {{
        const cp = cotD.pct_long;
        const cc = cp >= 60 ? '#00d4aa' : (cp <= 40 ? '#ff5f56' : '#ffbd2e');
        const netFmt = cotD.net_long > 0 ? '+' + cotD.net_long.toLocaleString() : cotD.net_long.toLocaleString();
        cotHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 12px;background:var(--bg);border-radius:6px;margin-bottom:6px;font-size:11px">
            <span style="color:var(--text-muted);min-width:60px">COT</span>
            <span style="color:${{cc}};font-weight:700;min-width:40px">${{cp}}% L</span>
            <span style="color:var(--text-muted);flex:1;font-size:9px">${{cotD.signal}}</span>
            <span style="color:${{cc}};font-size:9px;font-weight:700">${{netFmt}}</span>
        </div>`;
    }}

    // Options panel (PCR + VIX)
    const optD = a.options;
    let optHtml = '';
    if (optD && (optD.pcr || optD.vix)) {{
        const pcrVal = optD.pcr ? optD.pcr.toFixed(2) : '—';
        const vixVal = optD.vix ? optD.vix.toFixed(1) : '—';
        const pcr = optD.pcr || 0;
        const vix = optD.vix || 0;
        const pcrC = pcr >= 1.0 ? '#00d4aa' : (pcr <= 0.65 ? '#ff5f56' : '#ffbd2e');
        const vixC = vix >= 25 ? '#ff5f56' : (vix <= 14 ? '#ffbd2e' : '#00d4aa');
        optHtml = `<div style="display:flex;gap:6px;margin-bottom:6px">
            <div style="flex:1;display:flex;align-items:center;gap:6px;padding:8px 10px;background:var(--bg);border-radius:6px;font-size:11px">
                <span style="color:var(--text-muted);min-width:32px">P/C</span>
                <span style="color:${{pcrC}};font-weight:700">${{pcrVal}}</span>
                <span style="color:var(--text-muted);font-size:9px;flex:1">${{pcr >= 1.0 ? 'puts heavy' : pcr <= 0.65 ? 'calls heavy' : 'neutral'}}</span>
            </div>
            <div style="flex:1;display:flex;align-items:center;gap:6px;padding:8px 10px;background:var(--bg);border-radius:6px;font-size:11px">
                <span style="color:var(--text-muted);min-width:32px">VIX</span>
                <span style="color:${{vixC}};font-weight:700">${{vixVal}}</span>
                <span style="color:var(--text-muted);font-size:9px;flex:1">${{vix >= 25 ? 'fear' : vix <= 14 ? 'complacent' : 'normal'}}</span>
            </div>
        </div>`;
    }}

    let html = `
        <div style="text-align:center;padding:14px 0 16px;border-bottom:1px solid var(--border);margin-bottom:14px;">
            <div style="font-size:22px;opacity:0.4;margin-bottom:3px">${{icon}}</div>
            <div style="font-size:20px;font-weight:700;color:${{biasColor}};margin-bottom:3px">${{bias}}</div>
            <div style="font-size:10px;color:${{momColor}};margin-bottom:10px;height:14px">${{momArrow ? 'Momentum' + momArrow : ''}}</div>
            <div style="display:inline-block;background:${{tradeColor}}22;color:${{tradeColor}};padding:8px 28px;border-radius:8px;font-weight:700;font-size:14px;border:1px solid ${{tradeColor}}40">${{ok ? '✓ TRADE' : '⏳ WAIT'}}</div>
        </div>
        <div class="stats-row">
            <div class="stat-box"><div class="stat-val">${{quality}}</div><div class="stat-lbl">QUALITY</div></div>
            <div class="stat-box"><div class="stat-val">${{Math.round(conflict*100)}}%</div><div class="stat-lbl">CONFLICT</div></div>
            <div class="stat-box"><div class="stat-val">${{evCount}}</div><div class="stat-lbl">SIGNALS</div></div>
            <div class="stat-box"><div class="stat-val">${{velocity >= 0 ? '+' : ''}}${{velocity.toFixed(1)}}</div><div class="stat-lbl">VEL/H</div></div>
        </div>
        ${{fgHtml}}${{cotHtml}}${{optHtml}}
        <div class="view-section">
            <div class="view-label" style="display:flex;justify-content:space-between;align-items:center">
                <span>SCORE HISTORY (24H)</span>
                <span id="sparkStatus_${{sym}}" style="color:var(--text-muted);font-size:8px;font-weight:400">loading...</span>
            </div>
            <div id="sparkWrap_${{sym}}" style="position:relative;background:var(--bg);border-radius:4px;min-height:52px;overflow:hidden">
                <canvas id="spark_${{sym}}" height="52" style="width:100%;display:block"></canvas>
                <div id="sparkEmpty_${{sym}}" style="display:none;position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--text-muted);font-size:9px;flex-direction:column;gap:4px">
                    <span>📊</span>
                    <span>Accumulating data — check back after 1-2 refreshes</span>
                </div>
            </div>
            <div id="sigLog_${{sym}}" style="margin-top:8px;max-height:110px;overflow-y:auto"></div>
        </div>
    `;
    if (whyBias.length) {{
        html += `<div class="view-section"><div class="view-label">WHY ${{bias}}</div>`;
        whyBias.forEach(w => {{ html += `<div class="view-item why">${{esc(w)}}</div>`; }});
        html += `</div>`;
    }}
    if (blockers.length) {{
        html += `<div class="view-section"><div class="view-label">WHY WAIT</div>`;
        blockers.forEach(b => {{ html += `<div class="view-item block">${{esc(b)}}</div>`; }});
        html += `</div>`;
    }}
    if (unlock.length) {{
        html += `<div class="view-section"><div class="view-label">WILL UNLOCK WHEN</div>`;
        unlock.forEach(u => {{ html += `<div class="view-item unlock">${{esc(u)}}</div>`; }});
        html += `</div>`;
    }}
    if (whyOpp.length) {{
        html += `<div class="view-section"><div class="view-label">OPPOSING SIGNALS</div>`;
        whyOpp.forEach(w => {{ html += `<div class="view-item opposite">${{esc(w)}}</div>`; }});
        html += `</div>`;
    }}
    // Data sources status strip
    const hasCOT  = !!a.cot;
    const hasOpts = !!a.options;
    const hasFG   = !!a.fear_greed;
    html += `<div style="display:flex;gap:6px;flex-wrap:wrap;padding-top:12px;border-top:1px solid var(--border);margin-top:4px">
        <span style="font-size:8px;color:var(--text-muted)">DATA SOURCES:</span>
        <span style="font-size:8px;color:${{hasFG ? '#00d4aa' : '#555'}}">${{hasFG ? '●' : '○'}} F&G</span>
        <span style="font-size:8px;color:${{hasCOT ? '#00d4aa' : '#555'}}">${{hasCOT ? '●' : '○'}} COT</span>
        <span style="font-size:8px;color:${{hasOpts ? '#00d4aa' : '#555'}}">${{hasOpts ? '●' : '○'}} Options/VIX</span>
        <span style="font-size:8px;color:#00d4aa">● News RSS</span>
        <span style="font-size:8px;color:#00d4aa">● FRED</span>
    </div>`;
    openModal(`${{icon}} ${{sym}}`, html);

    // Async: load history and render sparkline + signal log
    try {{
        const r = await fetch(`/api/history/${{sym}}?hours=24`);
        const d = await r.json();
        const hist = (d.history || []).slice().reverse(); // oldest first for sparkline
        const statusEl = document.getElementById(`sparkStatus_${{sym}}`);
        const emptyEl  = document.getElementById(`sparkEmpty_${{sym}}`);
        const canvas   = document.getElementById(`spark_${{sym}}`);
        if (hist.length >= 2) {{
            if (canvas) canvas.style.display = 'block';
            if (emptyEl) emptyEl.style.display = 'none';
            renderSparkline(`spark_${{sym}}`, hist);
            if (statusEl) statusEl.textContent = `${{hist.length}} points`;
        }} else {{
            if (canvas) canvas.style.display = 'none';
            if (emptyEl) emptyEl.style.display = 'flex';
            if (statusEl) statusEl.textContent = 'no data yet';
        }}
        renderSignalLog(`sigLog_${{sym}}`, d.history || []);
    }} catch(e) {{
        const statusEl = document.getElementById(`sparkStatus_${{sym}}`);
        if (statusEl) statusEl.textContent = 'error';
    }}
}}

function renderSparkline(canvasId, hist) {{
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.offsetWidth || canvas.parentElement.offsetWidth || 300;
    const H = 52;
    canvas.width  = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width  = W + 'px';
    canvas.style.height = H + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, W, H);

    const scores = hist.map(h => h.score);
    const minS = Math.min(...scores, -0.5);
    const maxS = Math.max(...scores, 0.5);
    const range = maxS - minS || 1;
    const toY = s => H - 4 - ((s - minS) / range) * (H - 8);
    const toX = (i) => 4 + (i / (hist.length - 1)) * (W - 8);

    // Zero line
    const zeroY = toY(0);
    ctx.setLineDash([3,3]);
    ctx.strokeStyle = '#30363d';
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(4, zeroY); ctx.lineTo(W-4, zeroY); ctx.stroke();
    ctx.setLineDash([]);

    // Filled area under line
    ctx.beginPath();
    hist.forEach((h, i) => {{ i === 0 ? ctx.moveTo(toX(i), toY(h.score)) : ctx.lineTo(toX(i), toY(h.score)); }});
    ctx.lineTo(toX(hist.length-1), zeroY);
    ctx.lineTo(toX(0), zeroY);
    ctx.closePath();
    const lastScore = hist[hist.length-1]?.score || 0;
    const fillColor = lastScore >= 0 ? '#00d4aa' : '#ff5f56';
    ctx.fillStyle = fillColor + '22';
    ctx.fill();

    // Line
    ctx.beginPath();
    hist.forEach((h, i) => {{
        const x = toX(i), y = toY(h.score);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    }});
    ctx.strokeStyle = fillColor;
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // TRADE dots
    hist.forEach((h, i) => {{
        if (h.gate_ok) {{
            ctx.beginPath();
            ctx.arc(toX(i), toY(h.score), 2.5, 0, Math.PI*2);
            ctx.fillStyle = '#00d4aa';
            ctx.fill();
        }}
    }});
}}

function renderSignalLog(elId, hist) {{
    // hist: newest first
    const el = document.getElementById(elId);
    if (!el || !hist.length) return;
    const recent = hist.slice(0, 8);
    let html = '';
    recent.forEach(h => {{
        const bc = h.bias === 'BULLISH' ? '#00d4aa' : (h.bias === 'BEARISH' ? '#ff5f56' : '#666');
        const gate = h.gate_ok ? '<span style="color:#00d4aa;font-size:8px">TRADE</span>' : '<span style="color:#ffbd2e;font-size:8px">WAIT</span>';
        const t = new Date(h.ts * 1000);
        const hhmm = t.getHours().toString().padStart(2,'0') + ':' + t.getMinutes().toString().padStart(2,'0');
        html += `<div style="display:flex;gap:8px;align-items:center;padding:4px 0;border-bottom:1px solid #1c2128;font-size:9px;color:var(--text-muted)">
            <span style="min-width:32px">${{hhmm}}</span>
            <span style="color:${{bc}};font-weight:700;min-width:54px">${{h.bias}}</span>
            <span style="min-width:28px">Q${{h.quality}}</span>
            ${{gate}}
        </div>`;
    }});
    el.innerHTML = html;
}}

function openFeeds() {{
    const f = P.meta?.feeds_status || {{}};
    const items = Object.entries(f).sort((a,b) => a[0].localeCompare(b[0]));
    const ok = items.filter(([,v]) => v.ok).length;
    let html = `<div style="margin-bottom:12px;color:var(--text-muted);font-size:11px;">Active: ${{ok}}/${{items.length}} feeds</div>`;
    html += `<div class="feed-grid">`;
    items.forEach(([name, info]) => {{
        html += `<div class="feed-item"><span class="feed-name">${{esc(name)}}</span><span class="${{info.ok ? 'feed-ok' : 'feed-bad'}}">${{info.ok ? '●' : '○'}}</span></div>`;
    }});
    html += `</div>`;
    openModal('📡 FEEDS', html);
}}

function openCalendar() {{
    openModal('ECONOMIC CALENDAR', `
        <div style="height:65vh;margin:-16px;margin-top:0;">
            <iframe src="/myfx_calendar" style="width:100%;height:100%;border:0;filter:invert(1) hue-rotate(180deg) contrast(0.9);"></iframe>
        </div>
    `);
}}

document.addEventListener('keydown', e => {{
    if (e.key === 'Escape') closeModal();
    ASSETS.forEach((sym, i) => {{ if (e.key === String(i+1)) openView(sym); }});
}});
</script>
</body>
</html>''')
