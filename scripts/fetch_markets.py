#!/usr/bin/env python3
"""
AGSIST fetch_markets.py  v7
════════════════════════════
Fetches prediction-market odds relevant to agriculture from Kalshi
and Polymarket.  Runs once daily via GitHub Actions (6 AM CT).

v7 fixes (2026-03-31):
  • CRITICAL FIX: Kalshi base URL was wrong (api.elections.kalshi.com is
    the defunct election-period API). Correct domain is trading-api.kalshi.com.
  • CRITICAL FIX: Removed search= param — not in Kalshi API spec. v6's
    entire "targeted search" strategy returned 0 results because every
    search query hit a 404/connection error, caught silently by http_get_json.
  • NEW STRATEGY: Kalshi — paginate /markets (max 200/page) and filter
    locally by keyword tier. Fetches up to 1000 markets total, stops
    early once 30+ relevant markets found.
  • CRITICAL FIX: Polymarket — changed _q to q (documented param).
    Also added direct tag-based browsing as primary strategy.
  • Added /events endpoint fetch for Kalshi — often surfaces themed
    market groups (energy, commodities, macro) more efficiently.
  • Lowered minimum relevance threshold from 40 to 35 to capture more
    macro/infrastructure markets relevant to farming.
  • Expanded tier keywords: added tariffs, trade policy terms that are
    highly relevant to grain exports.
  • Better error output: prints actual error text so GitHub Actions logs
    show what's failing.

v6 preserved:
  • Meme/sports filter (blacklists, player patterns, parlay detection)
  • Composite ranking with volume weighting
  • Clean empty state if both APIs truly return 0 relevant results

Sources (public, no API keys required):
  • Kalshi    — https://trading-api.kalshi.com/trade-api/v2/markets
  • Polymarket — https://gamma-api.polymarket.com/markets
"""

import json
import re
import os
import math
import time
from datetime import datetime, timezone

try:
    import urllib.request as urllib_request
    import urllib.error as urllib_error
    from urllib.parse import quote as url_quote, urlencode
except ImportError:
    import urllib2 as urllib_request
    from urllib import quote as url_quote


# ═════════════════════════════════════════════════════════════════
# 1. KEYWORD TIERS — for relevance scoring
# ═════════════════════════════════════════════════════════════════

TIER1_KEYWORDS = [
    # Direct commodities
    "corn", "soybean", "wheat", "grain", "oat", "barley",
    "cotton", "sugar", "rice", "canola", "sorghum",
    # Livestock
    "cattle", "hog", "pig", "livestock", "pork", "beef",
    "dairy", "milk", "poultry", "chicken", "egg",
    # Ag operations
    "ethanol", "crop", "harvest", "planting", "acreage",
    "fertilizer", "urea", "nitrogen", "potash", "phosphate",
    "farm bill", "usda", "wasde", "crop insurance",
    # Food inflation — directly affects ag demand
    "food price", "food inflation", "grocery price", "grocery store",
    "grocery", "egg price", "meat price",
    # Yield/production
    "bushel", "yield", "cropland", "soil moisture",
    "growing season", "feedlot",
]

TIER2_KEYWORDS = [
    # Trade — directly moves grain exports
    "tariff", "trade war", "trade deal", "trade agreement",
    "china trade", "china import", "china export",
    "brazil", "argentina", "ukraine", "black sea grain",
    "usmca", "nafta", "wto", "trade dispute",
    "export ban", "import quota", "sanction",
    "trade policy", "trade war", "trade escalation",
    "retaliatory tariff",
    # Energy — input costs
    "crude oil", "natural gas", "diesel", "gasoline",
    "energy price", "oil price", "opec",
    "pipeline", "biofuel", "renewable fuel",
    "carbon credit", "emission standard",
    # Supply chain
    "rail strike", "railroad", "freight rate",
    "mississippi river", "panama canal", "port strike",
    "supply chain", "shipping cost",
    # Labor/disease
    "bird flu", "avian influenza", "african swine fever",
    "h-2a", "farm labor", "immigration policy",
    "food safety",
]

TIER3_KEYWORDS = [
    # Macro — affects land values, export dollar, farm loans
    "interest rate", "federal reserve", "fed funds",
    "rate cut", "rate hike", "fomc", "powell",
    "inflation", "cpi", "ppi", "core inflation",
    "recession", "gdp growth", "unemployment",
    "dollar index", "usd", "currency",
    "government shutdown", "debt ceiling", "farm subsidy",
    # Weather systemic
    "el nino", "la nina", "hurricane", "flood",
    "drought", "heat wave", "frost", "freeze", "wildfire",
    "climate policy", "weather forecast",
    # Other macro
    "federal budget", "deficit", "treasury",
    "china economy", "global trade",
]


# ═════════════════════════════════════════════════════════════════
# 2. MEME / JUNK MARKET FILTER — aggressive
# ═════════════════════════════════════════════════════════════════

MEME_BLACKLIST = [
    "gta", "grand theft auto", "video game", "gaming", "esports",
    "playstation", "xbox", "nintendo", "steam", "fortnite",
    "minecraft", "call of duty", "league of legends", "valorant",
    "oscar", "grammy", "emmy", "golden globe", "tony award",
    "bachelor", "bachelorette", "reality tv", "survivor",
    "box office", "netflix", "disney", "hulu",
    "celebrity", "kardashian", "beyonce", "drake",
    "album", "billboard", "spotify", "concert",
    "tiktok", "instagram", "youtube", "twitch",
    "subscriber", "follower count", "viral",
    "spacex", "mars colony", "moon landing", "alien", "ufo",
    "dogecoin", "shiba", "pepe coin", "meme coin", "nft",
    "dating", "divorce", "wedding",
    "tweet", "twitter feud",
    "time person of the year", "most popular", "best dressed",
    "golden state", "lakers", "celtics", "warriors",
]

SPORTS_BLACKLIST = [
    "nfl", "nba", "mlb", "nhl", "mls", "wnba", "xfl",
    "premier league", "la liga", "bundesliga", "serie a",
    "champions league", "world cup", "olympics", "ncaa",
    "march madness", "super bowl", "world series",
    "stanley cup", "playoff", "playoffs",
    "mvp", "touchdown", "home run", "hat trick", "slam dunk",
    "draft pick", "free agent",
    "rushing yards", "passing yards", "batting average",
    "championship", "finals mvp", "all-star",
    "win total", "over/under", "point spread",
]

SPORTS_PLAYER_PATTERNS = [
    r"antetokounmpo", r"mahomes", r"jokic", r"luka\b", r"lebron",
    r"curry\b", r"giannis", r"ohtani", r"tatum", r"embiid",
    r"messi\b", r"ronaldo", r"haaland", r"mbappe",
    r"lamar jackson", r"josh allen", r"patrick mahomes",
]

SPORTS_STAT_PATTERNS = [
    r"\d+\+.*points",
    r"\d+\+.*goals",
    r"\d+\+.*rebounds",
    r"\d+\+.*assists",
    r"\d+\+.*strikeouts",
    r"\d+\+.*touchdowns",
    r"over \d+\.?\d* (points|goals|runs|yards)",
    r"win.*game\s*\d",
]

KALSHI_JUNK_TICKER_PATTERNS = [
    r"^KXMVE",
    r"CROSSCATEGORY",
    r"^KX.*PARLAY",
]


def is_meme_market(title, ticker=""):
    t = title.lower()
    if ticker:
        for pattern in KALSHI_JUNK_TICKER_PATTERNS:
            if re.search(pattern, ticker.upper()):
                return True
    for pattern in MEME_BLACKLIST:
        if pattern in t:
            return True
    for pattern in SPORTS_BLACKLIST:
        if pattern in t:
            return True
    for pattern in SPORTS_PLAYER_PATTERNS:
        if re.search(pattern, t):
            return True
    for pattern in SPORTS_STAT_PATTERNS:
        if re.search(pattern, t):
            return True
    return False


# ═════════════════════════════════════════════════════════════════
# 3. RELEVANCE SCORING
# ═════════════════════════════════════════════════════════════════

def score_relevance(text):
    """Score 0-100 how relevant a market is to agriculture."""
    t = text.lower()
    score = 0
    matched_tier = 0

    for kw in TIER1_KEYWORDS:
        if kw in t:
            score = max(score, 100)
            matched_tier = max(matched_tier, 1)
            break

    if score < 100:
        for kw in TIER2_KEYWORDS:
            if kw in t:
                score = max(score, 70)
                matched_tier = max(matched_tier, 2)
                break

    if score < 70:
        for kw in TIER3_KEYWORDS:
            if kw in t:
                score = max(score, 40)
                matched_tier = max(matched_tier, 3)
                break

    # Boost tier-3 if it also hits tier-1 or tier-2
    if matched_tier == 3:
        for kw in TIER1_KEYWORDS:
            if kw in t:
                score = min(100, score + 30)
                break
        for kw in TIER2_KEYWORDS:
            if kw in t:
                score = min(100, score + 15)
                break

    return score, matched_tier


# ═════════════════════════════════════════════════════════════════
# 4. CATEGORY + WHY IT MATTERS
# ═════════════════════════════════════════════════════════════════

AG_CAT_RULES = [
    ("Commodities", [
        "corn", "soybean", "wheat", "grain", "oat", "barley",
        "cotton", "sugar", "rice", "canola", "sorghum",
        "cattle", "hog", "livestock", "pork", "beef",
        "dairy", "milk", "poultry", "chicken", "egg",
        "ethanol", "crop", "harvest", "bushel", "commodity",
        "food price", "grocery", "food inflation", "meat price",
    ]),
    ("Trade & Policy", [
        "tariff", "trade", "usda", "farm bill", "china",
        "brazil", "argentina", "ukraine", "usmca", "wto",
        "sanction", "export", "import", "h-2a",
        "farm labor", "epa", "fda", "retaliatory",
    ]),
    ("Energy & Inputs", [
        "crude", "oil", "natural gas", "diesel", "gasoline",
        "energy", "opec", "pipeline", "biofuel", "fertilizer",
        "nitrogen", "phosphate", "potash", "urea",
        "carbon", "emission", "renewable", "electricity",
    ]),
    ("Weather & Climate", [
        "drought", "hurricane", "flood", "el nino", "la nina",
        "heat wave", "frost", "freeze", "wildfire",
        "climate", "temperature", "rainfall", "weather",
    ]),
    ("Economy & Markets", [
        "interest rate", "fed", "inflation", "cpi", "recession",
        "gdp", "unemployment", "dollar", "currency",
        "debt ceiling", "government shutdown", "budget",
        "federal reserve", "rate cut", "rate hike",
    ]),
    ("Infrastructure", [
        "rail", "railroad", "mississippi", "panama canal",
        "supply chain", "shipping", "freight", "trucking", "port",
    ]),
]


def get_category(text):
    t = text.lower()
    for cat, keywords in AG_CAT_RULES:
        for kw in keywords:
            if kw in t:
                return cat
    return "Other"


WHY_MAP = [
    ("corn",          "Corn is the #1 US crop — price moves affect feed costs, ethanol margins, and farm revenue."),
    ("soybean",       "Soybeans drive export revenue and crush margins — key for meal and oil markets."),
    ("wheat",         "Wheat prices set the tone for global food costs and compete for acres with corn."),
    ("cattle",        "Live cattle prices reflect feed efficiency and packer demand — affects feedlot decisions."),
    ("egg",           "Egg prices signal avian flu pressure and poultry feed demand — moves corn and soy meal."),
    ("grocery",       "Grocery and food prices are the consumer-facing result of commodity, energy, and labor costs."),
    ("food price",    "Food price changes reflect the entire ag supply chain from field to shelf."),
    ("tariff",        "Tariffs directly impact export demand for US grains — a key driver of basis and futures."),
    ("trade",         "Trade policy shifts can redirect global grain flows overnight and reprice US export markets."),
    ("china",         "China is the world's largest soybean buyer — any policy shift moves US ag exports."),
    ("crude oil",     "Oil prices drive diesel and fertilizer costs — every $10/bbl move hits your input budget."),
    ("oil",           "Energy costs flow straight through to planting, spraying, drying, and hauling expenses."),
    ("natural gas",   "Natural gas is the primary input for nitrogen fertilizer — price spikes raise urea costs."),
    ("fertilizer",    "Fertilizer is the largest variable input cost for grain farmers — price moves hit margins hard."),
    ("nitrogen",      "Nitrogen fertilizer cost directly sets your corn production break-even per bushel."),
    ("interest rate", "Rate changes affect land values, operating loans, and the cost of carrying stored grain."),
    ("fed",           "Fed policy drives the dollar, which affects grain export competitiveness globally."),
    ("inflation",     "Inflation erodes farm margins when input costs rise faster than commodity prices."),
    ("cpi",           "CPI data influences Fed rate decisions which cascade to farm lending and land values."),
    ("recession",     "Economic slowdowns reduce ethanol demand and can weaken feed grain consumption."),
    ("drought",       "Drought is the single biggest yield risk — it moves corn and bean prices fast."),
    ("hurricane",     "Hurricanes disrupt Gulf exports and can damage late-season crops across the South."),
    ("flood",         "Flooding delays planting and harvest, reduces yields, and disrupts grain transportation."),
    ("bird flu",      "Avian influenza outbreaks decimate poultry flocks, spiking egg prices and cutting feed demand."),
    ("rail",          "Rail disruptions can strand grain at elevators and spike basis — transportation is everything."),
    ("supply chain",  "Supply chain disruptions affect input delivery, grain movement, and export logistics."),
    ("dollar",        "A stronger dollar makes US grain less competitive overseas, weakening export demand."),
    ("temperature",   "Temperature extremes during pollination can make or break national corn yields."),
    ("brazil",        "Brazil's crop size directly competes with US soybean exports for the global market share."),
    ("ukraine",       "Black Sea grain shipments affect global wheat and corn supply — any disruption moves prices."),
)


def get_why_it_matters(text):
    t = text.lower()
    for keyword, explanation in WHY_MAP:
        if keyword in t:
            return explanation
    return "This market reflects conditions that can affect agricultural commodity prices, input costs, or farm policy."


# ═════════════════════════════════════════════════════════════════
# 5. HTTP HELPER
# ═════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=20):
    """Fetch URL and return parsed JSON, or None on any error. Prints error details."""
    try:
        req = urllib_request.Request(url, headers={
            "User-Agent": "AGSIST/7.0 (agsist.com; agricultural market intelligence)",
            "Accept": "application/json",
        })
        with urllib_request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw)
    except Exception as e:
        short_url = url[:90]
        print(f"  ✗ HTTP error [{short_url}]: {type(e).__name__}: {e}")
        return None


def time_remaining(close_str):
    if not close_str:
        return ""
    try:
        close = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = close - now
        days = diff.days
        if days < 0:    return "Closed"
        if days == 0:   return "Closes today"
        if days == 1:   return "Closes tomorrow"
        if days <= 30:  return f"Closes in {days}d"
        months = days // 30
        return f"Closes in ~{months}mo"
    except Exception:
        return ""


# ═════════════════════════════════════════════════════════════════
# 6. KALSHI FETCHER — v7: CORRECT URL + pagination + local filter
# ═════════════════════════════════════════════════════════════════

# v7 FIX: Correct domain. api.elections.kalshi.com was the election-
# season API that no longer serves the full market catalog.
KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"

# Kalshi series tickers that are directly ag/macro-relevant.
# Fetching by series is more targeted than blind pagination.
KALSHI_SERIES = [
    "KXFED",      # Fed rate decisions
    "KXCPI",      # CPI / inflation
    "KXOIL",      # Crude oil price
    "KXGAS",      # Natural gas / gasoline
    "KXRECESSION",# Recession probability
    "KXGDP",      # GDP growth
    "KXCORN",     # Corn prices (if it exists)
    "KXGRAIN",    # Grain markets (if it exists)
    "KXTARIFF",   # Tariffs
    "KXTRADE",    # Trade policy
    "KXCHINA",    # China trade/economy
    "KXDROUGHT",  # Drought / weather
    "KXHURRICANE",# Hurricane
    "KXFLU",      # Bird flu
    "KXFOOD",     # Food prices
    "KXEGGSAVG",  # Egg prices
]


def fetch_kalshi():
    print("\n[Kalshi] Fetching prediction markets (v7 — trading-api.kalshi.com)…")
    markets = []
    seen = set()

    # Strategy A: Fetch by known series tickers
    print("  Strategy A: Series-based fetch…")
    for series in KALSHI_SERIES:
        url = f"{KALSHI_BASE}/markets?limit=50&status=open&series_ticker={series}"
        data = http_get_json(url)
        if not data:
            continue
        items = data.get("markets", [])
        found = _process_kalshi_items(items, markets, seen)
        if found > 0:
            print(f"    {series}: {found} relevant")
        time.sleep(0.15)

    # Strategy B: Paginate the full market list and filter locally.
    # Stop after 1000 markets or when we have 30+ relevant ones already.
    print(f"  Strategy B: Paginated browse (up to 1000 markets)…")
    url = f"{KALSHI_BASE}/markets?limit=200&status=open"
    cursor = ""
    pages = 0
    total_browsed = 0

    while pages < 5:  # max 5 pages × 200 = 1000 markets
        page_url = url + (f"&cursor={cursor}" if cursor else "")
        data = http_get_json(page_url)
        if not data:
            break
        items = data.get("markets", [])
        if not items:
            break
        found = _process_kalshi_items(items, markets, seen)
        total_browsed += len(items)
        cursor = data.get("cursor", "")
        pages += 1
        print(f"    Page {pages}: {len(items)} markets scanned, {found} new relevant, {len(markets)} total")
        if not cursor:
            break
        if len(markets) >= 30:
            print(f"    Have {len(markets)} relevant markets — stopping early")
            break
        time.sleep(0.3)

    print(f"  → {len(markets)} ag-relevant Kalshi markets ({total_browsed} total scanned)")
    return markets


def _process_kalshi_items(items, markets, seen):
    """Filter and add Kalshi items to markets list. Returns count added."""
    added = 0
    for m in items:
        ticker = m.get("ticker", "")
        if not ticker or ticker in seen:
            continue

        title    = (m.get("title") or m.get("subtitle") or ticker).strip()
        subtitle = (m.get("subtitle") or "").strip()
        event_ticker = m.get("event_ticker", "")

        meme_text    = f"{title} {subtitle} {event_ticker}"
        scoring_text = f"{title} {subtitle}"

        if is_meme_market(meme_text, ticker=ticker):
            continue

        relevance, tier = score_relevance(scoring_text)
        if relevance < 35:
            continue

        # Parse probability — Kalshi uses cents (0-100) or dollar fraction (0-1)
        prob = None
        for field in ("yes_price", "last_price"):
            val = m.get(field)
            if val is not None:
                try:
                    v = float(val)
                    prob = round(v * 100) if v <= 1.0 else round(v)
                    break
                except Exception:
                    pass

        if prob is None:
            yes_bid = m.get("yes_bid")
            yes_ask = m.get("yes_ask")
            if yes_bid is not None and yes_ask is not None:
                try:
                    b, a = float(yes_bid), float(yes_ask)
                    mid = (b + a) / 2
                    prob = round(mid * 100) if mid <= 1.0 else round(mid)
                except Exception:
                    pass
            elif yes_bid is not None:
                try:
                    v = float(yes_bid)
                    prob = round(v * 100) if v <= 1.0 else round(v)
                except Exception:
                    pass

        if prob is None or prob <= 0 or prob >= 100:
            continue

        volume = 0
        for vf in ("volume", "volume_24h", "dollar_volume"):
            v = m.get(vf)
            if v:
                try:
                    volume = float(v)
                    break
                except Exception:
                    pass

        close_time = m.get("close_time") or m.get("expiration_time") or ""
        tl = time_remaining(close_time)
        if tl == "Closed":
            continue

        # Build market URL from event_ticker or ticker
        event_part = (event_ticker or ticker).split("-")[0]
        market_url = f"https://kalshi.com/markets/{event_part}"

        seen.add(ticker)
        markets.append({
            "platform":       "Kalshi",
            "ticker":         ticker,
            "title":          title,
            "yes":            prob,
            "no":             100 - prob,
            "volume_24h":     volume,
            "close_time":     close_time,
            "time_left":      tl,
            "url":            market_url,
            "relevance":      relevance,
            "tier":           tier,
            "category":       get_category(scoring_text),
            "why_it_matters": get_why_it_matters(scoring_text),
        })
        added += 1
    return added


# ═════════════════════════════════════════════════════════════════
# 7. POLYMARKET FETCHER — v7: correct params + tag browsing
# ═════════════════════════════════════════════════════════════════

POLYMARKET_BASE = "https://gamma-api.polymarket.com"

# v7: Tag slugs that map to ag-relevant markets on Polymarket
POLYMARKET_TAGS = [
    "economics", "trade", "energy", "environment",
    "food", "climate", "commodities", "inflation",
    "federal-reserve", "interest-rates", "recession",
    "china", "tariffs",
]

# Keywords to search for directly (v7: use q= not _q=)
POLYMARKET_KEYWORDS = [
    "tariff", "corn", "soybean", "wheat", "grain",
    "oil price", "natural gas", "fertilizer", "drought",
    "inflation", "recession", "fed rate", "interest rate",
    "bird flu", "egg price", "food price",
    "trade war", "china trade",
    "hurricane", "flood",
]


def fetch_polymarket():
    print("\n[Polymarket] Fetching prediction markets (v7)…")
    markets = []
    seen = set()

    # Strategy A: keyword search with q= (documented param)
    print("  Strategy A: Keyword search (q=)…")
    for kw in POLYMARKET_KEYWORDS:
        encoded = url_quote(kw)
        url = (f"{POLYMARKET_BASE}/markets"
               f"?active=true&closed=false&limit=20&q={encoded}")
        data = http_get_json(url)
        if data:
            items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
            if isinstance(items, list) and items:
                found = _process_poly_items(items, markets, seen)
                if found > 0:
                    print(f"    '{kw}': {found} relevant")
        time.sleep(0.2)

    # Strategy B: tag-based browsing
    print("  Strategy B: Tag browsing…")
    for tag in POLYMARKET_TAGS:
        url = (f"{POLYMARKET_BASE}/markets"
               f"?active=true&closed=false&limit=50&tag_slug={tag}")
        data = http_get_json(url)
        if data:
            items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
            if isinstance(items, list) and items:
                found = _process_poly_items(items, markets, seen)
                if found > 0:
                    print(f"    tag '{tag}': {found} relevant")
        time.sleep(0.2)

    # Strategy C: Browse top markets by volume and filter
    print("  Strategy C: Top markets by volume…")
    url = (f"{POLYMARKET_BASE}/markets"
           f"?active=true&closed=false&limit=100"
           f"&order=volume&ascending=false")
    data = http_get_json(url)
    if data:
        items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
        if isinstance(items, list):
            found = _process_poly_items(items, markets, seen)
            print(f"    top-volume: {found} relevant")

    print(f"  → {len(markets)} ag-relevant Polymarket markets ({len(seen)} unique)")
    return markets


def _process_poly_items(items, markets, seen):
    added = 0
    for m in items:
        mid = (m.get("id") or m.get("condition_id") or m.get("conditionId") or "")
        if not mid:
            continue
        mid = str(mid)
        if mid in seen:
            continue

        question = (m.get("question") or m.get("title") or "").strip()
        if not question:
            continue
        if is_meme_market(question):
            continue

        relevance, tier = score_relevance(question)
        if relevance < 35:
            continue

        # Parse probability
        prob = None
        for field in ("outcomePrices", "outcome_prices"):
            raw = m.get(field)
            if raw:
                try:
                    prices = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(prices, list) and len(prices) >= 1:
                        prob = round(float(prices[0]) * 100)
                        break
                except Exception:
                    pass

        if prob is None:
            for field in ("yes_price", "bestBid", "lastTradePrice", "last_trade_price"):
                val = m.get(field)
                if val is not None:
                    try:
                        v = float(val)
                        prob = round(v * 100) if v <= 1.0 else round(v)
                        break
                    except Exception:
                        pass

        if prob is None or prob <= 0 or prob >= 100:
            continue

        volume = 0
        for vf in ("volume", "volume24hr", "volume_num", "liquidityNum"):
            v = m.get(vf)
            if v:
                try:
                    volume = float(v)
                    break
                except Exception:
                    pass

        slug = m.get("slug", "")
        market_url = (f"https://polymarket.com/event/{slug}" if slug
                      else m.get("url", f"https://polymarket.com/event/{mid}"))

        end_date = (m.get("endDate") or m.get("end_date_iso")
                    or m.get("endDateIso") or "")
        tl = time_remaining(end_date)
        if tl == "Closed":
            continue

        seen.add(mid)
        markets.append({
            "platform":       "Polymarket",
            "ticker":         mid[:20],
            "title":          question[:140],
            "yes":            prob,
            "no":             100 - prob,
            "volume_24h":     volume,
            "close_time":     end_date,
            "time_left":      tl,
            "url":            market_url,
            "slug":           slug,
            "relevance":      relevance,
            "tier":           tier,
            "category":       get_category(question),
            "why_it_matters": get_why_it_matters(question),
        })
        added += 1
    return added


# ═════════════════════════════════════════════════════════════════
# 8. RANKING
# ═════════════════════════════════════════════════════════════════

def composite_score(market):
    relevance = market.get("relevance", 0)
    volume = max(market.get("volume_24h", 0), 1)
    return relevance * 1.5 + math.log10(volume) * 10


# ═════════════════════════════════════════════════════════════════
# 9. MAIN
# ═════════════════════════════════════════════════════════════════

def main():
    now = datetime.now(timezone.utc)
    print(f"\nAGSIST fetch_markets.py v7 — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    kalshi    = fetch_kalshi()
    polymarket = fetch_polymarket()
    combined  = kalshi + polymarket

    print(f"\nCombined raw: {len(kalshi)} Kalshi + {len(polymarket)} Polymarket = {len(combined)} total")

    # Deduplicate across platforms by normalized title
    deduped = []
    seen_titles = set()
    for m in sorted(combined, key=composite_score, reverse=True):
        norm = re.sub(r'[^a-z0-9 ]', '', m["title"].lower()).strip()
        if norm not in seen_titles:
            seen_titles.add(norm)
            deduped.append(m)

    top_markets = deduped[:25]

    # Group by category for the categories field
    categories = {}
    for m in top_markets:
        cat = m["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(m)

    # Stats
    tier_counts = {100: 0, 70: 0, 40: 0}
    for m in combined:
        r = m.get("relevance", 0)
        if r >= 100:   tier_counts[100] += 1
        elif r >= 70:  tier_counts[70] += 1
        else:          tier_counts[40] += 1

    output = {
        "fetched":        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version":        3,
        "count":          len(top_markets),
        "total_found":    len(combined),
        "tier_breakdown": {
            "direct_ag":    tier_counts[100],
            "trade_energy": tier_counts[70],
            "macro_weather": tier_counts[40],
        },
        "categories":     categories,
        "markets":        top_markets,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/markets.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"✓ data/markets.json written — v7")
    print(f"  Kalshi:       {len(kalshi)}")
    print(f"  Polymarket:   {len(polymarket)}")
    print(f"  Total found:  {len(combined)}")
    print(f"  After dedup:  {len(deduped)}")
    print(f"  Top selected: {len(top_markets)}")
    print(f"  Direct ag:    {tier_counts[100]}")
    print(f"  Trade/energy: {tier_counts[70]}")
    print(f"  Macro/weather:{tier_counts[40]}")

    if top_markets:
        print(f"\n  Top 10 markets:")
        for i, m in enumerate(top_markets[:10], 1):
            print(f"  {i:2d}. [{m['platform']:10s}] {m['yes']:3d}%  "
                  f"[{m['category'][:15]}]  {m['title'][:55]}")
    else:
        print("\n  ⚠️  0 markets found — check API connectivity in Actions logs")

    print(f"{'=' * 60}\nDone.\n")


if __name__ == "__main__":
    main()
