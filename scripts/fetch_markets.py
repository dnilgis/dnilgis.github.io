#!/usr/bin/env python3
"""
AGSIST fetch_markets.py  v10
════════════════════════════
v10 changes (2026-04-23):

  STRIKE-LADDER DEDUP — v9 pushed 22 near-identical crude-strike markets
  to final output because each had a unique title. v10 detects
  strike-ladder families ("will X hit $N in TIMEFRAME") and keeps at
  most 2 per family: nearest-to-coinflip plus one representative tail.

  CATEGORY QUOTAS — v9 took top-25 by composite score with no diversity
  enforcement, so whichever category had the most candidates won 100%
  of slots. v10 enforces per-category caps (direct_ag up to 8, trade up
  to 4, energy up to 4, weather up to 4, macro up to 3, other up to 2).

  CONTEXT-AWARE why_it_matters — v9 used a keyword → fixed-string map, so
  22 crude markets shared one sentence verbatim. v10 synthesizes
  per-market copy from strike, odds, and time-to-close for oil and
  Russia/Ukraine markets; table fallback for everything else.

  RELEVANCE WITHIN TIER — v9 assigned flat 100/70/40 per tier. v10 adds
  small per-market bumps for multi-keyword matches, near-money odds,
  and high-value titles (so crude at $100 outranks crude at $200).

  COMPOSITE REBALANCE — tier bonus now dominates (tier1 +150,
  tier2 +75, tier3 +30); volume log contribution capped so a $20M meme
  can't outrank a $500K ag market.

  BROADER KALSHI COVERAGE — added newer ag/econ series names. Deeper
  pagination (up to 10 pages). Ticker-prefix fallback catches series
  like KXCORN even if the title field is empty. Distinct logging for
  auth errors vs empty responses vs network errors.

  Retains all v9 word-boundary fixes and blacklists.
"""

import json
import re
import os
import math
import time
from datetime import datetime, timezone

try:
    import urllib.request as urllib_request
    from urllib.parse import quote as url_quote
except ImportError:
    import urllib2 as urllib_request
    from urllib import quote as url_quote


# ================================================================
# 1. WORD-BOUNDARY HELPER  (retained from v9)
# ================================================================

_KW_RE_CACHE: dict = {}

def _has_word(text: str, word: str) -> bool:
    """True if word appears as a whole token (non-alnum boundaries)."""
    if word not in _KW_RE_CACHE:
        _KW_RE_CACHE[word] = re.compile(
            r'(?<![a-z0-9])' + re.escape(word) + r'(?![a-z0-9])',
            re.IGNORECASE,
        )
    return bool(_KW_RE_CACHE[word].search(text))


# ================================================================
# 2. KEYWORD TIERS  (retained from v9)
# ================================================================

TIER1_KEYWORDS = [
    "corn", "soybean", "soybeans", "wheat", "grain", "grains",
    "oat", "oats", "barley", "cotton", "sugar", "rice", "canola", "sorghum",
    "cattle", "hog", "hogs", "pig", "pigs", "livestock", "pork", "beef",
    "dairy", "milk", "poultry", "chicken", "egg", "eggs",
    "ethanol", "crop", "harvest", "planting", "acreage",
    "fertilizer", "urea", "nitrogen", "potash", "phosphate",
    "farm bill", "usda", "wasde", "crop insurance",
    "food price", "food inflation", "grocery price", "grocery store",
    "grocery", "egg price", "meat price",
    "bushel", "yield", "cropland", "growing season", "feedlot",
]

TIER2_KEYWORDS = [
    "tariff", "tariffs", "trade war", "trade deal", "trade agreement",
    "china trade", "china import", "china export",
    "brazil", "argentina", "ukraine", "black sea grain",
    "usmca", "nafta", "wto", "trade dispute",
    "export ban", "import quota", "sanction",
    "trade policy", "trade escalation", "retaliatory tariff",
    "crude oil", "natural gas", "diesel", "gasoline",
    "energy price", "oil price", "opec",
    "pipeline", "biofuel", "renewable fuel",
    "rail strike", "railroad", "freight rate",
    "mississippi river", "panama canal", "port strike",
    "supply chain", "shipping cost",
    "bird flu", "avian influenza", "african swine fever",
    "h-2a", "farm labor", "immigration policy", "food safety",
]

TIER3_KEYWORDS = [
    "interest rate", "federal reserve", "fed funds",
    "rate cut", "rate hike", "cut rate", "fed cut", "fomc", "powell",
    "inflation", "cpi", "ppi", "core inflation",
    "recession", "gdp growth", "unemployment",
    "dollar index", "usd", "currency",
    "government shutdown", "debt ceiling", "farm subsidy",
    "el nino", "la nina", "hurricane", "flood",
    "drought", "heat wave", "frost", "freeze", "wildfire",
    "climate policy", "weather forecast",
    "federal budget", "deficit", "treasury",
    "china economy", "global trade",
]

# v10: ticker-prefix → implicit tier-1 keyword for Kalshi markets where
# the title field may be empty (catches KXCORN-* etc.)
KALSHI_TICKER_HINTS = {
    "KXCORN":     "corn",
    "KXSOY":      "soybean",
    "KXWHEAT":    "wheat",
    "KXGRAIN":    "grain",
    "KXCATTLE":   "cattle",
    "KXHOG":      "hog",
    "KXPORK":     "pork",
    "KXBEEF":     "beef",
    "KXDAIRY":    "dairy",
    "KXMILK":     "milk",
    "KXEGGS":     "egg",
    "KXEGGSAVG":  "egg",
    "KXETHANOL":  "ethanol",
    "KXFOOD":     "food price",
    "KXGROC":     "grocery",
    "KXFLU":      "bird flu",
    "KXDROUGHT":  "drought",
    "KXHURRICANE":"hurricane",
    "KXFLOOD":    "flood",
    "KXOIL":      "crude oil",
    "KXGAS":      "natural gas",
    "KXFED":      "federal reserve",
    "KXCPI":      "cpi",
    "KXPPI":      "ppi",
    "KXRECESSION":"recession",
    "KXGDP":      "gdp growth",
    "KXTARIFF":   "tariff",
    "KXTRADE":    "trade deal",
    "KXCHINA":    "china trade",
}

MIN_RELEVANCE = 35


# ================================================================
# 3. MEME / SPORTS / POLITICAL FILTER  (retained from v9)
# ================================================================

MEME_BLACKLIST = [
    "gta", "grand theft auto", "video game", "gaming", "esports",
    "playstation", "xbox", "nintendo", "fortnite", "minecraft",
    "oscar", "grammy", "emmy", "golden globe",
    "bachelor", "bachelorette", "reality tv", "survivor",
    "box office", "netflix", "disney", "hulu",
    "celebrity", "kardashian", "beyonce", "drake",
    "album", "billboard", "spotify", "concert",
    "tiktok", "instagram", "youtube", "twitch",
    "viral", "follower count",
    "spacex", "moon landing", "alien", "ufo",
    "dogecoin", "shiba", "pepe coin", "meme coin", "nft",
    "dating", "divorce", "wedding",
    "tweet", "twitter feud",
    "movie", "marvel", "dc comics",
    "eurovision",
    "presidential election",
    "parliamentary election",
    "senate race",
    "congressional race",
    "prime minister race",
    "governor race",
    "mayor race",
    "republic primary",
    "democratic primary",
    "emmy award",
    "tony award",
]

SPORTS_BLACKLIST = [
    "nfl", "nba", "mlb", "nhl", "mls", "wnba", "xfl",
    "premier league", "la liga", "bundesliga", "serie a",
    "champions league", "world cup", "olympics", "ncaa",
    "march madness", "super bowl", "world series",
    "stanley cup", "playoff", "playoffs",
    "mvp", "touchdown", "home run", "hat trick", "slam dunk",
    "rushing yards", "passing yards", "batting average",
    "championship", "finals mvp", "all-star",
    "win total", "over/under", "point spread",
    "lakers", "celtics", "warriors", "chiefs", "eagles",
    "yankees", "dodgers", "golden state",
]

SPORTS_PLAYER_RE = [
    r"antetokounmpo", r"mahomes", r"jokic", r"luka\b", r"lebron",
    r"curry\b", r"giannis", r"ohtani", r"tatum", r"embiid",
    r"messi\b", r"ronaldo", r"haaland", r"mbappe",
    r"lamar jackson", r"josh allen", r"patrick mahomes",
]

KALSHI_JUNK_RE = [r"^KXMVE", r"CROSSCATEGORY", r"^KX.*PARLAY"]


def is_junk(title, ticker=""):
    t = title.lower()
    if ticker:
        for p in KALSHI_JUNK_RE:
            if re.search(p, ticker.upper()):
                return True
    for p in MEME_BLACKLIST + SPORTS_BLACKLIST:
        if p in t:
            return True
    for p in SPORTS_PLAYER_RE:
        if re.search(p, t):
            return True
    return False


# ================================================================
# 4. RELEVANCE SCORING  (v10: intra-tier bumps for differentiation)
# ================================================================

def score_relevance(text, ticker=""):
    """
    Returns (score, tier). v10 additions over v9:
      - Intra-tier bumps for multi-keyword matches (prevents flat 70s/40s)
      - Kalshi ticker-prefix hint fallback (catches KXCORN-* when title is empty)
    """
    t = text.lower()
    score, tier = 0, 0
    matched_kws = []

    # v10: Kalshi ticker-prefix fallback (runs BEFORE text match)
    if ticker:
        tkr = ticker.upper()
        for prefix, implied_kw in KALSHI_TICKER_HINTS.items():
            if tkr.startswith(prefix):
                # Check which tier the implied keyword belongs to
                if implied_kw in TIER1_KEYWORDS:
                    score, tier = 100, 1
                    matched_kws.append(implied_kw)
                elif implied_kw in TIER2_KEYWORDS:
                    score, tier = 70, 2
                    matched_kws.append(implied_kw)
                elif implied_kw in TIER3_KEYWORDS:
                    score, tier = 40, 3
                    matched_kws.append(implied_kw)
                break

    # TIER 1 — whole-word match
    if tier < 1:
        for kw in TIER1_KEYWORDS:
            if _has_word(t, kw):
                score, tier = 100, 1
                matched_kws.append(kw)
                break

    # TIER 2 — substring match (phrases OK)
    if score < 100:
        for kw in TIER2_KEYWORDS:
            if kw in t:
                if tier == 0:
                    score, tier = 70, 2
                matched_kws.append(kw)
                break

    # TIER 3 — substring match
    if score < 70:
        for kw in TIER3_KEYWORDS:
            if kw in t:
                score, tier = 40, 3
                matched_kws.append(kw)
                break

    # Tier-3 upgrade if it ALSO contains a tier-1/2 keyword
    if tier == 3:
        for kw in TIER1_KEYWORDS:
            if _has_word(t, kw):
                score = min(100, score + 30)
                matched_kws.append(kw)
                break
        else:
            for kw in TIER2_KEYWORDS:
                if kw in t:
                    score = min(100, score + 15)
                    matched_kws.append(kw)
                    break

    # v10: intra-tier bumps for differentiation
    if tier > 0:
        # Bonus for multiple keyword matches across any tier
        extra_hits = 0
        for kw in TIER1_KEYWORDS:
            if kw not in matched_kws and _has_word(t, kw):
                extra_hits += 1
                if extra_hits >= 3:
                    break
        for kw in TIER2_KEYWORDS:
            if kw not in matched_kws and kw in t:
                extra_hits += 1
                if extra_hits >= 3:
                    break
        score += min(extra_hits * 3, 9)

    return score, tier


# ================================================================
# 5. CATEGORIES + CONTEXT-AWARE WHY IT MATTERS  (v10 rewrite)
# ================================================================

AG_CATEGORIES = [
    ("Commodities", [
        "corn", "soybean", "wheat", "grain", "oat", "cattle", "hog",
        "livestock", "pork", "beef", "dairy", "milk", "poultry",
        "chicken", "egg", "ethanol", "crop", "harvest", "bushel",
        "food price", "grocery", "food inflation", "meat price",
    ]),
    ("Trade & Policy", [
        "tariff", "tariffs", "trade", "usda", "farm bill", "china",
        "brazil", "argentina", "ukraine", "usmca", "wto",
        "sanction", "export", "import", "retaliatory",
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
        "interest rate", "federal reserve", "fed", "rate cut", "rate hike",
        "fomc", "inflation", "cpi", "recession", "gdp",
        "unemployment", "dollar", "currency", "debt ceiling",
    ]),
    ("Infrastructure", [
        "rail", "railroad", "mississippi", "panama canal",
        "supply chain", "shipping", "freight", "trucking", "port",
    ]),
]


def get_category(text):
    t = text.lower()
    for cat, kws in AG_CATEGORIES:
        for kw in kws:
            if kw in t:
                return cat
    return "Other"


# ----------------------------------------------------------------
# Static fallback why_it_matters table (for non-ladder markets)
# ----------------------------------------------------------------
WHY_MAP = [
    ("egg price",     "Egg prices signal avian flu pressure and poultry feed demand."),
    ("egg",           "Egg prices signal avian flu pressure and poultry feed demand."),
    ("grocery",       "Grocery prices are the consumer-facing result of commodity, energy, and labor costs."),
    ("food price",    "Food prices reflect the entire ag supply chain from field to shelf."),
    ("food inflation","Food inflation erodes consumer purchasing power and shifts protein demand."),
    ("corn",          "Corn is the #1 US crop -- price moves affect feed costs, ethanol margins, and farm revenue."),
    ("soybean",       "Soybeans drive export revenue and crush margins -- key for meal and oil markets."),
    ("wheat",         "Wheat prices set the tone for global food costs and compete for acres with corn."),
    ("cattle",        "Live cattle prices reflect packer demand and feed efficiency -- key for feedlot break-evens."),
    ("hog",           "Hog futures reflect pork demand and corn-to-meat conversion economics."),
    ("dairy",         "Class III milk moves drive cheese demand and dairy-farm margins in the Upper Midwest."),
    ("ethanol",       "Ethanol margins affect roughly 40% of the US corn crop's annual demand."),
    ("tariff",        "Tariffs directly impact export demand for US grains -- a driver of basis and futures."),
    ("trade",         "Trade policy shifts can redirect global grain flows overnight and reprice US exports."),
    ("china",         "China is the world's largest soybean buyer -- any policy shift moves US ag exports."),
    ("brazil",        "Brazil's crop size directly competes with US soybean exports for the global market."),
    ("fertilizer",    "Fertilizer is the largest variable input cost for grain farmers."),
    ("nitrogen",      "Nitrogen fertilizer cost directly sets your corn production break-even per bushel."),
    ("natural gas",   "Natural gas is the primary input for nitrogen fertilizer -- price spikes raise urea costs."),
    ("interest rate", "Rate changes affect land values, operating loans, and the cost of carrying stored grain."),
    ("fed",           "Fed policy drives the dollar, which affects grain export competitiveness globally."),
    ("inflation",     "Inflation erodes farm margins when input costs rise faster than commodity prices."),
    ("cpi",           "CPI data influences Fed rate decisions which cascade to farm lending and land values."),
    ("recession",     "Economic slowdowns reduce ethanol demand and can weaken feed grain consumption."),
    ("drought",       "Drought is the single biggest yield risk -- it moves corn and bean prices fast."),
    ("hurricane",     "Hurricanes disrupt Gulf exports and can damage late-season crops across the South."),
    ("flood",         "Flooding delays planting and harvest, reduces yields, and disrupts grain transportation."),
    ("bird flu",      "Avian influenza outbreaks decimate poultry flocks, spiking egg prices and cutting feed demand."),
    ("rail",          "Rail disruptions strand grain at elevators and spike basis -- transportation is everything."),
    ("supply chain",  "Supply chain disruptions affect input delivery, grain movement, and export logistics."),
    ("dollar",        "A stronger dollar makes US grain less competitive overseas, weakening export demand."),
]


# ----------------------------------------------------------------
# v10: Context-aware generators for ladder markets
# ----------------------------------------------------------------
def _parse_strike(title):
    """Extract dollar strike from a crude/energy ladder title. Returns float or None."""
    m = re.search(r'\$\s?(\d+(?:[.,]\d+)?)', title)
    if not m:
        return None
    try:
        return float(m.group(1).replace(',', ''))
    except ValueError:
        return None


def _is_crude_market(title):
    t = title.lower()
    return ('crude' in t) or ('wti' in t) or (('oil' in t) and ('price' in t or 'hit' in t or 'reach' in t))


def _crude_why(title, yes_pct, time_left):
    """Generate contextual why_it_matters for a crude oil ladder market."""
    strike = _parse_strike(title)
    t = title.lower()
    is_low = ('low' in t) or ('drop below' in t) or ('below' in t and 'hit' in t)

    direction = "drop below" if is_low else "reach"
    strike_str = f"${strike:.0f}" if strike else "the strike"

    # Conviction language based on probability
    if yes_pct >= 60:
        open_line = f"Market pricing {yes_pct}% odds crude will {direction} {strike_str}"
    elif yes_pct >= 40:
        open_line = f"Near coin-flip odds ({yes_pct}% YES) crude {direction} {strike_str}"
    elif yes_pct >= 15:
        open_line = f"Lower-probability outcome ({yes_pct}%) that crude {direction} {strike_str}"
    else:
        open_line = f"Tail-risk bet ({yes_pct}% odds) on crude {direction} {strike_str}"

    # Impact clause keyed to direction
    if is_low:
        impact = "A move this low would ease diesel and nitrogen input cost pressure heading into side-dress and summer ops."
    else:
        impact = "Would lift farm diesel and pass through to nitrogen fertilizer cost within a few months."

    tail = f"{time_left}. {impact}" if time_left else impact
    return f"{open_line}. {tail}"


def _ukraine_why(title, yes_pct, time_left, close_time):
    """Contextual why for Russia/Ukraine ceasefire markets."""
    no_pct = 100 - yes_pct
    year = close_time[:4] if close_time else ""
    if year:
        opener = f"Market prices {no_pct}% odds the conflict continues through {year}"
    else:
        opener = f"Market prices {no_pct}% odds the conflict continues"
    follow = "Black Sea grain corridors remain a wild card for global wheat and corn supply; Ukraine has historically accounted for roughly 10% of world wheat exports when corridors are open."
    return f"{opener}. {follow}"


def _fed_why(title, yes_pct):
    """Contextual why for Fed/rate markets."""
    t = title.lower()
    if 'cut' in t:
        return f"{yes_pct}% odds of a rate cut. Lower rates reduce operating loan and land interest carrying cost -- historically supportive of commodity prices via weaker dollar."
    if 'hike' in t or 'raise' in t:
        return f"{yes_pct}% odds of a rate hike. Tighter policy strengthens the dollar and pressures grain export competitiveness."
    return f"Fed policy moves ripple through the dollar and farm lending -- {yes_pct}% YES on this outcome."


def get_why(title, yes_pct=50, time_left="", close_time=""):
    """v10: Context-aware synthesis, with static-table fallback."""
    t = title.lower()

    # Crude/energy ladder markets — synthesize per-market
    if _is_crude_market(title):
        return _crude_why(title, yes_pct, time_left)

    # Russia/Ukraine ceasefire — include odds and year
    if 'ukraine' in t and ('ceasefire' in t or 'cease-fire' in t or 'cease fire' in t):
        return _ukraine_why(title, yes_pct, time_left, close_time)

    # Fed/rate markets
    if ('fed' in t and ('cut' in t or 'hike' in t or 'rate' in t or 'raise' in t)) \
       or 'fomc' in t or 'powell' in t:
        return _fed_why(title, yes_pct)

    # Static fallback for everything else
    for kw, why in WHY_MAP:
        if kw in t:
            return why

    return "Reflects conditions that can affect agricultural commodity prices, input costs, or farm policy."


# ================================================================
# 6. HTTP HELPER  (v10: distinguish auth vs network vs empty)
# ================================================================

def http_get(url, timeout=20):
    """
    v10: Returns (data, error_kind) where error_kind is one of:
      None (success), "auth" (401/403), "network", "parse", "notfound"
    Callers expecting v9 behavior can ignore error_kind via a wrapper.
    """
    try:
        req = urllib_request.Request(url, headers={
            "User-Agent": "AGSIST/10.0 (agsist.com; agricultural market intelligence)",
            "Accept": "application/json",
        })
        with urllib_request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8")), None
    except Exception as e:
        name = type(e).__name__
        code = getattr(e, "code", None)
        if code in (401, 403):
            print(f"  AUTH [{url[:75]}]: HTTP {code} -- API may require credentials")
            return None, "auth"
        if code == 404:
            return None, "notfound"
        if "HTTPError" in name or "URLError" in name:
            print(f"  NET  [{url[:75]}]: {name}: {e}")
            return None, "network"
        print(f"  ERR  [{url[:75]}]: {name}: {e}")
        return None, "parse"


def _get(url, timeout=20):
    """v9-compatible wrapper: returns data or None."""
    data, _ = http_get(url, timeout)
    return data


def time_remaining(close_str):
    if not close_str:
        return ""
    try:
        close = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
        diff = (close - datetime.now(timezone.utc)).days
        if diff < 0:   return "Closed"
        if diff == 0:  return "Closes today"
        if diff == 1:  return "Closes tomorrow"
        if diff <= 30: return f"Closes in {diff}d"
        return f"Closes in ~{diff // 30}mo"
    except Exception:
        return ""


# ================================================================
# 7. KALSHI FETCHER  (v10: expanded coverage + ticker fallback)
# ================================================================

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"

# v10: Expanded series list (added newer/common ag + weather tickers)
KALSHI_SERIES = [
    # Macro
    "KXFED", "KXCPI", "KXPPI", "KXRECESSION", "KXGDP", "KXUNEMP",
    # Energy
    "KXOIL", "KXGAS", "KXDIESEL",
    # Direct ag
    "KXCORN", "KXSOY", "KXWHEAT", "KXGRAIN",
    "KXCATTLE", "KXHOG", "KXPORK", "KXBEEF",
    "KXDAIRY", "KXMILK",
    "KXEGGSAVG", "KXEGGS",
    "KXETHANOL",
    # Food
    "KXFOOD", "KXGROC", "KXFOODINFL",
    # Weather
    "KXDROUGHT", "KXHURRICANE", "KXFLOOD", "KXHEAT",
    # Animal health
    "KXFLU", "KXASF",
    # Trade / policy
    "KXTARIFF", "KXTRADE", "KXCHINA",
]


def fetch_kalshi():
    print("\n[Kalshi] series + pagination...")
    markets, seen = [], set()
    auth_fails = 0
    series_hits = 0

    for series in KALSHI_SERIES:
        data, err = http_get(f"{KALSHI_BASE}/markets?limit=50&status=open&series_ticker={series}")
        if err == "auth":
            auth_fails += 1
        elif data:
            n = _process_kalshi_items(data.get("markets", []), markets, seen)
            if n:
                series_hits += 1
                print(f"  {series}: {n}")
        time.sleep(0.15)

    if auth_fails >= len(KALSHI_SERIES) // 2:
        print(f"  [WARN] {auth_fails} series auth-failed -- Kalshi may now require credentials")

    # v10: deeper pagination (up to 10 pages) for broader coverage
    url = f"{KALSHI_BASE}/markets?limit=200&status=open"
    cursor, pages, browsed = "", 0, 0
    while pages < 10:
        data, err = http_get(url + (f"&cursor={cursor}" if cursor else ""))
        if err == "auth":
            print("  [stopping browse: auth-required]")
            break
        if not data:
            break
        items = data.get("markets", [])
        if not items:
            break
        n = _process_kalshi_items(items, markets, seen)
        browsed += len(items)
        cursor = data.get("cursor", "")
        pages += 1
        print(f"  Page {pages}: {len(items)} scanned, {n} new, {len(markets)} total")
        if not cursor or len(markets) >= 50:
            break
        time.sleep(0.3)

    print(f"  -> {len(markets)} Kalshi markets ({browsed} scanned, {series_hits} series produced results)")
    return markets


def _process_kalshi_items(items, markets, seen):
    added = 0
    for m in items:
        ticker = m.get("ticker", "") or ""
        if not ticker or ticker in seen:
            continue
        # v10: pull title from any of several fields, build comprehensive search text
        title = (m.get("title") or "").strip()
        sub = (m.get("subtitle") or "").strip()
        ev = (m.get("event_ticker") or "").strip()
        yes_sub = (m.get("yes_sub_title") or "").strip()
        no_sub = (m.get("no_sub_title") or "").strip()

        # Display title (prefer human-readable over ticker)
        display_title = title or sub or yes_sub or ticker

        # Scoring text: everything combined so keyword match has best chance
        search_text = " ".join(filter(None, [title, sub, yes_sub, no_sub, ev]))

        if is_junk(f"{display_title} {sub} {ev}", ticker):
            continue

        # v10: pass ticker too so KXCORN-* etc. can score tier-1 via prefix
        score, tier = score_relevance(search_text or ticker, ticker=ticker)
        if score < MIN_RELEVANCE:
            continue

        prob = None
        for f in ("yes_price", "last_price"):
            v = m.get(f)
            if v is not None:
                try:
                    n = float(v)
                    prob = round(n * 100) if n <= 1.0 else round(n)
                    break
                except Exception:
                    pass
        if prob is None:
            yb, ya = m.get("yes_bid"), m.get("yes_ask")
            if yb is not None and ya is not None:
                try:
                    mid = (float(yb) + float(ya)) / 2
                    prob = round(mid * 100) if mid <= 1.0 else round(mid)
                except Exception:
                    pass
        if prob is None or not (0 < prob < 100):
            continue

        vol = 0
        for f in ("volume", "volume_24h", "dollar_volume"):
            if m.get(f):
                try:
                    vol = float(m[f])
                    break
                except Exception:
                    pass

        close_time = m.get("close_time") or m.get("expiration_time") or ""
        tl = time_remaining(close_time)
        if tl == "Closed":
            continue

        ep = (ev or ticker).split("-")[0]
        seen.add(ticker)
        markets.append({
            "platform": "Kalshi",
            "ticker": ticker,
            "title": display_title,
            "yes": prob,
            "no": 100 - prob,
            "volume_24h": vol,
            "close_time": close_time,
            "time_left": tl,
            "url": f"https://kalshi.com/markets/{ep}",
            "relevance": score,
            "tier": tier,
            "category": get_category(search_text),
            "why_it_matters": get_why(display_title, prob, tl, close_time),
        })
        added += 1
    return added


# ================================================================
# 8. POLYMARKET FETCHER  (v10: richer tags + per-market why)
# ================================================================

POLY_BASE = "https://gamma-api.polymarket.com"

# v10: expanded tag list
POLY_TAGS = [
    "politics", "economics", "trade", "energy", "environment",
    "food", "climate", "commodities", "inflation",
    "federal-reserve", "interest-rates", "recession",
    "china", "tariffs",
    "agriculture", "ukraine", "geopolitics",
]


def fetch_polymarket():
    print("\n[Polymarket] /events + tag_slug + volume...")
    markets, seen = [], set()

    print("  A: /events by volume...")
    data = _get(f"{POLY_BASE}/events?active=true&closed=false&limit=100&order=volume&ascending=false")
    if data:
        events = data if isinstance(data, list) else data.get("events", data.get("results", []))
        n = _process_poly_events(events, markets, seen)
        print(f"     {n} relevant from {len(events)} events")
    time.sleep(0.3)

    print("  B: tag_slug...")
    for tag in POLY_TAGS:
        data = _get(f"{POLY_BASE}/markets?active=true&closed=false&limit=100&tag_slug={url_quote(tag)}")
        if data:
            items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
            n = _process_poly_markets(items, markets, seen)
            if n:
                print(f"     {tag}: {n}")
        time.sleep(0.2)

    print("  C: top by volume...")
    data = _get(f"{POLY_BASE}/markets?active=true&closed=false&limit=100&order=volume&ascending=false")
    if data:
        items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
        n = _process_poly_markets(items, markets, seen)
        print(f"     {n} relevant")

    print(f"  -> {len(markets)} Polymarket markets")
    return markets


def _parse_poly_prob(m):
    prob = None
    op = m.get("outcomePrices") or m.get("outcome_prices")
    if op:
        try:
            prices = json.loads(op) if isinstance(op, str) else op
            if isinstance(prices, list) and prices:
                v = float(prices[0])
                if not math.isnan(v):
                    prob = round(v * 100) if v <= 1.0 else round(v)
        except Exception:
            pass
    if prob is None:
        tokens = m.get("tokens")
        if isinstance(tokens, list) and tokens:
            try:
                v = float(tokens[0].get("price", ""))
                if v > 0 and not math.isnan(v):
                    prob = round(v * 100) if v <= 1.0 else round(v)
            except Exception:
                pass
    if prob is None:
        for f in ("yes_price", "bestBid", "lastTradePrice", "last_trade_price", "price"):
            val = m.get(f)
            if val is not None:
                try:
                    v = float(val)
                    if v > 0 and not math.isnan(v):
                        prob = round(v * 100) if v <= 1.0 else round(v)
                        break
                except Exception:
                    pass
    return prob if prob and 0 < prob < 100 else None


def _make_poly_record(m, question, seen):
    mid = str(m.get("id") or m.get("condition_id") or m.get("conditionId") or "").strip()
    if not mid or mid in seen:
        return None
    if is_junk(question):
        return None
    score, tier = score_relevance(question)
    if score < MIN_RELEVANCE:
        return None
    prob = _parse_poly_prob(m)
    if prob is None:
        return None
    vol = 0
    for f in ("volume", "volume24hr", "volume_num", "volumeNum", "liquidityNum"):
        if m.get(f):
            try:
                vol = float(m[f])
                break
            except Exception:
                pass
    slug = m.get("slug", "")
    url = f"https://polymarket.com/event/{slug}" if slug else m.get("url", f"https://polymarket.com/event/{mid}")
    ed = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso") or ""
    tl = time_remaining(ed)
    if tl == "Closed":
        return None
    seen.add(mid)
    return {
        "platform": "Polymarket",
        "ticker": mid[:20],
        "title": question[:140],
        "yes": prob,
        "no": 100 - prob,
        "volume_24h": vol,
        "close_time": ed,
        "time_left": tl,
        "url": url,
        "slug": slug,
        "relevance": score,
        "tier": tier,
        "category": get_category(question),
        "why_it_matters": get_why(question, prob, tl, ed),
    }


def _process_poly_events(events, markets, seen):
    added = 0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ev_title = (ev.get("title") or ev.get("name") or "").strip()
        nested = ev.get("markets", [])
        if nested:
            for m in nested:
                q = (m.get("question") or m.get("title") or ev_title).strip()
                if not q:
                    continue
                rec = _make_poly_record(m, q, seen)
                if rec:
                    markets.append(rec)
                    added += 1
        elif ev_title:
            rec = _make_poly_record(ev, ev_title, seen)
            if rec:
                markets.append(rec)
                added += 1
    return added


def _process_poly_markets(items, markets, seen):
    added = 0
    for m in items:
        if not isinstance(m, dict):
            continue
        q = (m.get("question") or m.get("title") or "").strip()
        if not q:
            continue
        rec = _make_poly_record(m, q, seen)
        if rec:
            markets.append(rec)
            added += 1
    return added


# ================================================================
# 9. STRIKE-LADDER DEDUP  (v10 NEW)
# ================================================================

# Regex to detect ladder-style market titles with a $strike + timeframe tail
# Matches: "Will WTI Crude Oil (WTI) hit (HIGH) $120 in April"
#          "Will Crude Oil (CL) hit (LOW) $70 by end of June"
#          "Will natural gas reach $8 by December"
_LADDER_RE = re.compile(
    r'^\s*(?:will\s+)?'
    r'(?P<asset>.+?)\s+'
    r'(?:hit|reach|touch|exceed|drop below|fall below|go above|go below)\s+'
    r'(?:\(?(?P<dir>high|low|above|below)\)?\s+)?'
    r'\$?(?P<strike>\d+(?:[.,]\d+)?)\s+'
    r'(?P<tail>.+)$',
    re.IGNORECASE,
)


def _ladder_family(title):
    """Return (family_key, strike_value) if title is a ladder entry, else (None, None)."""
    t = title.strip().rstrip("?").lower()
    m = _LADDER_RE.match(t)
    if not m:
        return None, None
    asset = re.sub(r"[\s()]+", " ", m.group("asset")).strip()
    # Normalize asset aliases: "wti crude oil wti" / "crude oil cl" / "crude oil" all → "crude oil"
    asset = re.sub(r"\b(cl|wti)\b", "", asset).strip()
    asset = re.sub(r"\s+", " ", asset)
    if "crude" in asset or "oil" in asset:
        asset = "crude oil"
    direction = (m.group("dir") or "").lower().strip()
    if direction in ("below", "low"):   direction = "low"
    elif direction in ("above", "high"):direction = "high"
    tail = re.sub(r"[\s]+", " ", m.group("tail")).strip()
    try:
        strike = float(m.group("strike").replace(",", ""))
    except ValueError:
        return None, None
    family = f"{asset}|{direction}|{tail}"
    return family, strike


def collapse_ladders(markets, max_per_family=2):
    """
    Collapse strike-ladder market families to at most max_per_family picks each.
    Within a family: keep the one closest to 50/50 odds, then the highest-volume
    of the remaining strikes (so we surface a tail bet with real liquidity too).
    Non-ladder markets pass through untouched.
    """
    families = {}
    singletons = []
    for m in markets:
        fam, strike = _ladder_family(m.get("title", ""))
        if fam is None:
            singletons.append(m)
            continue
        m["_strike"] = strike
        families.setdefault(fam, []).append(m)

    collected = list(singletons)
    collapsed_count = 0
    for fam, ms in families.items():
        if len(ms) <= max_per_family:
            for p in ms:
                p.pop("_strike", None)
                collected.append(p)
            continue

        # Sort by distance from 50% (closest-to-coinflip first)
        ms_by_flip = sorted(ms, key=lambda x: abs(x.get("yes", 50) - 50))
        picks = [ms_by_flip[0]]

        if max_per_family >= 2:
            # From the rest, grab the highest-volume representative at a different strike
            rest = [x for x in ms_by_flip[1:] if x["_strike"] != ms_by_flip[0]["_strike"]]
            if rest:
                rest.sort(key=lambda x: -x.get("volume_24h", 0))
                picks.append(rest[0])

        collapsed_count += len(ms) - len(picks)
        for p in picks:
            p.pop("_strike", None)
            collected.append(p)

    print(f"  Ladder dedup: collapsed {collapsed_count} redundant strike markets across {len(families)} families")
    return collected


# ================================================================
# 10. COMPOSITE SCORING  (v10 rebalanced)
# ================================================================

def composite_score(m):
    """
    v10: Tier bonus dominant, volume log-capped at ~$10M to prevent a
    meme-adjacent market with huge volume from outranking legitimate ag.
      tier 1: +150 base
      tier 2:  +75 base
      tier 3:  +30 base
      volume:  +log10(vol capped at 1e7) * 10  (max ~+70)
    Then add the per-market relevance score (which includes intra-tier bumps).
    """
    tier = m.get("tier", 0)
    tier_base = {1: 150, 2: 75, 3: 30}.get(tier, 0)
    vol = min(m.get("volume_24h", 0), 10_000_000)
    vol_bonus = math.log10(max(vol, 1)) * 10
    rel = m.get("relevance", 0)
    return tier_base + vol_bonus + rel


# ================================================================
# 11. CATEGORY QUOTAS  (v10 NEW — forces diversity)
# ================================================================

# Per-category caps for final output. Order matters: categories listed first
# get first pick of high-scoring candidates.
CATEGORY_QUOTAS = [
    ("Commodities",       8),
    ("Trade & Policy",    4),
    ("Energy & Inputs",   4),
    ("Weather & Climate", 4),
    ("Economy & Markets", 3),
    ("Infrastructure",    2),
    ("Other",             2),
]
TARGET_TOTAL = 20


def apply_quotas(markets):
    """
    Take up to N per category per CATEGORY_QUOTAS.
    v10: hard cap of 1.5x the soft cap per category. Prevents a data-sparse
    day (e.g. Kalshi silent, only oil Polymarket markets available) from
    drifting back toward single-category output.
    """
    by_cat = {}
    for m in markets:
        by_cat.setdefault(m.get("category", "Other"), []).append(m)
    for cat in by_cat:
        by_cat[cat].sort(key=composite_score, reverse=True)

    quota_map = dict(CATEGORY_QUOTAS)

    # Pass 1: soft cap per category
    result = []
    for cat, cap in CATEGORY_QUOTAS:
        if cat in by_cat:
            result.extend(by_cat[cat][:cap])

    # Pass 2: fill toward TARGET_TOTAL, but respect a hard cap of 1.5x soft per category
    if len(result) < TARGET_TOTAL:
        category_count = {}
        for m in result:
            c = m.get("category", "Other")
            category_count[c] = category_count.get(c, 0) + 1

        picked = {id(m) for m in result}
        leftovers = [m for m in markets if id(m) not in picked]
        leftovers.sort(key=composite_score, reverse=True)

        for m in leftovers:
            if len(result) >= TARGET_TOTAL:
                break
            c = m.get("category", "Other")
            soft_cap = quota_map.get(c, 2)
            hard_cap = max(int(soft_cap * 1.5), soft_cap + 1)
            if category_count.get(c, 0) >= hard_cap:
                continue
            result.append(m)
            category_count[c] = category_count.get(c, 0) + 1

    result.sort(key=composite_score, reverse=True)
    return result[:TARGET_TOTAL]


# ================================================================
# 12. MAIN
# ================================================================

def main():
    now = datetime.now(timezone.utc)
    print(f"\nAGSIST fetch_markets.py v10 -- {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    kalshi = fetch_kalshi()
    poly = fetch_polymarket()
    combined = kalshi + poly
    print(f"\nRaw: {len(kalshi)} Kalshi + {len(poly)} Polymarket = {len(combined)}")

    # v10: Collapse strike ladders BEFORE ranking
    print("\n[dedup] collapsing strike ladders...")
    collapsed = collapse_ladders(combined, max_per_family=2)
    print(f"  {len(combined)} -> {len(collapsed)} after ladder dedup")

    # Title-normalized dedup (v9 behavior) as second pass
    deduped, seen_titles = [], set()
    for m in sorted(collapsed, key=composite_score, reverse=True):
        norm = re.sub(r"[^a-z0-9 ]", "", m["title"].lower()).strip()
        if norm not in seen_titles:
            seen_titles.add(norm)
            deduped.append(m)
    print(f"  {len(collapsed)} -> {len(deduped)} after title dedup")

    # v10: Apply category quotas
    print("\n[quotas] applying category caps...")
    top = apply_quotas(deduped)
    print(f"  Final selection: {len(top)} markets")

    cats = {}
    for m in top:
        cats.setdefault(m["category"], []).append(m)

    tc = {100: 0, 70: 0, 40: 0}
    for m in combined:
        r = m.get("relevance", 0)
        if r >= 100:  tc[100] += 1
        elif r >= 70: tc[70]  += 1
        else:         tc[40]  += 1

    output = {
        "fetched":        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version":        5,
        "count":          len(top),
        "total_found":    len(combined),
        "tier_breakdown": {
            "direct_ag":     tc[100],
            "trade_energy":  tc[70],
            "macro_weather": tc[40],
        },
        "categories": cats,
        "markets":    top,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/markets.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"OK data/markets.json written -- v10")
    print(f"  Kalshi:      {len(kalshi)}")
    print(f"  Polymarket:  {len(poly)}")
    print(f"  After ladders: {len(collapsed)}")
    print(f"  Deduped:     {len(deduped)}")
    print(f"  Top saved:   {len(top)}")
    print(f"  Direct ag:   {tc[100]}  Trade/energy: {tc[70]}  Macro: {tc[40]}")
    print(f"  Categories:  " + ", ".join(f"{k}({len(v)})" for k, v in cats.items()))
    if top:
        print(f"\n  Top 10:")
        for i, m in enumerate(top[:10], 1):
            cat = m["category"][:14]
            print(f"  {i:2d}. [{m['platform']:10s}] {m['yes']:3d}%  [{cat:14s}]  {m['title'][:55]}")
    else:
        print("\n  WARNING: 0 markets found -- check API connectivity in Actions logs")
    print(f"{'=' * 60}\nDone.\n")


if __name__ == "__main__":
    main()
