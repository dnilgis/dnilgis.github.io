#!/usr/bin/env python3
"""
AGSIST fetch_markets.py  v8
════════════════════════════
Fetches prediction-market odds relevant to agriculture from Kalshi
and Polymarket.  Runs once daily via GitHub Actions (6 AM CT).
Output -> data/markets.json -> read by homepage widget + ag-odds page.

ARCHITECTURE
  Homepage (index.html): loads markets.json for 6-card snapshot
  ag-odds.html:          loads markets.json + live Polymarket browser fetch

v8 changes (2026-04-01):
  POLYMARKET OVERHAUL — dropped q= keyword Strategy A entirely.
  The gamma API ignores q= and returns group markets with
  outcomePrices:null, so zero markets survived probability parsing.

  NEW Strategy A: /events endpoint ordered by volume.
  Events have properly structured nested binary markets with
  outcomePrices populated. Best signal/noise ratio.

  NEW tokens price fallback: _parse_poly_prob() now checks
  tokens[0]['price'] — covers AMM/CLOB market format.

  Kept tag_slug Strategy B and top-volume Strategy C.
  Kalshi unchanged from v7 (correct URL, series + pagination).

Sources (no API keys required):
  Kalshi     -- https://trading-api.kalshi.com/trade-api/v2/
  Polymarket -- https://gamma-api.polymarket.com/
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
# 1. KEYWORD TIERS
# ================================================================

TIER1_KEYWORDS = [
    "corn", "soybean", "wheat", "grain", "oat", "barley",
    "cotton", "sugar", "rice", "canola", "sorghum",
    "cattle", "hog", "pig", "livestock", "pork", "beef",
    "dairy", "milk", "poultry", "chicken", "egg",
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

MIN_RELEVANCE = 35


# ================================================================
# 2. MEME / SPORTS FILTER
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
# 3. RELEVANCE SCORING
# ================================================================

def score_relevance(text):
    t = text.lower()
    score, tier = 0, 0
    for kw in TIER1_KEYWORDS:
        if kw in t:
            score, tier = 100, 1
            break
    if score < 100:
        for kw in TIER2_KEYWORDS:
            if kw in t:
                score, tier = 70, 2
                break
    if score < 70:
        for kw in TIER3_KEYWORDS:
            if kw in t:
                score, tier = 40, 3
                break
    if tier == 3:
        for kw in TIER1_KEYWORDS:
            if kw in t:
                score = min(100, score + 30)
                break
        for kw in TIER2_KEYWORDS:
            if kw in t:
                score = min(100, score + 15)
                break
    return score, tier


# ================================================================
# 4. CATEGORY + WHY IT MATTERS
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


WHY_MAP = [
    ("egg",           "Egg prices signal avian flu pressure and poultry feed demand."),
    ("grocery",       "Grocery prices are the consumer-facing result of commodity, energy, and labor costs."),
    ("food price",    "Food price changes reflect the entire ag supply chain from field to shelf."),
    ("food inflation","Food inflation erodes consumer purchasing power and shifts protein demand."),
    ("corn",          "Corn is the #1 US crop — price moves affect feed costs, ethanol margins, and farm revenue."),
    ("soybean",       "Soybeans drive export revenue and crush margins — key for meal and oil markets."),
    ("wheat",         "Wheat prices set the tone for global food costs and compete for acres with corn."),
    ("cattle",        "Live cattle prices reflect packer demand and feed efficiency — key for feedlot break-evens."),
    ("tariff",        "Tariffs directly impact export demand for US grains — a key driver of basis and futures."),
    ("trade",         "Trade policy shifts can redirect global grain flows overnight and reprice US export markets."),
    ("china",         "China is the world's largest soybean buyer — any policy shift moves US ag exports."),
    ("crude oil",     "Oil prices drive diesel and fertilizer costs — every $10/bbl move hits your input budget."),
    ("oil",           "Energy costs flow straight through to planting, spraying, drying, and hauling expenses."),
    ("natural gas",   "Natural gas is the primary input for nitrogen fertilizer — price spikes raise urea costs."),
    ("fertilizer",    "Fertilizer is the largest variable input cost for grain farmers."),
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
    ("rail",          "Rail disruptions strand grain at elevators and spike basis — transportation is everything."),
    ("supply chain",  "Supply chain disruptions affect input delivery, grain movement, and export logistics."),
    ("dollar",        "A stronger dollar makes US grain less competitive overseas, weakening export demand."),
    ("brazil",        "Brazil's crop size directly competes with US soybean exports for the global market."),
    ("ukraine",       "Black Sea grain shipments affect global wheat and corn supply — disruptions move prices."),
]


def get_why(text):
    t = text.lower()
    for kw, why in WHY_MAP:
        if kw in t:
            return why
    return "This market reflects conditions that can affect agricultural commodity prices, input costs, or farm policy."


# ================================================================
# 5. HTTP HELPER
# ================================================================

def http_get(url, timeout=20):
    try:
        req = urllib_request.Request(url, headers={
            "User-Agent": "AGSIST/8.0 (agsist.com; agricultural market intelligence)",
            "Accept": "application/json",
        })
        with urllib_request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"  ERROR [{url[:75]}]: {type(e).__name__}: {e}")
        return None


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
# 6. KALSHI FETCHER (v7 logic — correct URL + series + pagination)
# ================================================================

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"

KALSHI_SERIES = [
    "KXFED", "KXCPI", "KXOIL", "KXGAS", "KXRECESSION", "KXGDP",
    "KXCORN", "KXGRAIN", "KXTARIFF", "KXTRADE", "KXCHINA",
    "KXDROUGHT", "KXHURRICANE", "KXFLU", "KXFOOD", "KXEGGSAVG",
]


def fetch_kalshi():
    print("\n[Kalshi] series + pagination…")
    markets, seen = [], set()

    for series in KALSHI_SERIES:
        data = http_get(f"{KALSHI_BASE}/markets?limit=50&status=open&series_ticker={series}")
        if data:
            n = _process_kalshi_items(data.get("markets", []), markets, seen)
            if n:
                print(f"  {series}: {n}")
        time.sleep(0.15)

    url = f"{KALSHI_BASE}/markets?limit=200&status=open"
    cursor, pages, browsed = "", 0, 0
    while pages < 5:
        data = http_get(url + (f"&cursor={cursor}" if cursor else ""))
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
        if not cursor or len(markets) >= 30:
            break
        time.sleep(0.3)

    print(f"  -> {len(markets)} Kalshi markets ({browsed} scanned)")
    return markets


def _process_kalshi_items(items, markets, seen):
    added = 0
    for m in items:
        ticker = m.get("ticker", "")
        if not ticker or ticker in seen:
            continue
        title = (m.get("title") or m.get("subtitle") or ticker).strip()
        sub   = (m.get("subtitle") or "").strip()
        ev    = m.get("event_ticker", "")
        if is_junk(f"{title} {sub} {ev}", ticker):
            continue
        score, tier = score_relevance(f"{title} {sub}")
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
        tl = time_remaining(m.get("close_time") or m.get("expiration_time") or "")
        if tl == "Closed":
            continue
        ep = (ev or ticker).split("-")[0]
        seen.add(ticker)
        markets.append({
            "platform": "Kalshi", "ticker": ticker, "title": title,
            "yes": prob, "no": 100 - prob, "volume_24h": vol,
            "close_time": m.get("close_time") or "", "time_left": tl,
            "url": f"https://kalshi.com/markets/{ep}",
            "relevance": score, "tier": tier,
            "category": get_category(f"{title} {sub}"),
            "why_it_matters": get_why(f"{title} {sub}"),
        })
        added += 1
    return added


# ================================================================
# 7. POLYMARKET FETCHER — v8: /events first, then tag + volume
# ================================================================

POLY_BASE = "https://gamma-api.polymarket.com"

POLY_TAGS = [
    "politics", "economics", "trade", "energy", "environment",
    "food", "climate", "commodities", "inflation",
    "federal-reserve", "interest-rates", "recession",
    "china", "tariffs",
]


def fetch_polymarket():
    print("\n[Polymarket] /events + tag_slug + volume…")
    markets, seen = [], set()

    # Strategy A: /events ordered by volume — best source.
    # Returns event objects with nested binary markets, outcomePrices populated.
    print("  A: /events by volume…")
    data = http_get(f"{POLY_BASE}/events?active=true&closed=false&limit=100&order=volume&ascending=false")
    if data:
        events = data if isinstance(data, list) else data.get("events", data.get("results", []))
        n = _process_poly_events(events, markets, seen)
        print(f"     {n} relevant from {len(events)} events")
    time.sleep(0.3)

    # Strategy B: tag_slug browsing — reliable categorical filter
    print("  B: tag_slug…")
    for tag in POLY_TAGS:
        data = http_get(f"{POLY_BASE}/markets?active=true&closed=false&limit=100&tag_slug={url_quote(tag)}")
        if data:
            items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
            n = _process_poly_markets(items, markets, seen)
            if n:
                print(f"     {tag}: {n}")
        time.sleep(0.2)

    # Strategy C: top markets by volume — broad sweep
    print("  C: top by volume…")
    data = http_get(f"{POLY_BASE}/markets?active=true&closed=false&limit=100&order=volume&ascending=false")
    if data:
        items = data if isinstance(data, list) else data.get("results", data.get("markets", []))
        n = _process_poly_markets(items, markets, seen)
        print(f"     {n} relevant")

    print(f"  -> {len(markets)} Polymarket markets")
    return markets


def _parse_poly_prob(m):
    """Extract YES probability (1-99 int) from a Polymarket market object."""
    prob = None
    # 1. outcomePrices — standard binary field: '["0.65","0.35"]'
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
    # 2. tokens[0].price — AMM/CLOB format
    if prob is None:
        tokens = m.get("tokens")
        if isinstance(tokens, list) and tokens:
            try:
                v = float(tokens[0].get("price", ""))
                if v > 0 and not math.isnan(v):
                    prob = round(v * 100) if v <= 1.0 else round(v)
            except Exception:
                pass
    # 3. Scalar fallbacks
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
    """Build a standardised market record. Returns None if invalid/duplicate."""
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
    url  = f"https://polymarket.com/event/{slug}" if slug else m.get("url", f"https://polymarket.com/event/{mid}")
    ed   = m.get("endDate") or m.get("end_date_iso") or m.get("endDateIso") or ""
    tl   = time_remaining(ed)
    if tl == "Closed":
        return None
    seen.add(mid)
    return {
        "platform": "Polymarket", "ticker": mid[:20],
        "title": question[:140], "yes": prob, "no": 100 - prob,
        "volume_24h": vol, "close_time": ed, "time_left": tl,
        "url": url, "slug": slug,
        "relevance": score, "tier": tier,
        "category": get_category(question),
        "why_it_matters": get_why(question),
    }


def _process_poly_events(events, markets, seen):
    """Process /events response — flatten nested binary markets."""
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
    """Process flat /markets response."""
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
# 8. COMPOSITE RANKING
# ================================================================

def composite_score(m):
    return m.get("relevance", 0) * 1.5 + math.log10(max(m.get("volume_24h", 0), 1)) * 10


# ================================================================
# 9. MAIN
# ================================================================

def main():
    now = datetime.now(timezone.utc)
    print(f"\nAGSIST fetch_markets.py v8 -- {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    kalshi = fetch_kalshi()
    poly   = fetch_polymarket()
    combined = kalshi + poly
    print(f"\nRaw: {len(kalshi)} Kalshi + {len(poly)} Polymarket = {len(combined)}")

    # Deduplicate by normalised title, rank by composite score
    deduped, seen_titles = [], set()
    for m in sorted(combined, key=composite_score, reverse=True):
        norm = re.sub(r"[^a-z0-9 ]", "", m["title"].lower()).strip()
        if norm not in seen_titles:
            seen_titles.add(norm)
            deduped.append(m)

    top = deduped[:25]

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
        "version":        4,
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
    print(f"OK data/markets.json written -- v8")
    print(f"  Kalshi:      {len(kalshi)}")
    print(f"  Polymarket:  {len(poly)}")
    print(f"  Deduped:     {len(deduped)}")
    print(f"  Top saved:   {len(top)}")
    print(f"  Direct ag:   {tc[100]}  Trade/energy: {tc[70]}  Macro: {tc[40]}")
    if top:
        print(f"\n  Top 10:")
        for i, m in enumerate(top[:10], 1):
            print(f"  {i:2d}. [{m['platform']:10s}] {m['yes']:3d}%  [{m['category'][:14]}]  {m['title'][:55]}")
    else:
        print("\n  WARNING: 0 markets found -- check API connectivity in Actions logs")
    print(f"{'=' * 60}\nDone.\n")


if __name__ == "__main__":
    main()
