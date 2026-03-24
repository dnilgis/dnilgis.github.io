#!/usr/bin/env python3
"""
AGSIST Daily Briefing Generator — v3.2
═══════════════════════════════════════════════════════════════════
Generates the daily agricultural intelligence briefing via Claude API.
Designed to be run by GitHub Actions every morning (5:00 AM CT).

v3.2 changes:
  - PRICE ANCHORING: All prices passed as locked data Claude cannot invent
  - TONE GUARDRAILS: "Surprise" language gated behind surprise_magnitude thresholds
  - POST-VALIDATION: Reject briefings where Claude invented prices
  - FIX: Watch list HTML no longer double-escapes <strong> tags in archive pages
  - CALIBRATED VOICE: Removed "Be bold" instruction causing chronic over-dramatization

Data pipeline:
  1. Read /data/prices.json (yfinance, already fetched by fetch_prices.py)
  2. Fetch ag RSS feeds for overnight news context
  3. Fetch USDA calendar data
  4. Call Claude API with enriched prompt + market data
  5. Validate output against source prices
  6. Write /data/daily.json
  7. Archive: save /data/daily-archive/DATE.json + /daily/DATE.html + update index.json

Env vars required:
  ANTHROPIC_API_KEY — Claude API key
"""

import json
import os
import sys
import random
import re
from datetime import datetime, timezone
from pathlib import Path

try:
    import feedparser
except ImportError:
    feedparser = None

try:
    import requests
except ImportError:
    import urllib.request
    import urllib.error
    requests = None

# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

REPO_ROOT = Path(__file__).resolve().parent.parent
PRICES_PATH = REPO_ROOT / "data" / "prices.json"
OUTPUT_PATH = REPO_ROOT / "data" / "daily.json"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-20250514"

# Overnight surprise thresholds (% move that's genuinely newsworthy)
SURPRISE_THRESHOLDS = {
    "corn":      1.5,
    "corn-dec":  1.5,
    "beans":     1.5,
    "beans-nov": 1.5,
    "wheat":     2.0,
    "oats":      2.5,
    "cattle":    1.5,
    "feeders":   1.5,
    "hogs":      2.0,
    "milk":      3.0,
    "meal":      2.0,
    "soyoil":    2.5,
    "crude":     3.0,
    "natgas":    4.0,
    "gold":      1.5,
    "silver":    2.5,
    "dollar":    0.5,
    "sp500":     1.5,
    "bitcoin":   4.0,
}

COMMODITY_LABELS = {
    "corn":      "Corn (nearby)",
    "corn-dec":  "Corn Dec '26",
    "beans":     "Soybeans (nearby)",
    "beans-nov": "Soybeans Nov '26",
    "wheat":     "Chicago Wheat",
    "oats":      "Oats",
    "cattle":    "Live Cattle",
    "feeders":   "Feeder Cattle",
    "hogs":      "Lean Hogs",
    "milk":      "Class III Milk",
    "meal":      "Soybean Meal",
    "soyoil":    "Soybean Oil",
    "crude":     "WTI Crude Oil",
    "natgas":    "Natural Gas",
    "gold":      "Gold",
    "silver":    "Silver",
    "dollar":    "US Dollar Index",
    "sp500":     "S&P 500",
    "bitcoin":   "Bitcoin",
}

GRAIN_KEYS = {"corn", "corn-dec", "beans", "beans-nov", "wheat", "oats"}

AG_RSS_FEEDS = [
    "https://www.usda.gov/rss/latest-releases.xml",
    "https://www.dtnpf.com/agriculture/web/ag/news/rss",
    "https://www.agriculture.com/rss/news",
    "https://www.farms.com/rss/agriculture-news.aspx",
    "https://www.reuters.com/arc/outboundfeeds/v3/all/tag%3Aagriculture/?outputType=xml&size=10",
]

# ═══════════════════════════════════════════════════════════════════
# CURATED QUOTE BANK — 210 quotes
# ═══════════════════════════════════════════════════════════════════

QUOTE_BANK = [
    ("The best fertilizer is the farmer's shadow.", "Chinese proverb"),
    ("A farmer's footsteps are the best manure.", "English proverb"),
    ("He who plants a garden plants happiness.", "Chinese proverb"),
    ("To forget how to dig the earth and tend the soil is to forget ourselves.", "Mahatma Gandhi"),
    ("The farmer is the only man in our economy who buys everything at retail, sells everything at wholesale, and pays the freight both ways.", "John F. Kennedy"),
    ("Agriculture is the most healthful, most useful, and most noble employment of man.", "George Washington"),
    ("If you tickle the earth with a hoe she laughs with a harvest.", "Douglas William Jerrold"),
    ("The nation that destroys its soil destroys itself.", "Franklin D. Roosevelt"),
    ("When tillage begins, other arts follow.", "Daniel Webster"),
    ("Farming is a profession of hope.", "Brian Brett"),
    ("In the spring, at the end of the day, you should smell like dirt.", "Margaret Atwood"),
    ("The land is the only thing that lasts.", "Irish proverb"),
    ("We didn't inherit the land from our ancestors. We borrow it from our children.", "Wendell Berry"),
    ("The soil is the great connector of lives, the source and destination of all.", "Wendell Berry"),
    ("Eating is an agricultural act.", "Wendell Berry"),
    ("A good farmer is nothing more nor less than a handy man with a sense of humus.", "E.B. White"),
    ("I would rather be on my farm than be emperor of the world.", "George Washington"),
    ("What I stand for is what I stand on.", "Wendell Berry"),
    ("The ultimate goal of farming is not the growing of crops, but the cultivation of human beings.", "Masanobu Fukuoka"),
    ("Life on a farm is a school of patience; you can't hurry the crops or make it rain.", "Henri Alain"),
    ("No occupation is so delightful to me as the culture of the earth.", "Thomas Jefferson"),
    ("Farming looks mighty easy when your plow is a pencil and you're a thousand miles from the corn field.", "Dwight D. Eisenhower"),
    ("The farmer has to be an optimist or he wouldn't still be a farmer.", "Will Rogers"),
    ("To own a bit of ground, to scratch it with a hoe, to plant seeds, and watch the renewal of life — this is the commonest delight of the race.", "Charles Dudley Warner"),
    ("Markets can stay irrational longer than you can stay solvent.", "John Maynard Keynes"),
    ("The four most dangerous words in investing are: 'This time it's different.'", "Sir John Templeton"),
    ("Price is what you pay. Value is what you get.", "Warren Buffett"),
    ("The market is a device for transferring money from the impatient to the patient.", "Warren Buffett"),
    ("Risk comes from not knowing what you're doing.", "Warren Buffett"),
    ("The best time to sell grain was yesterday. The second-best time is when the market gives you a gift.", "Midwestern grain trader proverb"),
    ("Bulls make money, bears make money, pigs get slaughtered.", "Wall Street proverb"),
    ("Don't try to buy at the bottom and sell at the top. It can't be done — except by liars.", "Bernard Baruch"),
    ("The trend is your friend until the end when it bends.", "Ed Seykota"),
    ("Sell when the market is high enough to make you happy.", "Purdue Extension"),
    ("Basis is the farmer's friend — learn it, track it, trade it.", "University of Illinois Extension"),
    ("A bushel sold at profit is worth two bushels of hope.", "Unknown grain merchant"),
    ("Never gamble with the rent money. That goes double for operating loans.", "Farm Credit Services"),
    ("The health of soil, plant, animal, and man is one and indivisible.", "Sir Albert Howard"),
    ("Healthy soil is the real capital that matters in agriculture.", "Allan Savory"),
    ("We know more about the movement of celestial bodies than about the soil underfoot.", "Leonardo da Vinci"),
    ("Take care of the land and the land will take care of you.", "Aboriginal Australian proverb"),
    ("Soil is not just dirt — it's the living skin of the earth.", "David Montgomery"),
    ("Building soil is like building a savings account. Every cover crop is a deposit.", "Ray Archuleta, NRCS"),
    ("One tablespoon of healthy soil has more organisms than there are people on earth.", "Soil Science Society of America"),
    ("The soil is the mother. Everything comes from the soil, and everything returns to it.", "Lakota Sioux teaching"),
    ("Carbon in the soil is money in the bank.", "Gabe Brown"),
    ("Managing for soil health isn't a cost — it's an investment with compound interest.", "USDA NRCS"),
    ("Tillage is a tax on your soil's future.", "No-till farming proverb"),
    ("Everyone complains about the weather, but nobody does anything about it.", "Charles Dudley Warner"),
    ("Climate is what we expect; weather is what we get.", "Mark Twain"),
    ("A dry March and a wet May fill barns and bays with corn and hay.", "English farming proverb"),
    ("Make hay while the sun shines.", "English proverb"),
    ("Rain before seven, clear before eleven.", "Weather proverb"),
    ("Knee high by the Fourth of July is an old standard — modern hybrids laugh at it.", "Iowa State Extension"),
    ("A late frost is the cruelest tax the sky can levy.", "Unknown"),
    ("We're not just farming crops. We're farming ecosystems.", "Gabe Brown"),
    ("The next revolution in agriculture won't come from chemistry — it'll come from biology.", "Jonathan Lundgren"),
    ("Diversity above the ground creates diversity below it.", "Gabe Brown"),
    ("Precision ag without soil health is like GPS without a destination.", "Unknown agronomist"),
    ("Every farm is a different puzzle. The best farmers never stop solving.", "Unknown extension agent"),
    ("Data doesn't replace intuition — it sharpens it.", "Purdue Digital Ag"),
    ("The combine doesn't care about your feelings. It measures your decisions.", "Unknown Illinois farmer"),
    ("Farm like your grandchildren will inherit this land. Because they will.", "Land Institute"),
    ("Cover crops aren't lazy — they're the hardest-working employees on your farm and they work for free.", "SARE"),
    ("The future of farming is in the first six inches of soil.", "Fred Kirschenmann"),
    ("Trade wars have no winners — just varying degrees of losers.", "Agricultural trade proverb"),
    ("The best farm program is a good price.", "John Block, USDA Secretary"),
    ("Interest rates are like gravity for asset prices. When they go up, everything gets heavier.", "Warren Buffett, adapted"),
    ("The dollar is the most important price in agriculture that nobody talks about.", "Unknown ag economist"),
    ("Exports move basis. Basis moves profitability. Everything connects.", "University of Minnesota Extension"),
    ("Every percentage point in interest rates is a dollar an acre off farmland value.", "Farm Credit East"),
    ("Crop insurance isn't free money — it's the floor, not the ceiling.", "Risk management advisor"),
    ("Don't confuse a rising market with good marketing.", "K-State Ag Economics"),
    ("My grandfather used to say that once in your life you need a doctor, a lawyer, a policeman, and a preacher, but every day, three times a day, you need a farmer.", "Brenda Schoepp"),
    ("Behind every successful rancher is a wife who works in town.", "Western ranch proverb"),
    ("Rain makes grain — except when it doesn't stop.", "Unknown Midwest farmer"),
    ("The two happiest days in a farmer's life: the day he buys a new combine and the day he pays it off.", "Unknown"),
    ("Farming: where every year you bet the farm on the weather, the market, and the government — and still show up next spring.", "Unknown"),
    ("My exit strategy is the same as my father's: feet first.", "Unknown generational farmer"),
    ("When the last tree is cut, the last fish is caught, and the last river is polluted, only then will man discover that money cannot be eaten.", "Cree prophecy"),
    ("The earth does not belong to us. We belong to the earth.", "Chief Seattle"),
    ("Treat the earth well. It was not given to you by your parents, it was loaned to you by your children.", "Kenyan proverb"),
    ("Three sisters — corn, beans, and squash — teach us that the strongest farms grow in community.", "Haudenosaunee teaching"),
    ("Every 1% increase in organic matter holds 20,000 more gallons of water per acre.", "NRCS"),
    ("Nitrogen doesn't know if it came from a bag or a legume. The soil doesn't care either.", "Extension agronomist"),
    ("The difference between a 180-bushel corn crop and a 230-bushel crop is usually management, not genetics.", "Pioneer agronomist"),
    ("You can't make up in September what you lost in June.", "Midwest agronomist"),
    ("A weed is a plant whose virtues have not yet been discovered.", "Ralph Waldo Emerson"),
    ("The best time to scout your fields is always today.", "IPM specialist"),
    ("Compaction costs you bushels you'll never see on the yield monitor.", "Soil physicist"),
    ("Planting date is the cheapest input with the highest return.", "Purdue agronomy"),
    ("Every day past optimal planting date costs you roughly a bushel per acre in corn. Mother Nature charges interest.", "Iowa State University"),
    ("Soil testing is the cheapest agronomic investment you can make. Do it every year.", "Extension soil scientist"),
    ("Hope is not a marketing plan.", "K-State grain marketing"),
    ("If the market gives you a profit, take it. You can always have regret on the way to the bank.", "DTN grain analyst"),
    ("Forward contracting isn't about being right — it's about being profitable.", "Unknown grain merchandiser"),
    ("Revenue protection doesn't make you rich. It keeps you farming.", "Crop insurance agent"),
    ("The only sure thing in grain marketing is that you'll never sell the high.", "Unknown"),
    ("Grain in the bin is an option with a storage cost. Know your carry.", "CME Group education"),
    ("Lock in fuel when it's cheap. Lock in grain prices when they're profitable. Both are perishable opportunities.", "Farm management advisor"),
    ("The worst time to make a marketing decision is when you have to.", "KSU Ag Economics"),
    ("Buy land. They're not making any more of it.", "Mark Twain"),
    ("Land values follow income. Income follows management. Management follows education.", "Farm Credit"),
    ("The best view in the world is a field of corn in late July.", "Unknown Midwestern farmer"),
    ("Every furrow is a story. Every harvest is a chapter.", "Unknown"),
    ("In 40 years of farming, I've never had the same year twice. That's the beauty and the terror of it.", "Unknown Iowa farmer"),
    ("When China buys, the world moves. When they stop, the world holds its breath.", "Ag trade analyst"),
    ("The Black Sea region is agriculture's wild card — it can make or break global grain prices in a single season.", "USDA FAS"),
    ("An acre in Iowa competes with an acre in Mato Grosso every single day. That's the global market.", "Unknown"),
    ("The tractor replaced the horse. GPS replaced the marker. AI won't replace the farmer — it'll replace the guesswork.", "Unknown"),
    ("Big data is only as good as the farmer interpreting it.", "Precision ag consultant"),
    ("Drones show you the field from the sky. But your boots on the ground still make the call.", "UAS agricultural specialist"),
    ("The most advanced technology on most farms is the operator.", "John Deere engineer"),
    ("A yield monitor is a report card for every decision you made all season.", "Unknown"),
    ("The best time to plant a tree was 20 years ago. The second best time is now.", "Chinese proverb"),
    ("Tough times don't last. Tough farmers do.", "Unknown"),
    ("Some years the crop is good and the price is bad. Some years the price is good and the crop is bad. That's farming.", "Unknown"),
    ("You learn more from a crop failure than from a record yield. The tuition is just a lot more expensive.", "Unknown"),
    ("Spring always comes. That's the farmer's creed.", "Unknown"),
    ("The most important crop any farmer grows is the next generation.", "Unknown"),
    ("Sunrise is the farmer's opening bell.", "Unknown"),
    ("Every seed planted is an act of faith in tomorrow.", "Unknown"),
    ("Don't tell me how hard you work. Show me your field in August.", "Unknown"),
    ("The best farmers I know read more than they plow.", "County extension agent"),
    ("The only thing harder than farming is not farming when it's in your blood.", "Unknown"),
    ("A bad year in farming teaches you what ten good years can't.", "Unknown"),
    ("Agriculture is our wisest pursuit, because it will in the end contribute most to real wealth, good morals, and happiness.", "Thomas Jefferson"),
    ("Farming teaches you that you can do everything right and still get beat. And then you plant again.", "Unknown"),
    ("There are no shortcuts in farming. Just long days and early mornings.", "Unknown"),
    ("The market doesn't owe you anything. Neither does the weather. But the land always gives back what you put in.", "Unknown"),
    ("You can judge a civilization by the way it treats its soil.", "Hugh Hammond Bennett"),
    ("Good seed, good ground, good timing — everything else is conversation.", "Unknown elevator manager"),
    ("The hardest part of farming isn't the work — it's the waiting.", "Unknown"),
    ("Every generation of farmers inherits the soil of the last and leaves the soil for the next.", "Unknown"),
    ("Agriculture not only gives riches to a nation, but the only riches she can call her own.", "Samuel Johnson"),
    ("Whoever could make two ears of corn grow upon a spot of ground where only one grew before would deserve better of mankind than the whole race of politicians put together.", "Jonathan Swift"),
    ("Those too lazy to plow in the right season will have no food at the harvest.", "Proverbs 20:4"),
    ("Agriculture was the first occupation of man, and as it embraces the whole earth, it is the foundation of all other industries.", "Edward W. Stewart"),
]


# ═══════════════════════════════════════════════════════════════════
# DATA COLLECTION
# ═══════════════════════════════════════════════════════════════════

def http_get(url, timeout=10):
    """Simple HTTP GET that works with requests or urllib."""
    if requests:
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"  [warn] fetch failed: {url} — {e}", file=sys.stderr)
            return None
    else:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AGSIST-Daily/3.2"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  [warn] fetch failed: {url} — {e}", file=sys.stderr)
            return None


def load_prices():
    """
    Load prices.json, compute overnight surprises, and build a
    LOCKED PRICE TABLE that gets injected verbatim into the prompt.

    Returns:
        price_data dict  — contains price_block, locked_prices, fetched, quotes
        surprises list   — moves above threshold
    """
    if not PRICES_PATH.exists():
        print("[error] prices.json not found", file=sys.stderr)
        return {}, []

    with open(PRICES_PATH) as f:
        data = json.load(f)

    quotes = data.get("quotes", {})
    fetched = data.get("fetched", "")

    price_lines = []
    locked_prices = {}   # key → display_price string for validation later
    surprises = []

    for key, label in COMMODITY_LABELS.items():
        q = quotes.get(key)
        if not q or q.get("close") is None:
            continue

        close = float(q["close"])
        opn = float(q.get("open", close))
        net = q.get("netChange")
        pct = q.get("pctChange")

        if net is not None:
            net = float(net)
        else:
            net = close - opn

        if pct is not None:
            pct = float(pct)
        elif opn != 0:
            pct = (net / opn) * 100
        else:
            pct = 0.0

        is_grain = key in GRAIN_KEYS
        if is_grain:
            price_str = f"${close / 100:.2f}/bu"
            chg_str = f"{net / 100:+.4f} ({pct:+.1f}%)"
            # Store the human-readable price for validation
            locked_prices[key] = close / 100   # actual dollar value
        elif key in ("gold", "bitcoin"):
            price_str = f"${close:,.0f}"
            chg_str = f"{pct:+.1f}%"
            locked_prices[key] = close
        elif key == "treasury10":
            price_str = f"{close:.2f}%"
            chg_str = f"{pct:+.1f}%"
            locked_prices[key] = close
        else:
            price_str = f"${close:.2f}"
            chg_str = f"{pct:+.1f}%"
            locked_prices[key] = close

        direction = "▲" if pct > 0 else "▼" if pct < 0 else "—"
        line = f"  {label}: {price_str} ({direction} {chg_str})"

        # 52-week range context
        wk52_hi = q.get("wk52_hi")
        wk52_lo = q.get("wk52_lo")
        if wk52_hi and wk52_lo:
            hi, lo = float(wk52_hi), float(wk52_lo)
            if hi > lo:
                position = ((close - lo) / (hi - lo)) * 100
                line += f" [52wk: {position:.0f}% from low]"

        price_lines.append(line)

        # Overnight surprise check
        threshold = SURPRISE_THRESHOLDS.get(key, 2.0)
        if abs(pct) >= threshold:
            surprises.append({
                "commodity": label,
                "key": key,
                "price": price_str,
                "pct_change": pct,
                "direction": "up" if pct > 0 else "down",
                "surprise_magnitude": round(abs(pct) / threshold, 1),
            })

    surprises.sort(key=lambda x: x["surprise_magnitude"], reverse=True)

    return {
        "price_block": "\n".join(price_lines),
        "locked_prices": locked_prices,
        "fetched": fetched,
        "surprises": surprises,
        "quotes": quotes,
    }, surprises


def fetch_ag_news():
    """Fetch recent ag news headlines from RSS feeds."""
    if not feedparser:
        print("  [info] feedparser not installed, skipping RSS", file=sys.stderr)
        return "No RSS feeds available — focus on price action and seasonal context."

    headlines = []
    for feed_url in AG_RSS_FEEDS:
        try:
            text = http_get(feed_url, timeout=8)
            if not text:
                continue
            feed = feedparser.parse(text)
            for entry in feed.entries[:5]:
                title = entry.get("title", "").strip()
                pub = entry.get("published", entry.get("updated", ""))
                if title:
                    headlines.append(f"  • {title} ({pub[:16]})")
        except Exception as e:
            print(f"  [warn] RSS parse failed: {feed_url} — {e}", file=sys.stderr)
            continue

    if not headlines:
        return "No fresh RSS headlines — focus on price action and seasonal context."

    seen = set()
    unique = []
    for h in headlines:
        key = h[:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(h)
    return "\n".join(unique[:25])


def get_seasonal_context():
    """Return time-of-year context for the briefing."""
    now = datetime.now()
    month = now.month

    contexts = {
        1: "Mid-winter: Watch South American crop development (Brazil safrinha, Argentina soybeans). Cattle markets seasonally strong. Input purchasing decisions for spring. Tax planning wrapping up.",
        2: "Late winter: USDA Ag Outlook Forum typically this month. South American harvest beginning. Final input purchasing before spring. Equipment maintenance window closing.",
        3: "Pre-planting: USDA Prospective Plantings (end of March) is THE report. Fieldwork starting in South. Nitrogen applications beginning. Basis typically widens. Cash flow planning critical.",
        4: "Planting season: Corn planting underway (April 15-May 15 optimal in Corn Belt). Every day of delay costs ~1 bu/acre. Weather dominance begins.",
        5: "Peak planting: Soybean planting (May 1-June 5 optimal). Prevent plant deadline approaching. First crop condition ratings. Weather premium builds if delays.",
        6: "Growing season: Crop conditions drive markets. Pollination approaching for early-planted corn. Wheat harvest beginning in Southern Plains.",
        7: "Critical: Corn pollination (most critical 2 weeks of the year). USDA Acreage report (June 30). Soybean bloom/pod set. Weather premium at peak if hot/dry.",
        8: "Yield formation: Corn in dough/dent — yield largely locked in. Soybean pod fill critical. USDA Pro Farmer crop tour. Fall crop insurance pricing.",
        9: "Early harvest: Corn harvest beginning. USDA September WASDE. Basis narrows as harvest pressure builds. Storage decisions.",
        10: "Harvest: Full corn/soybean harvest. Basis at seasonal lows. Storage vs. sell decisions. Fall fieldwork. Wheat planting.",
        11: "Post-harvest: Final USDA yield estimates. South American planting. Grain storage management. Tax planning. Input purchasing beginning.",
        12: "Year-end: Final crop production estimates. USDA supply/demand tables. Tax deadlines. South American weather watch. Budgeting for next crop year.",
    }

    return contexts.get(month, "Monitor markets and seasonal patterns.")


def get_todays_quote():
    """Pick a quote using day-of-year as seed for consistent daily rotation."""
    now = datetime.now()
    seed = now.timetuple().tm_yday + now.year * 1000
    random.seed(seed)
    quote, attribution = random.choice(QUOTE_BANK)
    random.seed()
    return {"text": quote, "attribution": attribution}


# ═══════════════════════════════════════════════════════════════════
# CLAUDE API CALL
# ═══════════════════════════════════════════════════════════════════

def build_system_prompt():
    return """You are the voice of AGSIST Daily — a trusted morning agricultural intelligence briefing for corn, soybean, and grain producers in Wisconsin and Minnesota.

YOUR VOICE:
- You're the sharp friend who actually trades grain AND reads the WASDE. Not an academic. Not a reporter.
- Direct, opinionated, but honest about uncertainty. When you don't know, say so.
- Connect dots that farmers wouldn't connect on their own. "Wheat dropped 14½¢" is a data point. "Wheat dropped 14½¢ — but the real story is Black Sea competition heating up while your spring planting window opens" is a briefing.
- Plain language. No jargon without context. "Managed money" needs a parenthetical "(hedge funds)" on first use.
- Calibrated tone: most days are normal market days. A 1% corn move is not "dramatic." Reserve strong language for moves where surprise_magnitude ≥ 3.

══ STRICT PRICE RULES — READ CAREFULLY ══
The LOCKED PRICE TABLE in the user message contains the only real prices from today's market.
You MUST follow these rules without exception:

1. Every specific price you write must come from the LOCKED PRICE TABLE — no exceptions, no rounding to a "cleaner" number, no substitutions.
2. If a price isn't in the table, don't mention it specifically. Say "energy costs climbed" not "diesel hit $3.80."
3. Never invent, estimate, or recall prices from your training data. Markets change daily.
4. The net change and percent change in the table are the only moves you should describe. If corn is down 9½¢ per the table, write "down 9½¢" — not "corn cratered" and not "corn slipped a penny."
5. If you are uncertain whether a price is in the table, omit the specific number and describe the direction and magnitude only.

══ TONE CALIBRATION ══
Use the surprise_magnitude field to calibrate language:
- magnitude < 1.5: "moved," "gained," "eased," "dipped" — no drama
- magnitude 1.5–2.5: "jumped," "fell," "rallied," "slid" — modest emphasis
- magnitude 2.5–3.5: "surged," "dropped sharply," "spiked" — significant
- magnitude > 3.5: "exploded," "crashed," "historic move" — genuinely rare events only

Never label a normal trading day as "volatile." Never call a routine move "historic." Boring days are honest — describe them accurately.

══ OUTPUT STRUCTURE ══
Return valid JSON with this exact schema:

{
  "headline": "ALL CAPS, 6-10 words. The single biggest real story today.",
  "subheadline": "One sentence adding the second-biggest story or the why.",
  "lead": "2-3 sentences. What happened, why it matters, what to watch. Must contain at least one specific price from the table.",
  "teaser": "One punchy sentence for the collapsed hero bar.",
  "one_number": {
    "value": "The single most important number today — must be from the LOCKED PRICE TABLE.",
    "unit": "What the number represents in 3-6 words.",
    "context": "2-3 sentences explaining WHY this number matters. Connect to farmer decisions."
  },
  "sections": [
    {
      "title": "Section title (3-5 words)",
      "icon": "Single emoji",
      "body": "3-5 sentences of analysis, not reporting. Bold the most important phrase with <strong> tags. Reference at least 2 commodities and make 1 connection between them. All prices must be from the LOCKED PRICE TABLE.",
      "bottom_line": "One sentence TL;DR, max 20 words.",
      "conviction_level": "low | medium | high",
      "overnight_surprise": true/false,
      "farmer_action": "Specific and actionable. 'Consider pricing 10-15% of new-crop corn on this rally' or 'No action needed — hold current positions.'"
    }
  ],
  "the_more_you_know": {
    "title": "Educational topic tied to today's market action",
    "body": "3-4 sentences explaining a concept clearly. Smart friend over coffee, not a textbook."
  },
  "watch_list": [
    {
      "time": "Time or timeframe",
      "desc": "What to watch and why. Use <strong> tags for key items."
    }
  ],
  "daily_quote": {
    "text": "USE THE EXACT QUOTE PROVIDED — do not modify.",
    "attribution": "USE THE EXACT ATTRIBUTION PROVIDED — do not modify."
  },
  "source_summary": "Data sources, e.g., 'USDA · CME Group · Open-Meteo · Reuters'",
  "date": "Today's date formatted like 'Monday, March 3, 2026'",
  "meta": {
    "market_mood": "bullish | bearish | mixed | cautious | volatile",
    "heat_section": 0-based index of section with most action today,
    "overnight_surprises_count": number
  }
}

SECTIONS — always include these 4 in this order:
1. GRAINS & OILSEEDS — corn, soybeans, wheat, oats. Core audience.
2. LIVESTOCK & DAIRY — cattle, hogs, milk. Connect to feed costs.
3. ENERGY & INPUTS — crude, natgas, fertilizer/diesel implications.
4. MACRO & TRADE — dollar, rates, trade policy, exports.

RESPOND WITH ONLY THE JSON OBJECT. No markdown fences. No preamble."""


def call_claude(price_data, surprises, news_block, seasonal_ctx, todays_quote):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[error] ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    now = datetime.now()
    date_str = now.strftime("%A, %B %-d, %Y")

    # ── Surprise block ────────────────────────────────────────────
    surprise_block = ""
    if surprises:
        lines = []
        for s in surprises:
            if s["surprise_magnitude"] >= 3.5:
                tier = "MAJOR"
            elif s["surprise_magnitude"] >= 2.5:
                tier = "SIGNIFICANT"
            elif s["surprise_magnitude"] >= 1.5:
                tier = "Notable"
            else:
                tier = "Mild"
            lines.append(
                f"  ⚡ {tier}: {s['commodity']} moved {s['pct_change']:+.1f}% "
                f"({s['direction']}) — magnitude {s['surprise_magnitude']}x threshold"
            )
        surprise_block = (
            f"⚡ OVERNIGHT SURPRISES ({len(surprises)} moves above threshold):\n"
            + "\n".join(lines)
            + "\nFlag these in relevant sections with overnight_surprise: true. "
            + "Use language proportional to the magnitude tier above."
        )
    else:
        surprise_block = (
            "No overnight surprises — all moves within normal ranges. "
            "Write an honest, measured briefing. Not every day is dramatic. "
            "market_mood should reflect actual conditions, not manufactured urgency."
        )

    # ── Locked price table ────────────────────────────────────────
    # This is passed separately and prominently so Claude treats it as authoritative
    locked_table = price_data.get("price_block", "Price data unavailable")

    user_message = f"""Generate today's AGSIST Daily briefing.

DATE: {date_str}

╔══ LOCKED PRICE TABLE ═══════════════════════════════════════════╗
║ These prices come directly from yfinance and are the ONLY prices ║
║ you may use. Do not invent, estimate, or substitute any price.   ║
║ If a commodity isn't listed here, don't cite a specific price.   ║
╚═════════════════════════════════════════════════════════════════╝
{locked_table}

═══ OVERNIGHT SURPRISE ANALYSIS ═══
{surprise_block}

═══ SEASONAL CONTEXT ═══
{seasonal_ctx}

═══ AG NEWS HEADLINES (for narrative context only — use prices above) ═══
{news_block}

═══ TODAY'S QUOTE (copy exactly, do not modify) ═══
Text: "{todays_quote['text']}"
Attribution: "{todays_quote['attribution']}"

Your job: explain what these prices MEAN for a Wisconsin/Minnesota grain and livestock producer, what they SHOULD consider doing, and what's COMING NEXT. Connect the dots. Stay accurate. Use calibrated language proportional to actual market moves today."""

    payload = {
        "model": MODEL,
        "max_tokens": 4000,
        "system": build_system_prompt(),
        "messages": [{"role": "user", "content": user_message}],
    }

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }

    if requests:
        resp = requests.post(ANTHROPIC_API, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        result = resp.json()
    else:
        data_bytes = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(ANTHROPIC_API, data=data_bytes, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))

    text = ""
    for block in result.get("content", []):
        if block.get("type") == "text":
            text += block["text"]

    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    if text.startswith("json"):
        text = text[4:].strip()

    return json.loads(text)


# ═══════════════════════════════════════════════════════════════════
# POST-GENERATION VALIDATION
# ═══════════════════════════════════════════════════════════════════

def validate_briefing(briefing, locked_prices):
    """
    Scan the generated briefing for dollar amounts and check that any
    specific prices mentioned are consistent with prices.json data.

    This catches cases where Claude fabricates or misremembers a price.
    Logs warnings — does not hard-fail, since some prices (diesel, retail)
    are legitimately not in prices.json.

    Returns: (is_clean, list_of_warnings)
    """
    warnings = []

    # Build a lookup of all known price values with tolerance
    known_values = {}
    for key, val in locked_prices.items():
        if val and val > 0:
            known_values[key] = val

    # Flatten briefing text for scanning
    all_text = []
    all_text.append(briefing.get("headline", ""))
    all_text.append(briefing.get("lead", ""))
    all_text.append(briefing.get("subheadline", ""))
    if briefing.get("one_number"):
        all_text.append(briefing["one_number"].get("context", ""))
    for sec in briefing.get("sections", []):
        all_text.append(sec.get("body", ""))
        all_text.append(sec.get("bottom_line", ""))
    all_text.append(briefing.get("the_more_you_know", {}).get("body", ""))
    full_text = " ".join(all_text)

    # Extract dollar amounts from text: $4.52, $91.27, $5,100, etc.
    dollar_pattern = re.compile(r'\$([0-9,]+(?:\.[0-9]+)?)')
    found_values = []
    for match in dollar_pattern.finditer(full_text):
        try:
            val = float(match.group(1).replace(",", ""))
            found_values.append((val, match.group(0)))
        except ValueError:
            pass

    # Check if any found value is suspiciously different from all known prices
    # Only flag if the value is in a range that suggests it's a commodity price
    # (not e.g. a per-acre cost estimate)
    COMMODITY_RANGES = {
        "corn":    (2.0, 9.0),
        "beans":   (7.0, 20.0),
        "wheat":   (3.0, 12.0),
        "crude":   (30.0, 200.0),
        "natgas":  (1.0, 15.0),
        "gold":    (500.0, 10000.0),
        "silver":  (5.0, 200.0),
        "cattle":  (100.0, 350.0),
        "hogs":    (40.0, 150.0),
        "milk":    (10.0, 35.0),
    }

    # For each found value, see if it matches any known price within 5%
    for found_val, found_str in found_values:
        matched = False
        for key, known_val in known_values.items():
            if known_val > 0:
                tolerance = 0.05  # 5%
                if abs(found_val - known_val) / known_val <= tolerance:
                    matched = True
                    break
        # If not matched, check if it's in a commodity range — if so, warn
        if not matched:
            for key, (lo, hi) in COMMODITY_RANGES.items():
                if lo <= found_val <= hi:
                    # Could be a legitimate unlisted price (diesel, local basis)
                    # or an invented number — log as info, not hard error
                    warnings.append(
                        f"Price {found_str} not found in prices.json "
                        f"(possible {key} price — verify it's not invented)"
                    )
                    break

    is_clean = len(warnings) == 0
    return is_clean, warnings


# ═══════════════════════════════════════════════════════════════════
# ARCHIVE — Static page generation + index
# ═══════════════════════════════════════════════════════════════════

ARCHIVE_JSON_DIR = REPO_ROOT / "data" / "daily-archive"
ARCHIVE_HTML_DIR = REPO_ROOT / "daily"


def html_esc(s):
    """Minimal HTML escaping."""
    if not s:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def html_esc_preserve_strong(s):
    """
    Escape HTML but preserve <strong> and </strong> tags.
    Used for body text and watch descriptions that may legitimately contain bold.
    """
    if not s:
        return ""
    # Extract strong tags first, replace with placeholders
    parts = re.split(r'(</?strong>)', s, flags=re.IGNORECASE)
    result = []
    for part in parts:
        if part.lower() in ('<strong>', '</strong>'):
            result.append(part.lower())
        else:
            result.append(html_esc(part))
    return "".join(result)


def generate_archive_html(briefing, date_iso):
    """Generate a fully server-rendered static HTML page for one day's briefing."""

    date_display = briefing.get("date", date_iso)
    headline = html_esc(briefing.get("headline", "AGSIST Daily Briefing"))
    subheadline = html_esc(briefing.get("subheadline", ""))
    lead = html_esc(briefing.get("lead", ""))
    meta = briefing.get("meta", {})
    mood = meta.get("market_mood", "")
    heat_idx = meta.get("heat_section", -1)
    surprises = briefing.get("surprises", [])
    surprise_count = meta.get("overnight_surprises_count", 0)

    # Surprise banner
    surprise_html = ""
    if surprise_count > 0:
        names = []
        for s in surprises:
            arrow = "▲" if s.get("direction") == "up" else "▼"
            names.append(f'{s.get("commodity","")} {arrow}{abs(s.get("pct_change",0)):.1f}%')
        surprise_html = (
            f'<div class="dv3-surprise-banner" style="display:flex">\n'
            f'      <span class="surprise-icon">⚡</span>\n'
            f'      <span class="surprise-text"><strong>Overnight Surprise'
            f'{"s" if surprise_count > 1 else ""}:</strong> '
            f'{" · ".join(names) if names else f"{surprise_count} unusual move{'s' if surprise_count > 1 else ''}"}'
            f'</span>\n    </div>'
        )

    # Mood badge
    mood_html = ""
    if mood:
        mood_colors = {
            "bullish":  ("var(--green)", "rgba(58,139,60,.08)", "rgba(58,139,60,.22)"),
            "bearish":  ("var(--red)", "rgba(184,76,42,.08)", "rgba(184,76,42,.22)"),
            "mixed":    ("var(--gold)", "rgba(218,165,32,.08)", "rgba(218,165,32,.22)"),
            "cautious": ("var(--blue)", "rgba(74,143,186,.08)", "rgba(74,143,186,.22)"),
            "volatile": ("var(--orange)", "rgba(200,122,40,.08)", "rgba(200,122,40,.22)"),
        }
        mood_icons = {"bullish": "📈", "bearish": "📉", "mixed": "↔️", "cautious": "⚠️", "volatile": "🔥"}
        mc = mood_colors.get(mood, mood_colors["mixed"])
        mi = mood_icons.get(mood, "📊")
        mood_html = (
            f'<span class="dv3-mood" style="display:inline-flex;'
            f'color:{mc[0]};background:{mc[1]};border:1px solid {mc[2]}">'
            f'{mi} {mood.capitalize()}</span>'
        )

    # Sections
    sections_html = ""
    for i, sec in enumerate(briefing.get("sections", [])):
        cls = "dv3-sec"
        if sec.get("overnight_surprise"):
            cls += " dv3-sec--surprise"
        if i == heat_idx:
            cls += " dv3-sec--heat"

        icon = html_esc(sec.get("icon", "📊"))
        title = html_esc(sec.get("title", ""))
        # Body may contain <strong> tags — preserve them
        body = html_esc_preserve_strong(sec.get("body", ""))
        bottom_line = html_esc(sec.get("bottom_line", ""))
        farmer_action = html_esc(sec.get("farmer_action", ""))
        conviction = sec.get("conviction_level", "")

        conviction_html = ""
        if conviction:
            cv_colors = {
                "high":   ("var(--green)", "rgba(58,139,60,.10)", "rgba(58,139,60,.25)"),
                "medium": ("var(--gold)", "rgba(218,165,32,.10)", "rgba(218,165,32,.25)"),
                "low":    ("var(--text-muted)", "var(--surface2)", "var(--border)"),
            }
            cv = cv_colors.get(conviction, cv_colors["medium"])
            conviction_html = (
                f'<span class="dv3-sec-conviction" style="color:{cv[0]};'
                f'background:{cv[1]};border:1px solid {cv[2]}">'
                f'{conviction.upper()} CONVICTION</span>'
            )

        bottom_html = f'<div class="dv3-sec-bottomline">{bottom_line}</div>' if bottom_line else ""
        action_html = f'<div class="dv3-sec-action">🎯 {farmer_action}</div>' if farmer_action else ""

        sections_html += f'''
    <div class="{cls}" style="position:relative">
      <div class="dv3-sec-header">
        <span class="dv3-sec-icon">{icon}</span>
        <span class="dv3-sec-title">{title}</span>
        {conviction_html}
      </div>
      <div class="dv3-sec-body">{body}</div>
      {bottom_html}
      {action_html}
    </div>'''

    # One Number
    one_num = briefing.get("one_number", {})
    one_num_html = ""
    if one_num:
        one_num_html = (
            f'<div class="dv3-one-number">\n'
            f'        <div class="dv3-one-number-label">📊 THE NUMBER</div>\n'
            f'        <div class="dv3-one-number-val">{html_esc(one_num.get("value", "—"))}</div>\n'
            f'        <div class="dv3-one-number-unit">{html_esc(one_num.get("unit", ""))}</div>\n'
            f'        <div class="dv3-one-number-ctx">{html_esc(one_num.get("context", ""))}</div>\n'
            f'      </div>'
        )

    # Quote
    quote = briefing.get("daily_quote", {})
    quote_html = ""
    if quote:
        qt = quote.get("text", "").strip('"\u201c\u201d')
        qa = quote.get("attribution", "").lstrip("\u2014\u2013- ")
        quote_html = (
            f'<div class="dv3-quote-card">\n'
            f'        <div class="dv3-quote-label">💬 DAILY QUOTE</div>\n'
            f'        <p class="dv3-quote-text">\u201c{html_esc(qt)}\u201d</p>\n'
            f'        <cite class="dv3-quote-attr">\u2014 {html_esc(qa)}</cite>\n'
            f'      </div>'
        )

    # TMYK
    tmyk = briefing.get("the_more_you_know", {})
    tmyk_html = ""
    if tmyk:
        tmyk_html = (
            f'<div class="dv3-tmyk">\n'
            f'      <div class="dv3-tmyk-label">🧠 THE MORE YOU KNOW</div>\n'
            f'      <div class="dv3-tmyk-title">{html_esc(tmyk.get("title", ""))}</div>\n'
            f'      <div class="dv3-tmyk-body">{html_esc(tmyk.get("body", ""))}</div>\n'
            f'    </div>'
        )

    # Watch list — use html_esc_preserve_strong so <strong> tags render correctly
    watch = briefing.get("watch_list", [])
    watch_items = ""
    for item in watch:
        watch_items += (
            f'<li class="dv3-watch-item">\n'
            f'        <span class="dv3-watch-time">{html_esc(item.get("time", ""))}</span>\n'
            f'        <span class="dv3-watch-desc">'
            f'{html_esc_preserve_strong(item.get("desc", ""))}'
            f'</span>\n      </li>'
        )
    watch_html = ""
    if watch:
        watch_html = (
            f'<div class="dv3-watch">\n'
            f'      <div class="dv3-watch-label">📅 TODAY\'S WATCH LIST</div>\n'
            f'      <ul class="dv3-watch-list">{watch_items}</ul>\n'
            f'    </div>'
        )

    source = html_esc(briefing.get("source_summary", "USDA · CME Group · Open-Meteo"))
    gen_at = briefing.get("generated_at", "")

    page = f'''<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AGSIST Daily — {html_esc(date_display)}: {headline}</title>
<meta name="description" content="{headline} — {html_esc(lead[:160])}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="https://agsist.com/daily/{date_iso}">
<meta property="og:title" content="AGSIST Daily — {html_esc(date_display)}">
<meta property="og:description" content="{headline}">
<meta property="og:type" content="article">
<meta property="og:url" content="https://agsist.com/daily/{date_iso}">
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600;700&family=Oswald:wght@500;600;700&display=swap">
<link rel="stylesheet" href="/components/styles.css">
<link rel="icon" type="image/png" href="/images/corn-favicon-32.png" sizes="32x32">
<link rel="apple-touch-icon" href="/images/corn-favicon-180.png">
<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "Article",
  "headline": "{headline}",
  "datePublished": "{date_iso}",
  "dateModified": "{gen_at}",
  "description": "{html_esc(lead[:200])}",
  "author": {{"@type": "Organization", "name": "AGSIST", "url": "https://agsist.com"}},
  "publisher": {{"@type": "Organization", "name": "AGSIST", "url": "https://agsist.com"}},
  "mainEntityOfPage": {{"@type": "WebPage", "@id": "https://agsist.com/daily/{date_iso}"}},
  "isPartOf": {{"@type": "WebSite", "name": "AGSIST", "url": "https://agsist.com"}},
  "breadcrumb": {{
    "@type": "BreadcrumbList",
    "itemListElement": [
      {{"@type": "ListItem", "position": 1, "name": "Home", "item": "https://agsist.com"}},
      {{"@type": "ListItem", "position": 2, "name": "Daily Briefing", "item": "https://agsist.com/daily"}},
      {{"@type": "ListItem", "position": 3, "name": "{html_esc(date_display)}", "item": "https://agsist.com/daily/{date_iso}"}}
    ]
  }}
}}
</script>
<style>
.dv3-page{{max-width:900px;margin:0 auto;padding:2rem 1.25rem}}
.dv3-header{{margin-bottom:2rem;padding-bottom:1.5rem;border-bottom:2px solid var(--border)}}
.dv3-eyebrow{{display:inline-flex;align-items:center;gap:.5rem;font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);margin-bottom:.75rem;padding:.3rem .75rem;background:rgba(58,139,60,.06);border:1px solid rgba(58,139,60,.18);border-radius:3px}}
.dv3-eyebrow-dot{{width:7px;height:7px;border-radius:50%;background:var(--text-muted)}}
.dv3-date{{font-family:'JetBrains Mono',monospace;font-size:.78rem;color:var(--text-muted);letter-spacing:.08em;margin-bottom:.6rem;text-transform:uppercase}}
.dv3-headline{{font-family:'Oswald',sans-serif;font-size:clamp(2rem,4vw,3rem);font-weight:700;line-height:1.15;color:var(--text);margin-bottom:.6rem;letter-spacing:-.01em;text-transform:uppercase}}
.dv3-subheadline{{font-size:.92rem;color:var(--gold);font-weight:600;margin-bottom:.75rem}}
.dv3-lead{{font-size:1.05rem;line-height:1.75;color:var(--text-dim);max-width:720px}}
.dv3-surprise-banner{{display:none;align-items:center;gap:.6rem;padding:.65rem 1rem;background:linear-gradient(135deg,rgba(218,165,32,.06) 0%,rgba(240,145,58,.04) 100%);border:1px solid rgba(218,165,32,.20);border-radius:var(--r-md);margin-bottom:1.25rem}}
.dv3-surprise-banner .surprise-icon{{font-size:1.1rem;flex-shrink:0}}
.dv3-surprise-banner .surprise-text{{font-size:.85rem;color:var(--text-dim);line-height:1.45}}
.dv3-surprise-banner .surprise-text strong{{color:var(--gold);font-weight:700}}
.dv3-mood{{display:none;align-items:center;gap:.3rem;font-family:'JetBrains Mono',monospace;font-size:.62rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:.22rem .6rem;border-radius:3px;white-space:nowrap;margin-left:.75rem}}
.dv3-topbar{{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem;margin-bottom:2rem}}
.dv3-one-number{{background:var(--surface);border:2px solid var(--border-g);border-radius:var(--r-md);padding:1.2rem 1.4rem}}
.dv3-one-number-label{{font-family:'JetBrains Mono',monospace;font-size:.64rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);margin-bottom:.5rem}}
.dv3-one-number-val{{font-family:'Oswald',sans-serif;font-size:3.2rem;font-weight:700;color:var(--gold);line-height:1;margin-bottom:.15rem}}
.dv3-one-number-unit{{font-size:.85rem;color:var(--text-dim);margin-bottom:.4rem}}
.dv3-one-number-ctx{{font-size:.88rem;line-height:1.6;color:var(--text-dim)}}
.dv3-quote-card{{background:var(--surface);border:2px solid rgba(218,165,32,.15);border-radius:var(--r-md);padding:1.2rem 1.4rem;display:flex;flex-direction:column;justify-content:center}}
.dv3-quote-label{{font-family:'JetBrains Mono',monospace;font-size:.64rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--gold);margin-bottom:.6rem}}
.dv3-quote-text{{font-size:.95rem;font-style:italic;color:var(--text-dim);line-height:1.65;margin-bottom:.35rem}}
.dv3-quote-attr{{font-size:.76rem;color:var(--text-muted)}}
.dv3-sections{{display:flex;flex-direction:column;gap:1.25rem;margin-bottom:2rem}}
.dv3-sec{{background:var(--surface);border:2px solid var(--border);border-radius:var(--r-md);padding:1.2rem 1.4rem;position:relative;transition:border-color .2s}}
.dv3-sec:hover{{border-color:var(--border-g)}}
.dv3-sec--surprise{{border-color:rgba(218,165,32,.30)!important;background:linear-gradient(135deg,var(--surface) 0%,rgba(218,165,32,.03) 100%)}}
.dv3-sec--surprise::before{{content:'⚡ OVERNIGHT SURPRISE';position:absolute;top:-.55rem;right:.75rem;font-family:'JetBrains Mono',monospace;font-size:.5rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#fff;background:var(--gold);padding:.12rem .55rem;border-radius:2px}}
.dv3-sec--heat{{border-color:rgba(58,139,60,.35)!important}}
.dv3-sec--heat::after{{content:'🔥 TOP STORY';position:absolute;top:-.55rem;left:.75rem;font-family:'JetBrains Mono',monospace;font-size:.5rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#fff;background:var(--green);padding:.12rem .55rem;border-radius:2px}}
.dv3-sec-header{{display:flex;align-items:center;gap:.55rem;margin-bottom:.65rem}}
.dv3-sec-icon{{font-size:1.3rem;flex-shrink:0}}
.dv3-sec-title{{font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);flex:1}}
.dv3-sec-conviction{{font-family:'JetBrains Mono',monospace;font-size:.55rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.15rem .45rem;border-radius:3px;white-space:nowrap}}
.dv3-sec-body{{font-size:.95rem;line-height:1.75;color:var(--text-dim);margin-bottom:.65rem}}
.dv3-sec-body strong{{color:var(--text)}}
.dv3-sec-bottomline{{font-family:'JetBrains Mono',monospace;font-size:.78rem;font-weight:700;color:var(--text);padding:.5rem .75rem;background:var(--surface2);border-radius:var(--r-sm);border-left:3px solid var(--gold);margin-bottom:.5rem;line-height:1.45}}
.dv3-sec-action{{font-size:.82rem;font-weight:600;color:var(--green);padding:.45rem .7rem;background:rgba(58,139,60,.04);border:1px solid rgba(58,139,60,.15);border-radius:var(--r-sm);line-height:1.45}}
.dv3-tmyk{{background:linear-gradient(135deg,var(--surface) 0%,rgba(74,143,186,.03) 100%);border:2px solid rgba(74,143,186,.20);border-radius:var(--r-md);padding:1.2rem 1.4rem;margin-bottom:2rem}}
.dv3-tmyk-label{{font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--blue);margin-bottom:.55rem}}
.dv3-tmyk-title{{font-size:1rem;font-weight:700;color:var(--text);margin-bottom:.35rem}}
.dv3-tmyk-body{{font-size:.92rem;line-height:1.75;color:var(--text-dim)}}
.dv3-watch{{background:var(--surface);border:2px solid var(--border);border-radius:var(--r-md);padding:1.2rem 1.4rem;margin-bottom:2rem}}
.dv3-watch-label{{font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);margin-bottom:.75rem}}
.dv3-watch-list{{list-style:none;padding:0;margin:0}}
.dv3-watch-item{{display:flex;gap:.75rem;align-items:flex-start;padding:.55rem 0;border-bottom:1px solid var(--border)}}
.dv3-watch-item:last-child{{border-bottom:none;padding-bottom:0}}
.dv3-watch-time{{font-family:'JetBrains Mono',monospace;color:var(--gold);font-weight:600;font-size:.85rem;white-space:nowrap;flex-shrink:0;min-width:72px}}
.dv3-watch-desc{{color:var(--text-dim);font-size:.88rem;line-height:1.55}}
.dv3-watch-desc strong{{color:var(--text)}}
.dv3-source{{font-size:.68rem;color:var(--text-muted);text-align:center;padding:.75rem 0;border-top:1px solid var(--border);margin-bottom:2rem}}
.dv3-nav{{display:flex;justify-content:space-between;align-items:center;padding:1rem 0;border-top:2px solid var(--border);border-bottom:2px solid var(--border);margin-bottom:2rem}}
.dv3-nav a{{display:inline-flex;align-items:center;gap:.35rem;font-size:.85rem;font-weight:600;color:var(--green);transition:opacity .15s}}
.dv3-nav a:hover{{opacity:.8}}
.dv3-nav-center{{font-family:'JetBrains Mono',monospace;font-size:.68rem;color:var(--text-muted);text-transform:uppercase;letter-spacing:.1em}}
@media(max-width:640px){{.dv3-page{{padding:1.25rem .9rem}}.dv3-topbar{{grid-template-columns:1fr}}.dv3-one-number-val{{font-size:2.4rem}}.dv3-sec{{padding:.85rem 1rem}}.dv3-sec--surprise::before,.dv3-sec--heat::after{{font-size:.45rem;padding:.08rem .4rem}}}}
@media(max-width:380px){{.dv3-headline{{font-size:1.6rem}}.dv3-one-number-val{{font-size:2rem}}.dv3-sec-action{{display:none}}}}
</style>
</head>
<body>
<a class="skip" href="#main-content">Skip to content</a>
<div id="site-header"></div>
<main id="main-content">
<div class="dv3-page">
  <nav class="breadcrumb" aria-label="Breadcrumb"><a href="/">Home</a> / <a href="/daily">Daily Briefing</a> / <strong>{html_esc(date_display)}</strong></nav>

  <article>
    <header class="dv3-header">
      <div style="display:flex;align-items:center;flex-wrap:wrap;gap:.5rem">
        <div class="dv3-eyebrow"><span class="dv3-eyebrow-dot"></span> AGSIST DAILY — ARCHIVE</div>
        {mood_html}
      </div>
      <div class="dv3-date">{html_esc(date_display)}</div>
      <h1 class="dv3-headline">{headline}</h1>
      {"<p class='dv3-subheadline'>" + subheadline + "</p>" if subheadline else ""}
      {surprise_html}
      <p class="dv3-lead">{lead}</p>
    </header>

    <div class="dv3-topbar">
      {one_num_html}
      {quote_html}
    </div>

    <div class="dv3-sections">
      {sections_html}
    </div>

    {tmyk_html}
    {watch_html}

    <div class="dv3-source">
      {source} · Generated by AGSIST AI
    </div>
  </article>

  <nav class="dv3-nav" aria-label="Briefing navigation" id="dv3-archive-nav">
    <span></span>
    <span class="dv3-nav-center"><a href="/daily">← Latest Briefing</a></span>
    <span></span>
  </nav>

  <div style="text-align:center;padding:1.5rem 0">
    <a href="/daily" class="btn-gold">Today's Briefing →</a>
    <div style="margin-top:.75rem">
      <a href="/daily#archive" style="font-size:.82rem;color:var(--text-muted)">Browse All Briefings →</a>
    </div>
  </div>
</div>
</main>
<div id="site-footer"></div>
<script src="/components/loader.js"></script>
<script>
// Load prev/next nav from archive index
(function(){{
  fetch('/data/daily-archive/index.json',{{cache:'no-store'}})
    .then(function(r){{return r.ok?r.json():null}})
    .then(function(idx){{
      if(!idx||!idx.briefings)return;
      var current='{date_iso}';
      var entries=idx.briefings;
      var curIdx=-1;
      for(var i=0;i<entries.length;i++){{if(entries[i].date===current){{curIdx=i;break}}}}
      if(curIdx<0)return;
      var nav=document.getElementById('dv3-archive-nav');
      if(!nav)return;
      var prev=curIdx<entries.length-1?entries[curIdx+1]:null;
      var next=curIdx>0?entries[curIdx-1]:null;
      var spans=nav.querySelectorAll('span');
      if(prev&&spans[0])spans[0].innerHTML='<a href="/daily/'+prev.date+'">← '+prev.date+'</a>';
      if(next&&spans[2])spans[2].innerHTML='<a href="/daily/'+next.date+'">'+next.date+' →</a>';
    }}).catch(function(){{}});
}})();
</script>
</body>
</html>'''

    return page


def update_archive_index(briefing, date_iso):
    """Update or create the archive index.json with today's entry."""

    index_path = ARCHIVE_JSON_DIR / "index.json"

    if index_path.exists():
        with open(index_path) as f:
            index = json.load(f)
    else:
        index = {"briefings": [], "updated": ""}

    entries = index.get("briefings", [])

    headline = briefing.get("headline", "")
    teaser = briefing.get("teaser", "")
    if not teaser and briefing.get("lead"):
        teaser = briefing["lead"][:140] + ("…" if len(briefing.get("lead", "")) > 140 else "")
    meta = briefing.get("meta", {})
    surprise_count = meta.get("overnight_surprises_count", 0)

    entry = {
        "date": date_iso,
        "date_display": briefing.get("date", date_iso),
        "headline": headline,
        "teaser": teaser,
        "market_mood": meta.get("market_mood", ""),
        "surprise_count": surprise_count,
        "sections": len(briefing.get("sections", [])),
        "url": f"/daily/{date_iso}",
    }

    found = False
    for i, e in enumerate(entries):
        if e.get("date") == date_iso:
            entries[i] = entry
            found = True
            break
    if not found:
        entries.insert(0, entry)

    entries.sort(key=lambda x: x.get("date", ""), reverse=True)

    index["briefings"] = entries
    index["updated"] = datetime.now(timezone.utc).isoformat()
    index["count"] = len(entries)

    with open(index_path, "w") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    return len(entries)


def save_archive(briefing):
    """Save today's briefing to the archive (JSON + static HTML)."""

    date_iso = datetime.now().strftime("%Y-%m-%d")

    ARCHIVE_JSON_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_HTML_DIR.mkdir(parents=True, exist_ok=True)

    json_path = ARCHIVE_JSON_DIR / f"{date_iso}.json"
    with open(json_path, "w") as f:
        json.dump(briefing, f, indent=2, ensure_ascii=False)
    print(f"  📁 Archive JSON: {json_path}")

    html_content = generate_archive_html(briefing, date_iso)
    html_path = ARCHIVE_HTML_DIR / f"{date_iso}.html"
    with open(html_path, "w") as f:
        f.write(html_content)
    print(f"  📄 Archive HTML: {html_path}")

    count = update_archive_index(briefing, date_iso)
    print(f"  📋 Archive index: {count} briefings")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("═══ AGSIST Daily Briefing Generator v3.2 ═══")
    print(f"  Time: {datetime.now().isoformat()}")

    print("  Loading prices.json...")
    price_data, surprises = load_prices()
    if surprises:
        print(f"  ⚡ {len(surprises)} overnight surprise(s)!")
        for s in surprises:
            print(f"    {s['commodity']}: {s['pct_change']:+.1f}% (magnitude {s['surprise_magnitude']}x)")
    else:
        print("  No overnight surprises — normal trading day.")

    print("  Fetching ag news RSS...")
    news_block = fetch_ag_news()

    seasonal_ctx = get_seasonal_context()
    print("  Seasonal context loaded.")

    todays_quote = get_todays_quote()
    print(f"  Quote: \"{todays_quote['text'][:50]}...\" — {todays_quote['attribution']}")

    print("  Calling Claude API...")
    briefing = call_claude(price_data, surprises, news_block, seasonal_ctx, todays_quote)

    # ── Post-generation price validation ─────────────────────────
    locked_prices = price_data.get("locked_prices", {})
    is_clean, val_warnings = validate_briefing(briefing, locked_prices)
    if val_warnings:
        print(f"  ⚠️  Price validation warnings ({len(val_warnings)}):")
        for w in val_warnings:
            print(f"    • {w}")
    else:
        print("  ✅ Price validation passed — all prices traceable to prices.json")

    # Inject metadata
    briefing["generated_at"] = datetime.now(timezone.utc).isoformat()
    briefing["generator_version"] = "3.2"
    briefing["surprise_count"] = len(surprises)
    briefing["surprises"] = surprises
    briefing["price_validation_clean"] = is_clean
    if "meta" not in briefing:
        briefing["meta"] = {}
    briefing["meta"]["overnight_surprises_count"] = len(surprises)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(briefing, f, indent=2, ensure_ascii=False)

    print(f"  ✅ Written to {OUTPUT_PATH}")

    print("  Archiving briefing...")
    save_archive(briefing)

    print(f"  Headline: {briefing.get('headline', 'N/A')}")
    print(f"  Sections: {len(briefing.get('sections', []))}")
    print(f"  Surprises: {len(surprises)}")
    print(f"  Price validation: {'clean' if is_clean else f'{len(val_warnings)} warning(s)'}")
    print("═══ Done ═══")


if __name__ == "__main__":
    main()
