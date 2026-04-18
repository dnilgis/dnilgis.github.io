#!/usr/bin/env python3
"""
AGSIST Daily Briefing Generator — v3.5
═══════════════════════════════════════════════════════════════════
Generates the daily agricultural intelligence briefing via Claude API.
Runs every morning at 5:45 AM CT via GitHub Actions (7 days/week).

v3.5 changes (2026-04-17):
  - QUOTE POOL: Reads from data/quote-pool.json instead of in-file
    QUOTE_BANK. Never emits "Unknown" attributions. The schema
    validator will fail the workflow if a filler attribution leaks
    through.
  - NATIONAL SCOPE: Removed all "Wisconsin and Minnesota" language —
    AGSIST serves US grain producers. Voice and farmer-action prompts
    updated to reflect national audience.
  - NO EM-DASH RULE: Prompt now explicitly bans em dashes (U+2014)
    and en dashes (U+2013) in generated prose. Periods, commas, or
    parentheses instead. Post-generation validation catches drift.
  - FLEXIBLE SECTIONS: Minimum 2 sections instead of a fixed 4.
    Quiet days collapse naturally. Macro bucket folds into adjacent
    sections when nothing's happening.
  - CONDITIONAL FARMER ACTIONS: Generic boilerplate like "lock in
    diesel when prices soften" is explicitly disallowed. Action is
    omitted unless tied to a specific, thresholded recommendation.
  - ONE_NUMBER STRICTER: Must be a number that is either surprising
    on its own, or illuminated by meaningful context. Trivial
    non-events (like "down 0.2%") must be passed over in favor of a
    streak stat or context figure.
  - YESTERDAY'S TMYK EXCLUSION: Strengthened past-briefing prompt so
    the model cannot repeat a topic from the last 3 days.
  - CANONICAL FIELD NAMES: Output uses daily_quote, the_more_you_know,
    one_number, watch_list exclusively. Matches data/quote-pool.json
    and scripts/daily_schema.py.

Data pipeline:
  1. Read /data/prices.json (yfinance, fetched every 30 min including weekends)
  2. Detect weekend/holiday — adjust prompt accordingly
  3. Load last 3 /data/daily-archive/DATE.json for continuity
  4. Pick today's quote from /data/quote-pool.json
  5. Fetch ag RSS feeds
  6. Call Claude API
  7. Validate output against source prices + schema
  8. Write /data/daily.json
  9. Archive: /data/daily-archive/DATE.json + /daily/DATE.html + index.json

Env vars required:
  ANTHROPIC_API_KEY
"""

import json
import os
import sys
import random
import re
from datetime import datetime, timezone, timedelta
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
QUOTE_POOL_PATH = REPO_ROOT / "data" / "quote-pool.json"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-20250514"

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

FILLER_ATTRIBUTIONS = {"unknown", "anonymous", "n/a", "", "—", "-"}

# ═══════════════════════════════════════════════════════════════════
# WEEKEND / HOLIDAY DETECTION
# ═══════════════════════════════════════════════════════════════════

def get_market_status():
    """
    Determine whether US commodity markets (CME/CBOT) are open or closed.
    """
    now = datetime.now()
    weekday = now.weekday()
    month, day = now.month, now.day

    if weekday == 5:
        return {
            "is_closed": True,
            "reason": "weekend",
            "day_name": "Saturday",
            "note": (
                "TODAY IS SATURDAY. Commodity markets are CLOSED. "
                "All prices below are Friday's closing values. "
                "Write this as a WEEKEND RECAP and WEEK-AHEAD OUTLOOK, not an overnight recap. "
                "Say 'as of Friday's close' when referencing prices. "
                "Lead with what happened this week, and what it means going into next week. "
                "Do NOT use language like 'overnight' or 'this morning's session.' "
                "The farmer reading this on Saturday morning wants to know what to think about before markets open Monday."
            ),
        }
    if weekday == 6:
        return {
            "is_closed": True,
            "reason": "weekend",
            "day_name": "Sunday",
            "note": (
                "TODAY IS SUNDAY. Commodity markets are CLOSED. "
                "All prices below are Friday's closing values. "
                "Write this as a SUNDAY PREVIEW and WEEK-AHEAD OUTLOOK, not an overnight recap. "
                "Say 'as of Friday's close' when referencing prices. "
                "Lead with what to watch when markets open Monday morning. "
                "Do NOT use language like 'overnight' or 'this morning's session.' "
                "The farmer reading this Sunday wants to be ready for Monday's open."
            ),
        }

    fixed_holidays = {
        (1, 1):   "New Year's Day",
        (7, 4):   "Independence Day",
        (12, 25): "Christmas Day",
    }
    for (hm, hd), hname in fixed_holidays.items():
        if month == hm and day == hd:
            return {
                "is_closed": True,
                "reason": "holiday",
                "day_name": hname,
                "note": (
                    f"TODAY IS {hname.upper()}. Commodity markets are CLOSED. "
                    "All prices below are from the last trading session. "
                    "Write this as a HOLIDAY RECAP and OUTLOOK, not an overnight recap. "
                    "Acknowledge the holiday briefly, then cover what matters for farmers returning Monday. "
                    "Do NOT use language like 'overnight' or 'this morning's session.'"
                ),
            }
        if weekday == 4 and month == hm and day == hd - 1:
            return {
                "is_closed": True,
                "reason": "holiday",
                "day_name": f"{hname} (observed)",
                "note": (
                    f"TODAY IS {hname.upper()} OBSERVED. Commodity markets are CLOSED. "
                    "All prices below are from the last trading session. "
                    "Write as a holiday outlook. Do NOT reference overnight sessions."
                ),
            }
        if weekday == 0 and month == hm and day == hd + 1:
            return {
                "is_closed": True,
                "reason": "holiday",
                "day_name": f"{hname} (observed)",
                "note": (
                    f"TODAY IS {hname.upper()} OBSERVED. Commodity markets are CLOSED. "
                    "All prices below are from the last trading session. "
                    "Write as a holiday outlook. Do NOT reference overnight sessions."
                ),
            }

    # Good Friday (Butcher's Easter algorithm)
    y = now.year
    a = y % 19
    b = y // 100
    c = y % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m_val = (a + 11 * h + 22 * l) // 451
    easter_month = (h + l - 7 * m_val + 114) // 31
    easter_day = ((h + l - 7 * m_val + 114) % 31) + 1
    easter = datetime(y, easter_month, easter_day)
    good_friday = easter - timedelta(days=2)
    if now.month == good_friday.month and now.day == good_friday.day:
        return {
            "is_closed": True,
            "reason": "holiday",
            "day_name": "Good Friday",
            "note": (
                "TODAY IS GOOD FRIDAY. Commodity markets are CLOSED. "
                "All prices below are from Thursday's session. "
                "Write as a holiday outlook. Do NOT reference overnight sessions."
            ),
        }

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    return {
        "is_closed": False,
        "reason": "open",
        "day_name": day_names[weekday],
        "note": "",
    }


# ═══════════════════════════════════════════════════════════════════
# QUOTE SELECTION
# ═══════════════════════════════════════════════════════════════════

def get_todays_quote():
    """
    Pick today's quote from data/quote-pool.json. Seeded by day-of-year
    so the same quote shows on every run within the same day.

    Filters out any "Unknown" / "Anonymous" attributions before selection,
    even if they slip into the pool file. The schema validator will also
    catch this downstream, but we want to avoid emitting them at all.
    """
    if not QUOTE_POOL_PATH.exists():
        print(f"  [warn] quote pool not found at {QUOTE_POOL_PATH} — using fallback", file=sys.stderr)
        return {
            "text": "Agriculture is our wisest pursuit, because it will in the end contribute most to real wealth, good morals, and happiness.",
            "attribution": "Thomas Jefferson",
        }

    try:
        with open(QUOTE_POOL_PATH) as f:
            pool = json.load(f)
    except Exception as e:
        print(f"  [warn] quote pool unreadable: {e}", file=sys.stderr)
        return {
            "text": "Agriculture is our wisest pursuit, because it will in the end contribute most to real wealth, good morals, and happiness.",
            "attribution": "Thomas Jefferson",
        }

    quotes = pool.get("quotes", [])
    # Reject any quote with a filler attribution
    quotes = [
        q for q in quotes
        if q.get("text") and q.get("attribution")
        and q["attribution"].strip().lower() not in FILLER_ATTRIBUTIONS
    ]

    if not quotes:
        return {
            "text": "Agriculture is our wisest pursuit, because it will in the end contribute most to real wealth, good morals, and happiness.",
            "attribution": "Thomas Jefferson",
        }

    now = datetime.now()
    seed = now.timetuple().tm_yday + now.year * 1000
    random.seed(seed)
    q = random.choice(quotes)
    random.seed()
    return {"text": q["text"], "attribution": q["attribution"]}


# ═══════════════════════════════════════════════════════════════════
# DATA COLLECTION
# ═══════════════════════════════════════════════════════════════════

def http_get(url, timeout=10):
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
            req = urllib.request.Request(url, headers={"User-Agent": "AGSIST-Daily/3.5"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  [warn] fetch failed: {url} — {e}", file=sys.stderr)
            return None


def load_prices():
    if not PRICES_PATH.exists():
        print("[error] prices.json not found", file=sys.stderr)
        return {}, []

    with open(PRICES_PATH) as f:
        data = json.load(f)

    quotes = data.get("quotes", {})
    fetched = data.get("fetched", "")

    price_lines = []
    locked_prices = {}
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
            locked_prices[key] = close / 100
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

        direction_up = pct > 0
        direction_dn = pct < 0
        arrow = "UP" if direction_up else ("DN" if direction_dn else "FLAT")
        line = f"  {label}: {price_str} ({arrow} {chg_str})"

        wk52_hi = q.get("wk52_hi")
        wk52_lo = q.get("wk52_lo")
        if wk52_hi and wk52_lo:
            hi, lo = float(wk52_hi), float(wk52_lo)
            if hi > lo:
                position = ((close - lo) / (hi - lo)) * 100
                line += f" [52wk: {position:.0f}% from low]"

        price_lines.append(line)

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


def load_past_dailies(num_days=3):
    """Load last N briefings for narrative continuity and to avoid repeats."""
    archive_dir = REPO_ROOT / "data" / "daily-archive"
    index_path  = archive_dir / "index.json"

    if not index_path.exists():
        return "", []

    try:
        with open(index_path) as f:
            index = json.load(f)
    except Exception:
        return "", []

    briefings = index.get("briefings", [])
    if not briefings:
        return "", []

    today_iso = datetime.now().strftime("%Y-%m-%d")
    past = [b for b in briefings if b.get("date") != today_iso]
    past = sorted(past, key=lambda x: x.get("date", ""), reverse=True)[:num_days]

    if not past:
        return "", []

    blocks = []
    past_tmyk_topics = []
    for entry in past:
        date_iso = entry.get("date", "")
        json_path = archive_dir / f"{date_iso}.json"

        if json_path.exists():
            try:
                with open(json_path) as f:
                    b = json.load(f)
                headline      = b.get("headline", entry.get("headline", ""))
                mood          = b.get("meta", {}).get("market_mood", "")
                surprises     = b.get("surprises", [])
                surprise_names = [s.get("commodity","") + f" {s.get('pct_change',0):+.1f}%" for s in surprises[:4]]
                # canonical field first, then legacy fallbacks
                tmyk = b.get("the_more_you_know") or b.get("tmyk") or {}
                tmyk_title = tmyk.get("title", "")
                if tmyk_title:
                    past_tmyk_topics.append(tmyk_title)
                section_titles = [s.get("title","") for s in b.get("sections", [])]
                actions        = [s.get("farmer_action","") for s in b.get("sections", []) if s.get("farmer_action")]
                block = f"  DATE: {date_iso}"
                block += f"\n  HEADLINE: {headline}"
                if mood:
                    block += f"\n  MOOD: {mood}"
                if surprise_names:
                    block += f"\n  OVERNIGHT SURPRISES: {' / '.join(surprise_names)}"
                if tmyk_title:
                    block += f"\n  THE MORE YOU KNOW topic: {tmyk_title}"
                if section_titles:
                    block += f"\n  SECTIONS COVERED: {', '.join(section_titles)}"
                if actions:
                    block += f"\n  FARMER ACTIONS GIVEN: {' | '.join(actions[:3])}"
            except Exception:
                block = f"  DATE: {date_iso}\n  HEADLINE: {entry.get('headline','')}"
        else:
            block = f"  DATE: {date_iso}\n  HEADLINE: {entry.get('headline','')}"

        blocks.append(block)

    header = (
        "═══ PAST BRIEFINGS (last 3 days) ═══\n"
        "Use for narrative continuity and to AVOID repeating topics.\n"
        "Do NOT use past prices. Use ONLY today's LOCKED PRICE TABLE.\n"
        "TMYK topic MUST be different from any listed above.\n"
        "Do not recount yesterday's story as if it were fresh news.\n\n"
    )
    return header + "\n\n".join(blocks), past_tmyk_topics


def fetch_ag_news():
    if not feedparser:
        return "No RSS feeds available. Focus on price action and seasonal context."

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
                    headlines.append(f"  * {title} ({pub[:16]})")
        except Exception:
            continue

    if not headlines:
        return "No fresh RSS headlines. Focus on price action and seasonal context."

    seen = set()
    unique = []
    for h in headlines:
        key = h[:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(h)
    return "\n".join(unique[:25])


def get_seasonal_context():
    month = datetime.now().month
    contexts = {
        1: "Mid-winter: South American crop development (Brazil safrinha, Argentina soybeans). Cattle markets seasonally strong. Input purchasing decisions for spring.",
        2: "Late winter: USDA Ag Outlook Forum typically this month. South American harvest beginning. Final input purchasing before spring.",
        3: "Pre-planting: USDA Prospective Plantings at end of March is THE report. Fieldwork starting in South. Nitrogen applications beginning.",
        4: "Planting season: Corn planting underway (April 15 to May 15 optimal in Corn Belt). Every day of delay costs roughly 1 bu/acre. Weather dominance begins.",
        5: "Peak planting: Soybean planting (May 1 to June 5 optimal). Prevent plant deadline approaching. First crop condition ratings.",
        6: "Growing season: Crop conditions drive markets. Pollination approaching for early-planted corn. Wheat harvest beginning in Southern Plains.",
        7: "Critical: Corn pollination (most critical 2 weeks of the year). USDA Acreage report (June 30). Soybean bloom/pod set. Weather premium at peak.",
        8: "Yield formation: Corn in dough/dent. Soybean pod fill critical. USDA Pro Farmer crop tour. Fall crop insurance pricing.",
        9: "Early harvest: Corn harvest beginning. USDA September WASDE. Basis narrows as harvest pressure builds.",
        10: "Harvest: Full corn/soybean harvest. Basis at seasonal lows. Storage vs. sell decisions. Fall fieldwork. Wheat planting.",
        11: "Post-harvest: Final USDA yield estimates. South American planting. Grain storage management. Tax planning.",
        12: "Year-end: Final crop production estimates. USDA supply/demand tables. Tax deadlines. South American weather watch.",
    }
    return contexts.get(month, "Monitor markets and seasonal patterns.")


# ═══════════════════════════════════════════════════════════════════
# CLAUDE API CALL
# ═══════════════════════════════════════════════════════════════════

def build_system_prompt(market_status, past_tmyk_topics):
    weekend_instructions = ""
    if market_status["is_closed"]:
        day = market_status["day_name"]
        reason = market_status["reason"]
        if reason == "weekend" and "Saturday" in day:
            weekend_instructions = """
══ WEEKEND MODE: SATURDAY ══
Markets are CLOSED. Write a WEEK IN REVIEW plus WEEKEND OUTLOOK:
- Lead with what defined this week in ag markets.
- Reference prices as "as of Friday's close", not "overnight" or "this morning".
- What should farmers think about this weekend?
- The 2 or 3 most important things to watch when markets open Monday.
- Section titles should reflect this, for example "WEEK IN REVIEW", "WHAT TO WATCH MONDAY".
- market_mood should reflect the week's tone, not a single session.
- Do NOT use language implying active markets or overnight sessions."""
        elif reason == "weekend" and "Sunday" in day:
            weekend_instructions = """
══ WEEKEND MODE: SUNDAY ══
Markets are CLOSED. Write a SUNDAY PREVIEW plus WEEK AHEAD BRIEFING:
- Lead with what to expect when markets open Monday morning.
- Reference prices as "as of Friday's close".
- What risks and catalysts should farmers be aware of this coming week?
- Section titles should reflect this, for example "WEEK AHEAD", "MONDAY WATCH LIST".
- market_mood should reflect outlook for the coming week.
- Do NOT use language implying active markets or overnight sessions."""
        else:
            weekend_instructions = f"""
══ HOLIDAY MODE: {day.upper()} ══
Markets are CLOSED. Write a HOLIDAY OUTLOOK:
- Briefly acknowledge the holiday.
- Reference prices as "as of the last trading session".
- What should farmers think about before markets reopen?
- Do NOT use overnight or session language."""

    banned_tmyk = ""
    if past_tmyk_topics:
        banned_tmyk = (
            "\n\n══ TMYK TOPIC EXCLUSION ══\n"
            "The following topics were covered in the last 3 briefings. "
            "You MUST pick a different angle today:\n  - "
            + "\n  - ".join(past_tmyk_topics)
        )

    return f"""You are the voice of AGSIST Daily, a trusted morning agricultural intelligence briefing read every weekday by US grain and livestock producers.

YOUR VOICE:
- The sharp friend who actually trades grain AND reads the WASDE. Not an academic. Not a reporter.
- Direct, opinionated, honest about uncertainty.
- Connect dots that farmers wouldn't connect on their own.
- Plain language. "Managed money" needs a parenthetical "(hedge funds)" on first use.
- Calibrated tone. Most days are normal. A 1% corn move is not "dramatic".
- Reference ongoing story arcs when past briefings are provided.

GEOGRAPHIC SCOPE: National. AGSIST readers are across the US. NEVER narrow to "Wisconsin and Minnesota farmers" or any specific state. Use "US producers", "Corn Belt farmers", or "farmers across the country" when scope matters.
{weekend_instructions}
{banned_tmyk}

══ WRITING RULES ══
1. NO EM DASHES (U+2014) OR EN DASHES (U+2013) anywhere in your prose. Use periods, commas, semicolons, colons, or parentheses instead.
   - WRONG: "Corn eased to $4.58 — planting pressure builds"
   - RIGHT: "Corn eased to $4.58. Planting pressure builds."
   - RIGHT: "Corn eased to $4.58 (planting pressure builds)."
2. No hyphen-word-hyphen constructions used as pseudo em dashes (for example " - ").
3. Every specific price must come from the LOCKED PRICE TABLE. No exceptions.
4. If a price isn't in the table, don't mention it specifically.
5. Never invent, estimate, or recall prices from training data.
6. Describe moves exactly as shown. Don't round or reframe.
7. Vary sentence structure across the briefing. Avoid the template "X did Y because Z, while Q did R" repeating in every section.

══ TONE CALIBRATION ══
- magnitude below 1.5: "moved", "gained", "eased", "dipped"
- magnitude 1.5 to 2.5: "jumped", "fell", "rallied", "slid"
- magnitude 2.5 to 3.5: "surged", "dropped sharply", "spiked"
- magnitude above 3.5: "exploded", "crashed", "historic move". Genuinely rare only.

══ OUTPUT STRUCTURE ══
Return valid JSON. Use EXACTLY these field names:

{{
  "headline": "ALL CAPS, 6 to 10 words. The single biggest story.",
  "subheadline": "One sentence adding context.",
  "lead": "2 or 3 sentences. Must contain at least one specific price from the table.",
  "teaser": "One punchy sentence for the collapsed hero bar.",
  "one_number": {{
    "value": "The most interesting number of the day. Must be from the LOCKED PRICE TABLE or be a meaningful composite (streak count, days since, ratio). Trivial non-events (for example 'down 0.2%') must NOT be the one number. Pick something a farmer would actually find illuminating.",
    "unit": "3 to 6 words describing what the number represents.",
    "context": "2 or 3 sentences explaining why it matters."
  }},
  "sections": [
    {{
      "title": "3 to 5 words",
      "icon": "Single emoji",
      "body": "3 to 5 sentences. Bold a key phrase with <strong> tags. All prices from the LOCKED TABLE.",
      "bottom_line": "One sentence TL;DR, max 20 words.",
      "conviction_level": "low | medium | high",
      "overnight_surprise": true/false,
      "farmer_action": "OPTIONAL. Include ONLY when you have a specific, thresholded recommendation tied to today's data. Examples of WHAT TO INCLUDE: 'Price old crop if Dec corn closes above $4.85 on Monday.' 'Lock 50% of projected diesel if WTI breaks below $85.' Examples of WHAT NOT TO INCLUDE: 'Lock in diesel when prices soften.' 'Price remaining old crop aggressively on any weather scare.' Generic recommendations without a specific threshold are not allowed. If you cannot produce a genuinely specific action, OMIT this field entirely from the section."
    }}
  ],
  "the_more_you_know": {{
    "title": "Educational topic. MUST differ from any past TMYK topic listed above.",
    "body": "3 or 4 sentences. Smart friend over coffee. Teach something specific that ties to today's data if possible."
  }},
  "watch_list": [
    {{"time": "Time or timeframe", "desc": "What to watch. <strong> tags ok."}}
  ],
  "daily_quote": {{
    "text": "EXACT quote provided. Do not modify.",
    "attribution": "EXACT attribution provided. Do not modify."
  }},
  "source_summary": "Data sources",
  "date": "Full date like 'Saturday, April 4, 2026'",
  "meta": {{
    "market_mood": "bullish | bearish | mixed | cautious | volatile",
    "heat_section": 0,
    "overnight_surprises_count": 0
  }}
}}

══ SECTIONS ══
- Weekday default: Grains & Oilseeds / Livestock & Dairy / Energy & Inputs / Macro & Trade
- MINIMUM 2 sections, MAXIMUM 5. If there is no real story in one bucket (for example a flat Macro day), fold it into an adjacent section or OMIT IT entirely. Do not pad.
- Weekend/holiday: adjust titles to reflect review or outlook framing.

RESPOND WITH ONLY THE JSON OBJECT. No markdown. No preamble. No em dashes."""


def call_claude(price_data, surprises, news_block, seasonal_ctx, todays_quote, past_dailies_block, past_tmyk_topics, market_status):
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[error] ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    now = datetime.now()
    date_str = now.strftime("%A, %B %-d, %Y")

    if surprises and not market_status["is_closed"]:
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
                f"  {tier}: {s['commodity']} moved {s['pct_change']:+.1f}% "
                f"({s['direction']}), magnitude {s['surprise_magnitude']}x threshold"
            )
        surprise_block = (
            f"OVERNIGHT SURPRISES ({len(surprises)} moves above threshold):\n"
            + "\n".join(lines)
            + "\nFlag these in relevant sections with overnight_surprise: true."
        )
    elif market_status["is_closed"]:
        surprise_block = (
            "Markets are closed. Do not frame any price moves as 'overnight surprises.' "
            "These are simply Friday's closing prices vs Thursday's."
        )
    else:
        surprise_block = (
            "No overnight surprises. All moves within normal ranges. "
            "Write an honest, measured briefing. Quiet days deserve quiet briefings. "
            "Use fewer sections if warranted."
        )

    locked_table = price_data.get("price_block", "Price data unavailable")

    market_note = ""
    if market_status["is_closed"]:
        market_note = f"\nMARKET STATUS NOTE: {market_status['note']}\n"

    past_section = f"\n{past_dailies_block}\n" if past_dailies_block else ""

    user_message = f"""Generate today's AGSIST Daily briefing.

DATE: {date_str}
{market_note}
LOCKED PRICE TABLE (use ONLY these prices; do not invent or estimate):
{locked_table}

OVERNIGHT SURPRISE ANALYSIS:
{surprise_block}

SEASONAL CONTEXT:
{seasonal_ctx}
{past_section}
AG NEWS HEADLINES (context only, use prices above):
{news_block}

TODAY'S QUOTE (copy exactly, do not modify):
Text: "{todays_quote['text']}"
Attribution: "{todays_quote['attribution']}"

Your job: explain what these prices MEAN for US grain and livestock producers, what they SHOULD consider doing, and what's COMING NEXT. Remember: no em dashes, flexible section count, farmer_action only when genuinely specific."""

    payload = {
        "model": MODEL,
        "max_tokens": 4000,
        "system": build_system_prompt(market_status, past_tmyk_topics),
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
    warnings = []
    known_values = {k: v for k, v in locked_prices.items() if v and v > 0}

    all_text = []
    all_text.append(briefing.get("headline", ""))
    all_text.append(briefing.get("lead", ""))
    all_text.append(briefing.get("subheadline", ""))
    if briefing.get("one_number"):
        all_text.append(briefing["one_number"].get("context", ""))
    for sec in briefing.get("sections", []):
        all_text.append(sec.get("body", ""))
        all_text.append(sec.get("bottom_line", ""))
    # canonical + legacy for safety
    tmyk = briefing.get("the_more_you_know") or briefing.get("tmyk") or {}
    all_text.append(tmyk.get("body", ""))
    full_text = " ".join(all_text)

    # Em-dash / en-dash detection
    em_count = full_text.count("\u2014")
    en_count = full_text.count("\u2013")
    if em_count > 0:
        warnings.append(f"Em dash (U+2014) found {em_count} times. Prompt rule violation.")
    if en_count > 0:
        warnings.append(f"En dash (U+2013) found {en_count} times. Prompt rule violation.")

    # WI/MN scope creep
    lower = full_text.lower()
    for phrase in ("wisconsin", "minnesota", "wi/mn", "wi and mn"):
        if phrase in lower:
            warnings.append(f"Geographic scope violation: '{phrase}' found. AGSIST is national.")

    # Quote attribution filler check
    q = briefing.get("daily_quote") or briefing.get("quote") or {}
    attr = (q.get("attribution") or "").strip().lower()
    if attr in FILLER_ATTRIBUTIONS:
        warnings.append(f"daily_quote.attribution is filler ({q.get('attribution')!r}).")

    # Price invention check
    dollar_pattern = re.compile(r'\$([0-9,]+(?:\.[0-9]+)?)')
    found_values = []
    for match in dollar_pattern.finditer(full_text):
        try:
            val = float(match.group(1).replace(",", ""))
            found_values.append((val, match.group(0)))
        except ValueError:
            pass

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

    for found_val, found_str in found_values:
        matched = any(
            known_val > 0 and abs(found_val - known_val) / known_val <= 0.05
            for known_val in known_values.values()
        )
        if not matched:
            for key, (lo, hi) in COMMODITY_RANGES.items():
                if lo <= found_val <= hi:
                    warnings.append(f"Price {found_str} not in prices.json (possible {key})")
                    break

    return len(warnings) == 0, warnings


# ═══════════════════════════════════════════════════════════════════
# ARCHIVE (unchanged from v3.4 — same HTML output)
# ═══════════════════════════════════════════════════════════════════

ARCHIVE_JSON_DIR = REPO_ROOT / "data" / "daily-archive"
ARCHIVE_HTML_DIR = REPO_ROOT / "daily"


def html_esc(s):
    if not s:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def html_esc_preserve_strong(s):
    if not s:
        return ""
    parts = re.split(r'(</?strong>)', s, flags=re.IGNORECASE)
    result = []
    for part in parts:
        if part.lower() in ('<strong>', '</strong>'):
            result.append(part.lower())
        else:
            result.append(html_esc(part))
    return "".join(result)


def generate_archive_html(briefing, date_iso):
    date_display = briefing.get("date", date_iso)
    headline = html_esc(briefing.get("headline", "AGSIST Daily Briefing"))
    subheadline = html_esc(briefing.get("subheadline", ""))
    lead = html_esc(briefing.get("lead", ""))
    meta = briefing.get("meta", {})
    mood = meta.get("market_mood", "")
    heat_idx = meta.get("heat_section", -1)
    surprises = briefing.get("surprises", [])
    surprise_count = meta.get("overnight_surprises_count", 0)
    is_weekend_brief = briefing.get("market_closed", False)

    surprise_html = ""
    if surprise_count > 0 and not is_weekend_brief:
        names = []
        for s in surprises:
            arrow = "UP" if s.get("direction") == "up" else "DN"
            names.append(f'{s.get("commodity","")} {arrow} {abs(s.get("pct_change",0)):.1f}%')
        surprise_html = (
            f'<div class="dv3-surprise-banner" style="display:flex">\n'
            f'      <span class="surprise-icon">&#x26A1;</span>\n'
            f'      <span class="surprise-text"><strong>Overnight Surprise'
            f'{"s" if surprise_count > 1 else ""}:</strong> '
            f'{" / ".join(names) if names else str(surprise_count) + " unusual move" + ("s" if surprise_count > 1 else "")}'
            f'</span>\n    </div>'
        )

    mood_html = ""
    if mood:
        mood_colors = {
            "bullish":  ("var(--green)", "rgba(58,139,60,.08)", "rgba(58,139,60,.22)"),
            "bearish":  ("var(--red)", "rgba(184,76,42,.08)", "rgba(184,76,42,.22)"),
            "mixed":    ("var(--gold)", "rgba(218,165,32,.08)", "rgba(218,165,32,.22)"),
            "cautious": ("var(--blue)", "rgba(74,143,186,.08)", "rgba(74,143,186,.22)"),
            "volatile": ("var(--orange)", "rgba(200,122,40,.08)", "rgba(200,122,40,.22)"),
        }
        mood_icons = {"bullish": "\u2197", "bearish": "\u2198", "mixed": "\u2194", "cautious": "\u26A0\uFE0F", "volatile": "\U0001F525"}
        mc = mood_colors.get(mood, mood_colors["mixed"])
        mi = mood_icons.get(mood, "\U0001F4CA")
        mood_html = (
            f'<span class="dv3-mood" style="display:inline-flex;'
            f'color:{mc[0]};background:{mc[1]};border:1px solid {mc[2]}">'
            f'{mi} {mood.capitalize()}</span>'
        )

    sections_html = ""
    for i, sec in enumerate(briefing.get("sections", [])):
        cls = "dv3-sec"
        if sec.get("overnight_surprise") and not is_weekend_brief:
            cls += " dv3-sec--surprise"
        if i == heat_idx:
            cls += " dv3-sec--heat"

        icon = html_esc(sec.get("icon", "\U0001F4CA"))
        title = html_esc(sec.get("title", ""))
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
        action_html = f'<div class="dv3-sec-action">&#x1F3AF; {farmer_action}</div>' if farmer_action else ""

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

    one_num = briefing.get("one_number", {})
    one_num_html = ""
    if one_num:
        one_num_html = (
            f'<div class="dv3-one-number">\n'
            f'        <div class="dv3-one-number-label">&#x1F4CA; THE NUMBER</div>\n'
            f'        <div class="dv3-one-number-val">{html_esc(one_num.get("value", "\u2014"))}</div>\n'
            f'        <div class="dv3-one-number-unit">{html_esc(one_num.get("unit", ""))}</div>\n'
            f'        <div class="dv3-one-number-ctx">{html_esc(one_num.get("context", ""))}</div>\n'
            f'      </div>'
        )

    quote = briefing.get("daily_quote", {})
    quote_html = ""
    if quote:
        qt = quote.get("text", "").strip('"\u201c\u201d')
        qa = quote.get("attribution", "").lstrip("\u2014\u2013- ")
        quote_html = (
            f'<div class="dv3-quote-card">\n'
            f'        <div class="dv3-quote-label">&#x1F4AC; DAILY QUOTE</div>\n'
            f'        <p class="dv3-quote-text">\u201c{html_esc(qt)}\u201d</p>\n'
            f'        <cite class="dv3-quote-attr">{html_esc(qa)}</cite>\n'
            f'      </div>'
        )

    tmyk = briefing.get("the_more_you_know", {})
    tmyk_html = ""
    if tmyk:
        tmyk_html = (
            f'<div class="dv3-tmyk">\n'
            f'      <div class="dv3-tmyk-label">&#x1F9E0; THE MORE YOU KNOW</div>\n'
            f'      <div class="dv3-tmyk-title">{html_esc(tmyk.get("title", ""))}</div>\n'
            f'      <div class="dv3-tmyk-body">{html_esc(tmyk.get("body", ""))}</div>\n'
            f'    </div>'
        )

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
            f'      <div class="dv3-watch-label">&#x1F4C5; TODAY\'S WATCH LIST</div>\n'
            f'      <ul class="dv3-watch-list">{watch_items}</ul>\n'
            f'    </div>'
        )

    source = html_esc(briefing.get("source_summary", "USDA / CME Group / Open-Meteo"))
    gen_at = briefing.get("generated_at", "")

    weekend_badge = ""
    if is_weekend_brief:
        reason = briefing.get("market_status_reason", "")
        label = "WEEKEND EDITION" if reason == "weekend" else "HOLIDAY EDITION"
        weekend_badge = (
            f'<span style="display:inline-flex;align-items:center;gap:.3rem;'
            f'font-family:\'JetBrains Mono\',monospace;font-size:.58rem;font-weight:700;'
            f'letter-spacing:.1em;text-transform:uppercase;color:var(--gold);'
            f'background:rgba(218,165,32,.08);border:1px solid rgba(218,165,32,.22);'
            f'border-radius:3px;padding:.18rem .55rem;margin-left:.5rem">'
            f'&#x1F4C5; {label}</span>'
        )

    topbar_html = ""
    if one_num_html or quote_html:
        topbar_html = f'<div class="dv3-topbar">\n      {one_num_html}\n      {quote_html}\n    </div>'

    page = f'''<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AGSIST Daily &mdash; {html_esc(date_display)}: {headline}</title>
<meta name="description" content="{headline} &mdash; {html_esc(lead[:160])}">
<meta name="robots" content="index, follow">
<link rel="canonical" href="https://agsist.com/daily/{date_iso}">
<meta property="og:title" content="AGSIST Daily &mdash; {html_esc(date_display)}">
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
.dv3-eyebrow{{display:inline-flex;align-items:center;gap:.5rem;font-family:'JetBrains Mono',monospace;font-size:.68rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);margin-bottom:.75rem;padding:.3rem .75rem;background:rgba(74,171,76,.06);border:1px solid rgba(74,171,76,.18);border-radius:3px}}
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
.dv3-topbar{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:1.25rem;margin-bottom:2rem}}
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
.dv3-sec--surprise::before{{content:'\u26A1 OVERNIGHT SURPRISE';position:absolute;top:-.55rem;right:.75rem;font-family:'JetBrains Mono',monospace;font-size:.5rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#fff;background:var(--gold);padding:.12rem .55rem;border-radius:2px}}
.dv3-sec--heat{{border-color:rgba(74,171,76,.35)!important}}
.dv3-sec--heat::after{{content:'\U0001F525 TOP STORY';position:absolute;top:-.55rem;left:.75rem;font-family:'JetBrains Mono',monospace;font-size:.5rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#fff;background:var(--green);padding:.12rem .55rem;border-radius:2px}}
.dv3-sec-header{{display:flex;align-items:center;gap:.55rem;margin-bottom:.65rem}}
.dv3-sec-icon{{font-size:1.3rem;flex-shrink:0}}
.dv3-sec-title{{font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--green);flex:1}}
.dv3-sec-conviction{{font-family:'JetBrains Mono',monospace;font-size:.55rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:.15rem .45rem;border-radius:3px;white-space:nowrap}}
.dv3-sec-body{{font-size:.95rem;line-height:1.75;color:var(--text-dim);margin-bottom:.65rem}}
.dv3-sec-body strong{{color:var(--text)}}
.dv3-sec-bottomline{{font-family:'JetBrains Mono',monospace;font-size:.78rem;font-weight:700;color:var(--text);padding:.5rem .75rem;background:var(--surface2);border-radius:var(--r-sm);border-left:3px solid var(--gold);margin-bottom:.5rem;line-height:1.45}}
.dv3-sec-action{{font-size:.82rem;font-weight:600;color:var(--green);padding:.45rem .7rem;background:rgba(74,171,76,.04);border:1px solid rgba(74,171,76,.15);border-radius:var(--r-sm);line-height:1.45}}
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
@media(max-width:640px){{.dv3-page{{padding:1.25rem .9rem}}.dv3-topbar{{grid-template-columns:minmax(0,1fr)}}.dv3-one-number-val{{font-size:2.4rem}}.dv3-sec{{padding:.85rem 1rem}}.dv3-sec--surprise::before,.dv3-sec--heat::after{{font-size:.45rem;padding:.08rem .4rem}}}}
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
        <div class="dv3-eyebrow"><span class="dv3-eyebrow-dot"></span> AGSIST DAILY &mdash; ARCHIVE</div>
        {mood_html}
        {weekend_badge}
      </div>
      <div class="dv3-date">{html_esc(date_display)}</div>
      <h1 class="dv3-headline">{headline}</h1>
      {"<p class='dv3-subheadline'>" + subheadline + "</p>" if subheadline else ""}
      {surprise_html}
      <p class="dv3-lead">{lead}</p>
    </header>

    {topbar_html}

    <div class="dv3-sections">
      {sections_html}
    </div>

    {tmyk_html}
    {watch_html}

    <div class="dv3-source">
      {source} &middot; Generated by AGSIST AI
    </div>
  </article>

  <nav class="dv3-nav" aria-label="Briefing navigation" id="dv3-archive-nav">
    <span></span>
    <span class="dv3-nav-center"><a href="/daily">&larr; Latest Briefing</a></span>
    <span></span>
  </nav>

  <div style="text-align:center;padding:1.5rem 0">
    <a href="/daily" class="btn-gold">Today's Briefing &rarr;</a>
    <div style="margin-top:.75rem">
      <a href="/daily#archive" style="font-size:.82rem;color:var(--text-muted)">Browse All Briefings &rarr;</a>
    </div>
  </div>
</div>
</main>
<div id="site-footer"></div>
<script src="/components/loader.js"></script>
<script>
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
      if(prev&&spans[0])spans[0].innerHTML='<a href="/daily/'+prev.date+'">\u2190 '+prev.date+'</a>';
      if(next&&spans[2])spans[2].innerHTML='<a href="/daily/'+next.date+'">'+next.date+' \u2192</a>';
    }}).catch(function(){{}});
}})();
</script>
</body>
</html>'''

    return page


def update_archive_index(briefing, date_iso):
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
        teaser = briefing["lead"][:140] + ("..." if len(briefing.get("lead", "")) > 140 else "")
    meta = briefing.get("meta", {})

    entry = {
        "date": date_iso,
        "date_display": briefing.get("date", date_iso),
        "headline": headline,
        "teaser": teaser,
        "market_mood": meta.get("market_mood", ""),
        "surprise_count": meta.get("overnight_surprises_count", 0),
        "sections": len(briefing.get("sections", [])),
        "url": f"/daily/{date_iso}",
        "market_closed": briefing.get("market_closed", False),
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
    date_iso = datetime.now().strftime("%Y-%m-%d")
    ARCHIVE_JSON_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_HTML_DIR.mkdir(parents=True, exist_ok=True)

    json_path = ARCHIVE_JSON_DIR / f"{date_iso}.json"
    with open(json_path, "w") as f:
        json.dump(briefing, f, indent=2, ensure_ascii=False)
    print(f"  Archive JSON: {json_path}")

    html_content = generate_archive_html(briefing, date_iso)
    html_path = ARCHIVE_HTML_DIR / f"{date_iso}.html"
    with open(html_path, "w") as f:
        f.write(html_content)
    print(f"  Archive HTML: {html_path}")

    count = update_archive_index(briefing, date_iso)
    print(f"  Archive index: {count} briefings")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=== AGSIST Daily Briefing Generator v3.5 ===")
    print(f"  Time: {datetime.now().isoformat()}")

    market_status = get_market_status()
    if market_status["is_closed"]:
        print(f"  Markets CLOSED: {market_status['day_name']} ({market_status['reason']})")
    else:
        print(f"  Markets OPEN: {market_status['day_name']}")

    print("  Loading prices.json...")
    price_data, surprises = load_prices()

    if market_status["is_closed"]:
        surprises = []
        print("  Weekend/holiday: overnight surprise detection suppressed")
    elif surprises:
        print(f"  {len(surprises)} overnight surprise(s):")
        for s in surprises:
            print(f"    {s['commodity']}: {s['pct_change']:+.1f}% (magnitude {s['surprise_magnitude']}x)")
    else:
        print("  No overnight surprises: normal trading day.")

    print("  Loading past dailies for continuity...")
    past_dailies_block, past_tmyk_topics = load_past_dailies(num_days=3)
    if past_dailies_block:
        print(f"  Past daily context loaded ({len(past_tmyk_topics)} prior TMYK topics to avoid)")
    else:
        print("  No past dailies found")

    print("  Fetching ag news RSS...")
    news_block = fetch_ag_news()

    seasonal_ctx = get_seasonal_context()

    print("  Selecting today's quote from pool...")
    todays_quote = get_todays_quote()
    print(f"  Quote: \"{todays_quote['text'][:60]}...\" ({todays_quote['attribution']})")

    print("  Calling Claude API...")
    briefing = call_claude(
        price_data, surprises, news_block, seasonal_ctx,
        todays_quote, past_dailies_block, past_tmyk_topics, market_status
    )

    locked_prices = price_data.get("locked_prices", {})
    is_clean, val_warnings = validate_briefing(briefing, locked_prices)
    if val_warnings:
        print(f"  Validation warnings ({len(val_warnings)}):")
        for w in val_warnings:
            print(f"    - {w}")
    else:
        print("  Validation passed (no em dashes, no scope creep, no filler attributions)")

    briefing["generated_at"] = datetime.now(timezone.utc).isoformat()
    briefing["generator_version"] = "3.5"
    briefing["surprise_count"] = len(surprises)
    briefing["surprises"] = surprises
    briefing["price_validation_clean"] = is_clean
    briefing["market_closed"] = market_status["is_closed"]
    briefing["market_status_reason"] = market_status["reason"]
    if "meta" not in briefing:
        briefing["meta"] = {}
    briefing["meta"]["overnight_surprises_count"] = len(surprises)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(briefing, f, indent=2, ensure_ascii=False)
    print(f"  Written to {OUTPUT_PATH}")

    print("  Archiving briefing...")
    save_archive(briefing)

    print(f"  Headline: {briefing.get('headline', 'N/A')}")
    print(f"  Sections: {len(briefing.get('sections', []))}")
    print(f"  Market status: {'CLOSED (' + market_status['day_name'] + ')' if market_status['is_closed'] else 'open'}")
    print(f"  Surprises: {len(surprises)}")
    print(f"  Validation: {'clean' if is_clean else str(len(val_warnings)) + ' warning(s)'}")
    print("=== Done ===")


if __name__ == "__main__":
    main()
