#!/usr/bin/env python3
"""
fetch_bids.py — Barchart OnDemand getGrainBids fetcher for AGSIST
Runs via GitHub Actions every 30 min during market hours.
Fetches cash grain bids for a national grid of ZIP codes,
deduplicates, and writes /data/bids.json for the homepage preview card.

The full cash-bids.html page calls Barchart directly (client-side)
for any ZIP — this file only powers the homepage preview widget.

Environment:
  BARCHART_API_KEY — OnDemand API key (GitHub Secret)
"""

import json
import os
import sys
import time
import math
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

API_KEY = os.environ.get("BARCHART_API_KEY", "")
BASE_URL = "https://ondemand.websol.barchart.com/getGrainBids.json"
MAX_DISTANCE = 60  # miles from each ZIP
OUTPUT_PATH = "data/bids.json"

# ── National grid of ZIP codes ───────────────────────────────────
# ~50 ZIPs across all major US agricultural regions
# Each with 60mi radius gives good national overlap
ZIP_GRID = [
    # ── Upper Midwest ──
    {"zip": "53705", "lat": 43.07, "lng": -89.40, "label": "Madison, WI"},
    {"zip": "54703", "lat": 44.81, "lng": -91.50, "label": "Eau Claire, WI"},
    {"zip": "54481", "lat": 44.52, "lng": -89.57, "label": "Stevens Point, WI"},
    {"zip": "55101", "lat": 44.94, "lng": -93.10, "label": "St Paul, MN"},
    {"zip": "56001", "lat": 44.16, "lng": -93.99, "label": "Mankato, MN"},
    {"zip": "56560", "lat": 46.87, "lng": -96.77, "label": "Moorhead, MN"},
    {"zip": "55901", "lat": 44.02, "lng": -92.47, "label": "Rochester, MN"},
    # ── Corn Belt ──
    {"zip": "50010", "lat": 42.03, "lng": -93.62, "label": "Ames, IA"},
    {"zip": "52001", "lat": 42.50, "lng": -90.66, "label": "Dubuque, IA"},
    {"zip": "51501", "lat": 41.26, "lng": -95.86, "label": "Council Bluffs, IA"},
    {"zip": "50613", "lat": 42.47, "lng": -92.33, "label": "Cedar Falls, IA"},
    {"zip": "61701", "lat": 40.48, "lng": -88.99, "label": "Bloomington, IL"},
    {"zip": "61820", "lat": 40.12, "lng": -88.24, "label": "Champaign, IL"},
    {"zip": "62702", "lat": 39.80, "lng": -89.65, "label": "Springfield, IL"},
    {"zip": "47901", "lat": 40.42, "lng": -86.89, "label": "Lafayette, IN"},
    {"zip": "46077", "lat": 39.96, "lng": -86.16, "label": "Zionsville, IN"},
    {"zip": "43215", "lat": 39.96, "lng": -83.00, "label": "Columbus, OH"},
    {"zip": "45840", "lat": 40.99, "lng": -83.65, "label": "Findlay, OH"},
    {"zip": "48823", "lat": 42.74, "lng": -84.48, "label": "East Lansing, MI"},
    # ── Dakotas ──
    {"zip": "57101", "lat": 43.55, "lng": -96.73, "label": "Sioux Falls, SD"},
    {"zip": "57401", "lat": 45.46, "lng": -98.49, "label": "Aberdeen, SD"},
    {"zip": "58102", "lat": 46.88, "lng": -96.79, "label": "Fargo, ND"},
    {"zip": "58501", "lat": 46.81, "lng": -100.78, "label": "Bismarck, ND"},
    {"zip": "58701", "lat": 48.23, "lng": -101.30, "label": "Minot, ND"},
    # ── Plains ──
    {"zip": "68508", "lat": 40.81, "lng": -96.68, "label": "Lincoln, NE"},
    {"zip": "69101", "lat": 41.13, "lng": -100.76, "label": "North Platte, NE"},
    {"zip": "67002", "lat": 37.69, "lng": -97.33, "label": "Wichita, KS"},
    {"zip": "67501", "lat": 38.05, "lng": -97.93, "label": "Hutchinson, KS"},
    {"zip": "66502", "lat": 39.18, "lng": -96.57, "label": "Manhattan, KS"},
    {"zip": "65201", "lat": 38.95, "lng": -92.33, "label": "Columbia, MO"},
    {"zip": "64801", "lat": 37.08, "lng": -94.51, "label": "Joplin, MO"},
    # ── Southern / Delta ──
    {"zip": "73071", "lat": 35.22, "lng": -97.44, "label": "Norman, OK"},
    {"zip": "79101", "lat": 35.20, "lng": -101.83, "label": "Amarillo, TX"},
    {"zip": "38655", "lat": 34.37, "lng": -89.52, "label": "Oxford, MS"},
    {"zip": "72201", "lat": 34.75, "lng": -92.29, "label": "Little Rock, AR"},
    {"zip": "38301", "lat": 35.61, "lng": -88.81, "label": "Jackson, TN"},
    {"zip": "31201", "lat": 32.84, "lng": -83.63, "label": "Macon, GA"},
    {"zip": "36104", "lat": 32.38, "lng": -86.30, "label": "Montgomery, AL"},
    {"zip": "70503", "lat": 30.22, "lng": -92.02, "label": "Lafayette, LA"},
    # ── Mountain / West ──
    {"zip": "59715", "lat": 45.68, "lng": -111.04, "label": "Bozeman, MT"},
    {"zip": "59401", "lat": 47.51, "lng": -111.30, "label": "Great Falls, MT"},
    {"zip": "82001", "lat": 41.14, "lng": -104.82, "label": "Cheyenne, WY"},
    {"zip": "80525", "lat": 40.55, "lng": -105.07, "label": "Fort Collins, CO"},
    {"zip": "83301", "lat": 42.56, "lng": -114.46, "label": "Twin Falls, ID"},
    # ── Pacific Northwest ──
    {"zip": "99163", "lat": 46.73, "lng": -117.18, "label": "Pullman, WA"},
    {"zip": "99301", "lat": 46.24, "lng": -119.22, "label": "Pasco, WA"},
    {"zip": "97301", "lat": 44.94, "lng": -123.03, "label": "Salem, OR"},
    # ── Southeast / Mid-Atlantic ──
    {"zip": "27601", "lat": 35.78, "lng": -78.64, "label": "Raleigh, NC"},
    {"zip": "23219", "lat": 37.54, "lng": -77.44, "label": "Richmond, VA"},
    {"zip": "19901", "lat": 39.16, "lng": -75.52, "label": "Dover, DE"},
]


def fetch_bids_for_zip(zip_code, max_distance=MAX_DISTANCE):
    """Fetch grain bids for a single ZIP code."""
    params = urlencode({
        "apikey": API_KEY,
        "zipCode": zip_code,
        "maxDistance": max_distance,
    })
    url = f"{BASE_URL}?{params}"
    try:
        req = Request(url, headers={"User-Agent": "AGSIST/1.0"})
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data
    except (URLError, HTTPError, json.JSONDecodeError) as e:
        print(f"  ⚠ Error fetching ZIP {zip_code}: {e}", file=sys.stderr)
        return None


def normalize_bid(bid, source_zip):
    """Normalize a single bid record from the Barchart response."""
    location = bid.get("location", {})
    if isinstance(location, str):
        location = {}
    return {
        "id": bid.get("id", ""),
        "facility": location.get("name", bid.get("facility", bid.get("locationName", "Unknown"))),
        "city": location.get("city", bid.get("city", "")),
        "state": location.get("state", bid.get("state", "")),
        "zip": location.get("zip", bid.get("zip", "")),
        "lat": _float(location.get("lat", bid.get("latitude"))),
        "lng": _float(location.get("lng", bid.get("longitude"))),
        "phone": location.get("phone", bid.get("phone", "")),
        "distance": _float(bid.get("distance", location.get("distance"))),
        "commodity": bid.get("commodity", bid.get("commodityName", "")),
        "symbol": bid.get("symbol", bid.get("basisSymbol", "")),
        "cashPrice": _float(bid.get("cashPrice")),
        "basis": _float(bid.get("basis")),
        "notes": bid.get("notes", ""),
        "deliveryStart": bid.get("delivery_start", bid.get("deliveryStart", "")),
        "deliveryEnd": bid.get("delivery_end", bid.get("deliveryEnd", "")),
        "deliveryMonth": bid.get("deliveryMonth", ""),
        "sourceZip": source_zip,
    }


def _float(val):
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None


def deduplicate(bids):
    """Deduplicate by facility + commodity + delivery. Keep closest."""
    seen = {}
    for b in bids:
        key = f"{b['facility']}|{b['commodity']}|{b['deliveryStart']}|{b['deliveryEnd']}|{b['deliveryMonth']}"
        if key not in seen or (b["distance"] or 999) < (seen[key]["distance"] or 999):
            seen[key] = b
    return list(seen.values())


def classify_commodity(name):
    n = (name or "").lower()
    if "corn" in n:
        return "corn"
    if "soy" in n or "bean" in n:
        return "soybeans"
    if "wheat" in n or "hrw" in n or "srw" in n or "hrsw" in n:
        return "wheat"
    if "oat" in n:
        return "oats"
    if "sorghum" in n or "milo" in n:
        return "sorghum"
    return "other"


def main():
    if not API_KEY:
        print("ERROR: BARCHART_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    print(f"[fetch_bids] Starting — {len(ZIP_GRID)} ZIP codes, "
          f"max {MAX_DISTANCE}mi radius")

    all_bids = []
    errors = 0

    for entry in ZIP_GRID:
        z = entry["zip"]
        print(f"  📍 {entry['label']} ({z})…", end=" ")
        data = fetch_bids_for_zip(z)

        if data is None:
            errors += 1
            print("FAIL")
            continue

        results = (data.get("results", []) or
                   data.get("bids", []) or
                   data.get("data", []) or [])

        if not results:
            print("0 bids")
            continue

        for bid in results:
            normalized = normalize_bid(bid, z)
            if normalized["cashPrice"] is not None or normalized["basis"] is not None:
                normalized["category"] = classify_commodity(normalized["commodity"])
                all_bids.append(normalized)

        print(f"{len(results)} bids")
        time.sleep(0.3)

    before = len(all_bids)
    all_bids = deduplicate(all_bids)
    print(f"\n[fetch_bids] {before} raw → {len(all_bids)} after dedup")
    print(f"[fetch_bids] Errors: {errors}/{len(ZIP_GRID)} ZIPs")

    all_bids.sort(key=lambda b: (b["state"] or "", b["city"] or "", b["commodity"] or ""))

    zip_index = [{"zip": e["zip"], "lat": e["lat"], "lng": e["lng"], "label": e["label"]} for e in ZIP_GRID]

    commodities = {}
    states = {}
    facilities = set()
    for b in all_bids:
        cat = b.get("category", "other")
        commodities[cat] = commodities.get(cat, 0) + 1
        st = b.get("state", "??")
        states[st] = states.get(st, 0) + 1
        facilities.add(b.get("facility", ""))

    output = {
        "fetched": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Barchart OnDemand getGrainBids",
        "zip_grid": zip_index,
        "stats": {
            "total_bids": len(all_bids),
            "facilities": len(facilities),
            "states": len(states),
            "by_commodity": commodities,
            "by_state": dict(sorted(states.items())),
        },
        "bids": all_bids,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"[fetch_bids] Wrote {OUTPUT_PATH} ({size_kb:.1f} KB)")
    print(f"[fetch_bids] {len(all_bids)} bids, {len(facilities)} facilities, {len(states)} states")
    print(f"[fetch_bids] Commodities: {commodities}")


if __name__ == "__main__":
    main()
