#!/usr/bin/env python3
"""
fetch_crop_progress.py — USDA NASS Crop Progress weekly fetcher
Writes data/crop-progress.json with G/E ratings and planting pace for corn and soybeans.

Data source: USDA NASS QuickStats API (free key — see README)
Get your free key at: https://quickstats.nass.usda.gov/api/
Store as GitHub secret: NASS_API_KEY

Runs Mondays 4:30 PM CT via GitHub Actions (after 4:00 PM ET NASS release).
Off-season (Dec–Mar) still runs but writes in_season: false.
"""

import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

OUT_FILE = "data/crop-progress.json"
BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
API_KEY  = os.environ.get("NASS_API_KEY", "")


def nass_get(params: dict) -> list[dict]:
    params["key"] = API_KEY
    params["format"] = "JSON"
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    print(f"  GET {url[:120]}…", flush=True)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AGSIST/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        rows = data.get("data", [])
        print(f"    → {len(rows)} rows", flush=True)
        return rows
    except Exception as e:
        print(f"  NASS API error: {e}", flush=True)
        return []


def fetch_condition(commodity: str, year: int) -> list[dict]:
    return nass_get({
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "commodity_desc": commodity,
        "statisticcat_desc": "CONDITION",
        "agg_level_desc": "NATIONAL",
        "freq_desc": "WEEKLY",
        "year": str(year),
    })


def fetch_progress(commodity: str, year: int, unit: str = "PCT PLANTED") -> list[dict]:
    return nass_get({
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "commodity_desc": commodity,
        "statisticcat_desc": "PROGRESS",
        "unit_desc": unit,
        "agg_level_desc": "NATIONAL",
        "freq_desc": "WEEKLY",
        "year": str(year),
    })


def latest_ge(rows: list[dict]) -> dict | None:
    """
    Group rows by week_ending, sum GOOD + EXCELLENT for latest available week.
    Returns {date, good_excellent, week_ending_str} or None.
    """
    by_week: dict[str, dict] = {}
    for r in rows:
        week = r.get("week_ending", "")
        unit = r.get("unit_desc", "").upper()
        try:
            val = int(str(r.get("Value", "")).replace(",", "").strip())
        except ValueError:
            continue
        if not week:
            continue
        if week not in by_week:
            by_week[week] = {}
        if "EXCELLENT" in unit:
            by_week[week]["excellent"] = val
        elif "GOOD" in unit and "EXCELLENT" not in unit:
            by_week[week]["good"] = val

    if not by_week:
        return None

    latest_date = max(by_week.keys())
    w = by_week[latest_date]
    ge = (w.get("good", 0) or 0) + (w.get("excellent", 0) or 0)
    return {"date": latest_date, "good_excellent": ge}


def latest_planting(rows: list[dict]) -> dict | None:
    """Return most recent PCT PLANTED value."""
    valid = []
    for r in rows:
        week = r.get("week_ending", "")
        try:
            val = int(str(r.get("Value", "")).replace(",", "").strip())
        except ValueError:
            continue
        if week:
            valid.append((week, val))
    if not valid:
        return None
    valid.sort(key=lambda x: x[0], reverse=True)
    return {"date": valid[0][0], "pct": valid[0][1]}


def is_in_season() -> bool:
    """Crop Progress runs April through November."""
    m = datetime.now().month
    return 4 <= m <= 11


def main():
    os.makedirs("data", exist_ok=True)

    if not API_KEY:
        print("WARNING: NASS_API_KEY not set. Get a free key at https://quickstats.nass.usda.gov/api/")
        print("Writing off-season placeholder.")
        out = {
            "updated": datetime.now().strftime("%Y-%m-%d"),
            "report_date": None,
            "in_season": False,
            "error": "NASS_API_KEY not configured",
            "corn": None,
            "soybeans": None,
        }
        with open(OUT_FILE, "w") as f:
            json.dump(out, f, indent=2)
        print(f"Written {OUT_FILE}")
        sys.exit(0)

    year  = datetime.now().year
    year1 = year - 1  # prior year for comparison

    print(f"\nFetching crop progress for {year} (prior year {year1})…", flush=True)
    in_season = is_in_season()

    result: dict = {
        "updated":     datetime.now().strftime("%Y-%m-%d"),
        "report_date": None,
        "in_season":   in_season,
        "corn":        None,
        "soybeans":    None,
    }

    for commodity, key in [("CORN", "corn"), ("SOYBEANS", "soybeans")]:
        print(f"\n── {commodity} ──", flush=True)

        # Current year G/E
        cond_cur  = fetch_condition(commodity, year)
        cond_prev = fetch_condition(commodity, year1)

        cur  = latest_ge(cond_cur)
        prev = latest_ge(cond_prev)

        # Planting progress
        plant_cur  = fetch_progress(commodity, year)
        plant_prev = fetch_progress(commodity, year1)

        lp_cur  = latest_planting(plant_cur)
        lp_prev = latest_planting(plant_prev)

        # Prior week G/E (second-most-recent week in current year)
        ge_prev_week = None
        if cond_cur:
            by_week: dict[str, int] = {}
            for r in cond_cur:
                week = r.get("week_ending", "")
                unit = r.get("unit_desc", "").upper()
                try:
                    val = int(str(r.get("Value", "")).replace(",", "").strip())
                except ValueError:
                    continue
                if not week:
                    continue
                if week not in by_week:
                    by_week[week] = 0
                if "EXCELLENT" in unit or ("GOOD" in unit and "EXCELLENT" not in unit):
                    by_week[week] += val
            sorted_weeks = sorted(by_week.keys(), reverse=True)
            if len(sorted_weeks) >= 2:
                ge_prev_week = by_week[sorted_weeks[1]]

        result[key] = {
            "good_excellent":           cur["good_excellent"] if cur else None,
            "report_date":              cur["date"] if cur else None,
            "good_excellent_prev_week": ge_prev_week,
            "good_excellent_prev_year": prev["good_excellent"] if prev else None,
            "planting_pct":             lp_cur["pct"] if lp_cur else None,
            "planting_prev_year":       lp_prev["pct"] if lp_prev else None,
        }

        # Set overall report date from corn
        if key == "corn" and cur:
            result["report_date"] = cur["date"]

        print(f"  G/E: {result[key]['good_excellent']}% | prev wk: {result[key]['good_excellent_prev_week']}% | prev yr: {result[key]['good_excellent_prev_year']}%", flush=True)
        print(f"  Planting: {result[key]['planting_pct']}% | prev yr: {result[key]['planting_prev_year']}%", flush=True)

    if not result["corn"] and not result["soybeans"]:
        print("\nNo data returned — writing off-season placeholder.")
        result["in_season"] = False

    with open(OUT_FILE, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nWritten {OUT_FILE}")


if __name__ == "__main__":
    main()
