#!/usr/bin/env python3
"""
fetch_export_sales.py — USDA FAS Weekly Export Sales fetcher
Writes data/export-sales.json with weekly net sales and cumulative pace
for corn, soybeans, and wheat.

Data source: USDA Foreign Agricultural Service (FAS) Export Sales Reporting (ESR)
https://apps.fas.usda.gov/export-sales/
Released every Thursday at 8:30 AM ET.

No API key required — uses public ESR download endpoint.
"""

import csv
import io
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

OUT_FILE = "data/export-sales.json"

# FAS ESR endpoint for weekly cumulative data by commodity
FAS_ESR_URL = "https://apps.fas.usda.gov/export-sales/esrd1.asp"

# FAS commodity codes for ESR system
COMMODITIES = {
    "corn":     {"code": "0801100", "name": "CORN",     "icon": "🌽", "unit_factor": 1},
    "soybeans": {"code": "2222000", "name": "SOYBEANS", "icon": "🫘", "unit_factor": 1},
    "wheat":    {"code": "1001900", "name": "WHEAT",    "icon": "🌾", "unit_factor": 1},
}

# USDA annual export targets (MT) — update these after each WASDE
# Marketing year 2025/26: corn Oct-Sep, beans Sep-Aug, wheat Jun-May
USDA_TARGETS_MT = {
    "corn":     57900000,   # 2025/26 WASDE target
    "soybeans": 52200000,   # 2025/26 WASDE target
    "wheat":    21800000,   # 2025/26 WASDE target
}

# Marketing year start months
MKT_YEAR_START = {
    "corn":     10,  # October
    "soybeans": 9,   # September
    "wheat":    6,   # June
}


def get_marketing_year(commodity: str) -> tuple[int, int]:
    """Return (start_year, end_year) for current marketing year."""
    now = datetime.now()
    start_month = MKT_YEAR_START[commodity]
    if now.month >= start_month:
        return now.year, now.year + 1
    else:
        return now.year - 1, now.year


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def fetch_esr_csv(commodity_code: str, start_date: str, end_date: str) -> list[dict] | None:
    """
    Fetch weekly export sales data from USDA FAS ESR.
    Returns list of weekly rows or None on failure.
    """
    # ESR type=2 returns CSV-style data for cumulative weekly by country
    # type=1 returns by destination, type=2 returns totals
    params = {
        "type": "2",
        "commodityCode": commodity_code,
        "startdate": start_date,
        "enddate": end_date,
        "regionCode": "",
        "countryCode": "",
        "marketYear": "0",
    }
    url = FAS_ESR_URL + "?" + urllib.parse.urlencode(params)
    print(f"  Fetching {url[:100]}…", flush=True)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 AGSIST/1.0",
            "Accept": "text/html,application/xhtml+xml"
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return parse_esr_html(raw)
    except Exception as e:
        print(f"  FAS ESR error: {e}", flush=True)
        return None


def parse_esr_html(html: str) -> list[dict]:
    """
    Parse FAS ESR HTML response to extract weekly sales data.
    The ESR page embeds data in a table — we extract weekly net sales.
    """
    rows = []
    # Look for table rows with numeric data
    # FAS ESR returns data in HTML table format
    import re
    # Extract table rows
    table_match = re.search(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
    if not table_match:
        print("  No table found in ESR response", flush=True)
        return []

    table_html = table_match.group(1)
    row_pattern = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
    cell_pattern = re.compile(r'<t[dh][^>]*>(.*?)</t[dh]>', re.DOTALL | re.IGNORECASE)
    tag_pattern  = re.compile(r'<[^>]+>')

    for row_match in row_pattern.finditer(table_html):
        cells = cell_pattern.findall(row_match.group(1))
        clean = [tag_pattern.sub('', c).strip().replace(',', '') for c in cells]
        if len(clean) >= 3 and any(c.lstrip('-').replace('.','').isdigit() for c in clean[1:]):
            rows.append(clean)

    return rows


def parse_date_from_row(s: str) -> str | None:
    """Try to parse a date string from FAS ESR."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def main():
    os.makedirs("data", exist_ok=True)

    now   = datetime.now()
    today = fmt_date(now)

    output = {
        "updated":        today,
        "report_date":    None,
        "marketing_year": None,
        "note":           "USDA FAS Weekly Export Sales — released Thursdays 8:30am ET",
        "corn":           None,
        "soybeans":       None,
        "wheat":          None,
    }

    any_data = False

    for key, info in COMMODITIES.items():
        print(f"\n── {info['name']} ──", flush=True)
        start_yr, end_yr = get_marketing_year(key)
        start_month = MKT_YEAR_START[key]

        # Build marketing year start/end dates
        mkt_start = datetime(start_yr, start_month, 1)
        mkt_end   = now

        start_str = mkt_start.strftime("%m/01/%Y")
        end_str   = now.strftime("%m/%d/%Y")

        rows = fetch_esr_csv(info["code"], start_str, end_str)

        if rows is None or len(rows) == 0:
            print(f"  No data for {key} — using placeholder", flush=True)
            output[key] = {
                "weekly_net_mt":   None,
                "cumulative_mt":   None,
                "usda_target_mt":  USDA_TARGETS_MT[key],
                "pct_of_target":   None,
                "report_date":     None,
                "marketing_year":  f"{start_yr}/{str(end_yr)[-2:]}",
            }
            continue

        # Try to extract cumulative and weekly net from parsed rows
        # FAS ESR rows typically: [date, weekly_net, cumulative, ...]
        cumulative = None
        weekly_net = None
        report_date = None

        for row in reversed(rows):  # last row = most recent week
            if len(row) < 2:
                continue
            date_str = parse_date_from_row(row[0])
            if not date_str:
                continue
            try:
                if len(row) >= 3:
                    weekly_net = int(float(row[1])) if row[1].lstrip('-').replace('.','').isdigit() else None
                    cumulative = int(float(row[2])) if row[2].lstrip('-').replace('.','').isdigit() else None
                elif len(row) >= 2:
                    cumulative = int(float(row[1])) if row[1].lstrip('-').replace('.','').isdigit() else None
                if cumulative is not None:
                    report_date = date_str
                    break
            except (ValueError, IndexError):
                continue

        target = USDA_TARGETS_MT[key]
        pct = round(cumulative / target * 100, 1) if cumulative and target else None

        output[key] = {
            "weekly_net_mt":   weekly_net,
            "cumulative_mt":   cumulative,
            "usda_target_mt":  target,
            "pct_of_target":   pct,
            "report_date":     report_date,
            "marketing_year":  f"{start_yr}/{str(end_yr)[-2:]}",
        }

        if cumulative:
            any_data = True
            if output["report_date"] is None:
                output["report_date"] = report_date

        mkt_yr_str = f"{start_yr}/{str(end_yr)[-2:]}"
        output["marketing_year"] = mkt_yr_str
        print(f"  Weekly net: {weekly_net:,} MT | Cumulative: {cumulative:,} MT | {pct}% of target", flush=True)

    if not any_data:
        print("\nWARNING: No export sales data retrieved.")
        print("The FAS ESR system may require browser-like access.")
        print("Consider manual update of data/export-sales.json.")
        # Still write the file with targets so the widget renders partially
        output["note"] += " · Data pending — update manually if needed"

    with open(OUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nWritten {OUT_FILE}")


if __name__ == "__main__":
    main()
