#!/usr/bin/env python3
"""
fetch_outlooks.py v2 — pull NOAA outlook GIFs + USDM map locally so the homepage
isn't at the mercy of upstream browser-cache headers.

What changed in v2:
- 30-day NOAA outlook now considers BOTH off14 (mid-month) and off15 (end-of-
  month) and keeps whichever has the newer Last-Modified header. v1 used only
  off15, which only refreshes on the last day of each month — so for ~3 weeks
  out of every 4 we were serving the previous month's outlook.

NOAA cadence reminder:
  - off14 at /long_range/lead14/        — issued 3rd Thursday   (the "OFFICIAL")
  - off15 at /30day/                    — issued last day of month (the "Updated")
  - off01 at /long_range/lead01/        — issued 3rd Thursday   (3-month seasonal)

What this writes (always relative to repo root):
  data/outlooks/noaa_temp_30day.gif
  data/outlooks/noaa_prcp_30day.gif
  data/outlooks/noaa_temp_90day.gif
  data/outlooks/noaa_prcp_90day.gif
  data/outlooks/usdm_latest.png
  data/outlooks/manifest.json    (fetched_at, sources, sha256 per file,
                                  plus which 30-day variant was chosen)

Idempotent: if upstream bytes haven't changed, the file isn't rewritten.
"""

import email.utils
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR   = REPO_ROOT / 'data' / 'outlooks'
USER_AGENT = 'AGSIST-OutlookFetcher/2 (+https://agsist.com; sig@farmers1st.com)'
TIMEOUT_S = 30

NOAA_BASE = 'https://www.cpc.ncep.noaa.gov/products/predictions/'

# 30-day pair: try both, keep whichever has the newer Last-Modified.
NOAA_30DAY_VARIANTS = [
    ('lead14',
     NOAA_BASE + 'long_range/lead14/off14_temp.gif',
     NOAA_BASE + 'long_range/lead14/off14_prcp.gif'),
    ('lead15',
     NOAA_BASE + '30day/off15_temp.gif',
     NOAA_BASE + '30day/off15_prcp.gif'),
]

# 90-day pair: only one source. lead01 is the standard 3-month seasonal.
NOAA_90DAY = (
    NOAA_BASE + 'long_range/lead01/off01_temp.gif',
    NOAA_BASE + 'long_range/lead01/off01_prcp.gif',
)

GIF_MAGIC = (b'GIF87a', b'GIF89a')
PNG_MAGIC = b'\x89PNG\r\n\x1a\n'


def fetch_with_lm(url):
    """GET url, return (bytes, Last-Modified-as-datetime-or-None)."""
    req = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': '*/*'})
    with urlopen(req, timeout=TIMEOUT_S) as resp:
        if resp.status != 200:
            raise RuntimeError(f'HTTP {resp.status} for {url}')
        lm_header = resp.getheader('Last-Modified') or ''
        lm_dt = None
        if lm_header:
            try:
                lm_dt = email.utils.parsedate_to_datetime(lm_header)
                if lm_dt.tzinfo is None:
                    lm_dt = lm_dt.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                lm_dt = None
        return resp.read(), lm_dt


def looks_like(content, expected):
    if expected == 'gif':
        return any(content.startswith(m) for m in GIF_MAGIC)
    if expected == 'png':
        return content.startswith(PNG_MAGIC)
    return False


def write_if_changed(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == content:
        return False
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_bytes(content)
    tmp.replace(path)
    return True


def fetch_30day_pair():
    """
    Try every 30-day variant. For each variant, fetch both temp and prcp,
    record the temp file's Last-Modified, and keep the variant with the
    newest one. Variants whose temp file fetch fails or returns non-GIF
    bytes are skipped.

    Returns (best, attempts). best is a dict or None.
    """
    best = None
    attempts = []
    for label, temp_url, prcp_url in NOAA_30DAY_VARIANTS:
        try:
            t_bytes, t_lm = fetch_with_lm(temp_url)
        except (HTTPError, URLError, RuntimeError) as e:
            attempts.append((label, f'temp fetch failed: {e}'))
            continue
        if not looks_like(t_bytes, 'gif'):
            attempts.append((label, f'temp wrong content-type: {t_bytes[:8]!r}'))
            continue
        try:
            p_bytes, p_lm = fetch_with_lm(prcp_url)
        except (HTTPError, URLError, RuntimeError) as e:
            attempts.append((label, f'prcp fetch failed: {e}'))
            continue
        if not looks_like(p_bytes, 'gif'):
            attempts.append((label, f'prcp wrong content-type: {p_bytes[:8]!r}'))
            continue

        cand = dict(label=label, temp_url=temp_url, prcp_url=prcp_url,
                    temp_bytes=t_bytes, prcp_bytes=p_bytes,
                    temp_lm=t_lm, prcp_lm=p_lm)
        attempts.append((label, f'ok (temp Last-Modified: {t_lm})'))

        if best is None:
            best = cand
            continue
        # Prefer the one with a newer temp Last-Modified. If either side is
        # missing the header, the one that has a header wins; if neither has
        # one, the first successful variant sticks.
        if cand['temp_lm'] and (best['temp_lm'] is None or cand['temp_lm'] > best['temp_lm']):
            best = cand

    return best, attempts


def fetch_90day_pair():
    """Single source — just fetch both files."""
    temp_url, prcp_url = NOAA_90DAY
    out = {'temp_url': temp_url, 'prcp_url': prcp_url}
    try:
        out['temp_bytes'], out['temp_lm'] = fetch_with_lm(temp_url)
        if not looks_like(out['temp_bytes'], 'gif'):
            return None, f'temp wrong content-type: {out["temp_bytes"][:8]!r}'
    except (HTTPError, URLError, RuntimeError) as e:
        return None, f'temp fetch failed: {e}'
    try:
        out['prcp_bytes'], out['prcp_lm'] = fetch_with_lm(prcp_url)
        if not looks_like(out['prcp_bytes'], 'gif'):
            return None, f'prcp wrong content-type: {out["prcp_bytes"][:8]!r}'
    except (HTTPError, URLError, RuntimeError) as e:
        return None, f'prcp fetch failed: {e}'
    return out, 'ok'


def candidate_usdm_dates(now_utc):
    """
    USDM cuts data Tuesday 7am ET, releases map Thursday 8:30am ET. Treat
    "Thursday 14:30 UTC" as the publish boundary (covers EDT and EST).
    """
    weekday = now_utc.weekday()
    days_since_tue = (weekday - 1) % 7
    this_tue = (now_utc - timedelta(days=days_since_tue)).date()
    this_thu_publish = datetime.combine(
        this_tue + timedelta(days=2),
        datetime.min.time(),
        tzinfo=timezone.utc,
    ) + timedelta(hours=14, minutes=30)
    candidates = []
    if now_utc >= this_thu_publish:
        candidates.append(this_tue)
    for i in range(1, 5):
        candidates.append(this_tue - timedelta(days=7 * i))
    return candidates


def fetch_usdm():
    now_utc = datetime.now(timezone.utc)
    last_err = None
    for d in candidate_usdm_dates(now_utc):
        ymd = d.strftime('%Y%m%d')
        url = f'https://droughtmonitor.unl.edu/data/png/{ymd}/{ymd}_usdm.png'
        try:
            content, _lm = fetch_with_lm(url)
        except (HTTPError, URLError, RuntimeError) as e:
            last_err = f'{ymd}: {e}'
            continue
        if not looks_like(content, 'png'):
            last_err = f'{ymd}: wrong content-type'
            continue
        return d.isoformat(), url, content
    return None, last_err, None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        'fetched_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'files': {},
    }
    any_failure = False

    # ── 30-day (temp + prcp) ────────────────────────────────────────────
    best, attempts = fetch_30day_pair()
    if best is None:
        any_failure = True
        print('FAILED:    30-day pair — every variant failed:', file=sys.stderr)
        for lbl, msg in attempts:
            print(f'           {lbl}: {msg}', file=sys.stderr)
        manifest['files']['noaa_temp_30day.gif'] = {'status': 'failed', 'attempts': attempts}
        manifest['files']['noaa_prcp_30day.gif'] = {'status': 'failed', 'attempts': attempts}
    else:
        for which, fname in (('temp', 'noaa_temp_30day.gif'),
                             ('prcp', 'noaa_prcp_30day.gif')):
            content = best[f'{which}_bytes']
            url     = best[f'{which}_url']
            lm      = best[f'{which}_lm']
            path    = OUT_DIR / fname
            changed = write_if_changed(path, content)
            manifest['files'][fname] = {
                'source':    url,
                'variant':   best['label'],
                'last_modified': lm.isoformat() if lm else None,
                'bytes':     len(content),
                'sha256':    hashlib.sha256(content).hexdigest(),
                'changed':   changed,
                'status':    'ok',
            }
            print(f'{"updated" if changed else "unchanged"}: {fname} ({best["label"]}, {len(content):,} B)')

    # ── 90-day (temp + prcp) ────────────────────────────────────────────
    pair, err = fetch_90day_pair()
    for which, fname in (('temp', 'noaa_temp_90day.gif'),
                         ('prcp', 'noaa_prcp_90day.gif')):
        if pair is None:
            any_failure = True
            print(f'FAILED:    {fname} — {err}', file=sys.stderr)
            manifest['files'][fname] = {'status': f'failed: {err}'}
            continue
        content = pair[f'{which}_bytes']
        url     = pair[f'{which}_url']
        lm      = pair.get(f'{which}_lm')
        path    = OUT_DIR / fname
        changed = write_if_changed(path, content)
        manifest['files'][fname] = {
            'source':         url,
            'last_modified':  lm.isoformat() if lm else None,
            'bytes':          len(content),
            'sha256':         hashlib.sha256(content).hexdigest(),
            'changed':        changed,
            'status':         'ok',
        }
        print(f'{"updated" if changed else "unchanged"}: {fname} ({len(content):,} B)')

    # ── USDM ────────────────────────────────────────────────────────────
    valid_date, src_url, content = fetch_usdm()
    if content is not None:
        path = OUT_DIR / 'usdm_latest.png'
        changed = write_if_changed(path, content)
        manifest['files']['usdm_latest.png'] = {
            'source':     src_url,
            'data_valid': valid_date,
            'bytes':      len(content),
            'sha256':     hashlib.sha256(content).hexdigest(),
            'changed':    changed,
            'status':     'ok',
        }
        print(f'{"updated" if changed else "unchanged"}: usdm_latest.png (data valid {valid_date}, {len(content):,} B)')
    else:
        any_failure = True
        manifest['files']['usdm_latest.png'] = {'status': f'failed: {src_url}'}
        print(f'FAILED:    usdm_latest.png — {src_url}', file=sys.stderr)

    # ── Manifest write ──────────────────────────────────────────────────
    (OUT_DIR / 'manifest.json').write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )

    print()
    print(f'any-failure={"yes" if any_failure else "no"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
