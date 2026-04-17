#!/usr/bin/env python3
"""
post_pins.py — HappyPet Pinterest poster via IFTTT Maker webhooks
Reads staged _pin_queue/*.json files, fires one Maker webhook per board target,
then marks each slug as pinned in the Google Sheet (column F = 'YES').

Board routing:
  species=dog  (or both) -> happypet_pin_dogs
  species=cat  (or both) -> happypet_pin_cats
  topical=FOOD           -> happypet_pin_food
  topical=HEALTH         -> happypet_pin_health
  topical=HOME           -> happypet_pin_home
  topical=TOYS           -> happypet_pin_toys

Maker webhook payload:
  value1 = image_url (with ?v= cache-bust enforced)
  value2 = title
  value3 = source_url (article URL with UTM params)

Called by Stage 2 (publish.yml) after build confirmation.
Manual: python3 post_pins.py --slugs slug1,slug2
        python3 post_pins.py --dry-run
"""

import argparse
import datetime as _dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
LOG_PATH  = REPO_DIR / 'LOGS' / f"HappyPet_{_dt.date.today().isoformat()}.log"
LOG_PATH.parent.mkdir(exist_ok=True)

MAKER_URL = "https://maker.ifttt.com/trigger/{event}/with/key/{key}"

TOPICAL_EVENT = {
    'HAPPYPET_SHEET_ID_FOOD':   'happypet_pin_food',
    'HAPPYPET_SHEET_ID_HEALTH': 'happypet_pin_health',
    'HAPPYPET_SHEET_ID_HOME':   'happypet_pin_home',
    'HAPPYPET_SHEET_ID_TOYS':   'happypet_pin_toys',
}

MAX_RETRIES  = 3
BACKOFF_BASE = 15
RPM_SLEEP    = 2


def log(msg: str, level: str = 'INFO') -> None:
    line = f"{_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [POSTPINS] [{level}]  {msg}"
    print(line, flush=True)
    with LOG_PATH.open('a') as f:
        f.write(line + '\n')


def load_env() -> None:
    env_path = Path.home() / '.env'
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip())


def http_post(url: str, payload: bytes, headers: dict, *, label: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method='POST')
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors='replace')
            if exc.code == 429:
                wait = BACKOFF_BASE * (2 ** attempt)
                log(f"  {label} 429 -- wait {wait}s (attempt {attempt}/{MAX_RETRIES})", 'WARN')
                time.sleep(wait)
            else:
                raise RuntimeError(f"{label} HTTP {exc.code}: {body[:200]}")
        except urllib.error.URLError as exc:
            log(f"  {label} network error attempt {attempt}: {exc.reason}", 'WARN')
            time.sleep(RPM_SLEEP * 3)
    raise RuntimeError(f"{label} exhausted after {MAX_RETRIES} attempts")


def fire_webhook(event: str, value1: str, value2: str, value3: str, maker_key: str) -> bool:
    url     = MAKER_URL.format(event=event, key=maker_key)
    payload = json.dumps({'value1': value1, 'value2': value2, 'value3': value3}).encode()
    headers = {'Content-Type': 'application/json'}
    try:
        result = http_post(url, payload, headers, label=f"Maker:{event}")
        log(f"  FIRED {event} -- {result.strip()[:80]}")
        return True
    except Exception as exc:
        log(f"  FAIL {event} -- {exc}", 'ERROR')
        return False


def resolve_events(species: str, topical_sheet: str) -> list:
    events = []
    if species == 'dog':
        events.append('happypet_pin_dogs')
    elif species == 'cat':
        events.append('happypet_pin_cats')
    elif species == 'both':
        events.extend(['happypet_pin_dogs', 'happypet_pin_cats'])
    topical_event = TOPICAL_EVENT.get(topical_sheet)
    if topical_event:
        events.append(topical_event)
    return events


def ensure_cache_bust(image_url: str) -> str:
    if not image_url:
        return image_url
    if '?v=' not in image_url:
        v = _dt.date.today().strftime('%Y%m%d')
        image_url = f"{image_url}?v={v}"
        log(f"  WARN: image_url missing ?v= -- appended", 'WARN')
    return image_url


def mark_pinned_in_sheet(slug: str, gc, sheet_ids: dict) -> None:
    """Set column F = YES for rows matching this slug's article URL. Best-effort."""
    url_fragment = f"/{slug}/"
    for sheet_name, sheet_id in sheet_ids.items():
        if not sheet_id:
            continue
        try:
            ws   = gc.open_by_key(sheet_id).get_worksheet(0)
            rows = ws.get_all_values()
            for i, row in enumerate(rows[1:], start=2):
                if len(row) >= 2 and url_fragment in row[1]:
                    ws.update_cell(i, 6, 'YES')
                    log(f"  PINNED mark: {sheet_name} row {i}")
        except Exception as exc:
            log(f"  WARN: could not mark {sheet_name}: {exc}", 'WARN')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--slugs',   default='', help='Comma-separated slugs (empty = all queued)')
    parser.add_argument('--dry-run', action='store_true', help='Log targets without firing webhooks')
    args = parser.parse_args()

    load_env()

    maker_key = os.environ.get('IFTTT_MAKER_KEY', '').strip()
    if not maker_key:
        log('IFTTT_MAKER_KEY not set', 'ERROR')
        sys.exit(1)

    slug_filter = set(s.strip() for s in args.slugs.split(',') if s.strip()) if args.slugs else set()

    queue_dir = REPO_DIR / '_pin_queue'
    sent_dir  = queue_dir / 'sent'
    queue_dir.mkdir(exist_ok=True)
    sent_dir.mkdir(exist_ok=True)

    queue_files = sorted(queue_dir.glob('*.json'))
    if slug_filter:
        queue_files = [f for f in queue_files if any(s in f.stem for s in slug_filter)]

    if not queue_files:
        log('No queued pins found -- nothing to do')
        return

    log(f"START -- {len(queue_files)} pin(s) to process{' [DRY RUN]' if args.dry_run else ''}")

    # gspread for marking column F -- non-fatal if unavailable
    gc        = None
    sheet_ids = {}
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        key_file = REPO_DIR / 'happypet-sheets-key.json'
        if key_file.exists():
            scopes    = ['https://www.googleapis.com/auth/spreadsheets']
            creds     = Credentials.from_service_account_file(str(key_file), scopes=scopes)
            gc        = gspread.Client(auth=creds)
            sheet_ids = {k: v for k, v in os.environ.items() if k.startswith('HAPPYPET_SHEET_ID_')}
            log(f"  gspread ready -- will mark column F=YES after pinning")
        else:
            log('  happypet-sheets-key.json not found -- sheet marking skipped', 'WARN')
    except ImportError:
        log('  gspread not installed -- sheet marking skipped', 'WARN')

    processed = 0
    failed    = 0

    for qf in queue_files:
        try:
            if (sent_dir / qf.name).exists():
                log(f'SKIP (already sent): {qf.name}')
                continue

            data        = json.loads(qf.read_text())
            slug        = data.get('slug', qf.stem)
            title       = data.get('title', slug)
            article_url = data.get('article_url', '')
            image_url   = ensure_cache_bust(data.get('image_url', ''))
            species     = data.get('species', 'both')
            topical     = data.get('topical_sheet', '')

            events = resolve_events(species, topical)
            if not events:
                log(f'WARN: no events resolved for {slug} (species={species}, topical={topical})', 'WARN')
                failed += 1
                continue

            log(f'PIN [{slug}] -> {events}')
            log(f'  title: {title}')
            log(f'  image: {image_url[:80]}')
            log(f'  url:   {article_url[:80]}')

            if args.dry_run:
                log(f'  DRY RUN -- skipping webhook fire')
                continue

            pin_ok = True
            for event in events:
                ok = fire_webhook(event, image_url, title, article_url, maker_key)
                if not ok:
                    pin_ok = False
                time.sleep(RPM_SLEEP)

            if pin_ok:
                if gc:
                    mark_pinned_in_sheet(slug, gc, sheet_ids)
                processed += 1
            else:
                log(f'  PARTIAL/FULL failure for {slug} -- not moving to sent/', 'WARN')
                failed += 1
                continue

        except Exception as exc:
            log(f'FAIL: {qf.name} -- {exc}', 'ERROR')
            failed += 1

    log(f'DONE -- {processed} pinned, {failed} failed')


if __name__ == '__main__':
    main()
