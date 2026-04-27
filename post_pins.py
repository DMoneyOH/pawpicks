#!/usr/bin/env python3
"""
post_pins.py - HappyPet Pinterest poster via IFTTT Maker webhooks
Sheet role: AUDIT TRAIL only. Rows appended by push_pins_to_sheets.py.
post_pins.py marks column F = YES after successful pin.

Board routing:
  species=cat/both -> happypet_pin_cats
  species=dog/both -> happypet_pin_dogs
  FOOD   -> happypet_pin_food
  HEALTH -> happypet_pin_health
  HOME   -> happypet_pin_home
  TOYS   -> happypet_pin_toys

value1=image_url  value2="title | pin_desc"  value3=source_url

Usage:
  python3 post_pins.py
  python3 post_pins.py --slugs a,b
  python3 post_pins.py --dry-run
"""

import argparse, datetime as _dt, json, os, sys, time
import urllib.error, urllib.parse, urllib.request
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
LOG_PATH  = REPO_DIR / "LOGS" / f"HappyPet_{_dt.date.today().isoformat()}.log"
LOG_PATH.parent.mkdir(exist_ok=True)

MAKER_URL = "https://maker.ifttt.com/trigger/{event}/with/key/{key}"

TOPICAL_EVENT = {
    "HAPPYPET_SHEET_ID_FOOD":   "happypet_pin_food",
    "HAPPYPET_SHEET_ID_HEALTH": "happypet_pin_health",
    "HAPPYPET_SHEET_ID_HOME":   "happypet_pin_home",
    "HAPPYPET_SHEET_ID_TOYS":   "happypet_pin_toys",
}

MAX_RETRIES  = 3
BACKOFF_BASE = 15
RPM_SLEEP    = 2


def log(msg, level="INFO"):
    line = f"{_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [POSTPINS] [{level}]  {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def load_env():
    env_path = Path.home() / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())


def http_post(url, payload, headers, *, label):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            if exc.code == 429:
                wait = BACKOFF_BASE * (2 ** attempt)
                log(f"  {label} 429 attempt {attempt}/{MAX_RETRIES} -- wait {wait}s", "WARN")
                time.sleep(wait)
            else:
                raise RuntimeError(f"{label} HTTP {exc.code}: {body[:200]}")
        except urllib.error.URLError as exc:
            log(f"  {label} network error attempt {attempt}: {exc.reason}", "WARN")
            time.sleep(RPM_SLEEP * 3)
    raise RuntimeError(f"{label} exhausted after {MAX_RETRIES} attempts")


def fire_webhook(event, value1, value2, value3, maker_key):
    url     = MAKER_URL.format(event=event, key=maker_key)
    payload = urllib.parse.urlencode({
        "value1": value1, "value2": value2, "value3": value3,
    }).encode()
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        result = http_post(url, payload, headers, label=f"Maker:{event}")
        log(f"  FIRED {event} -- {result.strip()[:80]}")
        return True
    except Exception as exc:
        log(f"  FAIL {event} -- {exc}", "ERROR")
        return False


def resolve_events(species, topical_sheet):
    events = []
    if species in ("dog", "both"):
        events.append("happypet_pin_dogs")
    if species in ("cat", "both"):
        events.append("happypet_pin_cats")
    topical = TOPICAL_EVENT.get(topical_sheet)
    if topical:
        events.append(topical)
    if not events:
        log(f"  WARN: could not resolve events for species='{species}' topical='{topical_sheet}' -- falling back to happypet_pin_dogs", "WARN")
        events.append("happypet_pin_dogs")
    return events


def ensure_cache_bust(image_url):
    if not image_url:
        return image_url
    if "?v=" not in image_url:
        v = _dt.date.today().strftime("%Y%m%d")
        image_url = f"{image_url}?v={v}"
        log("  WARN: image_url missing ?v= -- appended", "WARN")
    return image_url


def check_url_live(url: str, timeout: int = 8) -> bool:
    """Return True if URL responds 200. Skips check if url is empty."""
    if not url:
        return False
    try:
        import urllib.request
        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": "HappyPetBot/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception as e:
        log(f"  WARN: URL check failed for {url[:60]} -- {e}", "WARN")
        return False


def mark_pinned_in_sheet(slug, gc, sheet_ids):
    """Mark column F = YES for this slug. Audit trail only."""
    url_fragment = f"/{slug}/"
    for sheet_name, sheet_id in sheet_ids.items():
        if not sheet_id:
            continue
        try:
            ws   = gc.open_by_key(sheet_id).get_worksheet(0)
            rows = ws.get_all_values()
            for i, row in enumerate(rows[1:], start=2):
                if len(row) >= 2 and url_fragment in row[1]:
                    ws.update_cell(i, 6, "YES")
                    log(f"  AUDIT: marked {sheet_name} row {i} = YES")
        except Exception as exc:
            log(f"  WARN: could not mark {sheet_name}: {exc}", "WARN")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--slugs",   default="", help="Comma-separated slugs (empty = all queued)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()

    maker_key = os.environ.get("IFTTT_MAKER_KEY", "").strip()
    if not maker_key:
        log("IFTTT_MAKER_KEY not set", "ERROR")
        sys.exit(1)

    slug_filter = set(s.strip() for s in args.slugs.split(",") if s.strip()) if args.slugs else set()

    queue_dir = REPO_DIR / "_pin_queue"
    sent_dir  = queue_dir / "sent"
    queue_dir.mkdir(exist_ok=True)
    sent_dir.mkdir(exist_ok=True)

    queue_files = sorted(queue_dir.glob("*.json"))
    if slug_filter:
        queue_files = [f for f in queue_files if any(s in f.stem for s in slug_filter)]

    if not queue_files:
        log("No queued pins found -- nothing to do")
        return

    log(f"START -- {len(queue_files)} pin(s){' [DRY RUN]' if args.dry_run else ''}")

    gc = None
    sheet_ids = {}
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        key_file = REPO_DIR / "happypet-sheets-key.json"
        if key_file.exists():
            creds     = Credentials.from_service_account_file(str(key_file),
                            scopes=["https://www.googleapis.com/auth/spreadsheets"])
            gc        = gspread.Client(auth=creds)
            sheet_ids = {k: v for k, v in os.environ.items() if k.startswith("HAPPYPET_SHEET_ID_")}
            log("  gspread ready -- audit trail marking active")
        else:
            log("  happypet-sheets-key.json not found -- audit marking skipped", "WARN")
    except ImportError:
        log("  gspread not installed -- audit marking skipped", "WARN")

    processed = 0
    failed    = 0

    for qf in queue_files:
        try:
            if (sent_dir / qf.name).exists():
                log(f"SKIP (already sent): {qf.name}")
                continue

            data        = json.loads(qf.read_text())
            slug        = data.get("slug", qf.stem)
            title       = data.get("title", slug)
            pin_desc    = data.get("description", title)
            article_url = data.get("article_url", "")
            image_url   = ensure_cache_bust(data.get("image_url", ""))
            species     = data.get("species", "both")
            topical     = data.get("topical_sheet", "")

            value2 = title[:100]  # Pinterest maxLength=100; title only avoids duplication and truncation errors

            events = resolve_events(species, topical)
            if not events:
                log(f"WARN: no events for {slug} (species={species} topical={topical})", "WARN")
                failed += 1
                continue

            log(f"PIN [{slug}] -> {events}")
            log(f"  image: {image_url[:80]}")
            log(f"  url:   {article_url[:80]}")
            log(f"  v2:    {value2[:80]}")

            if args.dry_run:
                log("  DRY RUN -- skipping")
                processed += 1
                continue

            if not check_url_live(article_url):
                log(f"  SKIP: article not live yet ({article_url[:60]})", "WARN")
                failed += 1
                continue

            pin_ok = True
            for event in events:
                ok = fire_webhook(event, image_url, value2, article_url, maker_key)
                if not ok:
                    pin_ok = False
                time.sleep(RPM_SLEEP)

            if pin_ok:
                if gc:
                    mark_pinned_in_sheet(slug, gc, sheet_ids)
                processed += 1
            else:
                log(f"  PARTIAL/full failure for {slug}", "WARN")
                failed += 1

        except Exception as exc:
            log(f"FAIL: {qf.name} -- {exc}", "ERROR")
            failed += 1

    log(f"DONE -- {processed} pinned, {failed} failed")


if __name__ == "__main__":
    main()
