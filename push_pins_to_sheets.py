#!/usr/bin/env python3
"""
push_pins_to_sheets.py
Reads staged _pin_queue/*.json files, appends rows to the correct Google Sheets,
then moves processed files to _pin_queue/sent/.
After each slug completes, retires it from products.json (rolling queue model).
When unpublished products.json count drops to 3, logs warning + sends alert email.
Run ONLY after GitHub Pages build is confirmed live (called by autopublish.sh).
"""

import argparse
import json
import os
import sys
import shutil
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from pathlib import Path

REPO_DIR          = Path(__file__).parent
QUEUE_LOW_THRESHOLD = 3
ALERT_FROM        = 'hello@happypetproductreviews.com'
ALERT_TO          = 'hello@happypetproductreviews.com'
SMTP_HOST         = 'smtp.gmail.com'
SMTP_PORT         = 587


def log(msg: str, level: str = 'INFO') -> None:
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [PUBLISHER] [{level}]  {msg}"
    print(line, flush=True)


def load_env():
    env_path = Path.home() / '.env'
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip())


def retire_from_products(slug: str) -> int:
    """Remove slug from products.json. Returns remaining entry count."""
    p = REPO_DIR / 'products.json'
    if not p.exists():
        return 0
    products = json.loads(p.read_text())
    before = len(products)
    products = [e for e in products if e.get('topic') != slug]
    if len(products) < before:
        p.write_text(json.dumps(products, indent=2))
        log(f'  RETIRED: {slug} removed from products.json ({before} -> {len(products)} entries)')
    return len(products)


def count_unpublished() -> int:
    """Count products.json entries whose slug is NOT yet in _posts/."""
    p = REPO_DIR / 'products.json'
    if not p.exists():
        return 0
    products = json.loads(p.read_text())
    published = set()
    for md in (REPO_DIR / '_posts').glob('*.md'):
        parts = md.stem.split('-', 3)
        if len(parts) == 4:
            published.add(parts[3])
    return sum(1 for e in products if e.get('topic') not in published)


def send_queue_alert(unpublished_count: int) -> None:
    """Send email alert when unpublished queue hits threshold."""
    smtp_user  = os.environ.get('GMAIL_SMTP_USER', ALERT_FROM)
    smtp_login = os.environ.get('GMAIL_ACCOUNT', smtp_user)
    smtp_pass  = os.environ.get('GMAIL_APP_PASSWORD', '')
    if not smtp_pass:
        log('GMAIL_APP_PASSWORD not set -- skipping email alert', 'WARN')
        return
    subject = f'[HappyPet] Queue low: only {unpublished_count} unpublished articles remaining'
    body = (
        f'Happy Pet Product Reviews queue alert\n\n'
        f'Only {unpublished_count} unpublished article(s) remain in products.json.\n\n'
        f'Action needed: Add new topic entries to products.json before the queue runs dry.\n\n'
        f'Current unpublished topics:\n'
    )
    try:
        p = REPO_DIR / 'products.json'
        if p.exists():
            products = json.loads(p.read_text())
            published = set()
            for md in (REPO_DIR / '_posts').glob('*.md'):
                parts = md.stem.split('-', 3)
                if len(parts) == 4:
                    published.add(parts[3])
            for e in products:
                if e.get('topic') not in published:
                    body += f"  - {e.get('topic')} ({e.get('title', '')})\n"
    except Exception:
        pass
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From']    = smtp_user
        msg['To']      = ALERT_TO
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(smtp_login, smtp_pass)
            s.sendmail(smtp_user, [ALERT_TO], msg.as_string())
        log(f'ALERT EMAIL sent to {ALERT_TO}')
    except Exception as e:
        log(f'ALERT EMAIL failed: {e}', 'ERROR')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--slugs', default='', help='Comma-separated slugs to process (empty = all queued)')
    args = parser.parse_args()
    slug_filter = set(s.strip() for s in args.slugs.split(',') if s.strip())

    load_env()

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log('gspread not installed. Run: pip install gspread google-auth --break-system-packages', 'ERROR')
        sys.exit(1)

    key_file = REPO_DIR / 'happypet-sheets-key.json'
    if not key_file.exists():
        log('happypet-sheets-key.json not found', 'ERROR')
        sys.exit(1)

    queue_dir = REPO_DIR / '_pin_queue'
    sent_dir  = queue_dir / 'sent'
    queue_dir.mkdir(exist_ok=True)
    sent_dir.mkdir(exist_ok=True)

    queue_files = sorted(queue_dir.glob('*.json'))
    if slug_filter:
        queue_files = [f for f in queue_files if f.stem in slug_filter]
        log(f'Slug filter active -- processing {len(queue_files)} file(s): {slug_filter}')
    if not queue_files:
        log('No queued pins found -- nothing to do')
        return

    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds  = Credentials.from_service_account_file(str(key_file), scopes=scopes)
    gc     = gspread.authorize(creds)

    dog_id = os.environ.get('HAPPYPET_SHEET_ID_DOGS', '')
    cat_id = os.environ.get('HAPPYPET_SHEET_ID_CATS', '')
    topical_ids = {
        k: v for k, v in os.environ.items()
        if k.startswith('HAPPYPET_SHEET_ID_')
        and k not in ('HAPPYPET_SHEET_ID_DOGS', 'HAPPYPET_SHEET_ID_CATS')
    }

    slug_topical_map = {}
    map_path = REPO_DIR / 'slug_topical_map.json'
    if map_path.exists():
        slug_topical_map = json.loads(map_path.read_text())

    today      = datetime.now().strftime('%Y-%m-%d')
    processed  = 0
    failed     = 0
    alert_sent = False


    for qf in queue_files:
        try:
            if (sent_dir / qf.name).exists():
                log(f'SKIP (already sent): {qf.name}')
                continue

            data        = json.loads(qf.read_text())
            title       = data['title']
            article_url = data['article_url']
            description = data.get('description', '')
            image_url   = data['image_url']
            species     = data.get('species', 'both')
            slug        = data.get('slug', '')

            row = [title, article_url, description, image_url, today, 'NO']

            targets = []
            if species in ('dog', 'both') and dog_id:
                targets.append(('Dogs', dog_id))
            if species in ('cat', 'both') and cat_id:
                targets.append(('Cats', cat_id))

            topical_key = data.get('topical_sheet') or slug_topical_map.get(slug)
            if topical_key:
                topical_id = topical_ids.get(topical_key)
                if topical_id:
                    targets.append((topical_key, topical_id))

            for label, sheet_id in targets:
                sh = gc.open_by_key(sheet_id)
                sh.get_worksheet(0).append_row(row)
                log(f'SHEET: appended to {label} -- {title}')

            shutil.move(str(qf), str(sent_dir / qf.name))
            log(f'SENT: {qf.name} -> _pin_queue/sent/')

            retire_from_products(slug)

            if not alert_sent:
                unpub = count_unpublished()
                if unpub <= QUEUE_LOW_THRESHOLD:
                    log(f'QUEUE LOW: {unpub} unpublished article(s) remain -- add new topics!', 'WARN')
                    send_queue_alert(unpub)
                    alert_sent = True

            processed += 1

        except Exception as e:
            log(f'FAIL: {qf.name} -- {e}', 'ERROR')
            failed += 1

    log(f'DONE -- {processed} pinned, {failed} failed')


if __name__ == '__main__':
    main()
