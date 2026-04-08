#!/usr/bin/env python3
"""
Standalone review pass for already-generated articles 11-20.
Reads each staged _posts/2026-04-07-*.md, runs review_and_rewrite(),
rewrites in-place if flagged, logs outcome to LOGS/HappyPet_YYYY-MM-DD.log.
Run: python3 review_pass.py >> LOGS/HappyPet_YYYY-MM-DD.log 2>&1
"""
import sys, os, re, time
sys.path.insert(0, '/home/derek/projects/pawpicks')
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path.home() / '.env')

api_key = os.environ.get('GEMINI_API_KEY', '').strip()
assert api_key, 'GEMINI_API_KEY not set'

import generate_posts as gp
gp.REVIEWER_ENABLED = True
gp.MAX_REVIEW_ATTEMPTS = 2
gp.REVIEW_PRE_SLEEP = 15

SLUGS = [
    'best-flea-prevention-dogs',
    'best-senior-dog-food',
    'best-wet-cat-food',
    'best-grain-free-cat-food',
    'best-dog-crates',
    'best-pet-cameras',
    'best-dog-dna-tests',
    'best-interactive-dog-toys',
    'best-interactive-cat-toys',
    'best-dog-grooming',
]

posts_dir = Path('/home/derek/projects/pawpicks/_posts')
passed_list, rewritten_list, failed_list, missing_list = [], [], [], []

INTER_REVIEW_SLEEP = 60  # seconds between articles to avoid 429

for i, slug in enumerate(SLUGS, 1):
    matches = list(posts_dir.glob('*-' + slug + '.md'))
    if not matches:
        print('MISSING: ' + slug)
        missing_list.append(slug)
        continue

    fpath = matches[0]
    raw = fpath.read_text(encoding='utf-8')

    # Split front matter from body
    if raw.startswith('---'):
        parts = raw.split('---', 2)
        fm = '---' + parts[1] + '---\n'
        content = parts[2].lstrip('\n')
    else:
        fm, content = '', raw

    # Extract title and keyword from front matter
    title_m = re.search(r'^title:\s*"?(.+?)"?\s*$', fm, re.MULTILINE)
    kw_m    = re.search(r'^tags:\s*\[(.+?)\]', fm, re.MULTILINE)
    title   = title_m.group(1) if title_m else slug
    keyword = kw_m.group(1).strip().strip('"') if kw_m else slug

    print('')
    print('[' + str(i) + '/10] REVIEWING: ' + slug)
    print('  title: ' + title)

    final, ok, flags = gp.review_and_rewrite(title, keyword, content, api_key)

    if ok and final == content:
        print('  RESULT: PASS (no rewrite needed)')
        passed_list.append(slug)
    elif ok:
        print('  RESULT: PASS after rewrite -- saving in-place')
        fpath.write_text(fm + '\n' + final, encoding='utf-8')
        rewritten_list.append(slug)
    else:
        print('  RESULT: FAIL -- opening GitHub issue')
        gp.create_github_issue(title, slug, flags)
        failed_list.append(slug)

    if i < len(SLUGS):
        print('  Waiting ' + str(INTER_REVIEW_SLEEP) + 's before next review...')
        time.sleep(INTER_REVIEW_SLEEP)

print('')
print('=== REVIEW PASS COMPLETE ===')
print('PASS (no change):  ' + str(passed_list))
print('PASS after rewrite: ' + str(rewritten_list))
print('FAIL (issue opened): ' + str(failed_list))
print('MISSING:           ' + str(missing_list))
