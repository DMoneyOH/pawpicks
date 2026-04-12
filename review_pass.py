#!/usr/bin/env python3
"""
Standalone review + affiliate-link repair pass for staged articles.
- Reads _posts/*-<slug>.md for each slug in products.json not yet reviewed
- Injects real affiliate link from products.json if placeholder text found
- Runs review_and_rewrite() -- rewrites if flagged, GitHub issue if both fail
- Saves in-place; all output logged to LOGS/HappyPet_YYYY-MM-DD.log via gp.log()
Run: python3 review_pass.py
"""
import sys, os, re, time, json
sys.path.insert(0, '/home/derek/Projects/HappyPet')
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path.home() / '.env')
api_key = os.environ.get('GROQ_API_KEY', '').strip()
assert api_key, 'GROQ_API_KEY not set -- check ~/.env'

import generate_posts as gp
gp.REVIEWER_ENABLED = True
gp.MAX_REVIEW_ATTEMPTS = 2
gp.REVIEW_PRE_SLEEP = 2   # matches generate_posts.py (Groq free tier, no long sleep needed)

REPO      = gp.REPO_DIR
POSTS_DIR = gp.POSTS_DIR

INTER_REVIEW_SLEEP = 0    # matches generate_posts.py (no inter-article sleep)


# Load products for affiliate URLs
with open(REPO / 'products.json') as f:
    products = {p['topic']: p for p in json.load(f)}

# Slugs = all products.json topics (generator skips already-published)
SLUGS = list(products.keys())

passed_list, rewritten_list, failed_list, missing_list = [], [], [], []


def inject_affiliate_link(content, product_name, affiliate_url):
    """Replace [Affiliate Link...] placeholder text with real amzn.to link."""
    pat = re.compile(r'\[Affiliate Link[^\]]*\](?:\([^\)]*\))?', re.IGNORECASE)
    real_link = f'[{product_name}]({affiliate_url})'
    fixed, count = pat.subn(real_link, content)
    return fixed, count


gp.log(f'START review_pass -- {len(SLUGS)} slugs to check')

for i, slug in enumerate(SLUGS, 1):
    matches = list(POSTS_DIR.glob(f'*-{slug}.md'))
    if not matches:
        gp.log(f'MISSING post file for {slug}', 'WARN')
        missing_list.append(slug)
        continue

    fpath = matches[0]
    raw = fpath.read_text(encoding='utf-8')

    if raw.startswith('---'):
        parts = raw.split('---', 2)
        fm = '---' + parts[1] + '---\n'
        content = parts[2].lstrip('\n')
    else:
        fm, content = '', raw

    title_m = re.search(r'^title:\s*"?(.+?)"?\s*$', fm, re.MULTILINE)
    kw_m    = re.search(r'^tags:\s*\[(.+?)\]', fm, re.MULTILINE)
    title   = title_m.group(1) if title_m else slug
    keyword = kw_m.group(1).strip().strip('"') if kw_m else slug

    gp.log(f'[{i}/{len(SLUGS)}] {slug}')

    product      = products.get(slug, {})
    affiliate_url = product.get('affiliate_url', '')
    product_name  = product.get('name', '')


    if affiliate_url and product_name:
        content, replacements = inject_affiliate_link(content, product_name, affiliate_url)
        if replacements:
            gp.log(f'  Replaced {replacements} placeholder link(s) -> {affiliate_url}')

    final, ok, flags = gp.review_and_rewrite(title, keyword, content, api_key)

    if ok and final == content:
        gp.log(f'  PASS (no changes)')
        fpath.write_text(fm + '\n' + content, encoding='utf-8')
        passed_list.append(slug)
    elif ok:
        gp.log(f'  PASS after rewrite -- saving')
        fpath.write_text(fm + '\n' + final, encoding='utf-8')
        rewritten_list.append(slug)
    else:
        gp.log(f'  FAIL after {gp.MAX_REVIEW_ATTEMPTS} attempts -- opening GitHub issue', 'WARN')
        gp.create_github_issue(title, slug, flags)
        failed_list.append(slug)

    if i < len(SLUGS) and INTER_REVIEW_SLEEP > 0:
        gp.log(f'  Sleeping {INTER_REVIEW_SLEEP}s before next...')
        time.sleep(INTER_REVIEW_SLEEP)

gp.log('=== REVIEW PASS COMPLETE ===')
gp.log(f'PASS (clean):     {passed_list}')
gp.log(f'PASS (rewritten): {rewritten_list}')
gp.log(f'FAIL (issue):     {failed_list}')
gp.log(f'MISSING:          {missing_list}')
