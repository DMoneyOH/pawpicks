#!/usr/bin/env python3
"""
fix_frontmatter.py — Patch missing image: and affiliate_url: into post front matter.
Reads products.json, matches by topic slug, injects fields into _posts/*.md front matter.
Safe: only adds fields if missing. Never overwrites existing values.
Usage: python3 fix_frontmatter.py [--dry-run]
"""
import re, json, sys
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
POSTS_DIR = REPO_DIR / "_posts"
DRY_RUN   = "--dry-run" in sys.argv

def load_products():
    """Build slug→product map from products.json."""
    with open(REPO_DIR / "products.json") as f:
        products = json.load(f)
    return {p["topic"]: p for p in products if "topic" in p}

def get_slug(filename):
    """Strip date prefix: 2026-04-07-best-dog-crates.md → best-dog-crates"""
    return re.sub(r"^\d{4}-\d{2}-\d{2}-", "", filename.replace(".md", ""))

def patch_post(path, product, dry_run=False):
    """Inject image: and affiliate_url: into front matter if missing."""
    content = path.read_text()
    fm_match = re.match(r"^(---\n)(.*?)(---\n)", content, re.DOTALL)
    if not fm_match:
        print(f"  SKIP {path.name} — no front matter found")
        return False

    fm = fm_match.group(2)
    changed = False
    additions = ""

    if "image:" not in fm:
        img = product.get("image", "")
        if img and img != "NEEDS_IMAGE":
            additions += f'image: "{img}"\n'
            changed = True

    if "affiliate_url:" not in fm:
        url = product.get("affiliate_url", "")
        if url:
            additions += f'affiliate_url: "{url}"\n'
            changed = True

    if not changed:
        print(f"  OK   {path.name} — already complete")
        return False

    # Insert additions before closing ---
    new_fm = fm_match.group(1) + fm + additions + fm_match.group(3)
    new_content = new_fm + content[fm_match.end():]

    if dry_run:
        print(f"  DRY  {path.name} — would add: {additions.strip()}")
    else:
        path.write_text(new_content)
        print(f"  FIX  {path.name} — added: {additions.strip()}")
    return True

def main():
    products = load_products()
    posts    = sorted(POSTS_DIR.glob("*.md"))
    patched  = 0
    skipped  = 0

    print(f"{'DRY RUN — ' if DRY_RUN else ''}Scanning {len(posts)} posts...\n")

    for post in posts:
        slug = get_slug(post.name)
        if slug not in products:
            print(f"  SKIP {post.name} — no products.json entry (articles 1-10 hardcoded)")
            skipped += 1
            continue
        if patch_post(post, products[slug], dry_run=DRY_RUN):
            patched += 1

    print(f"\nDone. Patched: {patched} | Skipped: {skipped}")

if __name__ == "__main__":
    main()
