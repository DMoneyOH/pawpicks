#!/usr/bin/env python3
"""
fix_affiliate_links.py — One-time fix for articles 12-20.
For each untracked _posts/ file:
  1. Loads matching affiliate_url from products.json by slug
  2. Replaces ALL fake internal URLs with real amzn.to link:
     - /go/* patterns
     - /affiliate-link-* patterns
     - /hills-science-diet-* and other product-name slugs
     - chewy.com links (wrong platform)
  3. Adds affiliate disclosure if missing
  4. Replaces banned clichés deterministically
  5. Writes fixed file back in place
NO git, NO publish, NO sheet writes.

Usage: python3 fix_affiliate_links.py [--dry-run]
"""
import re, json, sys
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
POSTS_DIR = REPO_DIR / "_posts"
DRY_RUN   = "--dry-run" in sys.argv

DISCLOSURE = (
    "\n\n*This post contains affiliate links. "
    "If you purchase through our links, we may earn a small commission "
    "at no extra cost to you. We only recommend products we genuinely believe in.*\n"
)

CLICHE_REPLACEMENTS = {
    r"\ba game-changer\b":      "a great choice",
    r"\bGame-Changer\b":        "Great Choice",
    r"\bA Game-Changer\b":      "A Great Choice",
    r"\bgame-changers\b":       "great options",
    r"\bdelve\b":               "explore",
    r"\bdelves\b":              "explores",
    r"\bit's worth noting\b":   "keep in mind",
    r"\bin conclusion\b":       "to wrap up",
    r"\blook no further\b":     "you've found it",
    r"\bcomprehensive guide\b": "complete guide",
    r"\bnavigate\b":            "find your way through",
}

# Matches any fake internal or wrong-platform link: [anchor](bad_url)
# Catches: /go/*, /affiliate-link-*, /hills-*, /purina-*, /royal-*, /blue-buffalo-*
# Also catches chewy.com links (wrong platform)
FAKE_LINK_PAT = re.compile(
    r'\[([^\]]+)\]'
    r'\('
    r'(?:'
    r'https://happypetproductreviews\.com/(?:go|affiliate-link|hills|purina|royal|blue-buffalo|barker)[^\)]*'
    r'|https?://(?:www\.)?chewy\.com/[^\)]*'
    r')'
    r'\)',
    re.IGNORECASE
)

DISCLOSURE_PAT = re.compile(
    r'(affiliate|commission|earn|sponsored)', re.IGNORECASE
)
AMZN_PAT = re.compile(r'amzn\.to/')

def load_products() -> dict:
    p = REPO_DIR / "products.json"
    data = json.loads(p.read_text())
    if isinstance(data, list):
        return {e["topic"]: e for e in data if "topic" in e}
    return data


def get_untracked_posts() -> list:
    import subprocess
    result = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "_posts/"],
        cwd=REPO_DIR, capture_output=True, text=True
    )
    return sorted([REPO_DIR / f.strip() for f in result.stdout.splitlines() if f.strip()])


def slug_from_path(fpath: Path) -> str:
    parts = fpath.stem.split("-", 3)
    return parts[3] if len(parts) == 4 else fpath.stem


def fix_article(fpath: Path, affiliate_url: str, product_name: str = "") -> tuple:
    original = fpath.read_text(encoding="utf-8")
    fixed    = original
    changes  = []

    # 1. Replace ALL fake/wrong URLs with real affiliate link
    fake_matches = FAKE_LINK_PAT.findall(fixed)
    if fake_matches:
        def replace_fake(m):
            anchor = m.group(1)
            return f"[{anchor}]({affiliate_url})"
        new_fixed = FAKE_LINK_PAT.sub(replace_fake, fixed)
        if new_fixed != fixed:
            count = len(FAKE_LINK_PAT.findall(fixed))
            changes.append(f"Replaced {count} fake/wrong URL(s) → {affiliate_url}")
            fixed = new_fixed

    # 2. If affiliate link still missing, inject on first plain-text product name mention in body
    if not AMZN_PAT.search(fixed):
        injected = False
        if product_name:
            # Try to linkify first occurrence of product name in body (after front matter)
            fm_end = fixed.find("\n---\n", 3)
            body_start = fm_end + 4 if fm_end != -1 else 0
            body = fixed[body_start:]
            # Match product name not already inside a markdown link
            name_pat = re.compile(re.escape(product_name), re.IGNORECASE)
            def inject_link(m):
                return f"[{m.group(0)}]({affiliate_url})"
            new_body, n = name_pat.subn(inject_link, body, count=1)
            if n:
                fixed = fixed[:body_start] + new_body
                changes.append(f"Injected affiliate link on first mention of '{product_name}'")
                injected = True
        if not injected and not AMZN_PAT.search(fixed):
            changes.append(f"WARNING: affiliate link still missing — manual check needed")

    # 3. Add disclosure if missing
    if not DISCLOSURE_PAT.search(fixed):
        fixed = fixed.rstrip() + DISCLOSURE
        changes.append("Added affiliate disclosure")

    # 4. Fix banned clichés
    for pattern, replacement in CLICHE_REPLACEMENTS.items():
        new_fixed = re.sub(pattern, replacement, fixed, flags=re.IGNORECASE)
        if new_fixed != fixed:
            changes.append(f"Cliché fix: '{pattern}' → '{replacement}'")
            fixed = new_fixed

    return original, fixed, changes

def main():
    products = load_products()
    posts    = get_untracked_posts()

    if not posts:
        print("No untracked posts found — nothing to fix.")
        return

    mode = "DRY RUN" if DRY_RUN else "LIVE FIX"
    print(f"\n{'='*60}")
    print(f"  fix_affiliate_links.py — {mode}")
    print(f"  {len(posts)} untracked article(s) found")
    print(f"{'='*60}\n")

    total_fixed = 0

    for fpath in posts:
        slug    = slug_from_path(fpath)
        product = products.get(slug)

        print(f"── {fpath.name}")

        if not product:
            print(f"   ⚠ No products.json entry for '{slug}' — SKIPPING\n")
            continue

        affiliate_url = product.get("affiliate_url", "")
        if not affiliate_url:
            print(f"   ⚠ No affiliate_url for '{slug}' — SKIPPING\n")
            continue

        product_name  = product.get("name", "")
        original, fixed, changes = fix_article(fpath, affiliate_url, product_name)

        if not changes:
            print(f"   ✓ No changes needed\n")
            continue

        print(f"   Changes ({len(changes)}):")
        for c in changes:
            prefix = "   ⚠" if c.startswith("WARNING") else "     •"
            print(f"{prefix} {c}")

        if original == fixed:
            print(f"   (no actual content diff — no write needed)\n")
            continue

        if DRY_RUN:
            print(f"   [DRY RUN] Would write {len(fixed)} chars → {fpath.name}\n")
        else:
            fpath.write_text(fixed, encoding="utf-8")
            print(f"   ✓ Written: {fpath.name} ({len(fixed)} chars)\n")
            total_fixed += 1

    print(f"{'='*60}")
    if DRY_RUN:
        print(f"  DRY RUN complete — no files written")
        print(f"  Run without --dry-run to apply fixes")
    else:
        print(f"  DONE — {total_fixed}/{len(posts)} file(s) updated")
    print(f"{'='*60}\n")
    if not DRY_RUN:
        print("Next: run python3 review_only.py best-dog-crates to verify pre-screen passes.")


if __name__ == "__main__":
    main()
