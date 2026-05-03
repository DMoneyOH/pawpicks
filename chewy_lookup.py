#!/usr/bin/env python3
"""
chewy_lookup.py — Chewy product matcher via Impact.com Catalog API
Happy Pet Product Reviews | feature/chewy-integration

Usage:
    python3 chewy_lookup.py "Blue Buffalo Life Protection Adult Chicken"

Output (JSON to stdout):
    {
        "chewy_url":    "https://chewy.sjv.io/..." | "REVIEW:{url}" | "REVIEW" | null,
        "chewy_price":  "22.98" | null,
        "chewy_stock":  "InStock" | null,
        "chewy_rating": 4.6 | null
    }

chewy_url sentinel values:
    Full URL          score >= 4, auto-accepted, rating scraped
    "REVIEW:{url}"    score 2-3, low confidence — human verification required
    "REVIEW"          score < 2 or no match — product not found on Chewy
    null              API credentials missing or hard error

Env vars required:
    IMPACT_ACCOUNT_SID
    IMPACT_AUTH_TOKEN

Env vars optional:
    CHEWY_CATALOG_ID   (default: 24727)
    CHEWY_CAMPAIGN_ID  (default: 32975)
"""

import os
import sys
import json
import re
import time
import base64
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ACCOUNT_SID       = os.environ.get("IMPACT_ACCOUNT_SID", "")
AUTH_TOKEN        = os.environ.get("IMPACT_AUTH_TOKEN", "")
CATALOG_ID        = os.environ.get("CHEWY_CATALOG_ID", "24727")
CAMPAIGN_ID       = os.environ.get("CHEWY_CAMPAIGN_ID", "32975")
BASE_URL          = f"https://api.impact.com/Mediapartners/{ACCOUNT_SID}"

SCORE_AUTO_ACCEPT = 4    # >= this: accept, scrape rating
SCORE_REVIEW      = 2    # >= this but < AUTO_ACCEPT: flag REVIEW:{url}
                         # <  SCORE_REVIEW: REVIEW (not found)

RATING_MAX_RETRY  = 3
RATING_RETRY_WAIT = 4    # seconds between 429 retries

# Stripped when building fallback keyword variants
STOP_WORDS = {
    "the", "a", "an", "and", "or", "for", "with", "in", "of", "to", "by",
    "recipe", "formula", "grain", "free",
}

# Species/life-stage terms kept OUT of stop words -- they are critical disambiguators.
# Stripping "kitten" causes puppy products to score equally, producing false positives.
SPECIES_TERMS = {"dog", "dogs", "cat", "cats", "puppy", "kitten", "adult", "senior"}

# Full category taxonomy — consumables get Chewy as primary link
CONSUMABLE_CATEGORIES = {
    "dog-food", "dog-health", "dog-treats",
    "cat-food", "cat-health", "cat-treats",
    "cat-litter",
}

# Hard goods — Amazon primary, Chewy secondary button only
HARD_GOOD_CATEGORIES = {
    "dog-gear", "dog-beds", "dog-crates", "dog-collars", "dog-harnesses",
    "dog-grooming", "dog-training", "dog-toys",
    "cat-gear", "cat-toys", "cat-carriers", "cat-scratching", "cat-feeders",
    "pet-tech", "pet-feeding",
}


# ---------------------------------------------------------------------------
# Impact.com API
# ---------------------------------------------------------------------------

def _impact_get(path: str, params: dict = None) -> dict:
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    creds = base64.b64encode(f"{ACCOUNT_SID}:{AUTH_TOKEN}".encode()).decode()
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Authorization": f"Basic {creds}",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"[chewy_lookup] HTTP {e.code} on {path}: {e.read().decode()}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"[chewy_lookup] Request error on {path}: {e}", file=sys.stderr)
        return {}


def search_catalog(keyword: str, page_size: int = 10) -> list:
    data = _impact_get("/Catalogs/ItemSearch", {"Keyword": keyword, "PageSize": page_size})
    return data.get("Items", [])


# ---------------------------------------------------------------------------
# Keyword variant strategy
# ---------------------------------------------------------------------------

def _keyword_variants(product_name: str) -> list[str]:
    """
    Four-tier search strategy (broadening on each step):
      1. Full product name
      2. Stop-word-stripped meaningful terms
      3. Brand + first 3 meaningful descriptors
      4. Brand name only (first word — widest net, last resort)
    """
    words = product_name.split()
    meaningful = [w for w in words if w.lower() not in STOP_WORDS]
    variants = [product_name]
    if meaningful != words:
        variants.append(" ".join(meaningful))
    if len(meaningful) > 3:
        variants.append(" ".join(meaningful[:3]))
    # Brand-only fallback — first word of product name
    brand_only = words[0] if words else ""
    if brand_only and brand_only not in variants:
        variants.append(brand_only)
    return list(dict.fromkeys(variants))


# ---------------------------------------------------------------------------
# Matching + scoring
# ---------------------------------------------------------------------------

def _score_item(item: dict, kw_meaningful: set, brand_word: str) -> float:
    """
    Score a catalog item against the original product keyword.
    - Word overlap (stop-word filtered) on both sides
    - +0.5 tiebreaker if brand word appears in item name
    - Manufacturer exact-brand check: +1.0 if Manufacturer starts with brand
    """
    name = item.get("Name", "").lower()
    name_words = set(name.split()) - STOP_WORDS
    overlap = len(kw_meaningful & name_words)
    brand_bonus = 0.5 if brand_word and brand_word in name else 0

    # Manufacturer field match — strong signal
    manufacturer = item.get("Manufacturer", "").lower()
    mfr_bonus = 1.0 if brand_word and manufacturer.startswith(brand_word) else 0

    return overlap + brand_bonus + mfr_bonus


def _filter_candidates(items: list) -> list:
    """Remove virtual bundles. Prefer InStock; fall back to all if none InStock."""
    clean = [
        i for i in items
        if "Virtual Bundle" not in i.get("Labels", [])
        and i.get("SubCategory", "") != "Virtual Bundle"
    ]
    in_stock = [i for i in clean if i.get("StockAvailability") == "InStock"]
    return in_stock if in_stock else clean


def best_match(items: list, product_name: str) -> tuple[dict | None, int]:
    """
    Returns (best_item, score). Score is int; fractional bonuses used only for sorting.
    Returns (None, 0) if no candidates after filtering.
    """
    candidates = _filter_candidates(items)
    if not candidates:
        return None, 0

    kw_meaningful = set(product_name.lower().split()) - STOP_WORDS
    brand_word = product_name.lower().split()[0] if product_name else ""

    scored = sorted(
        candidates,
        key=lambda i: _score_item(i, kw_meaningful, brand_word),
        reverse=True
    )
    top = scored[0]
    top_score = int(_score_item(top, kw_meaningful, brand_word))
    return top, top_score


def find_best_match(product_name: str) -> tuple[dict | None, int]:
    """
    Try keyword variants in order. Return (best_item, score) across all attempts.
    Stops early if score >= SCORE_AUTO_ACCEPT.
    """
    variants = _keyword_variants(product_name)
    best = None
    best_score = 0

    for kw in variants:
        print(f"[chewy_lookup] Trying: {kw!r}", file=sys.stderr)
        items = search_catalog(kw, page_size=10)
        if not items:
            continue
        match, score = best_match(items, product_name)
        if match and score > best_score:
            best, best_score = match, score
            print(f"[chewy_lookup] Match score={score}: {match.get('Name','')[:70]}", file=sys.stderr)
            if best_score >= SCORE_AUTO_ACCEPT:
                break

    return best, best_score


# ---------------------------------------------------------------------------
# Rating scraper
# ---------------------------------------------------------------------------

def _extract_direct_url(chewy_affiliate_url: str) -> str | None:
    if "chewy.sjv.io" not in chewy_affiliate_url:
        return chewy_affiliate_url
    parsed = urllib.parse.urlparse(chewy_affiliate_url)
    qs = urllib.parse.parse_qs(parsed.query)
    u_param = qs.get("u", [None])[0]
    if not u_param:
        return None
    decoded = urllib.parse.unquote(u_param)
    parts = urllib.parse.urlparse(decoded)
    safe_path  = urllib.parse.quote(parts.path, safe="/-_.")
    safe_query = urllib.parse.quote(parts.query, safe="=&%+")
    return urllib.parse.urlunparse((
        parts.scheme, parts.netloc, safe_path,
        parts.params, safe_query, parts.fragment
    ))


def scrape_chewy_rating(chewy_product_url: str) -> float | None:
    """Direct HTTP scrape of Chewy product page for star rating. Retries on 429."""
    direct_url = _extract_direct_url(chewy_product_url)
    if not direct_url or "chewy.com" not in direct_url:
        print("[chewy_lookup] Cannot extract direct URL for rating scrape", file=sys.stderr)
        return None

    print(f"[chewy_lookup] Rating scrape: {direct_url[:80]}", file=sys.stderr)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    html = None
    for attempt in range(1, RATING_MAX_RETRY + 1):
        try:
            req = urllib.request.Request(direct_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < RATING_MAX_RETRY:
                print(f"[chewy_lookup] 429 — retry {attempt}/{RATING_MAX_RETRY} in {RATING_RETRY_WAIT}s", file=sys.stderr)
                time.sleep(RATING_RETRY_WAIT)
            else:
                print(f"[chewy_lookup] Rating scrape HTTP {e.code}", file=sys.stderr)
                return None
        except Exception as e:
            print(f"[chewy_lookup] Rating scrape error: {e}", file=sys.stderr)
            return None

    if not html:
        return None

    # Strategy 1: JSON-LD aggregateRating
    for block in re.findall(r'"aggregateRating"\s*:\s*\{[^}]+\}', html):
        m = re.search(r'"ratingValue"\s*:\s*"?([\d.]+)"?', block)
        if m:
            try: return round(float(m.group(1)), 1)
            except ValueError: pass

    # Strategy 2: itemprop meta tag
    m = re.search(r'itemprop="ratingValue"[^>]*content="([\d.]+)"', html)
    if m:
        try: return round(float(m.group(1)), 1)
        except ValueError: pass

    # Strategy 3: data-score attribute
    m = re.search(r'data-score="([\d.]+)"', html)
    if m:
        try: return round(float(m.group(1)), 1)
        except ValueError: pass

    # Strategy 4: any ratingValue in scripts
    m = re.search(r'"ratingValue"\s*:\s*"?([\d.]+)"?', html)
    if m:
        try: return round(float(m.group(1)), 1)
        except ValueError: pass

    print("[chewy_lookup] Rating not found in page", file=sys.stderr)
    return None


# ---------------------------------------------------------------------------
# Main lookup
# ---------------------------------------------------------------------------

def lookup(product_name: str) -> dict:
    """
    Full lookup. chewy_url sentinel logic:
      >= SCORE_AUTO_ACCEPT  → full URL, rating scraped
      >= SCORE_REVIEW       → "REVIEW:{url}" — low confidence, human verify
      < SCORE_REVIEW        → "REVIEW" — not found on Chewy
      credentials missing   → all None
    """
    result = {
        "chewy_url":    None,
        "chewy_price":  None,
        "chewy_stock":  None,
        "chewy_rating": None,
    }

    if not ACCOUNT_SID or not AUTH_TOKEN:
        print("[chewy_lookup] IMPACT_ACCOUNT_SID or IMPACT_AUTH_TOKEN not set", file=sys.stderr)
        return result

    match, score = find_best_match(product_name)

    if not match or score < SCORE_REVIEW:
        print(f"[chewy_lookup] No match — setting REVIEW sentinel", file=sys.stderr)
        result["chewy_url"] = "REVIEW"
        return result

    raw_url    = match.get("Url") or None
    price      = match.get("CurrentPrice") or None
    stock      = match.get("StockAvailability") or None

    if score >= SCORE_AUTO_ACCEPT:
        print(f"[chewy_lookup] Auto-accepted (score={score})", file=sys.stderr)
        result["chewy_url"]   = raw_url
        result["chewy_price"] = price
        result["chewy_stock"] = stock
        if raw_url:
            time.sleep(1)
            result["chewy_rating"] = scrape_chewy_rating(raw_url)
    else:
        # SCORE_REVIEW <= score < SCORE_AUTO_ACCEPT — flag for human review
        print(f"[chewy_lookup] Low confidence (score={score}) — flagging REVIEW", file=sys.stderr)
        result["chewy_url"]   = f"REVIEW:{raw_url}" if raw_url else "REVIEW"
        result["chewy_price"] = price
        result["chewy_stock"] = stock
        # No rating scrape for unverified matches

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def is_consumable(category: str) -> bool:
    return category in CONSUMABLE_CATEGORIES


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 chewy_lookup.py \"Product Name\"", file=sys.stderr)
        sys.exit(1)
    product_name = " ".join(sys.argv[1:])
    result = lookup(product_name)
    print(json.dumps(result, indent=2))
