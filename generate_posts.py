#!/usr/bin/env python3
"""
Happy Pet Product Reviews Generator v13 — Real Product Pipeline
- Loads products.json for real affiliate links per topic
- Flexible article format: single_review, roundup, buying_guide
- Gemini-2.5-flash for content generation
- 15 min between articles, per-article git push
"""
import os, re, json, datetime, time, urllib.request, urllib.error, urllib.parse, subprocess
from pathlib import Path
try:
    import gspread
    from google.oauth2.service_account import Credentials as GCredentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

try:
    from generate_pin_images import make_pin_for_post
    PIN_GEN_AVAILABLE = True
except ImportError:
    PIN_GEN_AVAILABLE = False

REPO_DIR  = Path(__file__).parent.resolve()
POSTS_DIR = REPO_DIR / "_posts"
LOG_PATH  = Path("/tmp/pawpicks_gen.log")
LOCK_PATH = Path("/tmp/pawpicks_gen.lock")

MODEL       = "gemini-2.5-flash"
GEMINI_URL  = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
INTER_DELAY = 300
RPM_SLEEP   = 8
MAX_RETRIES = 3

# Topic definition: slug, title, keyword, article format
TOPICS = [
    ("best-dog-collars-small-breeds",    "Best Dog Collars for Small Breeds",          "dog collars small breeds",     "roundup"),
    ("best-cat-scratching-posts",        "Best Cat Scratching Posts That Last",         "cat scratching post",          "single_review"),
    ("best-no-pull-dog-harness",         "PetSafe Easy Walk Harness Review",            "no pull dog harness XL",       "single_review"),
    ("best-automatic-cat-feeder",        "PETLIBRO Automatic Cat Feeder Review",        "automatic cat feeder",         "single_review"),
    ("best-dog-toys-aggressive-chewers", "Best Dog Toys for Aggressive Chewers",        "dog toys aggressive chewers",  "roundup"),
    ("best-cat-litter-odor-control",     "Best Cat Litter for Odor Control",            "cat litter odor control",      "roundup"),
    ("best-dog-beds-large-breeds",       "Barker Beds Orthopedic Dog Bed Review",       "dog beds large breeds",        "single_review"),
    ("best-pet-water-fountain",          "PetSafe Drinkwell Platinum Fountain Review",  "pet water fountain",           "single_review"),
    ("best-puppy-training-pads",         "How to Choose Puppy Training Pads",           "puppy training pads",          "buying_guide"),
    ("best-cat-carrier-travel",          "How to Choose the Best Cat Carrier",          "cat carrier travel",           "buying_guide"),
]

INTERNAL_LINKS = {
    "best-dog-collars-small-breeds":    ("/pawpicks/pet-accessories/best-no-pull-dog-harness/", "no-pull harnesses for small dogs"),
    "best-cat-scratching-posts":        ("/pawpicks/pet-accessories/best-cat-litter-odor-control/", "cat litter for odor control"),
    "best-no-pull-dog-harness":         ("/pawpicks/pet-accessories/best-dog-collars-small-breeds/", "dog collars for small breeds"),
    "best-automatic-cat-feeder":        ("/pawpicks/pet-accessories/best-pet-water-fountain/", "pet water fountains"),
    "best-dog-toys-aggressive-chewers": ("/pawpicks/pet-accessories/best-dog-beds-large-breeds/", "dog beds for large breeds"),
    "best-cat-litter-odor-control":     ("/pawpicks/pet-accessories/best-cat-scratching-posts/", "cat scratching posts"),
    "best-dog-beds-large-breeds":       ("/pawpicks/pet-accessories/best-dog-toys-aggressive-chewers/", "toys for aggressive chewers"),
    "best-pet-water-fountain":          ("/pawpicks/pet-accessories/best-automatic-cat-feeder/", "automatic cat feeders"),
    "best-puppy-training-pads":         ("/pawpicks/pet-accessories/best-no-pull-dog-harness/", "no-pull dog harnesses"),
    "best-cat-carrier-travel":          ("/pawpicks/pet-accessories/best-automatic-cat-feeder/", "automatic cat feeders for travel"),
}

def log(msg: str) -> None:
    line = f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")

def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]", "", s.lower().replace(" ", "-"))

def load_products() -> dict:
    p = REPO_DIR / "products.json"
    if p.exists():
        with p.open() as f:
            return json.load(f)
    return {}

def extract_asin_from_url(url: str) -> str:
    """Extract ASIN from a full Amazon URL. Returns '' if not found."""
    m = re.search(r'/(?:dp|gp/product)/([A-Z0-9]{10})', url)
    return m.group(1) if m else ''

def resolve_short_url(short_url: str) -> str:
    """Follow amzn.to redirect to get the full Amazon URL. Returns '' on failure."""
    try:
        req = urllib.request.Request(short_url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.geturl()
    except Exception:
        return ''

def build_used_asins() -> set:
    """Scan all existing _posts/ files for affiliate_url ASINs already published."""
    used = set()
    asin_re = re.compile(r'affiliate_url:\s*["\']?(https?://[^\s"\']+)["\']?')
    for md in POSTS_DIR.glob("*.md"):
        try:
            text = md.read_text(encoding="utf-8")
            m = asin_re.search(text)
            if m:
                asin = extract_asin_from_url(m.group(1))
                if asin:
                    used.add(asin)
        except Exception:
            pass
    return used

def get_asin_for_product(product: dict) -> str:
    """Get ASIN from product dict — use stored asin if present, else resolve short URL."""
    if product.get('asin'):
        return product['asin']
    url = product.get('url', '')
    if 'amzn.to' in url:
        full = resolve_short_url(url)
        asin = extract_asin_from_url(full)
        return asin
    return extract_asin_from_url(url)

def append_to_sheet(title, article_url, description, image_url, species):
    """Append new article row to the correct Pinterest Queue Google Sheet."""
    if not GSHEETS_AVAILABLE:
        log("  WARN: gspread not installed, skipping sheet update")
        return
    key_file = REPO_DIR / 'happypet-sheets-key.json'
    if not key_file.exists():
        log("  WARN: happypet-sheets-key.json not found, skipping sheet update")
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(Path.home() / '.env')
        dog_id = os.getenv('HAPPYPET_SHEET_ID_DOGS')
        cat_id = os.getenv('HAPPYPET_SHEET_ID_CATS')
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds  = GCredentials.from_service_account_file(str(key_file), scopes=scopes)
        gc     = gspread.authorize(creds)
        today  = datetime.date.today().isoformat()
        row    = [title, article_url, description, image_url, today, 'NO']
        targets = []
        if species in ('dog', 'both') and dog_id:
            targets.append(('Dogs', dog_id))
        if species in ('cat', 'both') and cat_id:
            targets.append(('Cats', cat_id))
        for label, sid in targets:
            sh = gc.open_by_key(sid)
            sh.get_worksheet(0).append_row(row)
            log(f"  SHEET: appended to {label} Pinterest Queue")
    except Exception as e:
        log(f"  WARN: sheet append failed: {e}")

def front_matter(title: str, keyword: str, affiliate_url: str = "") -> str:
    today = datetime.date.today().isoformat()
    kw = keyword.lower()
    if any(w in kw for w in ['cat', 'kitten', 'feline', 'litter', 'scratch']):
        species = 'cat'
    elif any(w in kw for w in ['dog', 'puppy', 'canine', 'harness', 'collar', 'chew']):
        species = 'dog'
    else:
        species = 'both'
    fm = (
        f'---\nlayout: post\ntitle: "{title}"\ndate: {today}\n'
        f'categories: [pet-accessories]\nspecies: {species}\ntags: [{keyword}]\n'
        f'description: "{title} - expert reviews and buying guide."\n'
    )
    if affiliate_url:
        fm += f'affiliate_url: "{affiliate_url}"\n'
    fm += '---\n'
    return fm

def make_prompt(title: str, keyword: str, slug: str, fmt: str, product: dict) -> str:
    link = ""
    if slug in INTERNAL_LINKS:
        url, anchor = INTERNAL_LINKS[slug]
        link = f'\nNaturally include this markdown link once where relevant: [{anchor}]({url})'

    product_name = product.get("name", "")
    affiliate_url = product.get("url", "")

    affiliate_block = ""
    if product_name and affiliate_url:
        affiliate_block = (
            f'FEATURED PRODUCT: {product_name}\n'
            f'AFFILIATE LINK: {affiliate_url}\n'
            f'LINKING RULE: Every time {product_name} is mentioned by name in the article '
            f'-- including in comparison table cells -- render it as '
            f'[{product_name}]({affiliate_url}). '
            f'No plain-text mentions of the product name are allowed. '
            f'Every reference must be a clickable affiliate link.\n'
        )

    if fmt == "single_review":
        structure = f"""ARTICLE FORMAT: In-depth single product review of {product_name}

STRUCTURE (all sections required):
- Opening (100+ words): Relatable scenario where this product solves a real pet owner problem
- Product Overview (H2): What it is, who it's for, key specs
- What We Like (H2): 4-5 specific praised features with real-world context
- What Could Be Better (H2): 2-3 honest drawbacks -- be specific, not vague
- Real Owner Experiences (H2): Summarize common owner sentiment naturally
- Who Should Buy This (H2): Specific use cases and pet/owner types it suits best
- Verdict (H2, 80+ words): Clear recommendation, include the affiliate link naturally here
- Star rating line: "**Our Rating: X/5**" based on honest assessment"""

    elif fmt == "roundup":
        structure = f"""ARTICLE FORMAT: Roundup/comparison article -- {title}

STRUCTURE (all sections required):
- Opening (100+ words): Hook with a relatable pet owner problem this category solves
- Quick Picks (H2): 3-4 sentence summary of top recommendations
- Featured Pick -- {product_name} (H3): 80-100 word review, pros/cons bullets, include affiliate link
- 2-3 Additional Picks (H3 each): Use real well-known brands (Kong, PetSafe, Frisco, etc.)
  Each gets: 60-80 words, 3 pros, 2 cons
- Comparison Table (H2): Product | Best For | Price Range | Our Rating
- Buying Guide (H2, 150+ words): 4-5 practical selection tips
- Closing (80+ words): Clear recommendation with affiliate link"""

    else:  # buying_guide
        structure = f"""ARTICLE FORMAT: Buying guide -- {title}

STRUCTURE (all sections required):
- Opening (100+ words): Why choosing the right {keyword} matters
- What to Look For (H2): 5-6 key factors with detailed explanation each
- Our Top Pick -- {product_name} (H2): 100 word review, include affiliate link
- Common Mistakes to Avoid (H2): 3-4 specific pitfalls new pet owners make
- FAQ (H2): 4-5 real questions with concise answers
- Closing (80+ words): Actionable next steps, include affiliate link"""

    return f"""You are a senior writer for Happy Pet Product Reviews, a trusted budget-focused pet product review blog.

Write a complete, publish-ready blog post. Title: "{title}". Focus keyword: "{keyword}".

{affiliate_block}
LENGTH: 950-1100 words of body content. This is a firm requirement.

{structure}

WRITING STYLE:
- Conversational, warm, authoritative -- like advice from a trusted friend who owns pets
- Vary sentence length. Short punchy sentences mixed with longer flowing ones.
- NO AI cliches: never use "delve", "it's worth noting", "in conclusion", "look no further", "game-changer", "comprehensive guide", "navigate"
- Pet facts must be accurate -- breeds, behavior, materials, safety
- Use "{keyword}" naturally 4-6 times
- Write in first person plural ("we tested", "we found"){link}

FORMAT: Return ONLY clean Markdown. No YAML. No preamble. Start writing immediately.

The VERY FIRST LINE of your response must be exactly this format (one line, no label):
PIN_DESC: [one punchy sentence, max 20 words, that makes a Pinterest user stop scrolling]

Then start the article body immediately after."""


def call_gemini(prompt: str, api_key: str) -> str:
    payload = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8192,
        "temperature": 0.75,
    }).encode()
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(GEMINI_URL, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read())
                content = data["choices"][0]["message"]["content"]
                finish = data["choices"][0].get("finish_reason", "?")
                tokens = data.get("usage", {}).get("completion_tokens", "?")
                log(f"  API ok: {len(content)} chars, {tokens} tokens, finish={finish}")
                return content
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            if exc.code in (429, 503, 502):
                wait = 30 * (2 ** attempt)
                log(f"  {exc.code} attempt {attempt}/{MAX_RETRIES} -- wait {wait}s")
                time.sleep(wait)
            else:
                raise RuntimeError(f"HTTP {exc.code}: {body[:200]}")
        except urllib.error.URLError as exc:
            log(f"  Network error attempt {attempt}: {exc.reason}")
            time.sleep(RPM_SLEEP * 2)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts")


def git_push(count: int) -> None:
    env = {**os.environ, "PATH": "/home/derek/bin:/usr/local/bin:/usr/bin:/bin", "GIT_TERMINAL_PROMPT": "0"}
    for cmd in [
        ["git", "-C", str(REPO_DIR), "add", "_posts/", "assets/images/pins/"],
        ["git", "-C", str(REPO_DIR), "commit", "-m", f"auto: add {count} articles {datetime.date.today().isoformat()}"],
        ["git", "-C", str(REPO_DIR), "push", "origin", "main"],
    ]:
        r = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if r.returncode != 0:
            log(f"GIT FAIL: {r.stderr[:80]}")
            return
    log(f"GIT PUSH OK -- {count} posts live")


def main() -> None:
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()

    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text().strip()
        try:
            os.kill(int(old), 0)
            log(f"Already running (PID {old}). Exiting."); return
        except (OSError, ValueError):
            log(f"Stale lock (PID {old}) -- clearing"); LOCK_PATH.unlink()
    LOCK_PATH.write_text(str(os.getpid()))

    try:
        if not gemini_key:
            log("ERROR: GEMINI_API_KEY not set"); return

        products = load_products()
        log(f"Loaded products.json: {len(products)} entries")

        # Build set of ASINs already published in _posts/
        used_asins = build_used_asins()
        log(f"Dedup: {len(used_asins)} ASINs already published")

        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.date.today().isoformat()
        generated = skipped = failed = 0
        log(f"START v13 -- {len(TOPICS)} articles -- model={MODEL}")

        for i, (slug, title, keyword, fmt) in enumerate(TOPICS, 1):
            fname = f"{today}-{slugify(slug)}.md"
            fpath = POSTS_DIR / fname
            if fpath.exists() and fpath.stat().st_size > 7000:
                log(f"SKIP [{i}/{len(TOPICS)}] {fname} (already good)"); skipped += 1; continue
            if fpath.exists():
                log(f"REDO [{i}/{len(TOPICS)}] {fname} (truncated)"); fpath.unlink()

            product = products.get(slug, {})
            if product:
                asin = get_asin_for_product(product)
                if asin and asin in used_asins:
                    log(f"SKIP [{i}/{len(TOPICS)}] {slug} -- ASIN {asin} already published"); skipped += 1; continue
                log(f"  Product: {product['name']}" + (f" (ASIN: {asin})" if asin else " (ASIN: unresolved)"))
            else:
                log(f"  WARN: no product entry for {slug}")

            log(f"WRITE [{i}/{len(TOPICS)}] [{fmt}] {title}")
            time.sleep(RPM_SLEEP)

            try:
                prompt = make_prompt(title, keyword, slug, fmt, product)
                content = call_gemini(prompt, gemini_key)
                # Extract PIN_DESC from first line if present
                pin_desc = f'{title} - expert reviews and buying guide.'
                if content.startswith('PIN_DESC:'):
                    first_line, _, content = content.partition('\n')
                    pin_desc = first_line.replace('PIN_DESC:', '').strip()
                    log(f"  PIN_DESC: {pin_desc[:60]}")
                if len(content) < 2000:
                    log(f"  WARN: only {len(content)} chars -- may be truncated")
                affiliate_url = product.get("url", "")
                fm = front_matter(title, keyword, affiliate_url).replace(
                    f'description: "{title} - expert reviews and buying guide."',
                    f'description: "{pin_desc}"'
                )
                fpath.write_text(fm + "\n" + content, encoding="utf-8")
                log(f"  SAVED {fname} ({fpath.stat().st_size} bytes)")
                # Parse front matter from written file
                fm_data = {}
                written = fpath.read_text()
                m = __import__('re').match(r'^---\n(.*?)\n---', written, __import__('re').DOTALL)
                if m:
                    for line in m.group(1).splitlines():
                        if ':' in line:
                            k, _, v = line.partition(':')
                            fm_data[k.strip()] = v.strip().strip('"').strip("'")
                parts = fname.replace('.md','').split('-', 3)
                slug_only = parts[3] if len(parts) == 4 else fname.replace('.md','')
                category = fm_data.get('categories','').strip('[]')
                article_url = f"https://happypetproductreviews.com/{category}/{slug_only}/?utm_source=pinterest&utm_medium=social&utm_campaign=pin"
                species = fm_data.get('species','both')

                # 1. Generate branded Pinterest pin image FIRST
                pin_url = product.get('image','')  # fallback to raw product image
                if PIN_GEN_AVAILABLE:
                    try:
                        pin_url = make_pin_for_post(
                            title,
                            fm_data.get('description',''),
                            product.get('image',''),
                            category,
                            slug_only,
                            generated
                        )
                        log(f"  PIN: {pin_url}")
                    except Exception as pe:
                        log(f"  WARN: pin generation failed: {pe}")

                # 2. Append to sheet with branded pin URL in Column D
                append_to_sheet(title, article_url, fm_data.get('description',''), pin_url, species)

                # 3. Push article + pin image together
                generated += 1
                if product:
                    asin = get_asin_for_product(product)
                    if asin:
                        used_asins.add(asin)
                git_push(1)
            except Exception as exc:
                log(f"  FAIL: {exc}"); failed += 1

            if i < len(TOPICS):
                log(f"  Waiting {INTER_DELAY//60}min...")
                time.sleep(INTER_DELAY)

        log(f"DONE -- {generated} written, {skipped} skipped, {failed} failed")
    finally:
        if LOCK_PATH.exists(): LOCK_PATH.unlink()


if __name__ == "__main__":
    main()
