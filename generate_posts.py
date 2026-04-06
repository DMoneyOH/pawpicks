#!/usr/bin/env python3
"""
Happy Pet Product Reviews Generator v14 — Real Product Pipeline + AI Review Layer
- Loads products.json for real affiliate links per topic (20 topics)
- Flexible article format: single_review, roundup, buying_guide
- Gemini-2.5-flash for content generation
- AI reviewer: flag + auto-rewrite; GitHub issue on persistent failure
- Slug-based dedup across all dates
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

# Category map: slug -> Jekyll category (drives URL structure)
SLUG_CATEGORIES = {
    # Articles 1-10
    "best-dog-collars-small-breeds":    "dog-collars",
    "best-cat-scratching-posts":        "cat-scratching",
    "best-no-pull-dog-harness":         "dog-harnesses",
    "best-automatic-cat-feeder":        "cat-feeders",
    "best-dog-toys-aggressive-chewers": "dog-toys",
    "best-cat-litter-odor-control":     "cat-litter",
    "best-dog-beds-large-breeds":       "dog-beds",
    "best-pet-water-fountain":          "pet-feeding",
    "best-puppy-training-pads":         "dog-training",
    "best-cat-carrier-travel":          "cat-carriers",
    # Articles 11-20
    "best-gps-dog-collars":             "dog-collars",
    "best-self-cleaning-litter-boxes":  "cat-litter",
    "best-senior-dog-food":             "dog-food",
    "best-interactive-cat-toys":        "cat-toys",
    "best-dog-crates":                  "dog-crates",
    "best-grain-free-cat-food":         "cat-food",
    "best-pet-cameras":                 "pet-tech",
    "best-flea-prevention-dogs":        "dog-health",
    "best-wet-cat-food":                "cat-food",
    "best-dog-dna-tests":               "dog-health",
}
REVIEWER_MODEL   = "gemini-2.5-flash"
REVIEWER_ENABLED = True
MAX_REVIEW_ATTEMPTS = 2  # 1 rewrite attempt before GitHub issue + skip
GITHUB_REPO      = "DMoneyOH/pawpicks"
GITHUB_ASSIGNEE  = "DMoneyOH"

SITE_BASE = "https://happypetproductreviews.com"

def build_url(slug: str, utm: bool = False) -> str:
    """
    Single source of truth for constructing article URLs.
    Derives category from SLUG_CATEGORIES — never hardcoded elsewhere.
    utm=True appends Pinterest UTM params (sheet/pin URLs).
    """
    category = SLUG_CATEGORIES.get(slug, 'pet-accessories')
    base = f"{SITE_BASE}/{category}/{slug}/"
    if utm:
        return base + "?utm_source=pinterest&utm_medium=social&utm_campaign=pin"
    return base

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
    # Articles 11-20
    ("best-gps-dog-collars",             "Best GPS Dog Collars of 2026",               "gps dog collar tracker",       "roundup"),
    ("best-self-cleaning-litter-boxes",  "Best Self-Cleaning Litter Boxes",             "self cleaning litter box",     "roundup"),
    ("best-senior-dog-food",             "Best Dog Food for Senior Dogs",               "senior dog food",              "roundup"),
    ("best-interactive-cat-toys",        "Best Interactive Cat Toys to Keep Cats Busy", "interactive cat toys",         "roundup"),
    ("best-dog-crates",                  "Best Dog Crates for Every Size",              "dog crate",                    "roundup"),
    ("best-grain-free-cat-food",         "Best Grain-Free Cat Food",                    "grain free cat food",          "roundup"),
    ("best-pet-cameras",                 "Best Pet Cameras for Checking on Your Dog",   "pet camera treat dispenser",   "roundup"),
    ("best-flea-prevention-dogs",        "Best Flea Prevention for Dogs",               "flea prevention dogs",         "buying_guide"),
    ("best-wet-cat-food",                "Best Wet Cat Food Your Cat Will Actually Eat","wet cat food",                 "roundup"),
    ("best-dog-dna-tests",               "Best Dog DNA Tests Reviewed",                 "dog dna test kit",             "roundup"),
]

INTERNAL_LINKS = {
    # All URLs derived via build_url() — stays in sync with SLUG_CATEGORIES automatically
    "best-dog-collars-small-breeds":    (build_url("best-no-pull-dog-harness"),         "no-pull harnesses for small dogs"),
    "best-cat-scratching-posts":        (build_url("best-cat-litter-odor-control"),      "cat litter for odor control"),
    "best-no-pull-dog-harness":         (build_url("best-dog-collars-small-breeds"),     "dog collars for small breeds"),
    "best-automatic-cat-feeder":        (build_url("best-pet-water-fountain"),           "pet water fountains"),
    "best-dog-toys-aggressive-chewers": (build_url("best-dog-beds-large-breeds"),        "dog beds for large breeds"),
    "best-cat-litter-odor-control":     (build_url("best-cat-scratching-posts"),         "cat scratching posts"),
    "best-dog-beds-large-breeds":       (build_url("best-dog-toys-aggressive-chewers"),  "toys for aggressive chewers"),
    "best-pet-water-fountain":          (build_url("best-automatic-cat-feeder"),         "automatic cat feeders"),
    "best-puppy-training-pads":         (build_url("best-no-pull-dog-harness"),          "no-pull dog harnesses"),
    "best-cat-carrier-travel":          (build_url("best-automatic-cat-feeder"),         "automatic cat feeders for travel"),
    "best-gps-dog-collars":             (build_url("best-dog-collars-small-breeds"),     "dog collars for small breeds"),
    "best-self-cleaning-litter-boxes":  (build_url("best-cat-litter-odor-control"),      "best cat litter for odor control"),
    "best-senior-dog-food":             (build_url("best-dog-beds-large-breeds"),        "orthopedic dog beds for seniors"),
    "best-interactive-cat-toys":        (build_url("best-cat-scratching-posts"),         "cat scratching posts"),
    "best-dog-crates":                  (build_url("best-dog-beds-large-breeds"),        "dog beds for crate training"),
    "best-grain-free-cat-food":         (build_url("best-automatic-cat-feeder"),         "automatic cat feeders"),
    "best-pet-cameras":                 (build_url("best-gps-dog-collars"),              "GPS dog trackers"),
    "best-flea-prevention-dogs":        (build_url("best-no-pull-dog-harness"),          "no-pull dog harnesses"),
    "best-wet-cat-food":                (build_url("best-automatic-cat-feeder"),         "automatic cat feeders"),
    "best-dog-dna-tests":               (build_url("best-gps-dog-collars"),              "GPS dog collars"),
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

def build_used_slugs() -> set:
    """
    Scan all existing _posts/ files and return the set of slugs already published.
    Keys on slug (date-stripped filename) so duplicates across dates are caught.
    e.g. '2026-04-04-best-dog-collars-small-breeds.md' -> 'best-dog-collars-small-breeds'
    """
    used = set()
    for md in POSTS_DIR.glob("*.md"):
        parts = md.stem.split('-', 3)
        if len(parts) == 4:
            used.add(parts[3])
    return used

def make_review_prompt(title: str, keyword: str, content: str) -> str:
    return f"""You are a senior human editor for Happy Pet Product Reviews. Evaluate whether this article reads like it was written by a real person — a trusted, warm, knowledgeable pet owner — or like AI-generated content.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}

ARTICLE CONTENT:
---
{content}
---

Return ONLY a single valid JSON object. No preamble, no explanation, no markdown fences, no trailing text. Start your response with {{ and end with }}.

{{"pass": true or false, "scores": {{"human_voice": <1-5>, "warmth": <1-5>, "readability": <1-5>, "accuracy": <1-5>, "affiliate_link_present": true or false, "disclosure_present": true or false, "ai_cliches_found": ["list or empty array"]}}, "flags": ["list or empty array"], "rewrite_instructions": "specific instructions if pass is false, else empty string"}}

PASS criteria: all scores >= 3, affiliate_link_present true, no more than 1 ai_cliche.
If pass is false, rewrite_instructions must be specific and actionable — name exact sections and fixes needed."""

def make_rewrite_prompt(title: str, keyword: str, content: str, instructions: str) -> str:
    return f"""You are a senior writer for Happy Pet Product Reviews. A human editor has reviewed your article and flagged specific issues. Rewrite the article fixing ONLY the flagged issues — do not change what is already working.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}

EDITOR FEEDBACK:
{instructions}

ORIGINAL ARTICLE:
---
{content}
---

Return ONLY the rewritten article in clean Markdown. No preamble. No YAML. Start writing immediately.
The VERY FIRST LINE must be:
PIN_DESC: [one punchy sentence, max 20 words, Pinterest stop-scroll hook]

Then the article body immediately after."""

def review_and_rewrite(title: str, keyword: str, content: str, api_key: str) -> tuple:
    """
    Returns (final_content, passed, flags)
    Attempts up to MAX_REVIEW_ATTEMPTS rewrites before giving up.
    Cloud migration: swap REVIEWER_MODEL and this function becomes a Cloud Function.
    """
    if not REVIEWER_ENABLED:
        return content, True, []

    for attempt in range(1, MAX_REVIEW_ATTEMPTS + 1):
        log(f"  REVIEW attempt {attempt}/{MAX_REVIEW_ATTEMPTS}")
        review_prompt = make_review_prompt(title, keyword, content)
        payload = json.dumps({
            "model": REVIEWER_MODEL,
            "messages": [{"role": "user", "content": review_prompt}],
            "max_tokens": 2048,
            "temperature": 0.2,
        }).encode()
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        try:
            req = urllib.request.Request(GEMINI_URL, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
                raw = data["choices"][0]["message"]["content"].strip()
                # Strip markdown fences if present
                raw = re.sub(r'^```json\s*', '', raw, flags=re.MULTILINE)
                raw = re.sub(r'\s*```\s*$', '', raw, flags=re.MULTILINE)
                raw = raw.strip()
                # Extract first JSON object if extra text present
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m:
                    raw = m.group(0)
                scorecard = json.loads(raw)
        except json.JSONDecodeError as e:
            log(f"  WARN: review JSON parse failed: {e} | raw[:200]: {raw[:200]} -- skipping review")
            return content, True, []
        except Exception as e:
            log(f"  WARN: review call failed: {e} -- skipping review")
            return content, True, []
            return content, True, []

        passed  = scorecard.get("pass", False)
        flags   = scorecard.get("flags", [])
        scores  = scorecard.get("scores", {})
        cliches = scores.get("ai_cliches_found", [])
        log(f"  REVIEW {'PASS' if passed else 'FAIL'} | human_voice={scores.get('human_voice')} warmth={scores.get('warmth')} readability={scores.get('readability')}")
        if flags:
            log(f"  FLAGS: {'; '.join(flags)}")
        if cliches:
            log(f"  CLICHES: {', '.join(cliches)}")

        if passed:
            log(f"  REVIEW PASS -- proceeding")
            return content, True, []

        instructions = scorecard.get("rewrite_instructions", "")
        if attempt < MAX_REVIEW_ATTEMPTS and instructions:
            log(f"  REWRITING based on editor feedback...")
            time.sleep(RPM_SLEEP)
            rewrite_prompt = make_rewrite_prompt(title, keyword, content, instructions)
            try:
                content = call_gemini(rewrite_prompt, api_key)
                # Re-extract PIN_DESC if present
                if content.startswith('PIN_DESC:'):
                    _, _, content = content.partition('\n')
            except Exception as e:
                log(f"  WARN: rewrite call failed: {e}")
                return content, False, flags
        else:
            log(f"  REVIEW FAILED after {attempt} attempt(s) -- will create GitHub issue")
            return content, False, flags

    return content, False, []

def create_github_issue(title: str, slug: str, flags: list) -> None:
    """Create a GitHub issue assigned to GITHUB_ASSIGNEE for manual review."""
    env = {**os.environ, "PATH": "/home/derek/bin:/usr/local/bin:/usr/bin:/bin", "GIT_TERMINAL_PROMPT": "0"}
    flag_text = "\n".join(f"- {f}" for f in flags) if flags else "- Review failed quality threshold after rewrite attempt"
    body = (
        f"## Article Quality Review Failed\n\n"
        f"**Article:** {title}\n"
        f"**Slug:** `{slug}`\n"
        f"**Date:** {datetime.date.today().isoformat()}\n\n"
        f"### Flags\n{flag_text}\n\n"
        f"### Action Required\n"
        f"1. Review the generated `.md` file in `_posts/`\n"
        f"2. Edit manually or re-run the generator for this slug\n"
        f"3. Close this issue once published\n"
    )
    cmd = [
        "gh", "issue", "create",
        "--repo", GITHUB_REPO,
        "--title", f"[Review Required] {title}",
        "--body", body,
        "--assignee", GITHUB_ASSIGNEE,
        "--label", "content-review",
    ]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if r.returncode == 0:
        log(f"  GITHUB ISSUE created: {r.stdout.strip()}")
    else:
        # Label may not exist yet -- retry without label
        cmd_nolabel = [c for c in cmd if c != "--label" and c != "content-review"]
        r2 = subprocess.run(cmd_nolabel, env=env, capture_output=True, text=True)
        if r2.returncode == 0:
            log(f"  GITHUB ISSUE created (no label): {r2.stdout.strip()}")
        else:
            log(f"  WARN: GitHub issue creation failed: {r2.stderr[:120]}")

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

def front_matter(title: str, keyword: str, affiliate_url: str = "", slug: str = "") -> str:
    today = datetime.date.today().isoformat()
    kw = keyword.lower()
    if any(w in kw for w in ['cat', 'kitten', 'feline', 'litter', 'scratch']):
        species = 'cat'
    elif any(w in kw for w in ['dog', 'puppy', 'canine', 'harness', 'collar', 'chew']):
        species = 'dog'
    else:
        species = 'both'
    category = SLUG_CATEGORIES.get(slug, 'pet-accessories')
    fm = (
        f'---\nlayout: post\ntitle: "{title}"\ndate: {today}\n'
        f'categories: [{category}]\nspecies: {species}\ntags: [{keyword}]\n'
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

        # Build set of slugs already published across all dates
        used_slugs = build_used_slugs()
        log(f"Dedup: {len(used_slugs)} slugs already published")

        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.date.today().isoformat()
        generated = skipped = failed = 0
        log(f"START v14 -- {len(TOPICS)} articles -- model={MODEL} reviewer={'ON' if REVIEWER_ENABLED else 'OFF'}")

        for i, (slug, title, keyword, fmt) in enumerate(TOPICS, 1):
            # Slug-based dedup: skip if this slug exists under ANY date
            if slug in used_slugs:
                log(f"SKIP [{i}/{len(TOPICS)}] {slug} -- already published"); skipped += 1; continue

            fname = f"{today}-{slugify(slug)}.md"
            fpath = POSTS_DIR / fname

            product = products.get(slug, {})
            if product:
                log(f"  Product: {product['name']}")
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

                # Review + rewrite loop — sleep first to avoid 429 back-to-back
                time.sleep(RPM_SLEEP)
                content, review_passed, review_flags = review_and_rewrite(title, keyword, content, gemini_key)
                if not review_passed:
                    create_github_issue(title, slug, review_flags)
                    log(f"  SKIP {slug} -- quality check failed, GitHub issue created")
                    failed += 1
                    continue

                affiliate_url = product.get("url", "")
                fm = front_matter(title, keyword, affiliate_url, slug).replace(
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
                article_url = build_url(slug, utm=True)
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
                used_slugs.add(slug)
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
