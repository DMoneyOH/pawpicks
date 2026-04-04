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

FORMAT: Return ONLY clean Markdown. No YAML. No preamble. Start writing immediately."""


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
        ["git", "-C", str(REPO_DIR), "add", "_posts/"],
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

            log(f"WRITE [{i}/{len(TOPICS)}] [{fmt}] {title}")
            time.sleep(RPM_SLEEP)

            product = products.get(slug, {})
            if product:
                log(f"  Product: {product['name']}")
            else:
                log(f"  WARN: no product entry for {slug}")

            try:
                prompt = make_prompt(title, keyword, slug, fmt, product)
                content = call_gemini(prompt, gemini_key)
                if len(content) < 2000:
                    log(f"  WARN: only {len(content)} chars -- may be truncated")
                affiliate_url = product.get("url", "")
                fpath.write_text(
                    front_matter(title, keyword, affiliate_url) + "\n" + content,
                    encoding="utf-8"
                )
                log(f"  SAVED {fname} ({fpath.stat().st_size} bytes)")
                generated += 1
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
