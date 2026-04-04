#!/usr/bin/env python3
"""
PawPicks Content Generator v10 — CSE-Grounded Pipeline
- Queries Google Custom Search API for real product names before each article
- Injects real product names into Gemini prompt (no more fictional brands)
- Gemini-2.5-flash for content generation
- 15 min between articles, auto git push
"""
import os, re, json, datetime, time, urllib.request, urllib.error, urllib.parse, subprocess
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
POSTS_DIR = REPO_DIR / "_posts"
LOG_PATH  = Path("/tmp/pawpicks_gen.log")
LOCK_PATH = Path("/tmp/pawpicks_gen.lock")

MODEL       = "gemini-2.5-flash"
GEMINI_URL  = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
CSE_URL     = "https://www.googleapis.com/customsearch/v1"
INTER_DELAY = 900
RPM_SLEEP   = 8
MAX_RETRIES = 3
CSE_RESULTS = 5

TOPICS = [
    ("best-dog-collars-small-breeds",    "Best Dog Collars for Small Breeds",          "dog collars small breeds"),
    ("best-cat-scratching-posts",        "Best Cat Scratching Posts That Last",         "cat scratching post"),
    ("best-no-pull-dog-harness",         "Best No-Pull Dog Harnesses Reviewed",         "no pull dog harness"),
    ("best-automatic-cat-feeder",        "Best Automatic Cat Feeders for Busy Owners",  "automatic cat feeder"),
    ("best-dog-toys-aggressive-chewers", "Best Dog Toys for Aggressive Chewers",        "dog toys aggressive chewers"),
    ("best-cat-litter-odor-control",     "Best Cat Litter for Odor Control",            "cat litter odor control"),
    ("best-dog-beds-large-breeds",       "Best Dog Beds for Large Breeds on a Budget",  "dog beds large breeds"),
    ("best-pet-water-fountain",          "Best Pet Water Fountains for Cats and Dogs",  "pet water fountain"),
    ("best-puppy-training-pads",         "Best Puppy Training Pads Reviewed",           "puppy training pads"),
    ("best-cat-carrier-travel",          "Best Cat Carriers for Travel and Vet Visits", "cat carrier travel"),
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

def front_matter(title: str, keyword: str) -> str:
    today = datetime.date.today().isoformat()
    kw = keyword.lower()
    if any(w in kw for w in ['cat','kitten','feline','litter','scratch']):
        species = 'cat'
    elif any(w in kw for w in ['dog','puppy','canine','harness','collar','chew']):
        species = 'dog'
    else:
        species = 'both'
    return (
        f'---\nlayout: post\ntitle: "{title}"\ndate: {today}\n'
        f'categories: [pet-accessories]\nspecies: {species}\ntags: [{keyword}]\n'
        f'description: "{title} - expert reviews and buying guide."\n---\n'
    )

def fetch_real_products(keyword: str, cse_key: str, cse_cx: str) -> list:
    """Query Google CSE and extract real product names from result titles."""
    if not cse_key or not cse_cx:
        log("  WARN: CSE keys missing — skipping product lookup")
        return []
    query = urllib.parse.urlencode({
        "key": cse_key,
        "cx": cse_cx,
        "q": f"best {keyword} review",
        "num": CSE_RESULTS,
    })
    url = f"{CSE_URL}?{query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PawPicksBot/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        items = data.get("items", [])
        products = []
        for item in items:
            title = item.get("title", "")
            clean = re.split(r' [-|] ', title)[0].strip()
            if clean and len(clean) > 5:
                products.append(clean)
        log(f"  CSE: found {len(products)} product references for '{keyword}'")
        return products[:CSE_RESULTS]
    except Exception as exc:
        log(f"  CSE WARN: {exc} — proceeding without real products")
        return []

def make_prompt(title: str, keyword: str, slug: str, products: list) -> str:
    link = ""
    if slug in INTERNAL_LINKS:
        url, anchor = INTERNAL_LINKS[slug]
        link = f'\nNaturally include this markdown link once where relevant: [{anchor}]({url})'

    if products:
        product_block = (
            "REAL PRODUCTS TO FEATURE (use these actual product/brand names — do not invent names):\n"
            + "\n".join(f"- {p}" for p in products)
            + "\nIf you need a 4th or 5th product and the list is short, add one well-known real brand.\n"
        )
    else:
        product_block = (
            "Use real, well-known pet product brand names (e.g. Ruffwear, PetSafe, Frisco, Kong, "
            "Blue Buffalo, Chewy house brand). Do not invent fictional brand names.\n"
        )

    return f"""You are a senior writer and editor for PawPicks, a friendly budget-focused pet product review blog.

Write a complete, polished, publish-ready blog post. Title: "{title}". Focus keyword: "{keyword}".

{product_block}
LENGTH: 950-1050 words of body content (not counting front matter). This is a firm requirement.

STRUCTURE (all sections required):
- Opening paragraph (100+ words): Hook with a relatable pet owner moment. Warm, personal tone.
- 5 Product Reviews: Each gets an H3 heading with the real product name, 60-80 word description, 3 bullet pros, 2 bullet cons.
- Comparison Table: Markdown table — Product | Best For | Material | Adjustable | Our Rating
- Buying Guide (H2, 150+ words): 4-5 practical tips for choosing the right product.
- Closing paragraph (80+ words): Clear recommendation, warm sign-off.

WRITING STYLE:
- Conversational, warm, like a knowledgeable friend
- Vary sentence length. Mix short punchy sentences with longer flowing ones.
- NO AI clichés: never use "delve", "it's worth noting", "in conclusion", "look no further", "game-changer", "comprehensive guide"
- Pet care facts must be accurate (breeds, behavior, materials, sizing).
- Use "{keyword}" naturally 4-6 times
- Write in first person plural ("we tested", "we found") for authority{link}

FORMAT: Return ONLY clean Markdown body. No YAML. No preamble. Just start writing."""


def call_gemini(prompt: str, api_key: str) -> str:
    payload = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4096,
        "temperature": 0.75,
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
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
                log(f"  429 attempt {attempt}/{MAX_RETRIES} — wait {wait}s | {body[:60]}")
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
    log(f"GIT PUSH OK — {count} posts live")

def main() -> None:
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    cse_key    = os.environ.get("GOOGLE_CSE_KEY", "").strip()
    cse_cx     = os.environ.get("GOOGLE_CSE_CX", "").strip()

    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text().strip()
        try:
            os.kill(int(old), 0)
            log(f"Already running (PID {old}). Exiting."); return
        except (OSError, ValueError):
            log(f"Stale lock (PID {old}) — clearing"); LOCK_PATH.unlink()
    LOCK_PATH.write_text(str(os.getpid()))

    try:
        if not gemini_key:
            log("ERROR: GEMINI_API_KEY not set"); return
        if not cse_key or not cse_cx:
            log("WARN: CSE keys not set — articles will use fallback brand names")

        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.date.today().isoformat()
        generated = skipped = failed = 0
        log(f"START v10 (CSE-grounded) — {len(TOPICS)} articles — model={MODEL}")
        log(f"  CSE active: {'yes' if cse_key and cse_cx else 'NO — fallback mode'}")

        for i, (slug, title, keyword) in enumerate(TOPICS, 1):
            fname = f"{today}-{slugify(slug)}.md"
            fpath = POSTS_DIR / fname
            if fpath.exists() and fpath.stat().st_size > 2000:
                log(f"SKIP [{i}/{len(TOPICS)}] {fname} (already good)"); skipped += 1; continue
            if fpath.exists():
                log(f"REDO [{i}/{len(TOPICS)}] {fname} (was truncated)")
                fpath.unlink()

            log(f"WRITE [{i}/{len(TOPICS)}] {title}")
            time.sleep(RPM_SLEEP)

            # Step 1: fetch real product names via CSE
            products = fetch_real_products(keyword, cse_key, cse_cx)

            # Step 2: generate article grounded in real products
            try:
                content = call_gemini(make_prompt(title, keyword, slug, products), gemini_key)
                if len(content) < 2000:
                    log(f"  WARN: content only {len(content)} chars — may be truncated")
                fpath.write_text(front_matter(title, keyword) + "\n" + content, encoding="utf-8")
                log(f"  SAVED {fname} ({fpath.stat().st_size} bytes)")
                generated += 1
            except Exception as exc:
                log(f"  FAIL: {exc}"); failed += 1

            if i < len(TOPICS):
                log(f"  Waiting {INTER_DELAY//60}min...")
                time.sleep(INTER_DELAY)

        log(f"DONE — {generated} written, {skipped} skipped, {failed} failed")
        if generated > 0:
            git_push(generated)
    finally:
        if LOCK_PATH.exists(): LOCK_PATH.unlink()

if __name__ == "__main__":
    main()
