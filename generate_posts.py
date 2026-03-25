#!/usr/bin/env python3
"""
PawPicks Content Generator v9 — Single-Pass Pipeline
- One prompt: draft + grammar + humanize + fact-check combined
- Eliminates quality pass truncation bug
- gemini-2.5-flash, 4096 max_tokens
- 15 min between articles, auto git push
"""
import os, re, json, datetime, time, urllib.request, urllib.error, subprocess
from pathlib import Path

REPO_DIR  = Path(__file__).parent.resolve()
POSTS_DIR = REPO_DIR / "_posts"
LOG_PATH  = Path("/tmp/pawpicks_gen.log")
LOCK_PATH = Path("/tmp/pawpicks_gen.lock")

MODEL      = "gemini-2.5-flash"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
INTER_DELAY = 900
RPM_SLEEP   = 8
MAX_RETRIES = 3

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

def make_prompt(title: str, keyword: str, slug: str) -> str:
    link = ""
    if slug in INTERNAL_LINKS:
        url, anchor = INTERNAL_LINKS[slug]
        link = f'\nNaturally include this markdown link once where relevant: [{anchor}]({url})'

    return f"""You are a senior writer and editor for PawPicks, a friendly budget-focused pet product review blog.

Write a complete, polished, publish-ready blog post. Title: "{title}". Focus keyword: "{keyword}".

LENGTH: 950-1050 words of body content (not counting front matter). This is a firm requirement.

STRUCTURE (all sections required):
- Opening paragraph (100+ words): Hook with a relatable pet owner moment. Warm, personal tone.
- 5 Product Reviews: Each gets an H3 heading with product name, 60-80 word description, 3 bullet pros, 2 bullet cons.
- Comparison Table: Markdown table with columns — Product | Best For | Material | Adjustable | Our Rating
- Buying Guide (H2, 150+ words): 4-5 practical tips for choosing the right product.
- Closing paragraph (80+ words): Clear recommendation, warm sign-off.

WRITING STYLE (apply throughout — do not do these in a separate pass, write this way from the start):
- Conversational, warm, like a knowledgeable friend — not a corporate reviewer
- Vary sentence length. Mix short punchy sentences with longer flowing ones.
- NO AI clichés: never use "delve", "it's worth noting", "in conclusion", "look no further", "this article will explore", "game-changer", "comprehensive guide"
- Pet care facts must be accurate (breeds, behavior, materials, sizing). Products are fictional brands — that's fine.
- Use "{keyword}" naturally 4-6 times
- Write in first person plural ("we tested", "we found") for authority{link}

FORMAT: Return ONLY clean Markdown body. No YAML front matter. No preamble. No "Here is your article:" — just start writing."""


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
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text().strip()
        try:
            os.kill(int(old), 0)
            log(f"Already running (PID {old}). Exiting."); return
        except (OSError, ValueError):
            log(f"Stale lock (PID {old}) — clearing"); LOCK_PATH.unlink()
    LOCK_PATH.write_text(str(os.getpid()))
    try:
        if not api_key:
            log("ERROR: GEMINI_API_KEY not set"); return
        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.date.today().isoformat()
        generated = skipped = failed = 0
        log(f"START v9 (single-pass) — {len(TOPICS)} articles — model={MODEL}")

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
            try:
                content = call_gemini(make_prompt(title, keyword, slug), api_key)
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
