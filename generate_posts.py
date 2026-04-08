#!/usr/bin/env python3
"""
Happy Pet Product Reviews Generator v16 — Phase 1 Hardened Pipeline
- TOPICS derived entirely from products.json (no hardcoded list)
- products.json is single source of truth: slug, title, keyword, format, category, species, topical_sheet
- Dynamic internal links: resolved at runtime from published _posts/ by category
- Pre-publish validation gate: missing required fields = held, not published
- load_dotenv at top of main() for local runs; GHA uses env secrets directly
- Reviewer JSON parse hardened; content truncated before review prompt
- Slug-based dedup across all dates
- Pin queue staged for autopublish.sh
- DONE log includes held count
"""
import os, re, json, datetime, time, urllib.request, urllib.error, urllib.parse, subprocess
from pathlib import Path

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

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
LOG_PATH  = Path("/tmp/pawpicks.log")       # unified log: generator + publisher + pins
LOCK_PATH = Path("/tmp/pawpicks_gen.lock")

MODEL            = "gemini-2.5-flash"
GEMINI_URL       = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
REVIEWER_MODEL   = "gemini-2.5-flash"
REVIEWER_ENABLED = True
MAX_REVIEW_ATTEMPTS = 2
INTER_DELAY      = 300
RPM_SLEEP        = 8
REVIEW_PRE_SLEEP = 15   # seconds to wait before reviewer call to avoid 429
MAX_RETRIES      = 3
GITHUB_REPO      = "DMoneyOH/pawpicks"
GITHUB_ASSIGNEE  = "DMoneyOH"
SITE_BASE        = "https://happypetproductreviews.com"

# Articles 1-10 category map (predate products.json; remain hardcoded)
# Articles 11+ categories registered at runtime from products.json
SLUG_CATEGORIES = {
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
}

# Topical sheet map for articles 1-10 only (11+ use products.json topical_sheet field)
SLUG_TO_TOPICAL_SHEET_LEGACY = {
    "best-dog-collars-small-breeds":    "HAPPYPET_SHEET_ID_TOYS",
    "best-dog-toys-aggressive-chewers": "HAPPYPET_SHEET_ID_TOYS",
    "best-no-pull-dog-harness":         "HAPPYPET_SHEET_ID_TOYS",
    "best-puppy-training-pads":         "HAPPYPET_SHEET_ID_HOME",
    "best-dog-beds-large-breeds":       "HAPPYPET_SHEET_ID_HOME",
    "best-cat-carrier-travel":          "HAPPYPET_SHEET_ID_HOME",
    "best-automatic-cat-feeder":        "HAPPYPET_SHEET_ID_HOME",
    "best-cat-scratching-posts":        "HAPPYPET_SHEET_ID_TOYS",
    "best-cat-litter-odor-control":     "HAPPYPET_SHEET_ID_HOME",
    "best-pet-water-fountain":          "HAPPYPET_SHEET_ID_HOME",
}


def log(msg: str, level: str = "INFO") -> None:
    line = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [GENERATOR] [{level}]  {msg}"
    print(line, flush=True)


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]", "", s.lower().replace(" ", "-"))


def build_url(slug: str, utm: bool = False) -> str:
    category = SLUG_CATEGORIES.get(slug, "pet-accessories")
    base = f"{SITE_BASE}/{category}/{slug}/"
    if utm:
        return base + "?utm_source=pinterest&utm_medium=social&utm_campaign=pin"
    return base


def build_pin_image_url(slug: str) -> str:
    """Always version-stamp pin image URLs to bust Pinterest CDN cache."""
    version = datetime.date.today().strftime("%Y%m%d")
    return f"{SITE_BASE}/assets/images/pins/{slug}.jpg?v={version}"


def load_products() -> dict:
    """
    Load products.json keyed by topic slug.
    Registers each product category into SLUG_CATEGORIES for URL generation.
    products.json is the single source of truth for all article metadata.
    """
    p = REPO_DIR / "products.json"
    if not p.exists():
        log(f"products.json not found", "WARN")
        return {}
    with p.open() as f:
        data = json.load(f)
    if isinstance(data, list):
        result = {}
        for entry in data:
            slug = entry.get("topic")
            if not slug:
                continue
            if entry.get("category"):
                SLUG_CATEGORIES[slug] = entry["category"]
            result[slug] = entry
        return result
    return data


def build_used_slugs() -> set:
    """Scan _posts/ and return set of published slugs (date-stripped filename)."""
    used = set()
    for md in POSTS_DIR.glob("*.md"):
        parts = md.stem.split("-", 3)
        if len(parts) == 4:
            used.add(parts[3])
    return used


def find_related_published_slug(current_slug: str, current_category: str) -> tuple:
    """
    Find best internal link target at runtime from published _posts/.
    Scoring: same category = 3, same category prefix = 2, any published = 1.
    Returns (url, anchor_text) or (None, None) if _posts/ is empty.
    """
    candidates = []
    for md in POSTS_DIR.glob("*.md"):
        parts = md.stem.split("-", 3)
        if len(parts) != 4:
            continue
        slug = parts[3]
        if slug == current_slug:
            continue
        cat = SLUG_CATEGORIES.get(slug, "")
        score = 1
        if cat == current_category:
            score = 3
        elif cat.split("-")[0] == current_category.split("-")[0]:
            score = 2
        candidates.append((score, slug))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: -x[0])
    best_slug = candidates[0][1]
    anchor = best_slug.replace("best-", "").replace("-", " ")
    return build_url(best_slug), anchor


def validate_product(slug: str, product: dict) -> list:
    """
    Returns list of error strings. Empty = valid. Non-empty = hold article.
    Runs before any API call so we never waste tokens on a bad entry.
    """
    errors = []
    if not product:
        errors.append(f"No product entry in products.json for slug: {slug}")
        return errors
    for field in ("affiliate_url", "name", "species", "title", "keyword", "category", "format"):
        if not product.get(field):
            errors.append(f"Missing required field: {field}")
    return errors


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
                finish  = data["choices"][0].get("finish_reason", "?")
                tokens  = data.get("usage", {}).get("completion_tokens", "?")
                log(f"  API ok: {len(content)} chars, {tokens} tokens, finish={finish}")
                return content
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            if exc.code in (429, 503, 502):
                wait = 30 * (2 ** attempt)
                log(f"  {exc.code} attempt {attempt}/{MAX_RETRIES} -- wait {wait}s", "WARN")
                time.sleep(wait)
            else:
                raise RuntimeError(f"HTTP {exc.code}: {body[:200]}")
        except urllib.error.URLError as exc:
            log(f"  Network error attempt {attempt}: {exc.reason}", "WARN")
            time.sleep(RPM_SLEEP * 2)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts")


def make_review_prompt(title: str, keyword: str, content: str) -> str:
    # Truncate to 2000 chars — reviewer needs tone/quality signal, not full body
    content_sample = content[:2000] if len(content) > 2000 else content
    return f"""You are a senior human editor for Happy Pet Product Reviews.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}

ARTICLE CONTENT (sample):
---
{content_sample}
---

Return ONLY a single valid JSON object. No preamble, no markdown fences, no trailing text.
Begin with {{ and end with }}.

Example of exact format required:
{{"pass": true, "scores": {{"human_voice": 4, "warmth": 4, "readability": 4, "accuracy": 4, "affiliate_link_present": true, "disclosure_present": false, "ai_cliches_found": []}}, "flags": [], "rewrite_instructions": ""}}

PASS criteria: all scores >= 3, affiliate_link_present true, no more than 1 ai_cliche.
If pass is false, rewrite_instructions must name exact sections and fixes needed."""


def make_rewrite_prompt(title: str, keyword: str, content: str, instructions: str) -> str:
    return f"""You are a senior writer for Happy Pet Product Reviews. Rewrite fixing ONLY the flagged issues.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}
EDITOR FEEDBACK: {instructions}

ORIGINAL ARTICLE:
---
{content}
---

Return ONLY clean Markdown. No YAML. No preamble. First line must be:
PIN_DESC: [one punchy sentence, max 20 words, Pinterest stop-scroll hook]
Then article body immediately after."""


def make_prompt(title: str, keyword: str, slug: str, fmt: str, product: dict,
                related_url: str, related_anchor: str) -> str:
    affiliate_url = product.get("affiliate_url", "")
    product_name  = product.get("name", "")
    link = ""
    if related_url and related_anchor:
        link = f"\nNaturally include this markdown link once where relevant: [{related_anchor}]({related_url})"
    affiliate_block = ""
    if product_name and affiliate_url:
        affiliate_block = (
            f"FEATURED PRODUCT: {product_name}\n"
            f"AFFILIATE LINK: {affiliate_url}\n"
            f"LINKING RULE: Every mention of {product_name} by name must be a clickable affiliate link "
            f"[{product_name}]({affiliate_url}). No plain-text product name mentions allowed.\n"
        )
    if fmt == "single_review":
        structure = f"""ARTICLE FORMAT: In-depth single product review of {product_name}
STRUCTURE: Opening (100+ words) | Product Overview (H2) | What We Like (H2, 4-5 features) | What Could Be Better (H2, 2-3 honest drawbacks) | Real Owner Experiences (H2) | Who Should Buy This (H2) | Verdict (H2, 80+ words with affiliate link) | Star rating: **Our Rating: X/5**"""
    elif fmt == "roundup":
        structure = f"""ARTICLE FORMAT: Roundup/comparison -- {title}
STRUCTURE: Opening (100+ words) | Quick Picks (H2) | Featured Pick {product_name} (H3, 80-100 words, affiliate link, pros/cons) | 2-3 Additional Picks (H3 each, 60-80 words, real brands like Kong/PetSafe/Frisco) | Comparison Table (H2): Product|Best For|Price Range|Rating | Buying Guide (H2, 150+ words) | Closing (80+ words with affiliate link)"""
    else:
        structure = f"""ARTICLE FORMAT: Buying guide -- {title}
STRUCTURE: Opening (100+ words) | What to Look For (H2, 5-6 key factors) | Our Top Pick {product_name} (H2, 100 words, affiliate link) | Common Mistakes to Avoid (H2, 3-4 pitfalls) | FAQ (H2, 4-5 real questions) | Closing (80+ words with affiliate link)"""
    return f"""You are a senior writer for Happy Pet Product Reviews, a trusted budget-focused pet product review blog.

Write a complete, publish-ready blog post. Title: "{title}". Focus keyword: "{keyword}".

{affiliate_block}
LENGTH: 950-1100 words of body content. Firm requirement.

{structure}

WRITING STYLE:
- Conversational, warm, authoritative -- like advice from a trusted friend who owns pets
- Vary sentence length. Short punchy sentences mixed with longer flowing ones.
- NO AI cliches: never use "delve", "it's worth noting", "in conclusion", "look no further", "game-changer", "comprehensive guide", "navigate"
- Use "{keyword}" naturally 4-6 times. Write in first person plural ("we tested", "we found").{link}

FORMAT: Return ONLY clean Markdown. No YAML. No preamble. Start writing immediately.
FIRST LINE must be: PIN_DESC: [one punchy sentence, max 20 words, Pinterest stop-scroll hook]
Then article body immediately after."""


def review_and_rewrite(title: str, keyword: str, content: str, api_key: str) -> tuple:
    """Returns (final_content, passed, flags)"""
    if not REVIEWER_ENABLED:
        return content, True, []
    for attempt in range(1, MAX_REVIEW_ATTEMPTS + 1):
        log(f"  REVIEW attempt {attempt}/{MAX_REVIEW_ATTEMPTS}")
        log(f"  REVIEW pre-sleep {REVIEW_PRE_SLEEP}s...")
        time.sleep(REVIEW_PRE_SLEEP)
        try:
            payload = json.dumps({
                "model": REVIEWER_MODEL,
                "messages": [{"role": "user", "content": make_review_prompt(title, keyword, content)}],
                "max_tokens": 1024,
                "temperature": 0.2,
            }).encode()
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
            # Reviewer-specific retry loop for 429
            raw = None
            for r_attempt in range(1, MAX_RETRIES + 1):
                try:
                    req = urllib.request.Request(GEMINI_URL, data=payload, headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        raw = json.loads(resp.read())["choices"][0]["message"]["content"].strip()
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code == 429:
                        wait = 30 * (2 ** r_attempt)
                        log(f"  REVIEW 429 attempt {r_attempt}/{MAX_RETRIES} -- wait {wait}s", "WARN")
                        time.sleep(wait)
                    else:
                        raise
            if raw is None:
                log("  review call failed after retries -- skipping review", "WARN")
                return content, True, []
            raw = re.sub(r"```json|```", "", raw).strip()
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            raw = m.group(0) if m else raw
            scorecard = json.loads(raw)
        except json.JSONDecodeError as e:
            log(f"  review JSON parse failed: {e} -- skipping review", "WARN")
            return content, True, []
        except Exception as e:
            log(f"  review call failed: {e} -- skipping review", "WARN")
            return content, True, []
        passed = scorecard.get("pass", False)
        flags  = scorecard.get("flags", [])
        scores = scorecard.get("scores", {})
        log(f"  REVIEW {'PASS' if passed else 'FAIL'} | human_voice={scores.get('human_voice')} "
            f"warmth={scores.get('warmth')} readability={scores.get('readability')}")
        if flags:
            log(f"  FLAGS: {'; '.join(flags)}")
        if passed:
            return content, True, []
        instructions = scorecard.get("rewrite_instructions", "")
        if attempt < MAX_REVIEW_ATTEMPTS and instructions:
            log("  REWRITING based on editor feedback...")
            time.sleep(RPM_SLEEP)
            try:
                content = call_gemini(make_rewrite_prompt(title, keyword, content, instructions), api_key)
                if content.startswith("PIN_DESC:"):
                    _, _, content = content.partition("\n")
            except Exception as e:
                log(f"  WARN: rewrite call failed: {e}")
                return content, False, flags
        else:
            log(f"  REVIEW FAILED after {attempt} attempt(s) -- creating GitHub issue", "WARN")
            return content, False, flags
    return content, False, []


def create_github_issue(title: str, slug: str, flags: list) -> None:
    env = {**os.environ, "PATH": "/home/derek/bin:/usr/local/bin:/usr/bin:/bin", "GIT_TERMINAL_PROMPT": "0"}
    flag_text = "\n".join(f"- {f}" for f in flags) if flags else "- Review failed quality threshold"
    body = (
        f"## Article Quality Review Failed\n\n"
        f"**Article:** {title}\n**Slug:** `{slug}`\n"
        f"**Date:** {datetime.date.today().isoformat()}\n\n"
        f"### Flags\n{flag_text}\n\n"
        f"### Action Required\n"
        f"1. Review `_posts/` file\n2. Edit manually or re-run generator\n3. Close once published\n"
    )
    cmd = ["gh", "issue", "create", "--repo", GITHUB_REPO,
           "--title", f"[Review Required] {title}", "--body", body, "--assignee", GITHUB_ASSIGNEE]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True)
    log(f"  GITHUB ISSUE: {r.stdout.strip() if r.returncode == 0 else r.stderr[:80]}")


def front_matter(title: str, keyword: str, affiliate_url: str, slug: str,
                 species: str, category: str, description: str) -> str:
    today = datetime.date.today().isoformat()
    fm = (
        f'---\nlayout: post\ntitle: "{title}"\ndate: {today}\n'
        f'categories: [{category}]\nspecies: {species}\ntags: [{keyword}]\n'
        f'description: "{description}"\n'
    )
    if affiliate_url:
        fm += f'affiliate_url: "{affiliate_url}"\n'
    fm += '---\n'
    return fm


def append_to_sheet(title, article_url, description, image_url, species, slug, topical_sheet_key):
    if not GSHEETS_AVAILABLE:
        log("  WARN: gspread not installed, skipping sheet update"); return
    key_file = REPO_DIR / "happypet-sheets-key.json"
    if not key_file.exists():
        log("  WARN: happypet-sheets-key.json not found, skipping sheet update"); return
    try:
        if DOTENV_AVAILABLE:
            load_dotenv(Path.home() / ".env")
        dog_id = os.getenv("HAPPYPET_SHEET_ID_DOGS")
        cat_id = os.getenv("HAPPYPET_SHEET_ID_CATS")
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds  = GCredentials.from_service_account_file(str(key_file), scopes=scopes)
        gc     = gspread.authorize(creds)
        pin_image_url = build_pin_image_url(slug)
        row    = [title, article_url, pin_image_url, description, "NO"]
        targets = []
        if species in ("dog", "both") and dog_id:
            targets.append(("Dogs", dog_id))
        if species in ("cat", "both") and cat_id:
            targets.append(("Cats", cat_id))
        sheet_key = topical_sheet_key or SLUG_TO_TOPICAL_SHEET_LEGACY.get(slug)
        if sheet_key:
            tid = os.getenv(sheet_key)
            if tid:
                targets.append((sheet_key.replace("HAPPYPET_SHEET_ID_", "").title(), tid))
        for label, sid in targets:
            gc.open_by_key(sid).get_worksheet(0).append_row(row)
            log(f"  SHEET: appended to {label} Pinterest Queue")
    except Exception as e:
        log(f"  WARN: sheet append failed: {e}")


def main() -> None:
    # Load .env first -- local runs need this; GHA already has env vars from secrets
    if DOTENV_AVAILABLE:
        load_dotenv(Path.home() / ".env")

    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()

    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text().strip()
        try:
            os.kill(int(old), 0)
            log(f"Already running (PID {old}). Exiting.", "WARN"); return
        except (OSError, ValueError):
            log(f"Stale lock (PID {old}) -- clearing", "WARN"); LOCK_PATH.unlink()
    LOCK_PATH.write_text(str(os.getpid()))

    try:
        if not gemini_key:
            log("GEMINI_API_KEY not set", "ERROR"); return

        # Load products -- also registers categories into SLUG_CATEGORIES
        products = load_products()
        log(f"Loaded products.json: {len(products)} entries")

        # TOPICS entirely from products.json -- no hardcoded list
        topics = [
            (p["topic"], p["title"], p["keyword"], p["format"])
            for p in products.values()
            if all(k in p for k in ("topic", "title", "keyword", "format"))
        ]
        log(f"Topics from products.json: {len(topics)}")

        # Startup validation pass -- warn on missing fields before any API calls
        for slug, p in products.items():
            errors = validate_product(slug, p)
            if errors:
                log(f"  VALIDATION WARN [{slug}]: {'; '.join(errors)}", "WARN")

        used_slugs = build_used_slugs()
        log(f"Dedup: {len(used_slugs)} slugs already published")

        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today     = datetime.date.today().isoformat()
        generated = skipped = failed = held = 0
        log(f"START v15 -- {len(topics)} topics -- model={MODEL} reviewer={'ON' if REVIEWER_ENABLED else 'OFF'}")

        for i, (slug, title, keyword, fmt) in enumerate(topics, 1):
            if slug in used_slugs:
                log(f"SKIP [{i}/{len(topics)}] {slug} -- already published"); skipped += 1; continue

            product = products.get(slug, {})

            # Pre-publish validation gate -- hold before spending any API tokens
            errors = validate_product(slug, product)
            if errors:
                log(f"HOLD [{i}/{len(topics)}] {slug} -- {'; '.join(errors)}")
                held += 1; continue

            category          = product.get("category", "pet-accessories")
            species           = product.get("species", "both")
            topical_sheet_key = product.get("topical_sheet", "")

            log(f"  Product: {product['name']}")
            log(f"WRITE [{i}/{len(topics)}] [{fmt}] {title}")
            time.sleep(RPM_SLEEP)

            # Dynamic internal link resolved from live _posts/
            related_url, related_anchor = find_related_published_slug(slug, category)

            try:
                prompt  = make_prompt(title, keyword, slug, fmt, product, related_url, related_anchor)
                content = call_gemini(prompt, gemini_key)

                pin_desc = f"{title} - expert reviews and buying guide."
                if content.startswith("PIN_DESC:"):
                    first_line, _, content = content.partition("\n")
                    pin_desc = first_line.replace("PIN_DESC:", "").strip()
                    log(f"  PIN_DESC: {pin_desc[:60]}")
                if len(content) < 2000:
                    log(f"  only {len(content)} chars -- may be truncated", "WARN")

                time.sleep(RPM_SLEEP)
                content, review_passed, review_flags = review_and_rewrite(title, keyword, content, gemini_key)
                if not review_passed:
                    create_github_issue(title, slug, review_flags)
                    log(f"  HOLD {slug} -- quality check failed, GitHub issue created")
                    held += 1; continue

                fname = f"{today}-{slugify(slug)}.md"
                fpath = POSTS_DIR / fname
                fm    = front_matter(title, keyword, product.get("affiliate_url", ""),
                                     slug, species, category, pin_desc)
                fpath.write_text(fm + "\n" + content, encoding="utf-8")
                log(f"  SAVED {fname} ({fpath.stat().st_size} bytes)")

                article_url = build_url(slug, utm=True)
                pin_url     = product.get("image", "")
                if PIN_GEN_AVAILABLE:
                    try:
                        pin_url = make_pin_for_post(title, pin_desc, pin_url, category, slug, generated)
                        log(f"  PIN: {pin_url}")
                    except Exception as pe:
                        log(f"  pin generation failed: {pe}", "WARN")

                # Stage pin data for autopublish.sh -> push_pins_to_sheets.py
                pin_queue_dir = REPO_DIR / "_pin_queue"
                pin_queue_dir.mkdir(exist_ok=True)
                pin_data = {
                    "title": title, "article_url": article_url, "description": pin_desc,
                    "image_url": pin_url, "species": species, "slug": slug,
                    "topical_sheet": topical_sheet_key,
                }
                (pin_queue_dir / f"{slug}.json").write_text(json.dumps(pin_data, indent=2))
                log(f"  QUEUE: staged pin data -> {slug}.json")

                generated += 1
                used_slugs.add(slug)

            except Exception as exc:
                log(f"  FAIL: {exc}", "ERROR"); failed += 1

            if i < len(topics):
                log(f"  Waiting {INTER_DELAY // 60}min...")
                time.sleep(INTER_DELAY)

        log(f"DONE -- {generated} written, {skipped} skipped, {held} held, {failed} failed")

    finally:
        if LOCK_PATH.exists(): LOCK_PATH.unlink()


if __name__ == "__main__":
    main()
