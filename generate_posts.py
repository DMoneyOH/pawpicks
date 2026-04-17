#!/usr/bin/env python3
"""
Happy Pet Product Reviews Generator v21.3 — Fact-check token fix: 1500 char input, 4096 max_tokens
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
LOG_PATH  = Path(__file__).parent / "LOGS" / f"HappyPet_{datetime.date.today().isoformat()}.log"
LOCK_PATH = Path("/tmp/happypet_gen.lock")
LOG_PATH.parent.mkdir(exist_ok=True)  # ensure LOGS/ exists

# --- Provider config ---
# Generator:  Gemini 2.5 Flash (primary)          -> OpenRouter gpt-oss-120b:free (fallback)
# Reviewer:   Groq qwen3-32b (primary)             -> OpenRouter gpt-oss-120b:free (fallback)
# Rewriter:   Groq llama-4-scout (primary)         -> OpenRouter gpt-oss-120b:free (fallback)
# Fact-check: Groq llama-3.1-8b-instant (primary)  -> Groq llama-3.3-70b-versatile (fallback)
GEMINI_MODEL         = "gemini-2.5-flash"
GEMINI_URL           = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
GROQ_URL             = "https://api.groq.com/openai/v1/chat/completions"
OPENROUTER_URL       = "https://openrouter.ai/api/v1/chat/completions"
OR_GEN_MODEL         = "openai/gpt-oss-120b:free"
REVIEWER_MODEL       = "qwen/qwen3-32b"
REVIEWER_FALLBACK    = "openai/gpt-oss-120b:free"
REVIEWER_ENABLED     = True
GROQ_REWRITE_MODEL   = "meta-llama/llama-4-scout-17b-16e-instruct"
REWRITE_FALLBACK     = "openai/gpt-oss-120b:free"
GROQ_FACTCHECK_MODEL = "llama-3.1-8b-instant"
FACTCHECK_FALLBACK   = "llama-3.3-70b-versatile"
OR_HEADERS_EXTRA     = {"HTTP-Referer": "https://happypetproductreviews.com", "X-Title": "HappyPetReviews"}
MAX_REVIEW_ATTEMPTS  = 3
INTER_DELAY          = 300
RPM_SLEEP            = 8
REVIEW_PRE_SLEEP     = 2
MAX_RETRIES          = 2
GITHUB_REPO      = "DMoneyOH/pawpicks"
GITHUB_ASSIGNEE  = "DMoneyOH"
SITE_BASE        = "https://happypetproductreviews.com"

# Banned phrases — apply to pin descriptions AND article body
# Keep in sync with WRITING STYLE rules in make_prompt()
BANNED_PHRASE_MAP = [
    (r'pet parents',          'dog owners'),
    (r'pet parent',           'dog owner'),
    (r'furry family members', 'dogs'),
    (r'furry family member',  'dog'),
    (r'furry family',         'dogs'),
    (r'furry friend',         'dog'),
    (r'fur babies',           'dogs'),
    (r'fur baby',             'dog'),
    (r'paw-some',             'great'),
    (r'put our paws',         'done the research'),
    (r'tail wagging',         'impressive'),
    (r'tail-wagging',         'impressive'),
]

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

# Permanent -- covers articles 1-10 which predate products.json. Do not delete.
SLUG_TO_TOPICAL_SHEET_STATIC = {
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
    with LOG_PATH.open('a') as f: f.write(line + chr(10))


def log_reviewer(msg: str, level: str = "INFO") -> None:
    line = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [REVIEWER]  [{level}]  {msg}"
    print(line, flush=True)
    with LOG_PATH.open('a') as f: f.write(line + chr(10))


def log_pin(msg: str, level: str = "INFO") -> None:
    line = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [PINGEN]    [{level}]  {msg}"
    print(line, flush=True)
    with LOG_PATH.open('a') as f: f.write(line + chr(10))



# ---------------------------------------------------------------------------
# Shared HTTP helper -- all provider calls route through here
# ---------------------------------------------------------------------------
def http_post(url, payload, headers, *, label, log_fn=None, timeout=60, retries=None,
              backoff_base=60, backoff_exp=False, passthrough_codes=frozenset()):
    if log_fn is None: log_fn = log
    if retries is None: retries = MAX_RETRIES
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method='POST')
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors='replace')
            if exc.code in passthrough_codes:
                raise RuntimeError(label + ' HTTP ' + str(exc.code) + ': ' + body[:200])
            if exc.code in (429, 502, 503):
                wait = backoff_base * (2 ** attempt if backoff_exp else attempt)
                log_fn('  ' + label + ' ' + str(exc.code) + ' attempt ' + str(attempt) + '/' + str(retries) + ' -- wait ' + str(wait) + 's', 'WARN')
                time.sleep(wait)
            else:
                raise RuntimeError(label + ' HTTP ' + str(exc.code) + ': ' + body[:200])
        except urllib.error.URLError as exc:
            log_fn('  ' + label + ' network error attempt ' + str(attempt) + ': ' + str(exc.reason), 'WARN')
            time.sleep(RPM_SLEEP * 2)
    raise RuntimeError(label + ' exhausted after ' + str(retries) + ' attempt(s)')


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9\-]", "", s.lower().replace(" ", "-"))


def build_url(slug: str, utm: bool = False) -> str:
    category = SLUG_CATEGORIES.get(slug, "pet-accessories")
    base = f"{SITE_BASE}/{category}/{slug}/"
    if utm:
        return base + "?utm_source=pinterest&utm_medium=social&utm_campaign=pin"
    return base


def clean_pin_desc(text: str) -> str:
    """Strip banned phrases from pin description before writing to queue."""
    for pattern, replacement in BANNED_PHRASE_MAP:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text.strip()


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
    for field in ("affiliate_url", "name", "species", "title", "keyword", "category", "format", "image"):
        if not product.get(field):
            errors.append(f"Missing required field: {field}")
    if product.get("image") == "NEEDS_IMAGE":
        errors.append("image URL not sourced (NEEDS_IMAGE) -- use SiteStripe to get the URL")
    return errors


def call_generator(prompt: str, api_key: str) -> str:
    """
    Generator call chain:
      1. Gemini 2.5 Flash (AI Studio) -- primary, 2 retries at 60s
      2. Groq llama-3.3-70b-versatile -- fallback, 2 retries at 60s
    Raises RuntimeError if both exhausted.
    """
    or_gen_key = os.environ.get("OPENROUTER_API_KEY", "").strip()

    # --- Tier 1: Gemini 2.5 Flash (native endpoint) ---
    # Uses x-goog-api-key header -- compatible with AQ. format AI Studio keys on free tier
    native_url = f"{GEMINI_URL}?key={api_key}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": 8192,
            "temperature": 0.75,
        },
    }).encode()
    headers = {"Content-Type": "application/json"}
    try:
        raw  = http_post(native_url, payload, headers, label="Gemini-2.5-Flash",
                         timeout=90, retries=2, backoff_base=60,
                         passthrough_codes=frozenset({400, 401, 403, 404}))
        data = json.loads(raw)
        content = data["candidates"][0]["content"]["parts"][0]["text"]
        tokens  = data.get("usageMetadata", {}).get("candidatesTokenCount", "?")
        finish  = data["candidates"][0].get("finishReason", "?")
        log(f"  API ok (Gemini 2.5 Flash native): {len(content)} chars, {tokens} tokens, finish={finish}")
        return content
    except RuntimeError as exc:
        log(f"  Gemini failed: {exc} -- failing over to OpenRouter", "WARN")

    # --- Tier 2: OpenRouter gpt-oss-120b:free ---
    if not or_gen_key:
        raise RuntimeError("Gemini exhausted and OPENROUTER_API_KEY not set for failover")

    log("  Failing over to OpenRouter gpt-oss-120b:free", "WARN")
    or_gen_payload = json.dumps({
        "model": OR_GEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8192,
        "temperature": 0.75,
    }).encode()
    or_gen_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {or_gen_key}",
        **OR_HEADERS_EXTRA,
    }
    raw     = http_post(OPENROUTER_URL, or_gen_payload, or_gen_headers, label="OR-Gen",
                        timeout=90, retries=2, backoff_base=60)
    data    = json.loads(raw)
    content = data["choices"][0]["message"]["content"]
    finish  = data["choices"][0].get("finish_reason", "?")
    tokens  = data.get("usage", {}).get("completion_tokens", "?")
    log(f"  API ok (OpenRouter gpt-oss-120b:free): {len(content)} chars, {tokens} tokens, finish={finish}")
    return content

def make_review_prompt(title: str, keyword: str, content: str) -> str:
    # Full article up to 15K chars — 70B handles full context; 15K safety ceiling.
    content_sample = content[:15000] if len(content) > 15000 else content
    return f"""You are a senior human editor for Happy Pet Product Reviews, a budget-focused pet product affiliate blog.
Your job is to score this article honestly and flag specific problems. Do not be generous — a 3 means acceptable, not good.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}

ARTICLE CONTENT:
---
{content_sample}
---

SCORING RUBRIC — use these definitions exactly, do not interpret loosely:

human_voice:
  5 = Reads like a real person sharing a genuine opinion. Has specific details, natural imperfection, a distinct point of view.
  4 = Mostly natural. Minor stiffness in 1-2 places but feels written by a person.
  3 = Neutral. Competent but generic — could have been written by anyone or anything.
  2 = Noticeably AI-patterned. Lists of features, generic transitions, no personality, no point of view.
  1 = Pure marketing copy or feature dump. No human presence at all.

warmth:
  5 = Reader feels like they're getting advice from a knowledgeable friend who owns pets.
  4 = Friendly but slightly distant. Warm intent but not fully personal.
  3 = Neutral. Informative but transactional — no warmth, no coldness.
  2 = Clinical or detached. Reads like a spec sheet.
  1 = Cold, robotic, or condescending.

readability:
  5 = Flows effortlessly. Varied sentence length, zero re-reading required, logical section order.
  4 = Good flow with 1-2 awkward spots or overly long sentences.
  3 = Readable but some sentences need a second pass or sections feel out of order.
  2 = Frequent long or convoluted sentences. Reader has to work.
  1 = Difficult to follow. Structural or sentence-level problems throughout.

accuracy:
  5 = All product claims are verifiable from Amazon listings or appropriately hedged ("many owners report...").
  4 = Mostly accurate. One minor unverified detail that is plausible.
  3 = Some claims are soft assertions — plausible but not grounded in anything specific.
  2 = Multiple unverified specs or fabricated details presented as fact.
  1 = Significant factual errors or clearly invented specifications.

PASS criteria (ALL must be true to pass):
  - human_voice >= 4
  - warmth >= 4
  - readability >= 3
  - accuracy >= 3
  - affiliate_link_present = true (amzn.to link present in content)
  - If this is a roundup article (has "Additional Picks" or "Comparison Table" or multiple product sections): alternative products MUST have specific, concrete descriptions (not vague filler like "many owners find" or "tends to work well"). Each alternative needs at least one real distinguishing detail. If alternatives are generic filler, set pass=false and flag it.

Return ONLY a single valid JSON object. No preamble, no markdown fences, no trailing text.
Begin with {{ and end with }}.

JSON format (exact — do not add or remove keys):
{{"pass": true, "scores": {{"human_voice": 4, "warmth": 4, "readability": 4, "accuracy": 4}}, "affiliate_link_present": true, "em_dash_count": 0, "ai_cliches_found": [], "flags": [], "rewrite_instructions": ""}}

Rules:
- rewrite_instructions must name exact sections and specific fixes if pass is false; empty string if pass is true
- flags must list each specific problem as a plain string; empty array if none
- em_dash_count must be the exact integer count of em dash characters (—) found in the article
- ai_cliches_found must list any detected clichés from: delve, it's worth noting, in conclusion, look no further, game-changer, comprehensive guide, navigate, put our paws, paw-some, tail wagging, furry family member, furry friend, furry companion, for good reason, we've all been there, we've been there, there's nothing quite like, nothing quite like, we've got you covered, without breaking the bank, our furry, in today's world, when it comes to, at the end of the day, we all know, as pet owners, as dog owners, as cat owners
- NOTE: ai_cliches_found is for logging only -- cliché presence does NOT cause a fail unless human_voice < 4. Flag them in ai_cliches_found but do not fail on clichés alone."""


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
    stars         = product.get("stars", "")
    review_count  = product.get("review_count", "")
    price         = product.get("price", "")
    link = ""
    if related_url and related_anchor:
        link = f"\nNaturally include this markdown link once where relevant: [{related_anchor}]({related_url})"
    affiliate_block = ""
    if product_name and affiliate_url:
        affiliate_block = (
            f"FEATURED PRODUCT: {product_name}\n"
            f"AFFILIATE LINK: {affiliate_url}\n"
            f"LINKING RULE:\n"
            f"- Always link {product_name} on its FIRST mention in the article.\n"
            f"- Always link {product_name} in the CLOSING call-to-action.\n"
            f"- For all mentions in between, link no more than 3 additional times.\n"
            f"- MAXIMUM 5 affiliate links total per article. Do not exceed this.\n"
            f"- All other mentions of {product_name} must be plain text, no link.\n"
            f"- LINK FORMAT: Always use the product name as anchor text: [{product_name}]({affiliate_url}). NEVER display the raw URL as text or as anchor text. NEVER write [{affiliate_url}]({affiliate_url}).\n"
        )
    if fmt == "single_review":
        structure = f"""ARTICLE FORMAT: In-depth single product review of {product_name}
STRUCTURE: Opening (100+ words) | Product Overview (H2) | What We Like (H2, 4-5 features) | What Could Be Better (H2, 2-3 honest drawbacks) | Real Owner Experiences (H2) | Who Should Buy This (H2) | Verdict (H2, 80+ words with affiliate link) | Star rating: **Our Rating: X/5**"""
    elif fmt == "roundup":
        # Build verified data block — only include fields we actually have
        verified_data = ""
        if stars:
            verified_data += f"  - Star rating : {stars}/5 (verified from Amazon)\n"
        if review_count:
            verified_data += f"  - Review count: {int(review_count):,} Amazon reviews\n"
        if price:
            verified_data += f"  - Price        : ${price} (verified from Amazon)\n"
        verified_block = ""
        if verified_data:
            verified_block = (
                f"VERIFIED PRODUCT DATA (use exactly as shown — do not alter or invent):\n"
                f"{verified_data}"
            )
        structure = f"""ARTICLE FORMAT: Roundup/comparison -- {title}

{verified_block}
STRUCTURE:
  Opening (100+ words)
  Quick Picks (H2)

  Featured Pick: {product_name} (H3, 80-100 words)
    - Reference the verified star rating and review count naturally in prose if available
    - Pros bullet list: 3-4 genuine strengths
    - Cons bullet list: 1-2 honest limitations
    - Include affiliate link per LINKING RULE above
    - Do not fabricate specs; hedge unverified claims ("many owners report..." / "tends to...")

  Additional Picks: Use ONLY these real products from web search (H3 each, 60-75 words)
    {{ALTERNATIVE_PRODUCTS}}
    - Write each as a single prose paragraph — NO bullet lists
    - Naturally include 1-2 genuine strengths AND 1-2 honest limitations  
    - DO NOT include star ratings, prices, specific specs, or fabricated statistics/percentages you cannot verify -- omit numbers entirely
    - Hedge unverified claims: "tends to...", "most owners find...", "works well for..."
    - DO NOT fabricate review data like "88% of owners reported..." -- if you don't have the number, don't include one
    - Use ONLY the products listed above — do not add or invent others

  Comparison Table (H2): Product | Best For | Price Range | Chew Time
    - Price Range: use $, $$, $$$ only — do not invent specific dollar amounts for additional picks
    - Chew Time: Quick (under 5 min) / Moderate (5-15 min) / Long (15+ min) -- estimate based on chew density and product type. For non-consumable products, use a relevant attribute instead of Chew Time.
    - Do NOT include a ratings column — only use verified ratings from product data above

  Buying Guide (H2, 150+ words)
  Closing (80+ words with affiliate link per LINKING RULE above)"""
    else:
        structure = f"""ARTICLE FORMAT: Buying guide -- {title}
STRUCTURE: Opening (100+ words) | What to Look For (H2, 5-6 key factors) | Our Top Pick {product_name} (H2, 100 words, affiliate link) | Common Mistakes to Avoid (H2, 3-4 pitfalls) | FAQ (H2, 4-5 real questions) | Closing (80+ words with affiliate link)"""
    return f"""You are a senior writer for Happy Pet Product Reviews, a trusted budget-focused pet product review blog.

Write a complete, publish-ready blog post. Title: "{title}". Focus keyword: "{keyword}".

{affiliate_block}
LENGTH: 950-1100 words of body content. Firm requirement. CRITICAL: Complete ALL sections before stopping. Do not stop early. Write every section in STRUCTURE completely.

{structure}

WRITING STYLE:
- Conversational, warm, authoritative -- like advice from a trusted friend who owns pets
- Vary sentence length. Short punchy sentences mixed with longer flowing ones.
- Use hyphens (-) for compound words and standard dashes where needed. NEVER use em dashes (—). Rewrite the sentence instead.
- MINIMIZE stock phrases -- avoid where possible: "delve", "it's worth noting", "in conclusion", "look no further", "game-changer", "comprehensive guide", "navigate", "we've all been there", "we've been there", "there's nothing quite like", "we've got you covered", "for good reason", "without breaking the bank", "in today's world", "when it comes to", "at the end of the day", "we all know", "as pet owners", "as dog owners", "as cat owners", "furry friend", "furry companion"
- Write warmly but avoid stock pet-blog phrases that signal AI copy: never use "paw-some", "put our paws", "tail wagging" as metaphor, "furry family member", "fur baby", "pet parent", or "furry friend". Use "dog owner" or "cat owner" instead of "pet parent". Natural warmth through genuine voice is encouraged -- forced wordplay is not.
- FACTS: Only state product specs you are certain of from the product listing. If unsure, hedge with: "many owners report...", "tends to...", or "according to Amazon reviews...". Never invent dimensions, materials, weight, compatibility claims, percentages, statistics, or any number you were not given. Do NOT fabricate reviewer percentages like "85% of owners said..." -- if you don't have the real number, don't include one.
- SECTION HEADINGS: Never start a section with "In conclusion" or "In summary". Use a specific, descriptive heading instead.
- OPENING: If it makes sense for the article topic, open with a specific relatable moment a dog or cat owner would instantly recognize. Show, don't tell. Be SPECIFIC -- name a real scenario, not a generic one.
  Good examples: "My dog chewed through a couch cushion on a 45-minute Zoom call." / "Our cat knocked the water bowl over three times in one week." / "I spent $40 on a toy my dog sniffed once and walked away from."
  Bad examples (NEVER write openings like these): "We've all been there - [generic scenario]..." (cliché opener) / "As a pet owner, you know how important it is to..." (filler) / "Dogs need mental stimulation to stay happy and healthy." (generic) / "Standing in the kitchen when suddenly..." (AI-template setup) / Any opening that starts with a vague scenario followed by a product pitch.
  If the article topic is purely practical (e.g. flea prevention, nutrition), a direct factual opening is fine -- do not force an anecdote.
- Use "{keyword}" naturally 4-6 times. Write in first person plural ("we tested", "we found", "we noticed").{link}

FORMAT: Return ONLY clean Markdown. No YAML. No preamble. Start writing immediately.
FIRST LINE must be: PIN_DESC: [one punchy sentence, max 20 words, Pinterest stop-scroll hook]
Then article body immediately after."""


def fact_check_alternatives(content: str, primary_product: str, groq_key: str) -> str:
    """Strip unverifiable stats from alternative product sections. Runs only on roundups."""
    # Truncate to 3000 chars to stay within llama-3.1-8b-instant input limit (prevents 413)
    content_fc = content[:1500] if len(content) > 1500 else content
    prompt = f"""You are a fact-checker for a pet product review blog. The article below has a FEATURED product ({primary_product}) with verified data, and ALTERNATIVE products with potentially fabricated statistics.

TASK: Review the ALTERNATIVE product sections (not the featured product) for two types of problems:

PART 1 -- Specific numbers: Find ALL specific numbers in alternative sections. This includes:
- Star ratings (e.g. "4.5-star rating")
- Review counts (e.g. "over 12,000 reviews")
- Percentages (e.g. "85% of reviewers", "reduce by up to 80%")
- Study counts (e.g. "over 20 clinical studies")
- Any other specific numerical claim

For EACH number found, replace it with hedged language:
- "4.5-star rating on Amazon" -> "strong ratings on Amazon"
- "85% of Amazon reviewers" -> "most Amazon reviewers"
- "over 12,000 Amazon reviews" -> "thousands of Amazon reviews"
- "reduce bad breath by up to 80%" -> "shown to significantly reduce bad breath"
- "over 20 clinical studies" -> "multiple clinical studies"

PART 2 -- Ingredient and mechanism claims: Check each specific ingredient or clinical mechanism named for an alternative product. Apply this rule:
- If you are CONFIDENT the ingredient is correct for that exact product (e.g. delmopinol in OraVet), keep it.
- If you are NOT CONFIDENT the ingredient is correct for that exact product, replace the specific claim with general language.
  Examples:
  - "contains chlorhexidine" (if uncertain) -> "uses an active antimicrobial system"
  - "formulated with zinc gluconate" (if uncertain) -> "formulated with active ingredients to support oral health"
  - When uncertain, describe the FUNCTION, not the specific ingredient.
- Never leave a specific ingredient claim that you cannot verify. General is always safer than wrong.

DO NOT change anything in the Featured Pick section.
DO NOT change any other content, structure, headings, links, or prose.
Return the COMPLETE article with only the flagged claims replaced.

ARTICLE:
{content_fc}"""

    payload = json.dumps({
        "model": GROQ_FACTCHECK_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4096,
        "temperature": 0.1,
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {groq_key}",
        "User-Agent": "Mozilla/5.0 (compatible; HappyPetReviews/1.0)",
    }

    try:
        raw     = http_post(GROQ_URL, payload, headers, label="FactCheck-8b",
                            timeout=60, retries=2, backoff_base=60)
        cleaned = json.loads(raw)["choices"][0]["message"]["content"]
        if len(cleaned) < len(content) * 0.85:
            log(f"  Fact-check output too short ({len(cleaned)} vs {len(content)}), keeping original", "WARN")
            return content
        log(f"  Fact-check ok: {len(content)} -> {len(cleaned)} chars")
        return cleaned
    except Exception as exc:
        log(f"  Fact-check primary failed: {exc} -- trying fallback", "WARN")
    # Fallback: llama-3.3-70b-versatile
    try:
        fc_prompt = json.loads(payload.decode())["messages"][0]["content"]
        fb_payload = json.dumps({
            "model": FACTCHECK_FALLBACK,
            "messages": [{"role": "user", "content": fc_prompt}],
            "max_tokens": 4096, "temperature": 0.1,
        }).encode()
        raw     = http_post(GROQ_URL, fb_payload, headers, label="FactCheck-70b",
                            timeout=60, retries=2, backoff_base=60)
        cleaned = json.loads(raw)["choices"][0]["message"]["content"]
        if len(cleaned) < len(content) * 0.85:
            log(f"  Fact-check fallback too short ({len(cleaned)} vs {len(content)}), keeping original", "WARN")
            return content
        log(f"  Fact-check fallback ok: {len(content)} -> {len(cleaned)} chars")
        return cleaned
    except Exception as exc:
        log(f"  Fact-check fallback failed: {exc} -- keeping original", "WARN")
        return content


def find_alternative_products(keyword: str, primary_product: str, groq_key: str, count: int = 3) -> str:
    """Find real alternative products. Primary: compound-mini (web-grounded). Fallback: 8b-instant."""
    prompt = f"Name the top {count} popular alternatives to {primary_product} for '{keyword}'. For each, provide: brand name, product name, and one sentence that includes a SPECIFIC differentiating feature (e.g. a key ingredient, a unique design element, or a specific use case it excels at). Be concrete, not vague. Return as a simple numbered list: Brand - Product Name: Description"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {groq_key}",
        "User-Agent": "Mozilla/5.0 (compatible; HappyPetReviews/1.0)",
    }
    
    # Tier 1: compound-mini (web-grounded)
    payload = json.dumps({
        "model": "groq/compound-mini",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.3,
    }).encode()
    
    try:
        raw     = http_post(GROQ_URL, payload, headers, label="AltSearch-compound-mini", timeout=60, retries=1)
        content = json.loads(raw)["choices"][0]["message"]["content"]
        log(f"  Found {count} alternatives via compound-mini (web-grounded)")
        return content
    except Exception as exc:
        log(f"  compound-mini failed: {exc} -- trying 8b-instant fallback", "WARN")
    
    # Tier 2: llama-3.1-8b-instant (training knowledge)
    payload = json.dumps({
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.3,
    }).encode()
    
    try:
        raw     = http_post(GROQ_URL, payload, headers, label="AltSearch-8b-instant", timeout=60, retries=1)
        content = json.loads(raw)["choices"][0]["message"]["content"]
        log(f"  Found {count} alternatives via 8b-instant fallback")
        return content
    except Exception as exc:
        log(f"  Alternative search failed on both models: {exc}", "WARN")
        return ""


def review_and_rewrite(title: str, keyword: str, content: str, api_key: str) -> tuple:
    """Returns (final_content, passed, flags)"""
    if not REVIEWER_ENABLED:
        return content, True, []
    for attempt in range(1, MAX_REVIEW_ATTEMPTS + 1):
        log_reviewer(f"  REVIEW attempt {attempt}/{MAX_REVIEW_ATTEMPTS}")
        log_reviewer(f"  REVIEW pre-sleep {REVIEW_PRE_SLEEP}s...")
        time.sleep(REVIEW_PRE_SLEEP)
        try:
            payload = json.dumps({
                "model": REVIEWER_MODEL,
                "messages": [{"role": "user", "content": make_review_prompt(title, keyword, content)}],
                "max_tokens": 2048,
                "temperature": 0.2,
            }).encode()
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "User-Agent": "Mozilla/5.0 (compatible; groq-python/0.9.0)",
            }
            # Reviewer-specific retry loop for 429 with model fallback
            raw = None
            review_models = [REVIEWER_MODEL, REVIEWER_FALLBACK]
            for model_idx, rev_model in enumerate(review_models):
                if raw is not None:
                    break
                if model_idx > 0:
                    log_reviewer(f"  Primary reviewer failed. Falling back to {rev_model}", "WARN")
                    payload = json.dumps({
                        "model": rev_model,
                        "messages": [{"role": "user", "content": make_review_prompt(title, keyword, content)}],
                        "max_tokens": 2048,
                        "temperature": 0.2,
                    }).encode()
                try:
                    raw_resp = http_post(GROQ_URL, payload, headers,
                                        label=f"Reviewer({rev_model})",
                                        log_fn=log_reviewer, timeout=60,
                                        backoff_base=30, backoff_exp=True)
                    raw = json.loads(raw_resp)["choices"][0]["message"]["content"].strip()
                except RuntimeError:
                    pass  # model exhausted -- outer loop tries next model
            if raw is None:
                log_reviewer("  review call failed after retries -- skipping review", "WARN")
                return content, True, []
            raw = re.sub(r"```json|```", "", raw).strip()
            raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            raw = m.group(0) if m else raw
            scorecard = json.loads(raw)
        except json.JSONDecodeError as e:
            log_reviewer(f"  review JSON parse failed: {e} -- skipping review", "WARN")
            return content, True, []
        except Exception as e:
            log_reviewer(f"  review call failed: {e} -- skipping review", "WARN")
            return content, True, []
        passed = scorecard.get("pass", False)
        flags  = scorecard.get("flags", [])
        scores = scorecard.get("scores", {})
        log_reviewer(f"  REVIEW {'PASS' if passed else 'FAIL'} | human_voice={scores.get('human_voice')} "
            f"warmth={scores.get('warmth')} readability={scores.get('readability')}")
        if flags:
            log_reviewer(f"  FLAGS: {'; '.join(flags)}")
        # Hard override: fabrication/accuracy flags always fail regardless of pass=true
        if passed and flags:
            accuracy_keywords = ("fabricat", "unverif", "invent", "statistic", "percentag",
                                  "specific number", "no source", "not verif", "made up",
                                  "cited", "claimed", "without source")
            if any(kw in " ".join(flags).lower() for kw in accuracy_keywords):
                log_reviewer(f"  OVERRIDE: pass forced to FAIL -- accuracy/fabrication flags detected", "WARN")
                passed = False
        if passed:
            return content, True, []
        instructions = scorecard.get("rewrite_instructions", "")
        if attempt < MAX_REVIEW_ATTEMPTS and instructions:
            # Attempt 1 rewrite: use original model (Gemini)
            # Attempt 2 rewrite: use failover model (Groq Llama 70B)
            if attempt == 1:
                log_reviewer("  REWRITING via Gemini (original model)...")
                time.sleep(RPM_SLEEP)
                try:
                    content = call_generator(make_rewrite_prompt(title, keyword, content, instructions), api_key)
                    if content.startswith("PIN_DESC:"):
                        _, _, content = content.partition("\n")
                except Exception as e:
                    log_reviewer(f"  WARN: Gemini rewrite failed: {e}")
                    return content, False, flags
            else:
                log_reviewer("  REWRITING via Groq llama-3.3-70b (failover model)...")
                time.sleep(RPM_SLEEP)
                groq_key_rewrite = os.environ.get("GROQ_API_KEY", "").strip()
                if not groq_key_rewrite:
                    log_reviewer("  WARN: GROQ_API_KEY not set, cannot failover rewrite")
                    return content, False, flags
                rw_prompt_text = make_rewrite_prompt(title, keyword, content, instructions)
                rw_content = None
                # Tier 1: Groq llama-4-scout
                try:
                    rw_payload = json.dumps({
                        "model": GROQ_REWRITE_MODEL,
                        "messages": [{"role": "user", "content": rw_prompt_text}],
                        "max_tokens": 8192, "temperature": 0.7,
                    }).encode()
                    rw_headers = {"Content-Type": "application/json",
                                  "Authorization": f"Bearer {groq_key_rewrite}"}
                    raw_rw     = http_post(GROQ_URL, rw_payload, rw_headers,
                                           label="Rewriter-Groq", log_fn=log_reviewer,
                                           timeout=90, retries=2, backoff_base=60)
                    rw_content = json.loads(raw_rw)["choices"][0]["message"]["content"]
                    log_reviewer(f"  Groq rewrite ok: {len(rw_content)} chars")
                except Exception as e:
                    log_reviewer(f"  Groq rewrite failed: {e} -- trying OR fallback", "WARN")
                # Tier 2: OpenRouter gpt-oss-120b:free
                if not rw_content:
                    try:
                        or_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
                        rw_or_payload = json.dumps({
                            "model": REWRITE_FALLBACK,
                            "messages": [{"role": "user", "content": rw_prompt_text}],
                            "max_tokens": 8192, "temperature": 0.7,
                        }).encode()
                        rw_or_headers = {"Content-Type": "application/json",
                                         "Authorization": f"Bearer {or_key}",
                                         **OR_HEADERS_EXTRA}
                        raw_rw     = http_post(OPENROUTER_URL, rw_or_payload, rw_or_headers,
                                               label="Rewriter-OR", log_fn=log_reviewer,
                                               timeout=90, retries=2, backoff_base=60)
                        rw_content = json.loads(raw_rw)["choices"][0]["message"]["content"]
                        log_reviewer(f"  OR rewrite ok: {len(rw_content)} chars")
                    except Exception as e:
                        log_reviewer(f"  WARN: OR rewrite failed: {e}")
                        return content, False, flags
                content = rw_content
                if content.startswith("PIN_DESC:"):
                    _, _, content = content.partition("\n")
        else:
            log_reviewer(f"  REVIEW FAILED after {attempt} attempt(s) -- creating GitHub issue", "WARN")
            return content, False, flags
    return content, False, []


def create_github_issue(title: str, slug: str, flags: list) -> None:
    env = {**os.environ, "PATH": "/home/derek/bin:/usr/local/bin:/usr/bin:/bin", "GIT_TERMINAL_PROMPT": "0", "GH_TOKEN": os.environ.get("GITHUB_TOKEN", os.environ.get("GH_TOKEN", ""))}
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
    log_reviewer(f"  GITHUB ISSUE: {r.stdout.strip() if r.returncode == 0 else r.stderr[:80]}")


def front_matter(title: str, keyword: str, affiliate_url: str, slug: str,
                 species: str, category: str, description: str, image: str = "") -> str:
    today = datetime.date.today().isoformat()
    fm = (
        f'---\nlayout: post\ntitle: "{title}"\ndate: {today}\n'
        f'categories: [{category}]\nspecies: {species}\ntags: [{keyword}]\n'
        f'description: "{description}"\n'
    )
    if affiliate_url:
        fm += f'affiliate_url: "{affiliate_url}"\n'
    if image:
        fm += f'image: "{image}"\n'
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
        sheet_key = topical_sheet_key or SLUG_TO_TOPICAL_SHEET_STATIC.get(slug)
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

    # Reduce inter-article delay on multi-article runs to stay within Stage 1 timeout
    global INTER_DELAY
    if int(os.environ.get("MAX_ARTICLES", "1")) > 1:
        INTER_DELAY = 120
        log("Multi-article run detected -- INTER_DELAY reduced to 120s")

    groq_key = os.environ.get("GROQ_API_KEY", "").strip()

    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text().strip()
        try:
            os.kill(int(old), 0)
            log(f"Already running (PID {old}). Exiting.", "WARN"); return
        except (OSError, ValueError):
            log(f"Stale lock (PID {old}) -- clearing", "WARN"); LOCK_PATH.unlink()
    LOCK_PATH.write_text(str(os.getpid()))

    try:
        if not groq_key:
            log("GROQ_API_KEY not set", "ERROR"); return

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

        max_articles = int(os.environ.get("MAX_ARTICLES", "999"))
        topics = topics[:max_articles]
        log(f"Cap: {max_articles} -- {len(topics)} topic(s) queued this run")

        POSTS_DIR.mkdir(parents=True, exist_ok=True)
        today     = datetime.date.today().isoformat()
        generated = skipped = failed = held = 0
        log(f"START v21.3 -- {len(topics)} topics -- generator={GEMINI_MODEL} reviewer={'ON' if REVIEWER_ENABLED else 'OFF'}")

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
            _t0 = time.monotonic()
            time.sleep(RPM_SLEEP)

            # Dynamic internal link resolved from live _posts/
            related_url, related_anchor = find_related_published_slug(slug, category)

            try:
                # For roundup, use Apify runner-ups from products.json if available,
                # otherwise fall back to Groq Compound
                alternatives_text = ""
                if fmt == "roundup":
                    product_name = product.get("name", "")
                    runners_up = product.get("runners_up", "")
                    if runners_up:
                        alternatives_text = runners_up
                        log(f"  Alternatives: using Apify runner-ups from products.json")
                    else:
                        alternatives_text = find_alternative_products(keyword, product_name, groq_key, count=3)
                        log(f"  Alternatives: Groq fallback (no runners_up in products.json)")
                
                prompt  = make_prompt(title, keyword, slug, fmt, product, related_url, related_anchor)
                
                # Inject alternatives into roundup prompt
                if alternatives_text:
                    prompt = prompt.replace("{{ALTERNATIVE_PRODUCTS}}", alternatives_text)
                else:
                    prompt = prompt.replace("{{ALTERNATIVE_PRODUCTS}}", "(Search unavailable - use well-known brands)")
                
                content = call_generator(prompt, groq_key)
                log(f"  [timing] generate: {time.monotonic()-_t0:.1f}s")

                pin_desc = f"{title} - expert reviews and buying guide."
                if content.startswith("PIN_DESC:"):
                    first_line, _, content = content.partition("\n")
                    pin_desc = first_line.replace("PIN_DESC:", "").strip()
                    log_pin(f"  PIN_DESC: {pin_desc[:60]}")
                pin_desc = clean_pin_desc(pin_desc)
                if len(content) < 2000:
                    log(f"  only {len(content)} chars -- may be truncated", "WARN")

                # Fact-check: strip fabricated stats from alternative product sections (roundups only)
                if fmt == "roundup":
                    content = fact_check_alternatives(content, product.get("name", ""), groq_key)
                    log(f"  [timing] fact-check: {time.monotonic()-_t0:.1f}s")

                time.sleep(RPM_SLEEP)
                content, review_passed, review_flags = review_and_rewrite(title, keyword, content, groq_key)
                log(f"  [timing] review: {time.monotonic()-_t0:.1f}s")
                if not review_passed:
                    create_github_issue(title, slug, review_flags)
                    log(f"  HOLD {slug} -- quality check failed, GitHub issue created")
                    held += 1; continue

                fname = f"{today}-{slugify(slug)}.md"
                fpath = POSTS_DIR / fname
                fm    = front_matter(title, keyword, product.get("affiliate_url", ""),
                                     slug, species, category, pin_desc,
                                     product.get("image", ""))
                fpath.write_text(fm + "\n" + content, encoding="utf-8")
                log(f"  SAVED {fname} ({fpath.stat().st_size} bytes) -- total: {time.monotonic()-_t0:.1f}s")

                article_url = build_url(slug, utm=True)
                pin_url     = product.get("image", "")
                if PIN_GEN_AVAILABLE:
                    try:
                        pin_url = make_pin_for_post(title, pin_desc, pin_url, category, slug, generated)
                        log_pin(f"  PIN: {pin_url}")
                    except Exception as pe:
                        log_pin(f"  pin generation failed: {pe}", "WARN")

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