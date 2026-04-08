#!/usr/bin/env python3
"""
review_only.py — Standalone one-time reviewer for a single article.
Reads article from _posts/, runs rule-based pre-screen, then Gemini 1.5 Flash review.
NO git, NO publish, NO sheet writes. Read + report only.

Usage: python3 review_only.py [slug]
Default slug: best-dog-crates
"""
import os, re, json, sys, time, urllib.request, urllib.error, datetime
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
REPO_DIR       = Path(__file__).parent.resolve()
REVIEWER_MODEL = "llama-3.3-70b-versatile"
GROQ_URL       = "https://api.groq.com/openai/v1/chat/completions"
MAX_RETRIES    = 3

BANNED_CLICHES = [
    "delve", "it's worth noting", "in conclusion", "look no further",
    "game-changer", "comprehensive guide", "navigate",
]
EM_DASH        = "\u2014"
AFFILIATE_PAT  = re.compile(r"amzn\.to/\S+")
DISCLOSURE_PAT = re.compile(r"(affiliate|commission|earn|sponsored)", re.IGNORECASE)

# ── Helpers ─────────────────────────────────────────────────────────────────
def banner(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def load_env() -> str:
    env_path = Path.home() / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        print("ERROR: GROQ_API_KEY not set in ~/.env")
        sys.exit(1)
    return key

def find_article(slug: str) -> tuple:
    """Returns (filepath, title, keyword, content) or exits."""
    matches = list(REPO_DIR.glob(f"_posts/*{slug}*.md"))
    if not matches:
        print(f"ERROR: No article found matching slug '{slug}'")
        sys.exit(1)
    fpath   = matches[0]
    raw     = fpath.read_text(encoding="utf-8")
    # Parse front matter
    title   = ""
    keyword = ""
    if raw.startswith("---"):
        parts = raw.split("---", 2)
        if len(parts) >= 3:
            fm = parts[1]
            for line in fm.splitlines():
                if line.startswith("title:"):
                    title = line.split(":", 1)[1].strip().strip('"')
                if line.startswith("keyword:") or line.startswith("description:"):
                    keyword = line.split(":", 1)[1].strip().strip('"')
            content = parts[2].strip()
        else:
            content = raw
    else:
        content = raw
    return fpath, title, keyword, content, raw

# ── Rule-based pre-screen ────────────────────────────────────────────────────
def pre_screen(content: str) -> tuple:
    """Returns (results_dict, cleaned_content)."""
    results = {"hard_fails": [], "warnings": [], "info": {}}

    # Word count
    words = len(content.split())
    results["info"]["word_count"] = words
    if words < 800:
        results["hard_fails"].append(f"Word count too low: {words} words (minimum 800)")

    # Affiliate link
    aff_matches = AFFILIATE_PAT.findall(content)
    results["info"]["affiliate_links"] = aff_matches
    if not aff_matches:
        results["hard_fails"].append("No affiliate link found (amzn.to missing)")
    elif len(aff_matches) > 5:
        results["warnings"].append(
            f"Too many affiliate links: {len(aff_matches)} found (maximum 5 — first, closing + 3 in between)"
        )

    # Disclosure — INFO only. Layout (post.html) handles FTC disclosure site-wide.
    # Do NOT hard-fail on missing disclosure in article body.
    disc_match = DISCLOSURE_PAT.search(content)
    results["info"]["disclosure_found"] = bool(disc_match)

    # Banned clichés
    found_cliches = [c for c in BANNED_CLICHES if c.lower() in content.lower()]
    results["info"]["cliches_found"] = found_cliches
    if found_cliches:
        results["hard_fails"].append(f"Banned clichés detected: {found_cliches}")

    # PIN_DESC in body — auto-strip silently. It's generator metadata only;
    # by review time it's already been saved to _pin_queue/. Remove it so it
    # never renders on the live site. Log the strip as info.
    if re.search(r'^PIN_DESC:.*$', content, re.MULTILINE):
        content = re.sub(r'^PIN_DESC:.*\n?', '', content, flags=re.MULTILINE).lstrip()
        results["info"]["pin_desc_stripped"] = True
    else:
        results["info"]["pin_desc_stripped"] = False

    # Em dash warning
    em_count = content.count(EM_DASH)
    results["info"]["em_dash_count"] = em_count
    if em_count > 0:
        results["warnings"].append(f"Em dash found {em_count} time(s) — consider replacing with commas or restructuring")

    return results, content

# ── Groq llama-3.3-70b-versatile reviewer ────────────────────────────────────
def call_reviewer(title: str, keyword: str, content: str, api_key: str) -> dict:
    prompt = f"""You are a senior human editor for Happy Pet Product Reviews, a budget-focused pet product affiliate blog.
A DIFFERENT AI (Gemini 2.5 Flash) wrote this article. Your job is to score it honestly and flag specific problems. Do not be generous — a 3 means acceptable, not good.

ARTICLE TITLE: {title}
FOCUS KEYWORD: {keyword}

FULL ARTICLE:
---
{content}
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
  - All four scores >= 3
  - affiliate_link_present = true (amzn.to link present in content)
  - No more than 1 ai_cliche detected

Return ONLY a single valid JSON object. No preamble, no markdown fences, no trailing text.
Begin with {{ and end with }}.

JSON format (exact — do not add or remove keys):
{{"pass": true, "scores": {{"human_voice": 4, "warmth": 4, "readability": 4, "accuracy": 4}}, "affiliate_link_present": true, "em_dash_count": 0, "ai_cliches_found": [], "flags": [], "rewrite_instructions": ""}}

Rules:
- rewrite_instructions must name exact sections and specific fixes if pass is false; empty string if pass is true
- flags must list each specific problem as a plain string; empty array if none
- em_dash_count must be the exact integer count of em dash characters (—) found in the article
- ai_cliches_found must list any detected clichés from: delve, it's worth noting, in conclusion, look no further, game-changer, comprehensive guide, navigate, put our paws, paw-some, tail wagging, furry family member"""

    payload = json.dumps({
        "model": REVIEWER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2048,
        "temperature": 0.2,
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "Mozilla/5.0 (compatible; groq-python/0.9.0)",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(GROQ_URL, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = json.loads(resp.read())["choices"][0]["message"]["content"].strip()
            # Strip markdown fences if present
            raw = re.sub(r"^```json\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            return json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                wait = 30 * (2 ** attempt)
                print(f"  429 rate limit — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                return {"error": f"HTTP {exc.code}: {exc.reason}"}
        except json.JSONDecodeError as exc:
            return {"error": f"JSON parse failed: {exc}", "raw": raw}
        except Exception as exc:
            return {"error": str(exc)}
    return {"error": "Failed after max retries"}

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    slug    = sys.argv[1] if len(sys.argv) > 1 else "best-dog-crates"
    api_key = load_env()

    banner(f"REVIEW ONLY — {slug}")
    print(f"Reviewer model : {REVIEWER_MODEL}")
    print(f"Article        : _posts/*{slug}*.md")
    print(f"Timestamp      : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load article
    fpath, title, keyword, content, raw = find_article(slug)
    print(f"\nFile           : {fpath.name}")
    print(f"Title          : {title}")
    print(f"Keyword        : {keyword}")

    # ── BEFORE: Pre-screen ──────────────────────────────────────────────────
    banner("BEFORE — Rule-Based Pre-Screen (Python, zero API calls)")
    ps, content = pre_screen(content)

    if ps['info'].get('pin_desc_stripped'):
        print("  ℹ PIN_DESC stripped from article body (generator metadata)")
    print(f"\n  Word count     : {ps['info']['word_count']}")
    print(f"  Affiliate links: {len(ps['info']['affiliate_links'])} found {ps['info']['affiliate_links'] or '— NONE'}")
    print(f"  Disclosure     : {'✓ found' if ps['info']['disclosure_found'] else '✗ MISSING'}")
    print(f"  Clichés found  : {ps['info']['cliches_found'] or 'none'}")
    print(f"  Em dashes      : {ps['info']['em_dash_count']}")

    if ps["hard_fails"]:
        print(f"\n  HARD FAILS ({len(ps['hard_fails'])}):")
        for f in ps["hard_fails"]:
            print(f"    ✗ {f}")
    else:
        print("\n  ✓ All hard checks passed")

    if ps["warnings"]:
        print(f"\n  WARNINGS ({len(ps['warnings'])}):")
        for w in ps["warnings"]:
            print(f"    ⚠ {w}")

    pre_screen_passed = len(ps["hard_fails"]) == 0

    # ── AFTER: AI Reviewer ──────────────────────────────────────────────────
    banner(f"AFTER — Groq {REVIEWER_MODEL} AI Review")

    if not pre_screen_passed:
        print("\n  ⛔ Pre-screen FAILED — skipping AI reviewer call")
        print("  Fix hard fails above before running AI review.")
    else:
        print("\n  Pre-screen passed. Calling Groq reviewer...")
        print("  (2s pre-sleep...)")
        time.sleep(2)
        result = call_reviewer(title, keyword, content, api_key)  # content is PIN_DESC-stripped

        if "error" in result:
            print(f"\n  ERROR from reviewer: {result['error']}")
            if "raw" in result:
                print(f"  Raw response: {result['raw'][:500]}")
        else:
            verdict = "✓ PASS" if result.get("pass") else "✗ FAIL"
            print(f"\n  Verdict        : {verdict}")
            scores  = result.get("scores", {})
            print(f"  Scores         :")
            for k, v in scores.items():
                bar = "█" * v + "░" * (5 - v)
                print(f"    {k:<20} {bar} {v}/5")
            em_dashes = result.get("em_dash_count", "n/a")
            print(f"  Em dashes (AI) : {em_dashes}")
            cliches = result.get("ai_cliches_found", [])
            if cliches:
                print(f"  Clichés found  : {', '.join(cliches)}")
            flags = result.get("flags", [])
            if flags:
                print(f"\n  Flags ({len(flags)}):")
                for f in flags:
                    print(f"    • {f}")
            else:
                print("\n  No flags raised.")
            instructions = result.get("rewrite_instructions", "")
            if instructions:
                print(f"\n  Rewrite instructions:\n    {instructions}")

    banner("DONE — no files written, no publish, no git")

if __name__ == "__main__":
    main()
