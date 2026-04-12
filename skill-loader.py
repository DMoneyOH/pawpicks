#!/usr/bin/env python3
"""
skill-loader.py -- Load skill instructions into Claude Desktop context.
Usage:
  python3 skill-loader.py <skill-name> [skill-name2 ...]
  python3 skill-loader.py --list
  python3 skill-loader.py --tasks
  python3 skill-loader.py --task <type>
  python3 skill-loader.py --happypet
  python3 skill-loader.py --auto        ← NEW: infers tasks from NEXT PRIORITIES

Examples:
  python3 skill-loader.py jekyll-affiliate seo-fundamentals
  python3 skill-loader.py --task article_writing
  python3 skill-loader.py --happypet
  python3 skill-loader.py --auto
"""

import os, sys, re
from pathlib import Path

SKILLS_DIR      = '/home/derek/.claude/skills'
SESSION_CONTEXT = '/home/derek/Projects/HappyPet/.session-context.md'
OUT_PATH        = '/tmp/happypet-skills-context.md'

# Core skills always loaded regardless of task — useful in every session
CORE_SKILLS = [
    'systematic-debugging',
    'vibe-code-auditor',
    'python-pro',
]

# Task-type → skills map for autonomous selection
TASK_SKILL_MAP = {
    'article_writing':   ['beautiful-prose', 'avoid-ai-writing', 'professional-proofreader',
                          'content-creator', 'copywriting', 'jekyll-affiliate'],
    'article_review':    ['avoid-ai-writing', 'beautiful-prose', 'professional-proofreader',
                          'copy-editing', 'vibe-code-auditor'],
    'generator_scripts': ['python-pro', 'bash-pro', 'systematic-debugging',
                          'vibe-code-auditor'],
    'jekyll_site':       ['jekyll-affiliate', 'web-performance-optimization',
                          'schema-markup', 'seo-fundamentals'],
    'seo_work':          ['seo-audit', 'seo-fundamentals', 'programmatic-seo',
                          'schema-markup', 'web-performance-optimization'],
    'prompt_work':       ['llm-prompt-optimizer', 'systematic-debugging'],
    'pinterest_social':  ['social-content', 'copywriting'],
    'analytics':         ['analytics-tracking'],
    'code_review':       ['vibe-code-auditor', 'systematic-debugging'],
}

# Keyword → task type inference for --auto mode
# Keys are lowercase regex patterns; values are task types from TASK_SKILL_MAP
KEYWORD_TASK_MAP = [
    (r'rewrite|article|disclosure|writing|content|blog|post',   'article_writing'),
    (r'review|reviewer|proofreader|editorial|review_only',       'article_review'),
    (r'generator|generate|generate_posts|script|autopublish|'
     r'push_pins|cron|gha|migration|pipeline|deploy\b|stage',   'generator_scripts'),
    (r'commit|push|jekyll|build|layout|css|template|permalink',  'jekyll_site'),
    (r'seo|sitemap|schema|canonical|meta|search console',        'seo_work'),
    (r'prompt|reviewer prompt|make_prompt|rubric|scoring',       'prompt_work'),
    (r'pin|pinterest|ifttt|sheet|topical|board',                 'pinterest_social'),
    (r'ga4|analytics|google analytics|tracking',                 'analytics'),
    (r'audit|debug|syntax|fix|error|bug|fail',                   'code_review'),
]

# HappyPet preset — full load, used as fallback if --auto finds nothing
HAPPYPET_PRESET = [
    'jekyll-affiliate', 'python-pro', 'bash-pro',
    'seo-fundamentals', 'seo-audit', 'programmatic-seo',
    'schema-markup', 'web-performance-optimization',
    'copywriting', 'content-creator', 'beautiful-prose',
    'avoid-ai-writing', 'professional-proofreader',
    'llm-prompt-optimizer', 'systematic-debugging',
    'vibe-code-auditor', 'analytics-tracking', 'social-content',
]


def list_skills():
    skills = sorted([
        d for d in os.listdir(SKILLS_DIR)
        if os.path.isdir(os.path.join(SKILLS_DIR, d)) and not d.startswith('.')
    ])
    print(f"Available skills ({len(skills)}):\n")
    for s in skills:
        skill_md = os.path.join(SKILLS_DIR, s, 'SKILL.md')
        if os.path.exists(skill_md):
            with open(skill_md) as f:
                lines = f.readlines()
            desc = next((l.strip().replace('description:', '').strip().strip('"\'')
                        for l in lines if l.startswith('description:')), '')[:80]
            print(f"  {s:<35} {desc}")
    return skills


def load_skills(names, verbose=True):
    """Load skill markdown files, deduplicate, return combined content."""
    seen = []
    output = []
    for name in names:
        if name in seen:
            continue
        seen.append(name)
        skill_md = os.path.join(SKILLS_DIR, name, 'SKILL.md')
        if not os.path.exists(skill_md):
            print(f"  SKIP: {name} (not found)", file=sys.stderr)
            continue
        with open(skill_md) as f:
            content = f.read()
        # Strip YAML frontmatter, keep instructions
        if content.startswith('---'):
            end = content.find('\n---\n', 4)
            if end > 0:
                content = content[end+4:].strip()
        output.append(f"\n{'='*60}\n# SKILL: {name}\n{'='*60}\n{content}")
        if verbose:
            print(f"  LOADED: {name} ({len(content.split(chr(10)))} lines)")
    return '\n'.join(output)


def infer_tasks_from_priorities(verbose=True):
    """
    Read NEXT PRIORITIES from .session-context.md.
    Match keywords against KEYWORD_TASK_MAP.
    Return (matched_tasks, skill_list, priorities_text).
    """
    ctx = Path(SESSION_CONTEXT)
    if not ctx.exists():
        if verbose:
            print("  WARN: .session-context.md not found — falling back to --happypet",
                  file=sys.stderr)
        return [], HAPPYPET_PRESET, ""

    text = ctx.read_text()

    # Extract NEXT PRIORITIES block
    m = re.search(r'## NEXT PRIORITIES\n(.*?)(?=\n##|\Z)', text, re.DOTALL)
    priorities = m.group(1).strip() if m else text
    priorities_lower = priorities.lower()

    matched_tasks = []
    for pattern, task in KEYWORD_TASK_MAP:
        if re.search(pattern, priorities_lower):
            if task not in matched_tasks:
                matched_tasks.append(task)

    if not matched_tasks:
        if verbose:
            print("  WARN: No task keywords matched — falling back to --happypet",
                  file=sys.stderr)
        return [], HAPPYPET_PRESET, priorities

    # Build deduplicated skill list: core + matched task skills
    skill_list = list(CORE_SKILLS)
    for task in matched_tasks:
        for skill in TASK_SKILL_MAP.get(task, []):
            if skill not in skill_list:
                skill_list.append(skill)

    return matched_tasks, skill_list, priorities


def main():
    args = sys.argv[1:]

    if not args or '--help' in args:
        print(__doc__)
        return

    if '--list' in args:
        list_skills()
        return

    if '--tasks' in args:
        print("Task types available for --task <type>:\n")
        for task, skills in TASK_SKILL_MAP.items():
            print(f"  {task:<20} {', '.join(skills)}")
        return

    if '--task' in args:
        idx = args.index('--task')
        task_type = args[idx + 1] if idx + 1 < len(args) else None
        if not task_type or task_type not in TASK_SKILL_MAP:
            print(f"Unknown task type. Use --tasks to list options.")
            return
        skill_names = list(CORE_SKILLS)
        for s in TASK_SKILL_MAP[task_type]:
            if s not in skill_names:
                skill_names.append(s)
        print(f"Loading task preset '{task_type}' ({len(skill_names)} skills)...\n")
        content = load_skills(skill_names)
        with open(OUT_PATH, 'w') as f:
            f.write(f"# Active Skills — task: {task_type}\n")
            f.write(content)
        print(f"\nContext written to: {OUT_PATH}")
        print(f"Total chars: {len(content):,}")
        return

    if '--auto' in args:
        print("AUTO mode — inferring tasks from NEXT PRIORITIES...\n")
        matched_tasks, skill_names, priorities = infer_tasks_from_priorities(verbose=True)
        if matched_tasks:
            print(f"\n  Tasks inferred ({len(matched_tasks)}): {', '.join(matched_tasks)}")
            print(f"  Skills selected ({len(skill_names)}): {', '.join(skill_names)}\n")
        else:
            print("  Fallback: loading full --happypet preset\n")
        content = load_skills(skill_names)
        with open(OUT_PATH, 'w') as f:
            f.write(f"# Active Skills — auto: {', '.join(matched_tasks) or 'happypet fallback'}\n")
            f.write(f"# Skills: {', '.join(skill_names)}\n")
            f.write(content)
        print(f"\nContext written to: {OUT_PATH}")
        print(f"Total chars: {len(content):,}")
        return

    if '--happypet' in args:
        print(f"Loading HappyPet preset ({len(HAPPYPET_PRESET)} skills)...\n")
        content = load_skills(HAPPYPET_PRESET)
        skill_names = HAPPYPET_PRESET
    else:
        content = load_skills(args)
        skill_names = args

    with open(OUT_PATH, 'w') as f:
        f.write(f"# Active Skills Context\n# Loaded: {', '.join(skill_names)}\n")
        f.write(content)

    print(f"\nContext written to: {OUT_PATH}")
    print(f"Total chars: {len(content):,}")


if __name__ == '__main__':
    main()
