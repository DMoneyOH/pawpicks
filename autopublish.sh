#!/usr/bin/env bash
# autopublish.sh — Stage 2: git push → wait for live build → append pins to Google Sheets
# Cron: 0 7 * * 1,4  (Mon + Thu, 7:00 AM ET)  -- cron redirects stdout to /tmp/pawpicks.log
# Cadence: Monday = 2 articles, Thursday = 1 article
set -Eeuo pipefail

REPO_DIR="/home/derek/projects/pawpicks"
PATH="/home/derek/.local/bin:/home/derek/bin:/usr/local/bin:/usr/bin:/bin"
GH="/home/derek/bin/gh"
MAX_WAIT=600    # seconds to poll for build success (10 min)
POLL_INTERVAL=30

log()      { printf '%s [PUBLISHER] [INFO]  %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
log_warn() { printf '%s [PUBLISHER] [WARN]  %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
log_err()  { printf '%s [PUBLISHER] [ERROR] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

cd "$REPO_DIR"

if [[ -f "${HOME}/.env" ]]; then source "${HOME}/.env"; fi

log "START autopublish"

DOW=$(date '+%u')
if [[ "$DOW" == "1" ]]; then
    POST_CAP=2
    log "Monday run — publishing up to 2 articles"
elif [[ "$DOW" == "4" ]]; then
    POST_CAP=1
    log "Thursday run — publishing up to 1 article"
else
    log_warn "Running on unexpected day (DOW=${DOW}) — defaulting to cap of 1"
    POST_CAP=1
fi

mapfile -t ALL_NEW_POSTS < <(git ls-files --others --exclude-standard _posts/ | sort)

if [[ ${#ALL_NEW_POSTS[@]} -eq 0 ]]; then
    log "No new posts staged — nothing to publish"
    exit 0
fi

log "Found ${#ALL_NEW_POSTS[@]} staged post(s) — capping at ${POST_CAP}"

SELECTED_POSTS=("${ALL_NEW_POSTS[@]:0:$POST_CAP}")
HELD_COUNT=$(( ${#ALL_NEW_POSTS[@]} - ${#SELECTED_POSTS[@]} ))

for p in "${SELECTED_POSTS[@]}"; do log "  PUBLISHING: $p"; done
if [[ $HELD_COUNT -gt 0 ]]; then
    log "  HOLDING ${HELD_COUNT} post(s) for next scheduled run"
fi

SELECTED_SLUGS=()
for p in "${SELECTED_POSTS[@]}"; do
    fname=$(basename "$p" .md)
    slug=$(echo "$fname" | cut -d'-' -f4-)
    SELECTED_SLUGS+=("$slug")
done

git add "${SELECTED_POSTS[@]}"
for slug in "${SELECTED_SLUGS[@]}"; do
    pin_img="assets/images/pins/${slug}.jpg"
    [[ -f "$pin_img" ]] && git add "$pin_img"
done

git commit -m "auto: publish $(date '+%Y-%m-%d') [${#SELECTED_POSTS[@]} post(s)]" \
    || { log "Nothing to commit"; exit 0; }
git push origin main
log "PUSH complete — waiting for GitHub Pages build..."

ELAPSED=0
BUILD_OK=false
while [[ $ELAPSED -lt $MAX_WAIT ]]; do
    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))

    STATUS=$("$GH" run list --repo DMoneyOH/pawpicks --limit 1 \
        --json status,conclusion,displayTitle \
        --jq '.[0] | "\(.status)|\(.conclusion)"' 2>/dev/null || echo "error|error")

    RUN_STATUS=$(echo "$STATUS" | cut -d'|' -f1)
    RUN_CONCLUSION=$(echo "$STATUS" | cut -d'|' -f2)

    log "  Build check [${ELAPSED}s]: status=${RUN_STATUS} conclusion=${RUN_CONCLUSION}"

    if [[ "$RUN_STATUS" == "completed" && "$RUN_CONCLUSION" == "success" ]]; then
        BUILD_OK=true
        break
    elif [[ "$RUN_STATUS" == "completed" && "$RUN_CONCLUSION" != "success" ]]; then
        log_err "Build failed (conclusion=${RUN_CONCLUSION}) — aborting sheet append"
        log_warn "  Pin queue left intact for next run"
        exit 1
    fi
done

if [[ "$BUILD_OK" == "false" ]]; then
    log_err "Build did not complete within ${MAX_WAIT}s — aborting sheet append"
    exit 1
fi

log "Build confirmed live — appending pins to Google Sheets..."
SLUGS_ARG=$(IFS=','; echo "${SELECTED_SLUGS[*]}")
python3 "${REPO_DIR}/push_pins_to_sheets.py" --slugs "$SLUGS_ARG"

log "END autopublish"
