#!/usr/bin/env bash
# autopublish.sh — Stage 2: git push → wait for live build → append pins to Google Sheets
# Cron: 0 7 * * 1,4  (Mon + Thu, 7:00 AM ET)
set -Eeuo pipefail

REPO_DIR="/home/derek/projects/pawpicks"
LOG_FILE="/tmp/pawpicks-autopublish.log"
PATH="/home/derek/.local/bin:/home/derek/bin:/usr/local/bin:/usr/bin:/bin"
GH="/home/derek/bin/gh"
MAX_WAIT=600   # seconds to poll for build success (10 min)
POLL_INTERVAL=30

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"; }

cd "$REPO_DIR"

# Load env
if [[ -f "${HOME}/.env" ]]; then source "${HOME}/.env"; fi

log "START autopublish"

# Check for new posts to publish
NEW_FILES=$(git status --porcelain | grep -c "^??" || true)
MODIFIED=$(git status --porcelain | grep -c "^M\|^ M" || true)

if [[ "$NEW_FILES" -eq 0 && "$MODIFIED" -eq 0 ]]; then
    log "No new or modified files — nothing to push"
    exit 0
fi

log "Staging and committing ${NEW_FILES} new + ${MODIFIED} modified file(s)..."
git add _posts/ assets/images/pins/
git commit -m "auto: content $(date '+%Y-%m-%d')" || { log "Nothing to commit"; exit 0; }
git push origin main
log "PUSH complete — waiting for GitHub Pages build..."

# Poll gh run list until build succeeds or timeout
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
        log "ERROR: Build failed (conclusion=${RUN_CONCLUSION}) — aborting sheet append"
        log "  Pin queue left intact for next run"
        exit 1
    fi
done

if [[ "$BUILD_OK" == "false" ]]; then
    log "ERROR: Build did not complete within ${MAX_WAIT}s — aborting sheet append"
    exit 1
fi

log "Build confirmed live — appending pins to Google Sheets..."
python3 "${REPO_DIR}/push_pins_to_sheets.py" 2>&1 | tee -a "$LOG_FILE"

log "END autopublish"
