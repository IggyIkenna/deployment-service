#!/usr/bin/env bash
# Audit all Dockerfiles in the workspace for unpinned base images.
#
# A "pinned" base image uses the digest form:
#   FROM image@sha256:<digest>
# An "unpinned" base image uses a mutable tag:
#   FROM image:latest
#   FROM image:3.11-slim
#
# Usage:
#   bash scripts/audit/dockerfile-base-pin.sh                 # audit (warn on violations)
#   bash scripts/audit/dockerfile-base-pin.sh --fail          # exit 1 on any violation
#   bash scripts/audit/dockerfile-base-pin.sh --workspace /x  # custom workspace root
#   bash scripts/audit/dockerfile-base-pin.sh --fix-hints     # print crane digest commands
#
# Exit code:
#   0 — all FROM lines are pinned (or no Dockerfiles found)
#   1 — violations found AND --fail is set
#
# QG integration (STEP 5.79):
#   Called from base-service.sh with --fail after 2026-05-15.
#
# Dev-only Dockerfiles (always skip — never production-bound):
#   Dockerfile.dev, Dockerfile.test, Dockerfile.local
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
FAIL_ON_VIOLATIONS=false
SHOW_FIX_HINTS=false

usage() {
    echo "Usage: $0 [--fail] [--workspace <path>] [--fix-hints]"
    echo ""
    echo "  --fail           Exit 1 on any unpinned FROM line"
    echo "  --workspace <p>  Workspace root (default: auto-detect)"
    echo "  --fix-hints      Print 'crane digest' commands to resolve pinned SHAs"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fail) FAIL_ON_VIOLATIONS=true; shift ;;
        --fix-hints) SHOW_FIX_HINTS=true; shift ;;
        --workspace) WORKSPACE_ROOT="$2"; shift 2 ;;
        --help|-h) usage ;;
        *) echo "Unknown arg: $1"; usage ;;
    esac
done

log() { echo "$(date '+%H:%M:%S') $*"; }

# Skip dev-only Dockerfile variants
_is_dev_dockerfile() {
    local path="$1"
    local base
    base=$(basename "$path")
    case "$base" in
        Dockerfile.dev|Dockerfile.test|Dockerfile.local|Dockerfile.ci) return 0 ;;
        *) return 1 ;;
    esac
}

VIOLATIONS=()
PINNED=()
SKIPPED=()

while IFS= read -r -d '' dockerfile; do
    [[ -f "$dockerfile" ]] || continue
    if _is_dev_dockerfile "$dockerfile"; then
        SKIPPED+=("$dockerfile")
        continue
    fi

    rel_path="${dockerfile#"$WORKSPACE_ROOT/"}"
    # Collect stage aliases defined in this Dockerfile (to skip multi-stage FROM aliases)
    # bash3-safe: store as newline-delimited string
    _stage_aliases=""
    while IFS= read -r line; do
        if echo "$line" | grep -qE '^FROM .* AS [a-zA-Z0-9_-]+$'; then
            alias_name=$(echo "$line" | awk '{print $NF}')
            _stage_aliases="${_stage_aliases}${alias_name}"$'\n'
        fi
    done < "$dockerfile"

    # Now check each FROM line
    while IFS= read -r line; do
        [[ "$line" =~ ^FROM ]] || continue
        # Strip --platform and other options
        clean="${line#FROM}"
        # Remove --platform=... --build-arg ... style flags
        clean=$(echo "$clean" | sed 's/--[a-zA-Z_-]*=[^ ]*//g' | sed 's/--[a-zA-Z_-]*//g' | xargs)
        # Strip ' AS alias' suffix
        image="${clean%% AS *}"
        image="${image%% as *}"
        image=$(echo "$image" | xargs)

        # Skip scratch
        [[ "$image" == "scratch" ]] && continue
        # Skip build ARG interpolations (${...})
        [[ "$image" == *'${'* ]] && continue
        # Skip local stage aliases from this file (bash3-safe grep check)
        if echo "$_stage_aliases" | grep -qxF "$image" 2>/dev/null; then
            continue
        fi
        # Skip if image name has no registry prefix and no colon/at (bare local alias)
        if [[ "$image" != *"."* && "$image" != *"/"* ]]; then
            continue
        fi

        if [[ "$image" == *"@sha256:"* ]]; then
            PINNED+=("$rel_path: $image")
        else
            VIOLATIONS+=("$rel_path: $image")
            if $SHOW_FIX_HINTS; then
                # Normalize image ref for crane
                img_ref="$image"
                [[ "$img_ref" != *":"* ]] && img_ref="${img_ref}:latest"
                echo "  crane digest $img_ref  # pin in $rel_path"
            fi
        fi
    done < "$dockerfile"
done < <(find "$WORKSPACE_ROOT" -name "Dockerfile" -o -name "Dockerfile.*" \
    -not -path "*/.venv*" -not -path "*/node_modules/*" -not -path "*/.git/*" \
    -not -path "*/build/*" -not -path "*/dist/*" \
    -print0 2>/dev/null)

log ""
log "=== Dockerfile base-image pin audit ==="
log "Workspace: $WORKSPACE_ROOT"
log ""
log "Pinned:     ${#PINNED[@]} FROM lines"
log "Violations: ${#VIOLATIONS[@]} unpinned FROM lines"
log "Skipped:    ${#SKIPPED[@]} dev-only Dockerfiles"
log ""

if [[ ${#VIOLATIONS[@]} -gt 0 ]]; then
    log "VIOLATIONS — unpinned base images (use @sha256:digest instead of :tag):"
    for v in "${VIOLATIONS[@]}"; do
        log "  UNPINNED  $v"
    done
    log ""
    if $SHOW_FIX_HINTS; then
        log "Run 'crane digest <image>' to get the pinned digest, then:"
        log "  sed -i 's|image:tag|image@sha256:...|g' Dockerfile"
    else
        log "Re-run with --fix-hints to see crane digest commands."
    fi
    log ""
    if $FAIL_ON_VIOLATIONS; then
        log "FAIL: $((${#VIOLATIONS[@]})) unpinned Dockerfile FROM lines (QG STEP 5.79)"
        exit 1
    else
        log "WARN: $((${#VIOLATIONS[@]})) unpinned FROM lines — ratchet fails from 2026-05-15 (--fail mode)"
    fi
else
    log "OK: all FROM lines are pinned with @sha256:digest"
fi
