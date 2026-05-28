#!/usr/bin/env bash
# install-deps.sh — Packer provisioner: bake all bootstrap_vm.sh Steps 1-2 deps
# into the AMI so first-boot doesn't apt-update, doesn't curl NodeSource, doesn't
# npm install. Run as root by Packer via `sudo bash`.
#
# Idempotent — re-running on an already-baked image is safe but pointless.
# Designed to fail fast; Packer aborts on non-zero exit.
set -euo pipefail

log() { printf '[install-deps] %s\n' "$*"; }

# ── Step 1: System deps ──
log "STEP 1: apt update + base packages"
apt-get update -qq
apt-get install -yqq --no-install-recommends \
  git \
  tmux \
  python3 \
  python3-pip \
  python3-yaml \
  curl \
  jq \
  unzip \
  ca-certificates

# ── Step 2: Node.js 20 via NodeSource ──
log "STEP 2: Node.js 20 (NodeSource)"
# Remove Ubuntu-default node 12 if present
apt-get remove -yqq nodejs libnode-dev libnode72 npm 2>/dev/null || true
apt-get autoremove -yqq 2>/dev/null || true
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -yqq nodejs
node --version
npm --version

# ── Step 3: Claude Code CLI ──
log "STEP 3: @anthropic-ai/claude-code (global npm install)"
npm install -g @anthropic-ai/claude-code
command -v claude
claude --version 2>/dev/null || true

# ── Step 4: AWS CLI v2 ──
log "STEP 4: AWS CLI v2"
if ! command -v aws >/dev/null 2>&1 || ! aws --version 2>&1 | grep -q 'aws-cli/2'; then
  TMPDIR="$(mktemp -d)"
  cd "${TMPDIR}"
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o aws-cli.zip
  unzip -q aws-cli.zip
  ./aws/install --update
  cd /
  rm -rf "${TMPDIR}"
fi
aws --version

# ── Step 5: uv (Python package manager — orchestrator + venv toolchain) ──
log "STEP 5: uv"
if ! command -v uv >/dev/null 2>&1; then
  # uv install script puts uv in /root/.cargo/bin by default; symlink to /usr/local/bin
  curl -fsSL https://astral.sh/uv/install.sh | sh
  # Find installed uv and symlink
  if [[ -x /root/.cargo/bin/uv ]]; then
    ln -sf /root/.cargo/bin/uv /usr/local/bin/uv
  elif [[ -x /root/.local/bin/uv ]]; then
    ln -sf /root/.local/bin/uv /usr/local/bin/uv
  fi
fi
uv --version

# ── Step 6: tmux defaults dir + readwrite paths sanity ──
# The systemd unit needs /tmp writable; ProtectSystem=strict in the unit makes
# / read-only otherwise. bootstrap_vm.sh ensures the operator's ~/.{aws,config,claude,cache}
# dirs exist at first boot; we don't pre-create those here because the operator
# user is determined per-launch.

log "install-deps.sh complete"
