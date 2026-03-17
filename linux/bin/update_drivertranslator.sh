#!/usr/bin/env bash
set -euo pipefail

# Pull latest from GitHub and restart service.

REPO_DIR_DEFAULT="/opt/drivertranslator"
SERVICE_NAME="drivertranslator.service"
BRANCH_DEFAULT="main"

usage() {
  cat <<'EOF'
update_drivertranslator.sh

Usage:
  sudo ./linux/bin/update_drivertranslator.sh --repo-dir /opt/drivertranslator --branch main

What it does:
- git fetch / git pull
- restarts drivertranslator.service
EOF
}

REPO_DIR="$REPO_DIR_DEFAULT"
BRANCH="$BRANCH_DEFAULT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir) REPO_DIR="$2"; shift 2;;
    --branch) BRANCH="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Must run as root (use sudo)." >&2
  exit 1
fi

cd "$REPO_DIR"
if [[ ! -d .git ]]; then
  echo "ERROR: $REPO_DIR is not a git repo" >&2
  exit 1
fi

git fetch --all --prune
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

systemctl daemon-reload
systemctl restart "$SERVICE_NAME"
systemctl --no-pager --full status "$SERVICE_NAME" || true

