#!/bin/bash
# Installs Python dependencies so `python -m pytest` works in Claude Code
# cloud sessions. No-op on local machines.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"
python -m pip install --quiet --disable-pip-version-check \
  -r requirements.txt -r requirements-dev.txt
