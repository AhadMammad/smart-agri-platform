#!/usr/bin/env bash
# PostToolUse hook: format a Python file the moment it is edited.
#
# Keeps every diff CI-clean without a format-then-check round trip, and stops
# `make fmt` from burying a real change in unrelated reformatting.
#
# Reads the tool payload as JSON on stdin and no-ops unless the edited path is
# a .py file inside etl/. Never fails the tool call: a formatting problem must
# not block an edit, and ruff would have reported it anyway.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

payload="$(cat)"

# python3 rather than jq: jq is not a dependency of this repo, python3 is.
file="$(printf '%s' "$payload" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))
except Exception:
    print("")
' 2>/dev/null)"

[ -n "$file" ] || exit 0
[ -f "$file" ] || exit 0
case "$file" in
  "$ROOT"/etl/*.py) ;;
  *) exit 0 ;;
esac

# .venv, caches and anything ruff is configured to skip.
case "$file" in
  *"/.venv/"*) exit 0 ;;
esac

uv --directory "$ROOT/etl" run ruff format --quiet "$file" >/dev/null 2>&1
uv --directory "$ROOT/etl" run ruff check --fix --quiet "$file" >/dev/null 2>&1

exit 0