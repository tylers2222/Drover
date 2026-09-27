#!/usr/bin/env bash
# Usage: ./read_pane.sh <pane_id> [lines]
#   pane_id   pane id (wK:p1) or agent name
#   lines     how many trailing lines of scrollback; default 120
#
# Prints plain text (no JSON, no ANSI) — herdr's recent-unwrapped snapshot,
# so long lines are not broken at the terminal width.
set -euo pipefail

pane_id="$1"
lines="${2:-120}"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

case "$lines" in
  ''|*[!0-9]*)
    echo "Invalid lines '$lines', expected a positive integer" >&2
    exit 1
    ;;
esac

herdr pane read "$pane_id" --source recent-unwrapped --lines "$lines" --format text
