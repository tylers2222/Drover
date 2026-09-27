#!/usr/bin/env bash
# Usage: ./kill_pane.sh <pane_id>
#   pane_id   pane id (wK:p1)
#
# Closes the pane and whatever process tree it hosts. For a --workspace spawn
# the pane is its workspace's only pane, so closing it also removes the
# workspace — no separate `workspace close` needed. Run arrange_grid.sh
# afterwards to re-tile the survivors.
# Prints {"pane_id": ..., "closed": true}; unknown pane → herdr's error JSON
# on stderr, exit 1.
set -euo pipefail

pane_id="$1"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

herdr pane close "$pane_id" >/dev/null

jq -n --arg pane "$pane_id" '{pane_id: $pane, closed: true}'
