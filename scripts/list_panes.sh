#!/usr/bin/env bash
# Usage: ./list_panes.sh [workspace_id]
#   workspace_id   optional; restrict to one workspace (wK)
#
# Prints a JSON array, one compact object per pane:
#   {pane_id, workspace_id, tab_id, cwd, agent, agent_status, title}
# `agent` is null for a plain shell pane; agent_status is then "unknown".
set -euo pipefail

workspace_id="${1:-}"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

scope=()
if [ -n "$workspace_id" ]; then
  scope=(--workspace "$workspace_id")
fi

herdr pane list ${scope[@]+"${scope[@]}"} | jq '[
  .result.panes[] | {
    pane_id,
    workspace_id,
    tab_id,
    cwd,
    agent: (.agent // null),
    agent_status,
    title: (.terminal_title_stripped // null)
  }
]'
