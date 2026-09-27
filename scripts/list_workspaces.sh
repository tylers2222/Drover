#!/usr/bin/env bash
# Usage: ./list_workspaces.sh
#
# Prints a JSON array, one object per workspace:
#   {workspace_id, label, pane_count, tab_count, agent_status, focused}
# agent_status is the workspace roll-up herdr shows in the sidebar.
set -euo pipefail

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

herdr workspace list | jq '[
  .result.workspaces[] | {
    workspace_id,
    label,
    pane_count,
    tab_count,
    agent_status,
    focused
  }
]'
