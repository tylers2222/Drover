#!/usr/bin/env bash
# Usage: ./current_pane.sh
#
# Who am I. Prints the pane this script is running in:
#   {pane_id, workspace_id, tab_id, cwd, agent, agent_status}
# The orchestrator uses workspace_id for list_panes and pane_id as the parent
# that spawn_pane splits from.
set -euo pipefail

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

herdr pane current | jq '.result.pane | {
  pane_id,
  workspace_id,
  tab_id,
  cwd: .foreground_cwd,
  agent: (.agent // null),
  agent_status
}'
