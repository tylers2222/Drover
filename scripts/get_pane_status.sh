#!/usr/bin/env bash
# Usage: ./get_pane_status.sh <pane_id>
#   pane_id   pane id (wK:p1)
#
# Merges `pane get` (agent lifecycle) with `pane process-info` (what the shell
# is actually running) into one object:
#   {pane_id, workspace_id, cwd, agent, agent_status, shell_pid,
#    foreground, shell_idle}
# agent_status  idle | working | blocked | unknown  (unknown = no agent, or
#               the agent kind has no integration installed)
# foreground    cmdline of the innermost foreground process, or null
# shell_idle    true when the foreground process group is the shell itself,
#               i.e. a spawned command has finished and the prompt is back.
#               This is the signal for non-agent panes, which never report
#               idle/working through agent_status.
set -euo pipefail

pane_id="$1"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

# Unknown pane → herdr writes {"error":{"code":"pane_not_found",...}} to
# stderr and exits 1; `set -e` stops us right there.
pane_json="$(herdr pane get "$pane_id")"
proc_json="$(herdr pane process-info --pane "$pane_id")"

jq -n --argjson pane "$pane_json" --argjson proc "$proc_json" '
  ($pane.result.pane) as $p
  | ($proc.result.process_info) as $pi
  | {
      pane_id: $p.pane_id,
      workspace_id: $p.workspace_id,
      cwd: $p.foreground_cwd,
      agent: ($p.agent // null),
      agent_status: $p.agent_status,
      shell_pid: $pi.shell_pid,
      foreground: ($pi.foreground_processes[0].cmdline // null),
      shell_idle: ($pi.foreground_process_group_id == $pi.shell_pid)
    }'
