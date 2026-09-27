#!/usr/bin/env bash
# Usage: ./spawn_pane.sh [--workspace] [--wait[=<until>]] <command> [cwd] [KEY=VALUE ...]
#   --workspace  put the pane in a NEW workspace instead of beside the caller
#   --wait       block until the pane is ready before returning. Bare --wait
#                waits for agent state idle; --wait=<until> takes anything
#                wait_pane.sh accepts (a state, or an anchored output regex
#                such as '^READY$' for plain commands). 2 min timeout.
#   command      shell line to run in the new pane (quote it as one argument)
#   cwd          working directory; defaults to the caller's $PWD. Pass "." to
#                keep the default when you only want to add env vars.
#   KEY=VALUE    zero or more env vars exported into the pane's shell
#
# Default: split the pane this script runs in, so children land in the
# orchestrator's own workspace. --workspace: one workspace per pane, the
# heavier isolation boundary (and a separate entry in the sidebar).
# Prints {workspace, pane_id, cwd, command, mode, ready}; ready is null
# without --wait, otherwise wait_pane's result. Use pane_id with
# read_pane / write_to_pane / get_pane_status / kill_pane.
set -euo pipefail

mode=split
wait_until=""
while [ $# -gt 0 ]; do
  case "$1" in
    --workspace)  mode=workspace ;;
    --wait)       wait_until=idle ;;
    --wait=*)     wait_until="${1#--wait=}" ;;
    *) break ;;
  esac
  shift
done

command="$1"
cwd="${2:-$PWD}"
shift "$(( $# < 2 ? $# : 2 ))"   # remaining args are env pairs

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

# herdr resolves --cwd server-side, so hand it an absolute path.
cwd="$(cd "$cwd" && pwd)"

# Collect --env KEY=VALUE flags; reject anything without an '='.
env_args=()
for pair in "$@"; do
  case "$pair" in
    *=*) env_args+=(--env "$pair") ;;
    *)
      echo "Invalid env pair '$pair', expected KEY=VALUE" >&2
      exit 1
      ;;
  esac
done

# split  → `pane split --current` returns .result.pane
# workspace → `workspace create` returns .result.root_pane
# Both pane objects carry pane_id and workspace_id, so one jq path serves both.
if [ "$mode" = split ]; then
  created="$(herdr pane split --current --direction right --cwd "$cwd" ${env_args[@]+"${env_args[@]}"} --no-focus \
    | jq '.result.pane')"
else
  label="$(basename "${command%% *}")"   # first word of the command
  created="$(herdr workspace create --cwd "$cwd" --label "$label" ${env_args[@]+"${env_args[@]}"} --no-focus \
    | jq '.result.root_pane')"
fi
pane_id="$(jq -r '.pane_id' <<<"$created")"
ws_id="$(jq -r '.workspace_id' <<<"$created")"

# `pane run` = send-text + Enter. Silent on success. If it fails, close the
# pane so a broken spawn never leaves an orphan behind (closing a workspace's
# only pane also removes the workspace).
if ! herdr pane run "$pane_id" "$command"; then
  herdr pane close "$pane_id" >/dev/null
  echo "{\"error\":\"run_failed\",\"pane_id\":\"$pane_id\"}" >&2
  exit 1
fi

# --wait: hand off to wait_pane.sh so there is one wait implementation. On
# timeout the pane is left running (the command was sent; only readiness is
# unknown) and wait_pane's error goes to stderr with exit 1.
ready=null
if [ -n "$wait_until" ]; then
  ready="$("$(dirname "$0")/wait_pane.sh" "$pane_id" "$wait_until")"
fi

jq -n \
  --arg ws "$ws_id" --arg pane "$pane_id" --arg cwd "$cwd" --arg cmd "$command" --arg mode "$mode" \
  --argjson ready "$ready" \
  '{workspace: $ws, pane_id: $pane, cwd: $cwd, command: $cmd, mode: $mode, ready: $ready}'
