#!/usr/bin/env bash
# Usage: ./write_to_pane.sh <target_pane> <user_prompt> <ask|steer> [inject]
#   target_pane        agent name or pane id hosting an agent
#   user_prompt        the prompt (ask) or a key like esc / enter / ctrl+c (steer)
#   user_prompt_type   ask | steer
#   inject             true = skip the busy check and write even if the agent is working
set -euo pipefail

target_pane="$1"
user_prompt="$2"
user_prompt_type="$3"
inject="${4:-false}"     # optional 4th arg; defaults to false when omitted

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

# ----------------------------------------------------------------------
# Exact match on the type. case is bash's switch; `ask|steer)` = either,
# `*)` = anything else. Substring matching would let "asking" through.
# ----------------------------------------------------------------------
case "$user_prompt_type" in
  ask|steer) ;;
  *)
    echo "No matching user_prompt_type, require: 'ask' or 'steer'" >&2
    exit 1
    ;;
esac

case "$inject" in
  true|false) ;;
  *)
    echo "Invalid inject variable passed, pass true or false" >&2
    exit 1
    ;;
esac

# ----------------------------------------------------------------------
# Unless the caller purposely wants to inject into a running agent, refuse
# to write while it is working or sitting at a dialog. `agent get` takes
# a name or pane id and returns JSON, so no loop over pane list is needed.
# ----------------------------------------------------------------------
if [ "$inject" = false ]; then
  target_pane_status="$(herdr agent get "$target_pane" | jq -r '.result.agent.agent_status')"
  case "$target_pane_status" in
    working|blocked)
      echo "{\"error\":\"agent_${target_pane_status}\"}" >&2
      exit 1
      ;;
  esac
fi

# ----------------------------------------------------------------------
# ask   → text + Enter through the agent surface.
#         A slash command never leaves idle, so no --wait; a question
#         starts a turn, so --wait until it settles.
# steer → a single key, for driving the harness UI.
# ----------------------------------------------------------------------
if [ "$user_prompt_type" = steer ]; then
  herdr agent send-keys "$target_pane" "$user_prompt"
elif [[ "$user_prompt" == /* ]]; then
  herdr agent prompt "$target_pane" "$user_prompt"
else
  herdr agent prompt "$target_pane" "$user_prompt" --wait
fi
