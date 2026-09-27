#!/usr/bin/env bash
# Usage: ./wait_pane.sh <pane_id> [until] [timeout_ms]
#   pane_id     pane id (wJ:p3) or agent name
#   until       idle | working | blocked | done | unknown   → agent lifecycle state
#               anything else                              → regex matched against
#                                                            the pane's output
#               default: idle
#   timeout_ms  fail after this long; default 120000 (2 min)
#
# Blocks until the condition holds, then prints
#   {pane_id, until, agent_status}   for a state wait
#   {pane_id, until, matched_line}   for an output wait
# Timeout → herdr's {"error":{"code":"timeout",...}} on stderr, exit 1.
#
# Use a state wait for agent panes (after spawn_pane, after write_to_pane ask).
# Use an output wait for plain-command panes, which never report agent state.
# Output waits search the scrollback, which includes the ECHOED COMMAND LINE.
# `READY` would match `echo READY` the instant it is typed; anchor the pattern
# to the produced line instead: `^READY$`.

pane_id="$1"
until="${2:-idle}"
timeout_ms="${3:-120000}"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

case "$timeout_ms" in
  ''|*[!0-9]*)
    echo "Invalid timeout_ms '$timeout_ms', expected a positive integer" >&2
    exit 1
    ;;
esac

case "$until" in
  idle|working|blocked|done|unknown)
    # A freshly spawned agent pane is not an "agent" until its integration
    # reports in, and `agent wait` fails instantly with agent_not_found
    # instead of waiting. Poll through that window until the deadline.
    # herdr writes error JSON to stderr; capture both streams to read .error.code.
    deadline=$(( SECONDS + (timeout_ms + 999) / 1000 ))
    while :; do
      remaining_ms=$(( (deadline - SECONDS) * 1000 ))
      [ "$remaining_ms" -gt 0 ] || remaining_ms=1
      if out="$(herdr agent wait "$pane_id" --until "$until" --timeout "$remaining_ms" 2>&1)"; then
        break
      fi
      code="$(jq -r '.error.code // empty' <<<"$out")"
      if [ "$code" != agent_not_found ] || [ "$SECONDS" -ge "$deadline" ]; then
        echo "$out" >&2
        exit 1
      fi
      sleep 0.25
    done
    jq --arg until "$until" \
      '{pane_id: .result.agent.pane_id, until: $until, agent_status: .result.agent.agent_status}' \
      <<<"$out"
    ;;
  *)
    # herdr's error JSON already goes to stderr; nothing to forward.
    out="$(herdr pane wait-output "$pane_id" --regex "$until" --source recent-unwrapped --timeout "$timeout_ms")"
    jq --arg until "$until" \
      '{pane_id: .result.pane_id, until: $until, matched_line: .result.matched_line}' \
      <<<"$out"
    ;;
esac
