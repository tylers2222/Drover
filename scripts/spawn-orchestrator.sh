set -euo pipefail

# ---------------------------------------------------------------------------
# 1. INJECTABLES = positional arguments. $1 is the first thing after the script
#    name, $2 the second, etc. Assigning them to names is just for readability.
# ---------------------------------------------------------------------------
project_dir="$1"     # e.g. services/inventory
agent_name="$2"      # e.g. inventory-orch   (herdr rule: [a-z][a-z0-9_-]{0,31}, unique)
kind="$3"            # e.g. omp | claude | codex  (run `herdr agent` to see the list)

# Try it: comment everything below out, run `./spawn-orchestrator.sh a b c`,
# then add  echo "$project_dir $agent_name"  and see the args land.

# ---------------------------------------------------------------------------
# 2. GUARD. Env vars are NOT arguments — they're inherited from the parent
#    process. Herdr sets HERDR_ENV=1 in every pane it owns. Outside a pane it's
#    unset, and `${HERDR_ENV:-}` means "the value, or empty if unset" (so -u
#    doesn't blow up). >&2 sends the message to stderr; exit 2 = "refused".
# ---------------------------------------------------------------------------
if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

# Try it:  printenv | grep HERDR   — those are the free injectables.
#          HERDR_ENV= ./spawn-orchestrator.sh a b c   — forces the guard to trip.

# ---------------------------------------------------------------------------
# 3. CREATE THE WORKSPACE. $( ... ) runs a command and captures its stdout into
#    a variable. herdr returns JSON, so the variable now holds a JSON string.
#    One workspace per project — that's the process-isolation boundary.
# ---------------------------------------------------------------------------
ws_json="$(herdr workspace create --cwd "$project_dir" --label "$agent_name" --no-focus)"

# Try it by hand first:  herdr workspace create --cwd . --label play --no-focus
# Read the JSON it prints. Clean up with:  herdr workspace close <id from the JSON>

# ---------------------------------------------------------------------------
# 4. PULL IDS OUT OF THE JSON. jq is a JSON query tool. -r = raw string, no quotes.
#    <<<"$var" feeds the variable to jq as stdin ("here-string").
#    Field names below are guesses from the docs — verify against the real output
#    from step 3 and fix them here. This is the exact spot the model used to guess.
# ---------------------------------------------------------------------------
pane_id="$(jq -r '.result.root_pane.pane_id' <<<"$ws_json")"
ws_id="$(jq -r '.result.workspace.workspace_id' <<<"$ws_json")"

# Try it:  herdr workspace list | jq .          — see the shape
#          herdr workspace list | jq '.result'  — walk down one level at a time

# ---------------------------------------------------------------------------
# 5. START AN AGENT IN THAT PANE. Returns only once herdr sees the agent ready
#    for input. >/dev/null throws away stdout because we don't need its JSON.
#    Real idle/working/blocked states need `herdr integration install <kind>`.
# ---------------------------------------------------------------------------
herdr agent start "$agent_name" --kind "$kind" --pane "$pane_id" >/dev/null

# ---------------------------------------------------------------------------
# 6. RETURN THE HANDLE. jq -n builds JSON from nothing; --arg injects shell vars
#    as jq vars. The caller keeps this and uses `agent` for every later prompt/
#    read/wait, and `workspace` to close it. Always this shape.
# ---------------------------------------------------------------------------
jq -n \
  --arg ws "$ws_id" --arg pane "$pane_id" --arg agent "$agent_name" \
  '{workspace: $ws, pane_id: $pane, agent: $agent}'
