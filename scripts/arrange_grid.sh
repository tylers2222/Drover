#!/usr/bin/env bash
# Usage: ./arrange_grid.sh [cols]
#   cols   columns in the grid; default = ceil(sqrt(pane count)), i.e. squares
#
# Re-tiles every pane in the caller's tab into an equal-size grid. The caller's
# own pane is cell 0 (top-left); the rest fill row-major in their current
# reading order (top-to-bottom, left-to-right). A short last row gets wider
# cells. Idempotent: run it after spawn_pane or kill_pane to square things up.
#
# How: herdr's layout is a binary split tree and every split has a ratio (the
# fraction the ORIGINAL pane keeps). So instead of resizing after the fact, each
# pane is moved into place with the ratio that makes its cell equal:
#   rows   → split the previous row head `down`,  keeping 1/(rows left)
#   cells  → split the previous cell     `right`, keeping 1/(cells left in row)
# `pane move` refuses same-tab moves (reason: same_tab), so each pane bounces
# out to a temp tab and back in at its target. The temp tab auto-closes when
# emptied. Processes keep running; only the terminal's position changes.
# Prints {rows, cols, cells: [{pane_id, x, y, width, height}]}.
set -euo pipefail

cols="${1:-0}"

if [ "${HERDR_ENV:-}" != 1 ]; then
  echo '{"error":"not_in_herdr"}' >&2
  exit 2
fi

case "$cols" in
  *[!0-9]*)
    echo "Invalid cols '$cols', expected a positive integer" >&2
    exit 1
    ;;
esac

anchor="$(herdr pane current | jq -r '.result.pane.pane_id')"
layout="$(herdr pane layout --current | jq '.result.layout')"
tab_id="$(jq -r '.tab_id' <<<"$layout")"

# ----------------------------------------------------------------------
# Plan. jq does the arithmetic and emits one move per line:
#   <pane> <target> <right|down> <ratio>
# ----------------------------------------------------------------------
plan="$(jq -r --arg anchor "$anchor" --argjson cols "$cols" '
  ( [ .panes[] | select(.pane_id != $anchor) ]
    | sort_by(.rect.y, .rect.x) | map(.pane_id) ) as $others
  | ([$anchor] + $others) as $cells
  | ($cells | length) as $total
  | (if $cols > 0 then $cols else ($total | sqrt | ceil) end) as $c
  | (($total / $c) | ceil) as $r
  # rows: chain the row heads downward from the anchor
  | ( [ range(1; $r) | . as $i
        | "\($cells[$i * $c]) \($cells[($i - 1) * $c]) down \(1 / ($r - $i + 1))" ] )
  # cells: chain each row rightward from its head
  + ( [ range(0; $r) | . as $i
        | ([$c, $total - $i * $c] | min) as $n
        | range(1; $n) | . as $j
        | "\($cells[$i * $c + $j]) \($cells[$i * $c + $j - 1]) right \(1 / ($n - $j + 1))" ] )
  | .[]
' <<<"$layout")"

while read -r pane target direction ratio; do
  [ -n "$pane" ] || continue
  herdr pane move "$pane" --new-tab --no-focus >/dev/null
  changed="$(herdr pane move "$pane" --tab "$tab_id" --target-pane "$target" \
               --split "$direction" --ratio "$ratio" --no-focus \
             | jq -r '.result.move_result.changed')"
  if [ "$changed" != true ]; then
    echo "{\"error\":\"move_failed\",\"pane_id\":\"$pane\",\"target\":\"$target\"}" >&2
    exit 1
  fi
done <<<"$plan"

# ----------------------------------------------------------------------
# Report what herdr actually laid out.
# ----------------------------------------------------------------------
herdr pane layout --current | jq --argjson cols "$cols" '
  .result.layout
  | (.panes | length) as $total
  | (if $cols > 0 then $cols else ($total | sqrt | ceil) end) as $c
  | {
      rows: (($total / $c) | ceil),
      cols: $c,
      cells: ( [ .panes[] | {pane_id, x: .rect.x, y: .rect.y, width: .rect.width, height: .rect.height} ]
               | sort_by(.y, .x) )
    }
'
