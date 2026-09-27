---
name: herd-orchestrator
description: This skill should be used when the user asks to "spawn an agent pane", "process-native orchestration", "herdr agent", "arrange panes", "orchestrate agents in separate processes", "run a child harness in its own pane", "kill the agent pane", "read the pane scrollback", or wants one orchestrator agent to run omp / claude / codex children as separate OS processes inside herdr panes instead of in-runtime sub-agents.
version: 0.1.0
---

# Herd Orchestrator

## The idea: process-native agent orchestration

Today's sub-agents run inside the parent's shared runtime: same process, same harness, same context limits, and one crash takes everything with it. This skill makes each unit of work its own OS process in its own herdr pane. One orchestrator agent spawns them; each child is a full harness running in its own terminal.

Consequences:

1. **Children can be different harnesses.** `omp`, `claude`, `codex` - chosen per task by the command you spawn.
2. **Children are turned on and off per task.** Spawn for a phase, kill when the phase is done. Nothing lingers unless you leave it.
3. **Children keep their own in-runtime sub-agents.** The orchestrator holds the high-level solve; each process holds the detail and fans out internally as it sees fit.
4. **Isolation.** A child that hangs or crashes does not take the orchestrator down. Kill the pane and respawn.
5. **Everything is observable through the terminal.** Read the scrollback, check the agent status; there is no hidden channel.

All of this runs on herdr. herdr sets `HERDR_ENV=1` in every pane it owns; every script below checks it and exits `2` with `{"error":"not_in_herdr"}` when it is unset.

## Setup

herdr identifies a harness by process name, but `idle` / `working` / `blocked` come only from that harness's herdr integration. Run `herdr integration install <kind>` once per harness you will spawn (`omp`, `claude`, `codex`, ...); check with `herdr integration status`.

Without it: `agent_status` stays `idle` while the agent works, `spawn_pane --wait` and `wait_pane idle` return immediately, `write_to_pane`'s busy check is blind, and `ask` fails with `agent_prompt_stalled`. Agents already running when you install do not pick it up; `kill_pane` and respawn them.

This is the one place a raw `herdr` command is allowed, because it is setup, not orchestration.

## The tools

All scripts live in `scripts/` next to this file. Pane ids look like `wK:pN` (workspace K, pane N). Where a script accepts an agent name it says so.

### `spawn_pane.sh`

```
./spawn_pane.sh [--workspace] [--wait[=<until>]] <command> [cwd] [KEY=VALUE ...]
```

- `--workspace` puts the pane in a new workspace instead of beside the caller. Default splits the pane the script runs in, so children land in the orchestrator's own workspace. `--workspace` is the heavier isolation boundary and gets its own sidebar entry.
- `--wait` blocks until the pane is ready. Bare `--wait` waits for agent state `idle`; `--wait=<until>` takes anything `wait_pane.sh` accepts (a state, or an anchored output regex such as `'^READY$'` for plain commands). 2 min timeout; on timeout the pane is left running and `wait_pane`'s error goes to stderr with exit 1.
- `command` is the shell line to run in the new pane; quote it as one argument.
- `cwd` defaults to the caller's `$PWD`. Pass `.` to keep the default when you only want to add env vars.
- `KEY=VALUE` pairs are exported into the pane's shell. Anything without `=` is rejected.

If the command fails to send, the pane is closed so a broken spawn never leaves an orphan. Output:

```json
{"workspace": "wK", "pane_id": "wK:pN", "cwd": "/abs/path", "command": "omp", "mode": "split|workspace", "ready": null}
```

`ready` is `null` without `--wait`, otherwise `wait_pane`'s result object.

### `write_to_pane.sh`

```
./write_to_pane.sh <target_pane> <user_prompt> <ask|steer> [inject]
```

- `target_pane` is an agent name or a pane id hosting an agent.
- `ask` sends text plus Enter through the agent surface. A prompt that starts a turn waits until the agent settles; a slash command (`/...`) never leaves idle, so it returns immediately.
- `steer` sends a single key (`esc`, `enter`, `ctrl+c`) for driving the harness UI.
- `inject` (`true|false`, default `false`). With `false` the script refuses to write to an agent whose status is `working` or `blocked`, exiting 1 with `{"error":"agent_working"}` or `{"error":"agent_blocked"}`. `true` skips the busy check.

Output is herdr's own response for the prompt/send-keys call.

### `read_pane.sh`

```
./read_pane.sh <pane_id> [lines]
```

- `pane_id` is a pane id or agent name.
- `lines` is how many trailing scrollback lines; default 120.

Prints plain text, no JSON, no ANSI. Uses herdr's recent-unwrapped snapshot so long lines are not broken at terminal width.

### `list_panes.sh`

```
./list_panes.sh [workspace_id]
```

Optional `workspace_id` restricts to one workspace. Output is a JSON array:

```json
[{"pane_id": "wK:pN", "workspace_id": "wK", "tab_id": "...", "cwd": "...", "agent": null, "agent_status": "unknown", "title": null}]
```

`agent` is `null` for a plain shell pane, and `agent_status` is then `unknown`.

### `get_pane_status.sh`

```
./get_pane_status.sh <pane_id>
```

Merges pane lifecycle with what the shell is actually running:

```json
{"pane_id": "wK:pN", "workspace_id": "wK", "cwd": "...", "agent": null, "agent_status": "idle|working|blocked|unknown", "shell_pid": 123, "foreground": "cmdline or null", "shell_idle": true}
```

Which field to trust:

- **Agent panes**: `agent_status`. `idle` / `working` / `blocked` come from the harness integration. `unknown` means no agent, or the agent kind has no integration installed.
- **Plain command panes**: `shell_idle`. It is `true` when the foreground process group is the shell itself, i.e. the spawned command finished and the prompt is back. Plain panes never report idle/working through `agent_status`.

`foreground` is the cmdline of the innermost foreground process. Unknown pane: herdr's `pane_not_found` error on stderr, exit 1.

### `kill_pane.sh`

```
./kill_pane.sh <pane_id>
```

Closes the pane and the whole process tree it hosts. For a `--workspace` spawn the pane is the workspace's only pane, so closing it also removes the workspace. Output: `{"pane_id": "wK:pN", "closed": true}`. Unknown pane: herdr's error JSON on stderr, exit 1. Run `arrange_grid.sh` afterwards.

### `wait_pane.sh`

```
./wait_pane.sh <pane_id> [until] [timeout_ms]
```

- `pane_id` is a pane id or agent name.
- `until` is `idle | working | blocked | done | unknown` for an agent lifecycle state wait (default `idle`); anything else is a regex matched against the pane's output.
- `timeout_ms` default 120000. Timeout: herdr's `{"error":{"code":"timeout",...}}` on stderr, exit 1.

Output:

```json
{"pane_id": "wK:pN", "until": "idle", "agent_status": "idle"}          // state wait
{"pane_id": "wK:pN", "until": "^READY$", "matched_line": "READY"}     // output wait
```

State waits are for agent panes (after `spawn_pane`, after `write_to_pane ... ask`). A freshly spawned agent pane is not an agent until its integration reports in; the script polls through that `agent_not_found` window until the deadline.

Output waits are for plain-command panes, which never report agent state. **Footgun:** output waits search the scrollback, which includes the echoed command line. `READY` matches `echo READY` the instant it is typed. Anchor to the produced line: `^READY$`.

### `current_pane.sh`

```
./current_pane.sh
```

Who am I. Output:

```json
{"pane_id": "wK:pN", "workspace_id": "wK", "tab_id": "...", "cwd": "...", "agent": null, "agent_status": "idle"}
```

Use `workspace_id` for `list_panes` and `pane_id` as the parent that `spawn_pane` splits from.

### `list_workspaces.sh`

```
./list_workspaces.sh
```

Output is a JSON array:

```json
[{"workspace_id": "wK", "label": "...", "pane_count": 1, "tab_count": 1, "agent_status": "idle", "focused": true}]
```

`agent_status` is the workspace roll-up herdr shows in the sidebar.

### `arrange_grid.sh`

```
./arrange_grid.sh [cols]
```

`cols` defaults to `ceil(sqrt(pane count))`, i.e. a square. Re-tiles every pane in the caller's tab into an equal-size grid: the caller's own pane is cell 0 (top-left), the rest fill row-major in their current reading order. A short last row gets wider cells. Idempotent: run it after every spawn or kill.

Why it moves panes rather than resizing: herdr's layout is a binary split tree and every split carries a ratio (the fraction the original pane keeps). Resizing after the fact fights that tree; instead each pane is re-inserted at its target with the ratio that makes its cell equal (rows split `down` keeping `1/(rows left)`, cells split `right` keeping `1/(cells left in row)`). `pane move` refuses same-tab moves, so each pane bounces out to a temp tab and back. Processes keep running; only the terminal's position changes.

Output:

```json
{"rows": 2, "cols": 2, "cells": [{"pane_id": "wK:pN", "x": 0, "y": 0, "width": 80, "height": 24}]}
```

### `spawn-orchestrator.sh`

```
./spawn-orchestrator.sh <project_dir> <agent_name> <kind>
```

- `project_dir` e.g. `services/inventory`.
- `agent_name` herdr rule `[a-z][a-z0-9_-]{0,31}`, unique.
- `kind` e.g. `omp | claude | codex` (run `herdr agent` for the list).

Creates one workspace per project (the process-isolation boundary), then starts a named agent of the given kind in its root pane via `herdr agent start`; returns once herdr sees the agent ready for input. Real idle/working/blocked states need `herdr integration install <kind>`. Output:

```json
{"workspace": "wK", "pane_id": "wK:pN", "agent": "inventory-orch"}
```

Use `agent` for later prompt/read/wait calls and `workspace` to close it.

## The lifecycle

One worked example, scripts only. herdr detects `omp` automatically, so `agent_status` works on a plain `spawn_pane.sh --wait omp .` (command `omp`, cwd `.`) without going through `spawn-orchestrator.sh`.

```bash
S=~/.agents/skills/herd-orchestrator/scripts

# 1. Know where you are.
$S/current_pane.sh                                   # → {"pane_id":"w1:p1","workspace_id":"w1",...}

# 2. Give each child its own worktree, then spawn it there, blocking until idle.
#    Branch and directory share the child's name; cwd is the second positional.
BASE=$(git rev-parse --abbrev-ref HEAD)
for n in parser cli docs; do git worktree add -b "herd/$n" "../wt-$n" "$BASE"; done
A=$($S/spawn_pane.sh --wait omp ../wt-parser | jq -r .pane_id)
B=$($S/spawn_pane.sh --wait omp ../wt-cli    | jq -r .pane_id)
C=$($S/spawn_pane.sh --wait omp ../wt-docs   | jq -r .pane_id)

# 3. Square the layout.
$S/arrange_grid.sh

# 4. Hand each child its task. Tell it to commit on its own branch when done.
$S/write_to_pane.sh "$A" "Implement the parser in src/parse.ts. Skip tests. Commit when done." ask
$S/write_to_pane.sh "$B" "Write the CLI wrapper in src/cli.ts. Skip tests. Commit when done." ask
$S/write_to_pane.sh "$C" "Update docs/usage.md for the new CLI. Commit when done." ask

# 5. Wait for each to settle, then read what it did.
$S/wait_pane.sh "$A" idle
$S/read_pane.sh "$A" 80
$S/wait_pane.sh "$B" idle
$S/read_pane.sh "$B" 80
$S/wait_pane.sh "$C" idle
$S/read_pane.sh "$C" 80

# 6. Tear down what you spawned and re-tile.
$S/kill_pane.sh "$A"
$S/kill_pane.sh "$B"
$S/kill_pane.sh "$C"
$S/arrange_grid.sh

# 7. Merge each branch back into the local branch, then drop the worktree.
for n in parser cli docs; do
  git merge --no-ff --no-edit "herd/$n" || { git merge --abort; echo "conflict: herd/$n left for manual merge"; continue; }
  git worktree remove "../wt-$n" && git branch -d "herd/$n"
done
```

For a plain command pane (no agent), use an anchored output wait instead of a state wait:

```bash
P=$($S/spawn_pane.sh --wait='^BUILD OK$' 'make && echo BUILD OK' . | jq -r .pane_id)
$S/get_pane_status.sh "$P" | jq .shell_idle    # true once the prompt is back
```

## Worktrees

Every child gets its own git worktree; children never share a checkout with each other or with the orchestrator. Two harnesses editing one tree race on the index, stomp each other's files, and make `read_pane` output impossible to attribute.

- Create before spawning: `git worktree add -b herd/<name> ../wt-<name> <base>`. Pass the worktree path as `spawn_pane.sh`'s `cwd` argument.
- Tell the child to commit on its branch. Uncommitted work in a worktree is invisible to the merge.
- Merge back to the local branch every time, after `wait_pane ... idle` and `kill_pane`, never while the child is still running. Merge from the orchestrator's checkout with `--no-ff`; on conflict `git merge --abort`, leave the branch and worktree in place, and resolve by hand (or spawn a child to resolve it). Never force, never `reset --hard` the local branch.
- Only after a clean merge: `git worktree remove ../wt-<name>` then `git branch -d herd/<name>`. `-d` refuses if the merge did not actually land; treat that as a signal, not an obstacle to `-D` around.
- `git worktree list` shows what is still checked out; a leftover worktree is a leaked branch the same way an unkilled pane is a leaked process.

## Rules

- Always use the scripts. Never issue free-form `herdr` calls; the scripts encode the argument shapes, the readiness polling, and the cleanup on failure.
- Spawn with `--wait`, or call `wait_pane.sh` before the first prompt. A fresh pane is not an agent until its integration reports in; `write_to_pane.sh` against it fails. Requires the integration from Setup.
- Anchor output regexes (`^READY$`, not `READY`). The scrollback contains the echoed command line.
- Re-run `arrange_grid.sh` after any spawn or kill.
- Kill what you spawned. Children do not exit on their own; an unkilled pane is a leaked process.
- Trust `agent_status` for agent panes and `shell_idle` for plain command panes; never the other way round.
- Do not `inject=true` unless you mean to interrupt a working agent.
- One worktree per child, created before spawn and passed as `cwd`. Merge its branch back to local after every child finishes; abort and keep the branch on conflict; remove the worktree and branch only once the merge landed.
