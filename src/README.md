# drover

Process-native orchestration for AI coding agents. Each unit of work is its own
OS process in its own [herdr](https://herdr.dev) pane; drover is the workflow
engine that drives the herd.

Status: boilerplate. Structure is the deliverable; nothing here is verified end
to end yet.

## Why

In-runtime sub-agents share their parent's process, harness, and context
window. One crash takes everything; the job list lives in tokens. drover moves
that state into a SQLite table and the work into separate processes:

- children can be different harnesses (omp, claude, codex), chosen per job
- children are spawned for a job and killed after it
- each child keeps its own in-runtime sub-agents for the fine-grained work
- the engine is a plain process: `kill -9` it, restart it, the run resumes
  from the table

## Shape

```
orchestrator pane (LLM, talks to the human)
        │ drover decide <id> '<recipe>'
        ▼
┌─────────────────────────────┐   events.subscribe   ┌────────┐
│ drover run                  │◄─────────────────────│ herdr  │
│  slot semaphore             │  pane.split / run /  │ socket │
│  per-pane step FSM          │─────────────────────►│        │
│  reconcile on start/tick    │  agent.prompt ...    └────────┘
└──────────────┬──────────────┘                          │
               │ rows                                    ▼
          ┌────▼────┐                        panes: omp | claude | codex
          │ SQLite  │                              │ writes $HERD_RESULT
          └─────────┘                              ▼
                                            .drover/results/<job>.json
```

- **store** — SQLite (WAL). Truth. Every transition is a row write before it
  is a side effect.
- **engine** — holds N slots, subscribes to herdr events, never polls. Each
  live pane has a step FSM; an event during step 1 of 3 advances step 1, never
  the job. Only the last step's terminal state ends the job.
- **recipe** — the program for one job, returned by the orchestrator when the
  engine asks. Data, recorded before execution, replayable without the LLM.
- **result** — the report a child writes to `$HERD_RESULT`. Missing or invalid
  means failed, never silently accepted.
- **herdr** — socket client: request/response plus the push event stream.

## Flow

1. Orchestrator (a pane) starts `drover run -orchestrator <its pane id>`.
2. Jobs arrive via `drover enqueue jobs.jsonl` or `-source <cmd>` (exec'd when
   the queue is empty; prints JSONL).
3. Engine claims a job (`queued → deciding`). Recipe on the job, else cached
   recipe for the job's class → start. Miss → creates a decision row and, when
   the orchestrator is idle, prompts it once.
4. Orchestrator answers with `drover decide <id> recipe.json`.
5. Engine spawns the pane with `HERD_JOB_ID` / `HERD_RESULT` in env, runs the
   recipe's steps, validates the report, closes the pane, releases the slot.
6. `blocked` holds its pane and slot until the orchestrator resolves it.

## Recipe

```json
{
  "class": "implement",
  "spawn": {"command": "omp", "cwd": "."},
  "steps": [
    {"send": "text", "text": "/advisor", "wait": {"kind": "output", "regex": "advisor.*on", "timeout": "10s"}},
    {"send": "keys", "keys": ["enter"], "wait": {"kind": "none"}},
    {"send": "ask",  "text": "Implement src/parse.ts. Write your report to $HERD_RESULT.",
     "wait": {"kind": "status", "status": "done", "timeout": "30m"}}
  ]
}
```

`send`: `ask` (agent.prompt, starts a turn; Enter is resent up to 3 times if no
turn starts within `-submit-check`) · `text` (literal, no Enter) ·
`keys` · `steer` (one key). `wait`: `none` · `settle` · `status` · `output`
(anchor the regex; the scrollback echoes the command).

## Enqueue

One job per line. Only `id`, `class`, `title`, `spec`, `max_attempts` and
`recipe` are accepted; every field is optional.

```json
{"id": "j1", "class": "implement", "title": "parse.ts", "spec": {"file": "src/parse.ts"},
 "max_attempts": 3, "recipe": {"spawn": {"command": "omp"}, "steps": [...]}}
```

`recipe` takes the same object as the Recipe section above and goes through
the same validation as `drover decide`. An invalid recipe rejects the whole
batch with the line number and the reason, and nothing is enqueued. A valid one
is stored in `jobs.recipe`. A job with an inline recipe never prompts the
orchestrator and never writes `class_recipes`. Omit `recipe` (or set it to
`null`) to use the class recipe or ask the orchestrator. `-source` output uses
the same format.

`drover run` still needs `-orchestrator`. If every job carries a recipe, that
pane is never prompted, so it can be any pane, such as the shell that runs
drover.

## Result

```json
{"job_id": "j1", "status": "success|failure|blocked", "summary": "...",
 "artifacts": ["src/parse.ts"], "error": "...", "question": "..."}
```

## CLI

```
drover run -orchestrator wJ:p1 [-slots 2] [-source ./pull.sh] [-db .drover/drover.db] [-debug]
drover enqueue jobs.jsonl
drover decide <id> recipe.json
drover status | jobs [-status running] | events <job> | doctor
```

## Prerequisites

- herdr 0.9+ running; `herdr integration install <kind>` for every harness
  you spawn, or `agent_status` never leaves `idle`.
- Run inside a herdr pane (`HERDR_ENV=1`).

## Layout

```
cmd/drover          CLI
internal/herdr      socket client (api.go is the contract)
internal/store      SQLite store (types.go is the contract)
internal/recipe     recipe types + validation
internal/result     child report + validation
internal/jsonx      strict JSON decoding shared by the contracts
internal/engine     the loop: slots, FSM, reconcile, decide, source
docs/               herdr API schema snapshot
```

The bash scripts in `../scripts/` are the same verbs for LLM callers; drover
talks to the socket directly.
