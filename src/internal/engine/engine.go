package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/recipe"
	"github.com/tylers2222/drover/internal/store"
)

const defaultSafetyTick = 30 * time.Second

type Engine struct {
	cfg  Config
	conn herdr.Conn
	st   store.Store
	log  *slog.Logger

	events  chan herdr.Event // every subscription, merged
	signals chan signal      // settle timers and output waits
	dead    chan error       // the base stream's failure

	panes map[string]*paneFSM // running and step-blocked jobs, by pane
	asks  map[string]*ask     // deciding jobs, by job id

	orchestratorStatus herdr.AgentStatus
	inflight           string // job whose ask the orchestrator holds; at most one
	inflightWorked     bool   // the orchestrator went working after that ask
}

// A signal whose gen is not its FSM's current gen belongs to an earlier arm.
type signal struct {
	pane string
	gen  int
	err  error
}

func newEngine(cfg Config, conn herdr.Conn, st store.Store) *Engine {
	cfg.Slots = max(cfg.Slots, 1)
	if cfg.ReadyTimeout <= 0 {
		cfg.ReadyTimeout = recipe.DefaultReadyTimeout
	}
	if cfg.StepTimeout <= 0 {
		cfg.StepTimeout = recipe.DefaultStepTimeout
	}
	if cfg.SafetyTick <= 0 {
		cfg.SafetyTick = defaultSafetyTick
	}
	return &Engine{
		cfg:     cfg,
		conn:    conn,
		st:      st,
		log:     slog.Default(),
		events:  make(chan herdr.Event),
		signals: make(chan signal),
		dead:    make(chan error, 1),
		panes:   make(map[string]*paneFSM),
		asks:    make(map[string]*ask),
	}
}

func (e *Engine) run(ctx context.Context) error {
	if e.cfg.Orchestrator == "" {
		return errors.New("engine: no orchestrator pane configured")
	}
	dir, err := filepath.Abs(e.cfg.ResultDir)
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return fmt.Errorf("engine: result dir: %w", err)
	}
	e.cfg.ResultDir = dir

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = e.subscribe(ctx, true,
		herdr.Subscription{Type: herdr.EventPaneClosed},
		herdr.Subscription{Type: herdr.EventPaneExited},
		herdr.Subscription{Type: herdr.EventAgentStatusChanged, PaneID: e.cfg.Orchestrator},
	)
	if err != nil {
		return fmt.Errorf("engine: subscribe: %w", err)
	}
	if err := e.sweep(ctx); err != nil {
		return fmt.Errorf("engine: %w", err)
	}

	tick := time.NewTicker(e.cfg.SafetyTick)
	defer tick.Stop()
	for {
		busy := e.busy()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-e.dead:
			return fmt.Errorf("engine: %w", err)
		case ev := <-e.events:
			e.onEvent(ctx, ev)
		case sig := <-e.signals:
			e.onSignal(ctx, sig)
		case <-tick.C:
			e.checkDeadlines(ctx)
			if err := e.sweep(ctx); err != nil {
				e.log.Error("safety sweep", "err", err)
			}
			continue
		}
		if e.busy() < busy {
			e.fill(ctx)
		}
	}
}

// sweep runs at start and on every tick: it heals restarts and missed events,
// then delivers asks and fills free slots.
func (e *Engine) sweep(ctx context.Context) error {
	if err := e.reconcile(ctx); err != nil {
		return err
	}
	e.checkDecisions(ctx)
	e.deliverPendingAsk(ctx)
	e.fill(ctx)
	return nil
}

// herdr serves one subscription per connection and cannot extend it, so every
// stream gets a forwarder into e.events; only the base stream ending is fatal.
func (e *Engine) subscribe(ctx context.Context, base bool, subs ...herdr.Subscription) error {
	ch, err := e.conn.Subscribe(ctx, subs)
	if err != nil {
		return err
	}
	go func() {
		for ev := range ch {
			select {
			case e.events <- ev:
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		err := e.conn.Err()
		if err == nil {
			err = errors.New("herdr event stream closed")
		}
		if !base {
			e.log.Warn("pane event stream ended", "subs", subs, "err", err)
			return
		}
		e.dead <- err
	}()
	return nil
}

func (e *Engine) onEvent(ctx context.Context, ev herdr.Event) {
	if ev.PaneID == e.cfg.Orchestrator {
		e.onOrchestratorEvent(ctx, ev)
		return
	}
	if f := e.panes[ev.PaneID]; f != nil {
		e.onPaneEvent(ctx, f, ev)
	}
}

func (e *Engine) busy() int { return len(e.panes) + len(e.asks) }

// fill claims queued jobs while slots are free, running Source at most once
// when the queue is empty.
func (e *Engine) fill(ctx context.Context) {
	pulled := false
	for e.busy() < e.cfg.Slots {
		job, err := e.st.Claim(ctx)
		if err != nil {
			e.log.Error("claim", "err", err)
			return
		}
		if job == nil {
			if pulled || e.cfg.Source == "" || !e.pull(ctx) {
				return
			}
			pulled = true
			continue
		}
		e.log.Info("job", "job", job.ID, "from", store.JobQueued, "to", job.Status)
		if err := e.dispatch(ctx, job); err != nil {
			e.log.Error("dispatch", "job", job.ID, "err", err)
			return
		}
	}
}

func (e *Engine) pull(ctx context.Context) bool {
	jobs, err := e.pullSource(ctx)
	if err == nil && len(jobs) > 0 {
		err = e.st.Enqueue(ctx, jobs)
	}
	if err != nil {
		e.log.Error("source", "err", err)
		return false
	}
	if len(jobs) == 0 {
		return false
	}
	e.log.Info("source enqueued", "jobs", len(jobs))
	return true
}

// A job without a usable recipe (none, no class recipe, or one that no longer
// validates) goes to the orchestrator and holds its slot while deciding.
func (e *Engine) dispatch(ctx context.Context, job *store.Job) error {
	raw := job.Recipe
	if raw == nil && job.Class != "" {
		var err error
		if raw, err = e.st.ClassRecipe(ctx, job.Class); err != nil {
			e.log.Error("class recipe", "job", job.ID, "class", job.Class, "err", err)
		}
	}
	if raw == nil {
		return e.askOrchestrator(ctx, job, "")
	}
	rec, err := recipe.Parse(raw)
	if err != nil {
		return e.askOrchestrator(ctx, job, err.Error())
	}
	job.Recipe = raw
	return e.startJob(ctx, job, rec)
}

// transition persists a status change along with whatever fields the caller set.
func (e *Engine) transition(ctx context.Context, job *store.Job, to store.JobStatus) error {
	from := job.Status
	job.Status = to
	if err := e.st.Update(ctx, job); err != nil {
		e.log.Error("persist job", "job", job.ID, "pane", job.PaneID, "from", from, "to", to, "err", err)
		return err
	}
	args := []any{"job", job.ID, "pane", job.PaneID, "from", from, "to", to}
	if job.Error != "" {
		args = append(args, "error", job.Error)
	}
	e.log.Info("job", args...)
	return nil
}

// retry ends a failed attempt: requeued while attempts remain, else failed.
func (e *Engine) retry(ctx context.Context, job *store.Job, reason string) {
	job.Attempts++
	e.log.Warn("attempt failed", "job", job.ID, "pane", job.PaneID, "attempt", job.Attempts, "reason", reason)
	job.PaneID, job.WorkspaceID, job.Step, job.Error, job.FinishedAt = "", "", 0, reason, nil
	to := store.JobQueued
	if job.Attempts >= e.attemptLimit(job) {
		to = store.JobFailed
		now := time.Now()
		job.FinishedAt = &now
	}
	e.transition(ctx, job, to)
}

// Config.MaxAttempts caps each job's own limit; 0 means no cap.
func (e *Engine) attemptLimit(job *store.Job) int {
	if c := e.cfg.MaxAttempts; c > 0 && c < job.MaxAttempts {
		return c
	}
	return job.MaxAttempts
}

func (e *Engine) closePane(ctx context.Context, pane string) {
	if err := e.conn.ClosePane(ctx, pane); err != nil && !herdr.IsCode(err, "pane_not_found") {
		e.log.Warn("close pane", "pane", pane, "err", err)
	}
}

func (e *Engine) record(ctx context.Context, jobID, kind string, v any) {
	data, err := json.Marshal(v)
	if err == nil {
		err = e.st.AppendEvent(ctx, jobID, kind, data)
	}
	if err != nil {
		e.log.Error("record event", "job", jobID, "kind", kind, "err", err)
	}
}
