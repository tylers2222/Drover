package engine

import (
	"context"
	"fmt"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/store"
)

// reconcile squares memory, store and live panes. It runs before the first fill
// and on every tick, so a restart or a missed event heals within one tick.
func (e *Engine) reconcile(ctx context.Context) error {
	jobs, err := e.st.List(ctx, store.JobRunning, store.JobDeciding, store.JobBlocked)
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	panes, err := e.conn.ListPanes(ctx, "")
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	live := make(map[string]herdr.PaneInfo, len(panes))
	for _, p := range panes {
		live[p.PaneID] = p
	}
	if p, ok := live[e.cfg.Orchestrator]; ok {
		e.orch.observe(p.AgentStatus)
	} else {
		e.orch.status = ""
		e.log.Error("orchestrator pane not found", "pane", e.cfg.Orchestrator)
	}
	e.log.Debug("reconcile", "jobs", len(jobs), "live_panes", len(panes), "tracked", len(e.panes), "asks", len(e.asks), "orchestrator", e.orch.status)

	var pending []store.Decision
	loaded := false
	active := make(map[string]bool, len(jobs))
	for i := range jobs {
		job := &jobs[i]
		active[job.ID] = true
		if job.Status == store.JobDeciding {
			if e.asks[job.ID] != nil {
				continue
			}
			if !loaded {
				if pending, err = e.st.PendingDecisions(ctx); err != nil {
					return fmt.Errorf("reconcile: %w", err)
				}
				loaded = true
			}
			if err := e.redecide(ctx, job, pending); err != nil {
				return err
			}
			continue
		}
		f := e.panes[job.PaneID]
		p, alive := live[job.PaneID]
		switch {
		case !alive:
			if f != nil {
				job = f.job
				e.drop(f)
			}
			if err := e.retry(ctx, job, "pane gone"); err != nil {
				return err
			}
		case f == nil && job.FinishedAt == nil:
			if err := e.resume(ctx, job, p); err != nil {
				return err
			}
		}
	}

	for _, f := range e.panes {
		if !active[f.job.ID] {
			e.log.Warn("job moved on outside the engine; releasing its slot", "job", f.job.ID, "pane", f.job.PaneID)
			e.drop(f)
		}
	}
	for id := range e.asks {
		if !active[id] {
			e.log.Warn("job moved on outside the engine; dropping its decision", "job", id)
			e.forgetAsk(id)
		}
	}
	return nil
}
