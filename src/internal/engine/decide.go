package engine

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/recipe"
	"github.com/tylers2222/drover/internal/store"
)

type ask struct {
	job      *store.Job
	decision int64
	question string
}

// orchestrator is what the engine knows of the orchestrator pane. It holds at
// most one ask; every other ask is unasked.
type orchestrator struct {
	status   herdr.AgentStatus // "" while its pane is missing
	inflight string            // job whose ask it holds
	worked   bool              // it went working after that ask
}

func (o *orchestrator) observe(s herdr.AgentStatus) {
	if s == herdr.StatusWorking && o.inflight != "" {
		o.worked = true
	}
	o.status = s
}

func (o *orchestrator) available() bool {
	return o.status == herdr.StatusIdle || o.status == herdr.StatusDone
}

// rejected, when set, is why the job's previous recipe failed validation.
func (e *Engine) askOrchestrator(ctx context.Context, job *store.Job, rejected string) error {
	q := question(job, rejected)
	d, err := e.st.CreateDecision(ctx, job.ID, q)
	if err != nil {
		return fmt.Errorf("create decision for job %s: %w", job.ID, err)
	}
	e.asks[job.ID] = &ask{job: job, decision: d.ID, question: q}
	e.log.Info("decision opened", "job", job.ID, "decision", d.ID, "class", job.Class)
	e.deliverPendingAsk(ctx)
	return nil
}

// The stored question omits the decision id, which only exists once stored;
// prompt adds it.
func question(job *store.Job, rejected string) string {
	var spec bytes.Buffer
	if json.Compact(&spec, job.Spec) != nil {
		spec.WriteString("null")
	}
	q := fmt.Sprintf("job %s (class %s): %s", job.ID, cmp.Or(job.Class, "-"), spec.Bytes())
	if rejected != "" {
		q += ". Previous recipe rejected: " + rejected
	}
	return q
}

func (e *Engine) prompt(a *ask) string {
	p := fmt.Sprintf("drover decision %d for %s. Reply by running: drover decide %d '<recipe json>'",
		a.decision, a.question, a.decision)
	if e.cfg.DecisionPrompt != "" {
		p += " " + e.cfg.DecisionPrompt
	}
	return p
}

// deliverPendingAsk prompts an available orchestrator that holds no ask with
// the oldest open decision.
func (e *Engine) deliverPendingAsk(ctx context.Context) {
	if e.orch.inflight != "" || !e.orch.available() {
		return
	}
	var next *ask
	for _, a := range e.asks {
		if next == nil || a.decision < next.decision {
			next = a
		}
	}
	if next == nil {
		return
	}
	if _, err := e.conn.Prompt(ctx, e.cfg.Orchestrator, e.prompt(next)); err != nil {
		e.log.Error("ask orchestrator", "job", next.job.ID, "decision", next.decision, "err", err)
		return
	}
	if err := e.st.MarkAsked(ctx, next.decision); err != nil {
		e.log.Error("mark asked", "decision", next.decision, "err", err)
	}
	e.orch.inflight, e.orch.worked = next.job.ID, false
	e.log.Info("orchestrator asked", "job", next.job.ID, "decision", next.decision)
}

// checkDecisions consumes answers: a valid recipe becomes its class's recipe and
// starts the job; an invalid one is re-asked with the validation error.
func (e *Engine) checkDecisions(ctx context.Context) error {
	ids := make([]int64, 0, len(e.asks))
	for _, a := range e.asks {
		ids = append(ids, a.decision)
	}
	answered, err := e.st.AnsweredDecisions(ctx, ids)
	if err != nil {
		e.log.Error("answered decisions", "err", err)
		return nil
	}
	// Each job has one ask, and acting on an answer touches only its own.
	for _, d := range answered {
		a := e.asks[d.JobID]
		e.forgetAsk(d.JobID)
		rec, err := recipe.Parse(d.Response)
		if err != nil {
			e.log.Warn("recipe rejected", "job", d.JobID, "decision", d.ID, "err", err)
			e.record(ctx, d.JobID, "recipe_rejected", map[string]any{"decision": d.ID, "error": err.Error()})
			if err = e.askOrchestrator(ctx, a.job, err.Error()); err != nil {
				return err
			}
			continue
		}
		a.job.Recipe = d.Response
		if a.job.Class != "" {
			if err := e.st.SaveClassRecipe(ctx, a.job.Class, d.Response); err != nil {
				e.log.Error("save class recipe", "class", a.job.Class, "err", err)
			}
		}
		if err := e.startJob(ctx, a.job, rec); err != nil {
			return err
		}
	}
	return nil
}

// redecide re-attaches a deciding job to its open decision after a restart, or
// dispatches it afresh. Pending is oldest first, so the last match is newest.
func (e *Engine) redecide(ctx context.Context, job *store.Job, pending []store.Decision) error {
	var open *store.Decision
	for i := range pending {
		if pending[i].JobID == job.ID {
			open = &pending[i]
		}
	}
	if open == nil {
		return e.dispatch(ctx, job)
	}
	e.asks[job.ID] = &ask{job: job, decision: open.ID, question: open.Question}
	e.log.Info("decision restored", "job", job.ID, "decision", open.ID)
	return nil
}

func (e *Engine) onOrchestratorEvent(ctx context.Context, ev herdr.Event) error {
	switch ev.Kind {
	case herdr.EventPaneClosed, herdr.EventPaneExited:
		e.orch.status = ""
		e.log.Error("orchestrator pane gone; decisions stall", "pane", ev.PaneID, "event", ev.Kind)
		return nil
	case herdr.EventAgentStatusChanged:
		e.orch.observe(ev.AgentStatus)
	default:
		return nil
	}
	if !e.orch.available() {
		return nil
	}
	if err := e.checkDecisions(ctx); err != nil {
		return err
	}
	if e.orch.inflight != "" && e.orch.worked {
		// It worked a turn and ended it without answering: ask again.
		e.log.Warn("orchestrator ended a turn without answering", "job", e.orch.inflight)
		e.orch.inflight = ""
	}
	e.deliverPendingAsk(ctx)
	return nil
}

func (e *Engine) forgetAsk(jobID string) {
	delete(e.asks, jobID)
	if e.orch.inflight == jobID {
		e.orch.inflight = ""
	}
}
