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
	asked    bool
}

// rejected, when set, is why the job's previous recipe failed validation.
func (e *Engine) askOrchestrator(ctx context.Context, job *store.Job, rejected string) error {
	q := question(job, rejected)
	d, err := e.st.CreateDecision(ctx, job.ID, q)
	if err != nil {
		return fmt.Errorf("create decision: %w", err)
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

// deliverPendingAsk prompts an available orchestrator with the oldest unasked
// decision. Only one ask is ever in flight.
func (e *Engine) deliverPendingAsk(ctx context.Context) {
	if e.inflight != "" || !available(e.orchestratorStatus) {
		return
	}
	var next *ask
	for _, a := range e.asks {
		if !a.asked && (next == nil || a.decision < next.decision) {
			next = a
		}
	}
	if next == nil {
		return
	}
	if _, err := e.conn.Prompt(ctx, e.cfg.Orchestrator, e.prompt(next), nil); err != nil {
		e.log.Error("ask orchestrator", "job", next.job.ID, "decision", next.decision, "err", err)
		return
	}
	if err := e.st.MarkAsked(ctx, next.decision); err != nil {
		e.log.Error("mark asked", "decision", next.decision, "err", err)
	}
	next.asked = true
	e.inflight, e.inflightWorked = next.job.ID, false
	e.log.Info("orchestrator asked", "job", next.job.ID, "decision", next.decision)
}

// checkDecisions consumes answers: a valid recipe becomes its class's recipe and
// starts the job; an invalid one is re-asked with the validation error.
func (e *Engine) checkDecisions(ctx context.Context) {
	for id, a := range e.asks {
		d, err := e.st.GetDecision(ctx, a.decision)
		if err != nil {
			e.log.Error("get decision", "job", id, "decision", a.decision, "err", err)
			continue
		}
		if d.Response == nil {
			continue
		}
		e.forgetAsk(id)
		rec, err := recipe.Parse(d.Response)
		if err != nil {
			e.log.Warn("recipe rejected", "job", id, "decision", d.ID, "err", err)
			e.record(ctx, id, "recipe_rejected", map[string]any{"decision": d.ID, "error": err.Error()})
			if err := e.askOrchestrator(ctx, a.job, err.Error()); err != nil {
				e.log.Error("re-ask", "job", id, "err", err)
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
			e.log.Error("start job", "job", id, "err", err)
		}
	}
}

// redecide re-attaches a deciding job to its open decision after a restart, or
// dispatches it afresh. Pending is oldest first, so the last match is newest.
func (e *Engine) redecide(ctx context.Context, job *store.Job, pending []store.Decision) {
	var open *store.Decision
	for i := range pending {
		if pending[i].JobID == job.ID {
			open = &pending[i]
		}
	}
	if open == nil {
		if err := e.dispatch(ctx, job); err != nil {
			e.log.Error("dispatch", "job", job.ID, "err", err)
		}
		return
	}
	e.asks[job.ID] = &ask{job: job, decision: open.ID, question: open.Question}
	e.log.Info("decision restored", "job", job.ID, "decision", open.ID)
}

func (e *Engine) onOrchestratorEvent(ctx context.Context, ev herdr.Event) {
	switch ev.Kind {
	case herdr.EventPaneClosed, herdr.EventPaneExited:
		e.orchestratorStatus = ""
		e.log.Error("orchestrator pane gone; decisions stall", "pane", ev.PaneID, "event", ev.Kind)
		return
	case herdr.EventAgentStatusChanged:
		e.observeOrchestrator(ev.AgentStatus)
	default:
		return
	}
	if !available(ev.AgentStatus) {
		return
	}
	e.checkDecisions(ctx)
	if e.inflight != "" && e.inflightWorked {
		// It worked a turn and ended it without answering: ask again.
		if a := e.asks[e.inflight]; a != nil {
			a.asked = false
		}
		e.log.Warn("orchestrator ended a turn without answering", "job", e.inflight)
		e.inflight = ""
	}
	e.deliverPendingAsk(ctx)
}

func (e *Engine) observeOrchestrator(s herdr.AgentStatus) {
	if s == herdr.StatusWorking && e.inflight != "" {
		e.inflightWorked = true
	}
	e.orchestratorStatus = s
}

func available(s herdr.AgentStatus) bool {
	return s == herdr.StatusIdle || s == herdr.StatusDone
}

func (e *Engine) forgetAsk(jobID string) {
	delete(e.asks, jobID)
	if e.inflight == jobID {
		e.inflight = ""
	}
}
