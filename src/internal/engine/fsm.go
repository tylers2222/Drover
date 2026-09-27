package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/recipe"
	"github.com/tylers2222/drover/internal/result"
	"github.com/tylers2222/drover/internal/store"
)

// paneFSM runs one job's recipe in its pane; step -1 is the spawn command and
// its ready wait. Events settle only the current step; the last one ends the job.
type paneFSM struct {
	ctx    context.Context // ends the pane's stream, settle timer and output wait
	cancel context.CancelFunc
	job    *store.Job
	rec    *recipe.Recipe
	pane   string
	result string

	step     int
	gen      int
	status   herdr.AgentStatus // latest status seen on the pane
	left     bool              // seen off the target since arming, so reaching it is fresh
	armedAt  time.Time
	timeout  time.Duration
	deadline time.Time
	timer    *time.Timer
}

func (f *paneFSM) wait() recipe.Wait {
	switch {
	case f.step >= 0:
		return f.rec.Steps[f.step].Wait
	case f.rec.Spawn.Ready != nil:
		return *f.rec.Spawn.Ready
	}
	return recipe.Wait{Kind: recipe.WaitStatus, Status: herdr.StatusIdle}
}

func (f *paneFSM) label() string {
	if f.step < 0 {
		return "ready"
	}
	return "step " + strconv.Itoa(f.step)
}

func (e *Engine) startJob(ctx context.Context, job *store.Job, rec *recipe.Recipe) error {
	path := filepath.Join(e.cfg.ResultDir, job.ID+".json")
	// An earlier attempt's report must never pass for this attempt's.
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		e.retry(ctx, job, "clear old report: "+err.Error())
		return err
	}
	env := make(map[string]string, len(rec.Spawn.Env)+2)
	maps.Copy(env, rec.Spawn.Env)
	env["HERD_JOB_ID"] = job.ID
	env["HERD_RESULT"] = path

	var info herdr.PaneInfo
	var err error
	if rec.Spawn.Workspace {
		info, err = e.conn.CreateWorkspace(ctx, herdr.WorkspaceCreateParams{CWD: rec.Spawn.CWD, Label: job.ID, Env: env})
	} else {
		info, err = e.conn.SplitPane(ctx, herdr.SplitParams{TargetPaneID: e.cfg.ParentPane, Direction: "right", CWD: rec.Spawn.CWD, Env: env})
	}
	if err != nil {
		e.retry(ctx, job, "spawn: "+err.Error())
		return err
	}

	now := time.Now()
	job.PaneID, job.WorkspaceID, job.ResultPath = info.PaneID, info.WorkspaceID, path
	job.Step, job.Error, job.StartedAt, job.FinishedAt = -1, "", &now, nil
	if err := e.transition(ctx, job, store.JobRunning); err != nil {
		e.closePane(ctx, info.PaneID)
		return err
	}
	f, err := e.track(ctx, job, rec, info)
	if err != nil {
		return err
	}
	e.arm(ctx, f, true)
	return nil
}

// resume rebuilds the FSM of a job whose pane outlived an engine restart.
func (e *Engine) resume(ctx context.Context, job *store.Job, p herdr.PaneInfo) {
	rec, err := recipe.Parse(job.Recipe)
	if err == nil && (job.Step < -1 || job.Step >= len(rec.Steps)) {
		err = fmt.Errorf("step %d out of range", job.Step)
	}
	if err != nil {
		e.retry(ctx, job, "resume: "+err.Error())
		e.closePane(ctx, p.PaneID)
		return
	}
	f, err := e.track(ctx, job, rec, p)
	if err != nil {
		return
	}
	e.log.Info("resumed", "job", job.ID, "pane", f.pane, "at", f.label())
	e.syncBlocked(ctx, f)
	// The step's write happened before the restart; only its wait is re-armed.
	e.arm(ctx, f, false)
}

func (e *Engine) track(ctx context.Context, job *store.Job, rec *recipe.Recipe, p herdr.PaneInfo) (*paneFSM, error) {
	fctx, cancel := context.WithCancel(ctx)
	f := &paneFSM{
		ctx:    fctx,
		cancel: cancel,
		job:    job,
		rec:    rec,
		pane:   p.PaneID,
		result: job.ResultPath,
		step:   job.Step,
		status: p.AgentStatus,
	}
	e.panes[f.pane] = f
	err := e.subscribe(fctx, false, herdr.Subscription{Type: herdr.EventAgentStatusChanged, PaneID: f.pane})
	if err != nil {
		e.abort(ctx, f, "subscribe: "+err.Error())
		return nil, err
	}
	return f, nil
}

// arm performs the current step's write (skipped when resuming) and starts its wait.
func (e *Engine) arm(ctx context.Context, f *paneFSM, write bool) {
	f.gen++
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	w := f.wait()
	f.timeout = w.Timeout.Std()
	if f.timeout <= 0 {
		f.timeout = e.cfg.StepTimeout
		if f.step < 0 {
			f.timeout = e.cfg.ReadyTimeout
		}
	}
	// Stamped before the write so the events it causes are never taken as stale.
	f.armedAt = time.Now()
	f.deadline = f.armedAt.Add(f.timeout)
	if write {
		if err := e.write(ctx, f); err != nil {
			e.abort(ctx, f, f.label()+": "+err.Error())
			return
		}
	}
	switch w.Kind {
	case recipe.WaitNone:
		e.stepDone(ctx, f)
	case recipe.WaitSettle:
		sig := signal{pane: f.pane, gen: f.gen}
		f.timer = time.AfterFunc(w.Settle.Std(), func() { e.post(f.ctx, sig) })
	case recipe.WaitStatus:
		f.left = f.status != w.Status
		// Resuming at the target is ambiguous (it may predate the write) unless the
		// last step's report is on disk: then the child ended its final turn.
		if !write && !f.left && f.step == len(f.rec.Steps)-1 && fileExists(f.result) {
			e.stepDone(ctx, f)
		}
	case recipe.WaitOutput:
		go e.waitOutput(f.ctx, signal{pane: f.pane, gen: f.gen}, w.Regex, f.timeout)
	}
}

func (e *Engine) write(ctx context.Context, f *paneFSM) error {
	if f.step < 0 {
		return e.conn.RunCommand(ctx, f.pane, f.rec.Spawn.Command)
	}
	s := f.rec.Steps[f.step]
	switch s.Send {
	case recipe.SendAsk:
		_, err := e.conn.Prompt(ctx, f.pane, s.Text, nil)
		return err
	case recipe.SendText:
		return e.conn.SendText(ctx, f.pane, s.Text)
	case recipe.SendKeys, recipe.SendSteer:
		return e.conn.SendKeys(ctx, f.pane, s.Keys)
	}
	return fmt.Errorf("unknown send %q", s.Send)
}

// herdr blocks server-side until the regex matches or the timeout expires.
func (e *Engine) waitOutput(ctx context.Context, sig signal, regex string, timeout time.Duration) {
	_, sig.err = e.conn.WaitOutput(ctx, sig.pane, regex, timeout)
	e.post(ctx, sig)
}

func (e *Engine) post(ctx context.Context, sig signal) {
	select {
	case e.signals <- sig:
	case <-ctx.Done():
	}
}

func (e *Engine) onPaneEvent(ctx context.Context, f *paneFSM, ev herdr.Event) {
	e.record(ctx, f.job.ID, ev.Kind, ev.Data)
	switch ev.Kind {
	case herdr.EventPaneClosed, herdr.EventPaneExited:
		e.retry(ctx, f.job, strings.Replace(ev.Kind, ".", " ", 1))
		e.drop(f)
		return
	case herdr.EventAgentStatusChanged:
	default:
		return
	}
	f.status = ev.AgentStatus
	e.syncBlocked(ctx, f)
	w := f.wait()
	// Only a fresh arrival at the target settles the step. Anything else, including
	// a status received before this step's write, stays a recorded event.
	if w.Kind != recipe.WaitStatus || (!ev.ReceivedAt.IsZero() && ev.ReceivedAt.Before(f.armedAt)) {
		return
	}
	if ev.AgentStatus != w.Status {
		f.left = true
	} else if f.left {
		e.stepDone(ctx, f)
	}
}

// A blocked agent parks its job as blocked; it keeps its pane and slot until the
// orchestrator resolves it, and the step deadline restarts once it moves on.
func (e *Engine) syncBlocked(ctx context.Context, f *paneFSM) {
	w := f.wait()
	blocked := f.status == herdr.StatusBlocked && (w.Kind != recipe.WaitStatus || w.Status != herdr.StatusBlocked)
	switch {
	case blocked && f.job.Status == store.JobRunning:
		e.transition(ctx, f.job, store.JobBlocked)
	case !blocked && f.job.Status == store.JobBlocked:
		f.deadline = time.Now().Add(f.timeout)
		e.transition(ctx, f.job, store.JobRunning)
	}
}

func (e *Engine) onSignal(ctx context.Context, sig signal) {
	f := e.panes[sig.pane]
	if f == nil || f.gen != sig.gen {
		return
	}
	if sig.err != nil {
		e.abort(ctx, f, f.label()+": wait output: "+sig.err.Error())
		return
	}
	e.stepDone(ctx, f)
}

func (e *Engine) stepDone(ctx context.Context, f *paneFSM) {
	if f.step == len(f.rec.Steps)-1 {
		e.finish(ctx, f)
		return
	}
	from := f.label()
	f.step++
	f.job.Step = f.step
	// Persisted before the write, so a restart resumes this step's wait instead
	// of repeating its write.
	if err := e.st.Update(ctx, f.job); err != nil {
		e.log.Error("persist step", "job", f.job.ID, "err", err)
	}
	e.log.Info("step", "job", f.job.ID, "pane", f.pane, "from", from, "to", f.label())
	e.arm(ctx, f, true)
}

// finish settles the job from its report. A blocked report leaves the pane open
// for the orchestrator; FinishedAt stops reconcile from resuming it.
func (e *Engine) finish(ctx context.Context, f *paneFSM) {
	job := f.job
	rep, err := result.Load(f.result, job.ID)
	to := store.JobFailed
	switch {
	case err != nil:
		job.Error = err.Error()
	case rep.Status == result.Success:
		to, job.Error = store.JobDone, ""
	case rep.Status == result.Blocked:
		to, job.Error = store.JobBlocked, ""
	default:
		job.Error = rep.Error
	}
	if rep != nil {
		e.record(ctx, job.ID, "result", rep)
	}
	now := time.Now()
	job.FinishedAt = &now
	e.transition(ctx, job, to)
	e.drop(f)
	if to != store.JobBlocked {
		e.closePane(ctx, f.pane)
	}
}

func (e *Engine) checkDeadlines(ctx context.Context) {
	now := time.Now()
	var late []*paneFSM
	for _, f := range e.panes {
		if f.job.Status != store.JobBlocked && now.After(f.deadline) {
			late = append(late, f)
		}
	}
	for _, f := range late {
		e.abort(ctx, f, f.label()+" timeout")
	}
}

// abort ends a live attempt; the store records it before the pane is closed.
func (e *Engine) abort(ctx context.Context, f *paneFSM, reason string) {
	e.retry(ctx, f.job, reason)
	e.drop(f)
	e.closePane(ctx, f.pane)
}

func (e *Engine) drop(f *paneFSM) {
	delete(e.panes, f.pane)
	f.cancel()
	if f.timer != nil {
		f.timer.Stop()
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
