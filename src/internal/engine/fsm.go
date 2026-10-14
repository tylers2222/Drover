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

// op is one write and the wait that settles it. A job's ops are its recipe
// normalised once: ops[0] runs the spawn command and waits for ready, ops[i]
// is recipe step i; job.Step indexes them.
type op struct {
	label   string
	write   func(ctx context.Context) error
	wait    recipe.Wait
	timeout time.Duration
}

// paneFSM runs one job's ops in its pane. Events settle only the current op;
// the last one ends the job.
type paneFSM struct {
	ctx    context.Context // ends the pane's stream, settle timer and output wait
	cancel context.CancelFunc
	job    *store.Job
	ops    []op

	gen      int
	status   herdr.AgentStatus // latest status seen on the pane
	left     bool              // seen off the target since arming, so reaching it is fresh
	armedAt  time.Time
	deadline time.Time
	timer    *time.Timer
}

func (f *paneFSM) current() *op { return &f.ops[f.job.Step] }

// stamp starts a new arm of the current op, so signals from earlier arms go
// stale. It precedes the write, so the events the write causes never do.
func (f *paneFSM) stamp() {
	f.gen++
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
	f.armedAt = time.Now()
	f.deadline = f.armedAt.Add(f.current().timeout)
}

// Waits that set no timeout take the configured ready or step timeout.
func (e *Engine) compile(rec *recipe.Recipe, pane string) []op {
	ops := make([]op, 1, len(rec.Steps)+1)
	ops[0] = op{
		label:   "ready",
		write:   func(ctx context.Context) error { return e.conn.RunCommand(ctx, pane, rec.Spawn.Command) },
		wait:    *rec.Spawn.Ready,
		timeout: timeoutOr(*rec.Spawn.Ready, e.cfg.ReadyTimeout),
	}
	for i, s := range rec.Steps {
		ops = append(ops, op{
			label:   "step " + strconv.Itoa(i+1),
			write:   func(ctx context.Context) error { return e.send(ctx, pane, s) },
			wait:    s.Wait,
			timeout: timeoutOr(s.Wait, e.cfg.StepTimeout),
		})
	}
	return ops
}

func timeoutOr(w recipe.Wait, fallback time.Duration) time.Duration {
	if t := w.Timeout.Std(); t > 0 {
		return t
	}
	return fallback
}

func (e *Engine) send(ctx context.Context, pane string, s recipe.Step) error {
	switch s.Send {
	case recipe.SendAsk:
		_, err := e.conn.Prompt(ctx, pane, s.Text)
		return err
	case recipe.SendText:
		return e.conn.SendText(ctx, pane, s.Text)
	case recipe.SendKeys, recipe.SendSteer:
		return e.conn.SendKeys(ctx, pane, s.Keys)
	}
	return fmt.Errorf("unknown send %q", s.Send)
}

func (e *Engine) startJob(ctx context.Context, job *store.Job, rec *recipe.Recipe) error {
	path := filepath.Join(e.cfg.ResultDir, job.ID+".json")
	// An earlier attempt's report must never pass for this attempt's.
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return e.retry(ctx, job, "clear old report: "+err.Error())
	}
	env := make(map[string]string, len(rec.Spawn.Env)+2)
	maps.Copy(env, rec.Spawn.Env)
	env[recipe.EnvJobID] = job.ID
	env[recipe.EnvResult] = path

	e.log.Debug("spawn", "job", job.ID, "workspace", rec.Spawn.Workspace, "cwd", rec.Spawn.CWD, "parent", e.cfg.ParentPane, "result", path)
	var info herdr.PaneInfo
	var err error
	if rec.Spawn.Workspace {
		info, err = e.conn.CreateWorkspace(ctx, herdr.WorkspaceCreateParams{CWD: rec.Spawn.CWD, Label: job.ID, Env: env})
	} else {
		info, err = e.conn.SplitPane(ctx, herdr.SplitParams{TargetPaneID: e.cfg.ParentPane, Direction: "right", CWD: rec.Spawn.CWD, Env: env})
	}
	if err != nil {
		return e.retry(ctx, job, "spawn: "+err.Error())
	}
	e.log.Debug("spawned", "job", job.ID, "pane", info.PaneID, "workspace_id", info.WorkspaceID, "status", info.AgentStatus)

	now := time.Now()
	job.PaneID, job.WorkspaceID, job.ResultPath = info.PaneID, info.WorkspaceID, path
	job.Step, job.Error, job.StartedAt, job.FinishedAt = 0, "", &now, nil
	if err := e.transition(ctx, job, store.JobRunning); err != nil {
		e.closePane(ctx, info.PaneID)
		return err
	}
	f, err := e.track(ctx, job, rec, info.AgentStatus)
	if f == nil {
		return err
	}
	return e.arm(ctx, f)
}

// resume rebuilds the FSM of a job whose pane outlived an engine restart. The
// current op's write is never repeated; only its wait is re-armed.
func (e *Engine) resume(ctx context.Context, job *store.Job, p herdr.PaneInfo) error {
	rec, err := recipe.Parse(job.Recipe)
	if err == nil && (job.Step < 0 || job.Step > len(rec.Steps)) {
		err = fmt.Errorf("step %d out of range", job.Step)
	}
	if err != nil {
		if err = e.retry(ctx, job, "resume: "+err.Error()); err == nil {
			e.closePane(ctx, p.PaneID)
		}
		return err
	}
	f, err := e.track(ctx, job, rec, p.AgentStatus)
	if f == nil {
		return err
	}
	e.log.Info("resumed", "job", job.ID, "pane", job.PaneID, "at", f.current().label)
	if err := e.syncBlocked(ctx, f); err != nil {
		return err
	}
	f.stamp()
	// Resuming at the target is ambiguous (it may predate the write) unless the
	// last op's report is on disk: then the child ended its final turn.
	w := &f.current().wait
	if job.Step == len(f.ops)-1 && w.Kind == recipe.WaitStatus && f.status.Satisfies(w.Status) && fileExists(job.ResultPath) {
		return e.stepDone(ctx, f)
	}
	return e.await(ctx, f)
}

// track registers the job's FSM and subscribes to its pane's status. A failed
// subscribe ends the attempt: the FSM is then nil and the error is abort's.
func (e *Engine) track(ctx context.Context, job *store.Job, rec *recipe.Recipe, status herdr.AgentStatus) (*paneFSM, error) {
	fctx, cancel := context.WithCancel(ctx)
	f := &paneFSM{
		ctx:    fctx,
		cancel: cancel,
		job:    job,
		ops:    e.compile(rec, job.PaneID),
		status: status,
	}
	e.panes[job.PaneID] = f
	err := e.subscribe(fctx, false, herdr.Subscription{Type: herdr.EventAgentStatusChanged, PaneID: job.PaneID})
	if err != nil {
		return nil, e.abort(ctx, f, "subscribe: "+err.Error())
	}
	return f, nil
}

// arm performs the current op's write and starts its wait.
func (e *Engine) arm(ctx context.Context, f *paneFSM) error {
	f.stamp()
	o := f.current()
	e.log.Debug("op write", "job", f.job.ID, "pane", f.job.PaneID, "op", o.label, "gen", f.gen)
	if err := o.write(ctx); err != nil {
		return e.abort(ctx, f, o.label+": "+err.Error())
	}
	return e.await(ctx, f)
}

// await starts the current op's wait; kind none settles at once.
func (e *Engine) await(ctx context.Context, f *paneFSM) error {
	o := f.current()
	e.log.Debug("op wait", "job", f.job.ID, "pane", f.job.PaneID, "op", o.label, "gen", f.gen,
		"kind", o.wait.Kind, "status", o.wait.Status, "regex", o.wait.Regex, "settle", o.wait.Settle.Std(),
		"timeout", o.timeout, "deadline", f.deadline, "pane_status", f.status)
	switch o.wait.Kind {
	case recipe.WaitNone:
		return e.stepDone(ctx, f)
	case recipe.WaitSettle:
		sig := signal{pane: f.job.PaneID, gen: f.gen}
		f.timer = time.AfterFunc(o.wait.Settle.Std(), func() { e.post(f.ctx, sig) })
	case recipe.WaitStatus:
		f.left = !f.status.Satisfies(o.wait.Status)
	case recipe.WaitOutput:
		go e.waitOutput(f.ctx, signal{pane: f.job.PaneID, gen: f.gen}, o.wait.Regex, o.timeout)
	}
	return nil
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

func (e *Engine) onPaneEvent(ctx context.Context, f *paneFSM, ev herdr.Event) error {
	e.record(ctx, f.job.ID, ev.Kind, ev.Data)
	switch ev.Kind {
	case herdr.EventPaneClosed, herdr.EventPaneExited:
		e.drop(f)
		return e.retry(ctx, f.job, strings.Replace(ev.Kind, ".", " ", 1))
	case herdr.EventAgentStatusChanged:
	default:
		return nil
	}
	f.status = ev.AgentStatus
	if err := e.syncBlocked(ctx, f); err != nil {
		return err
	}
	w := &f.current().wait
	stale := !ev.ReceivedAt.IsZero() && ev.ReceivedAt.Before(f.armedAt)
	e.log.Debug("pane status", "job", f.job.ID, "pane", f.job.PaneID, "op", f.current().label, "status", ev.AgentStatus,
		"wait", w.Kind, "want", w.Status, "left", f.left, "stale", stale, "job_status", f.job.Status)
	// Only a fresh arrival at the target settles the op. Anything else, including
	// a status received before this op's write, stays a recorded event.
	if w.Kind != recipe.WaitStatus || stale {
		return nil
	}
	switch {
	case !ev.AgentStatus.Satisfies(w.Status):
		f.left = true
	case f.left:
		return e.stepDone(ctx, f)
	}
	return nil
}

// A blocked agent parks its job as blocked; it keeps its pane and slot until the
// orchestrator resolves it, and the op's deadline restarts once it moves on.
func (e *Engine) syncBlocked(ctx context.Context, f *paneFSM) error {
	o := f.current()
	blocked := f.status == herdr.StatusBlocked && (o.wait.Kind != recipe.WaitStatus || o.wait.Status != herdr.StatusBlocked)
	switch {
	case blocked && f.job.Status == store.JobRunning:
		return e.transition(ctx, f.job, store.JobBlocked)
	case !blocked && f.job.Status == store.JobBlocked:
		f.deadline = time.Now().Add(o.timeout)
		return e.transition(ctx, f.job, store.JobRunning)
	}
	return nil
}

func (e *Engine) onSignal(ctx context.Context, sig signal) error {
	f := e.panes[sig.pane]
	if f == nil || f.gen != sig.gen {
		e.log.Debug("stale signal", "pane", sig.pane, "gen", sig.gen, "tracked", f != nil, "err", sig.err)
		return nil
	}
	e.log.Debug("signal", "job", f.job.ID, "pane", sig.pane, "op", f.current().label, "gen", sig.gen, "err", sig.err)
	if sig.err != nil {
		return e.abort(ctx, f, f.current().label+": wait output: "+sig.err.Error())
	}
	return e.stepDone(ctx, f)
}

func (e *Engine) stepDone(ctx context.Context, f *paneFSM) error {
	if f.job.Step == len(f.ops)-1 {
		return e.finish(ctx, f)
	}
	from := f.current().label
	f.job.Step++
	// Persisted before the write, so a restart resumes this op's wait instead
	// of repeating its write.
	if err := e.st.Update(ctx, f.job); err != nil {
		return fmt.Errorf("persist job %s step %d: %w", f.job.ID, f.job.Step, err)
	}
	e.log.Info("step", "job", f.job.ID, "pane", f.job.PaneID, "from", from, "to", f.current().label)
	return e.arm(ctx, f)
}

// finish settles the job from its report. A blocked report leaves the pane open
// for the orchestrator; FinishedAt stops reconcile from resuming it.
func (e *Engine) finish(ctx context.Context, f *paneFSM) error {
	job := f.job
	rep, err := result.Load(job.ResultPath, job.ID)
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
		e.log.Debug("report", "job", job.ID, "path", job.ResultPath, "status", rep.Status, "artifacts", len(rep.Artifacts))
		e.record(ctx, job.ID, "result", rep)
	} else {
		e.log.Debug("report", "job", job.ID, "path", job.ResultPath, "err", err)
	}
	now := time.Now()
	job.FinishedAt = &now
	e.drop(f)
	if err := e.transition(ctx, job, to); err != nil {
		return err
	}
	if to != store.JobBlocked {
		e.closePane(ctx, job.PaneID)
	}
	return nil
}

func (e *Engine) checkDeadlines(ctx context.Context) error {
	now := time.Now()
	for _, f := range e.panes {
		if f.job.Status != store.JobBlocked && now.After(f.deadline) {
			if err := e.abort(ctx, f, f.current().label+" timeout"); err != nil {
				return err
			}
		}
	}
	return nil
}

// abort ends a live attempt; the store records it before the pane is closed.
func (e *Engine) abort(ctx context.Context, f *paneFSM, reason string) error {
	pane := f.job.PaneID // retry clears it
	e.drop(f)
	if err := e.retry(ctx, f.job, reason); err != nil {
		return err
	}
	e.closePane(ctx, pane)
	return nil
}

// drop releases the FSM's slot; it must run before retry clears job.PaneID.
func (e *Engine) drop(f *paneFSM) {
	e.log.Debug("slot released", "job", f.job.ID, "pane", f.job.PaneID, "op", f.current().label)
	delete(e.panes, f.job.PaneID)
	f.cancel()
	if f.timer != nil {
		f.timer.Stop()
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
