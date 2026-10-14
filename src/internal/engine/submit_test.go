package engine

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/store"
)

// askWith runs the inline recipe's job up to its ask, with the child dropping
// the first dropEnters Enters, and returns once the prompt has been typed.
func askWith(t *testing.T, dropEnters int) (context.Context, *Engine, *fakeConn, store.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	e, conn, st := newTestEngine(t, `{"id":"j1","class":"plan","recipe":`+inlineRecipe+"}")
	conn.dropEnters = dropEnters
	if err := e.fill(ctx); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if err := e.onEvent(ctx, statusEvent(herdr.StatusIdle)); err != nil {
		t.Fatalf("ready event: %v", err)
	}
	if conn.prompts[childPane] != 1 {
		t.Fatalf("child prompted %d times after ready, want 1", conn.prompts[childPane])
	}
	return ctx, e, conn, st
}

func statusEvent(s herdr.AgentStatus) herdr.Event {
	return herdr.Event{Kind: herdr.EventAgentStatusChanged, PaneID: childPane, AgentStatus: s, ReceivedAt: time.Now()}
}

// pump stands in for Run's loop for d: the child's status changes and the
// engine's timers reach the FSM in the order they happen.
func pump(t *testing.T, ctx context.Context, e *Engine, conn *fakeConn, d time.Duration) {
	t.Helper()
	end := time.After(d)
	for {
		var err error
		select {
		case s := <-conn.statuses:
			err = e.onEvent(ctx, statusEvent(s))
		case sig := <-e.signals:
			err = e.onSignal(ctx, sig)
		case <-end:
			return
		}
		if err != nil {
			t.Fatalf("engine: %v", err)
		}
	}
}

func getJob(t *testing.T, st store.Store) *store.Job {
	t.Helper()
	job, err := st.Get(context.Background(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestAskSubmittedFirstTimeSendsNoExtraEnter(t *testing.T) {
	ctx, e, conn, st := askWith(t, 0)
	pump(t, ctx, e, conn, 10*e.cfg.SubmitCheck)
	if len(conn.keys) != 0 {
		t.Fatalf("keys sent = %q, want none once the agent turned working", conn.keys)
	}
	if job := getJob(t, st); job.Status != store.JobRunning || job.Step != 1 {
		t.Fatalf("job %s at step %d, want running at step 1", job.Status, job.Step)
	}
}

func TestAskResendsEnterWhenFirstSubmitDropped(t *testing.T) {
	ctx, e, conn, st := askWith(t, 1)
	pump(t, ctx, e, conn, 10*e.cfg.SubmitCheck)
	if !slices.Equal(conn.keys, []string{"Enter"}) {
		t.Fatalf("keys sent = %q, want one Enter resend", conn.keys)
	}
	if conn.input != "" {
		t.Fatalf("prompt %q still unsubmitted", conn.input)
	}
	if job := getJob(t, st); job.Status != store.JobRunning || job.Step != 1 {
		t.Fatalf("job %s at step %d error %q, want running at step 1", job.Status, job.Step, job.Error)
	}
}

func TestAskFailsAttemptWhenSubmitNeverTakes(t *testing.T) {
	ctx, e, conn, st := askWith(t, 1000)
	pump(t, ctx, e, conn, 10*e.cfg.SubmitCheck)
	if !slices.Equal(conn.keys, []string{"Enter", "Enter", "Enter"}) {
		t.Fatalf("keys sent = %q, want exactly %d Enter resends", conn.keys, maxSubmitResends)
	}
	job := getJob(t, st)
	if job.Status != store.JobQueued || job.Attempts != 1 || !strings.Contains(job.Error, "step 1: prompt not submitted") {
		t.Fatalf("job %s attempts %d error %q, want the attempt failed for an unsubmitted prompt", job.Status, job.Attempts, job.Error)
	}
	if !slices.Equal(conn.closed, []string{childPane}) {
		t.Fatalf("closed panes = %q, want the stuck child closed", conn.closed)
	}
}
