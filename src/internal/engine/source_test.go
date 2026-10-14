package engine

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/recipe"
	"github.com/tylers2222/drover/internal/store"
)

const inlineRecipe = `{"class":"plan","spawn":{"command":"omp-stage","cwd":"/tmp/job/project","ready":{"kind":"status","status":"idle","timeout":"2m"}},` +
	`"steps":[{"send":"ask","text":"Run factory stage plan.","wait":{"kind":"status","status":"done","timeout":"45m"}}]}`

func TestParseJobsInlineRecipe(t *testing.T) {
	line := `{"id":"job_1-plan-1","class":"plan","title":"plan","recipe":` + inlineRecipe + "}\n"
	jobs, err := ParseJobs(strings.NewReader(line))
	if err != nil {
		t.Fatalf("ParseJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	if string(jobs[0].Recipe) != inlineRecipe {
		t.Fatalf("Recipe = %s, want the inline recipe verbatim", jobs[0].Recipe)
	}
}

func TestParseJobsWithoutRecipe(t *testing.T) {
	for name, line := range map[string]string{
		"absent": `{"id":"j1","class":"c","title":"t","spec":{"x":1},"max_attempts":2}`,
		"null":   `{"id":"j1","class":"c","title":"t","spec":{"x":1},"max_attempts":2,"recipe":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			jobs, err := ParseJobs(strings.NewReader(line))
			if err != nil {
				t.Fatalf("ParseJobs: %v", err)
			}
			j := jobs[0]
			if j.Recipe != nil {
				t.Fatalf("Recipe = %s, want nil so dispatch falls through to class recipes", j.Recipe)
			}
			if j.ID != "j1" || j.Class != "c" || j.Title != "t" || string(j.Spec) != `{"x":1}` || j.MaxAttempts != 2 {
				t.Fatalf("job = %+v", j)
			}
		})
	}
}

func TestParseJobsRejectsInvalidRecipe(t *testing.T) {
	cases := map[string]struct{ recipe, want string }{
		"no steps": {
			`{"spawn":{"command":"omp"},"steps":[]}`,
			"at least one step",
		},
		"last step not a status wait": {
			`{"spawn":{"command":"omp"},"steps":[{"send":"text","text":"hi","wait":{"kind":"none"}}]}`,
			"last step must wait",
		},
		"unknown field": {
			`{"spawn":{"command":"omp"},"steps":[],"bogus":1}`,
			"unknown field",
		},
		"not an object": {
			`"omp"`,
			"decode recipe",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			input := `{"id":"ok"}` + "\n" + `{"id":"bad","recipe":` + c.recipe + "}\n"
			_, err := ParseJobs(strings.NewReader(input))
			if err == nil {
				t.Fatal("ParseJobs accepted an invalid recipe")
			}
			if msg := err.Error(); !strings.HasPrefix(msg, "line 2: recipe:") || !strings.Contains(msg, c.want) {
				t.Fatalf("error %q: want prefix %q and %q", msg, "line 2: recipe:", c.want)
			}
		})
	}
}

func TestEnqueueStoresInlineRecipe(t *testing.T) {
	ctx := context.Background()
	st := openStore(t)
	jobs, err := ParseJobs(strings.NewReader(`{"id":"j1","recipe":` + inlineRecipe + "}"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, "j1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Recipe) != inlineRecipe {
		t.Fatalf("jobs.recipe = %s, want the inline recipe", got.Recipe)
	}
}

func TestDispatchInlineRecipeSkipsOrchestrator(t *testing.T) {
	e, conn, st := newTestEngine(t, `{"id":"j1","class":"plan","recipe":`+inlineRecipe+"}")
	if err := e.fill(context.Background()); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if conn.prompts[orchestratorPane] != 0 {
		t.Fatal("orchestrator was prompted for a job with an inline recipe")
	}
	pending, err := st.PendingDecisions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("%d decisions opened, want 0", len(pending))
	}
	job, err := st.Get(context.Background(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != store.JobRunning || job.PaneID != childPane {
		t.Fatalf("job status %q pane %q, want running in %s", job.Status, job.PaneID, childPane)
	}
	if conn.split.CWD != "/tmp/job/project" || conn.split.Env[recipe.EnvJobID] != "j1" {
		t.Fatalf("split = %+v", conn.split)
	}
	if len(conn.commands) != 1 || conn.commands[0] != "omp-stage" {
		t.Fatalf("commands = %q, want [omp-stage]", conn.commands)
	}
}

// Control for the test above: the same arrangement without a recipe must reach
// the orchestrator, or the absence asserted there proves nothing.
func TestDispatchWithoutRecipeAsksOrchestrator(t *testing.T) {
	e, conn, st := newTestEngine(t, `{"id":"j1","class":"plan"}`)
	if err := e.fill(context.Background()); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if conn.prompts[orchestratorPane] != 1 {
		t.Fatalf("orchestrator prompted %d times, want 1", conn.prompts[orchestratorPane])
	}
	if conn.split.Direction != "" {
		t.Fatal("a pane was spawned before any recipe existed")
	}
	job, err := st.Get(context.Background(), "j1")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != store.JobDeciding {
		t.Fatalf("job status %q, want %q", job.Status, store.JobDeciding)
	}
}

const (
	orchestratorPane = "w1:p1"
	childPane        = "w1:p2"
)

func openStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "drover.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func newTestEngine(t *testing.T, jsonl string) (*Engine, *fakeConn, store.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st := openStore(t)
	jobs, err := ParseJobs(strings.NewReader(jsonl))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	conn := &fakeConn{prompts: map[string]int{}, statuses: make(chan herdr.AgentStatus, 16)}
	e := New(Config{
		Slots:        1,
		Orchestrator: orchestratorPane,
		ParentPane:   orchestratorPane,
		ResultDir:    t.TempDir(),
		ReadyTimeout: recipe.DefaultReadyTimeout,
		StepTimeout:  recipe.DefaultStepTimeout,
		SubmitCheck:  20 * time.Millisecond,
		MaxAttempts:  store.DefaultMaxAttempts,
	}, conn, st)
	// An idle orchestrator takes any ask at once, so a wrongly opened decision
	// shows up as a prompt.
	e.orch.status = herdr.StatusIdle
	return e, conn, st
}

// fakeConn implements the herdr calls a job makes; any other call panics on
// the nil embedded Conn. The child pane models omp's input box: a prompt is
// typed, and only an Enter that is not dropped submits it and turns the agent
// working, which the fake reports on statuses.
type fakeConn struct {
	herdr.Conn

	mu         sync.Mutex
	split      herdr.SplitParams
	commands   []string
	prompts    map[string]int
	dropEnters int
	input      string
	keys       []string
	closed     []string
	statuses   chan herdr.AgentStatus
}

func (c *fakeConn) SplitPane(_ context.Context, p herdr.SplitParams) (herdr.PaneInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.split = p
	return herdr.PaneInfo{PaneID: childPane, AgentStatus: herdr.StatusUnknown}, nil
}

func (c *fakeConn) RunCommand(_ context.Context, _, command string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = append(c.commands, command)
	return nil
}

func (c *fakeConn) Prompt(_ context.Context, target, text string) (herdr.PaneInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prompts[target]++
	if target != childPane {
		return herdr.PaneInfo{PaneID: target, AgentStatus: herdr.StatusWorking}, nil
	}
	c.input = text
	c.enter()
	return herdr.PaneInfo{PaneID: target, AgentStatus: herdr.StatusIdle}, nil
}

func (c *fakeConn) SendKeys(_ context.Context, _ string, keys []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		c.keys = append(c.keys, k)
		if k == "Enter" {
			c.enter()
		}
	}
	return nil
}

// enter runs with c.mu held.
func (c *fakeConn) enter() {
	switch {
	case c.input == "":
	case c.dropEnters > 0:
		c.dropEnters--
	default:
		c.input = ""
		c.statuses <- herdr.StatusWorking
	}
}

func (c *fakeConn) ClosePane(_ context.Context, pane string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = append(c.closed, pane)
	return nil
}

func (c *fakeConn) Subscribe(ctx context.Context, _ []herdr.Subscription) (<-chan herdr.Event, error) {
	ch := make(chan herdr.Event)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (c *fakeConn) Err() error { return nil }
