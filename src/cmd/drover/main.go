package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/tylers2222/drover/internal/engine"
	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/recipe"
	"github.com/tylers2222/drover/internal/store"
)

type command struct {
	name, args, help string
	run              func(ctx context.Context, fs *flag.FlagSet, args []string) error
}

func commands() []command {
	return []command{
		{"run", "-orchestrator <pane> [flags]", "run the engine until SIGINT/SIGTERM", cmdRun},
		{"enqueue", "<file|-|json> [-db path]", "queue jobs from JSONL", cmdEnqueue},
		{"decide", "<id> <file|-|json> [-db path]", "answer a decision with a recipe", cmdDecide},
		{"status", "[-db path]", "job counts and pending decisions", cmdStatus},
		{"jobs", "[-status s] [-db path]", "jobs as JSON lines", cmdJobs},
		{"events", "<job> [-db path]", "a job's events as JSON lines", cmdEvents},
		{"doctor", "[-socket path]", "check the herdr connection", cmdDoctor},
	}
}

// Bad invocation: the problem and usage are already on stderr; exit 2.
type usageError struct{}

func (usageError) Error() string { return "usage" }

func main() { os.Exit(dispatch(context.Background(), os.Args[1:])) }

func dispatch(ctx context.Context, args []string) int {
	cmds := commands()
	if len(args) == 0 {
		usage(os.Stderr, cmds)
		return 2
	}
	switch args[0] {
	case "help", "-h", "-help", "--help":
		usage(os.Stdout, cmds)
		return 0
	}
	for _, c := range cmds {
		if c.name != args[0] {
			continue
		}
		fs := flag.NewFlagSet(c.name, flag.ContinueOnError)
		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), "usage: drover %s %s\n", c.name, c.args)
			fs.PrintDefaults()
		}
		err := c.run(ctx, fs, args[1:])
		switch {
		case err == nil, errors.Is(err, flag.ErrHelp):
			return 0
		case errors.Is(err, usageError{}):
			return 2
		}
		fmt.Fprintf(os.Stderr, "drover %s: %v\n", c.name, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "drover: unknown command %q\n", args[0])
	usage(os.Stderr, cmds)
	return 2
}

func usage(w io.Writer, cmds []command) {
	fmt.Fprint(w, "usage: drover <command> [args]\n\ncommands:\n")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, c := range cmds {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", c.name, c.args, c.help)
	}
	tw.Flush()
}

func cmdRun(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	socket := socketFlag(fs)
	orch := fs.String("orchestrator", "", "orchestrator pane `id` (required)")
	parent := fs.String("parent", "", "pane `id` that job panes split beside (default -orchestrator)")
	source := fs.String("source", "", "`command` (sh -c) that prints job JSONL when the queue is empty")
	results := fs.String("results", ".drover/results", "result report `dir`")
	slots := fs.Int("slots", engine.DefaultSlots, "concurrent job panes")
	maxAttempts := fs.Int("max-attempts", 0, "cap on attempts per job; 0 = each job's max_attempts")
	stepTimeout := fs.Duration("step-timeout", recipe.DefaultStepTimeout, "step wait timeout when the recipe sets none")
	readyTimeout := fs.Duration("ready-timeout", recipe.DefaultReadyTimeout, "spawn ready timeout when the recipe sets none")
	tick := fs.Duration("tick", engine.DefaultSafetyTick, "safety interval to re-check decisions and panes")
	debug := fs.Bool("debug", false, "log every herdr request, event and FSM step")
	if _, err := parseArgs(fs, args, 0); err != nil {
		return err
	}
	if *debug {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}
	// The engine takes Config as given; nothing past this point repairs a bad value.
	switch {
	case *orch == "":
		return badUsage(fs, "-orchestrator is required")
	case *slots < 1:
		return badUsage(fs, "-slots must be at least 1, got %d", *slots)
	case *stepTimeout <= 0:
		return badUsage(fs, "-step-timeout must be positive, got %v", *stepTimeout)
	case *readyTimeout <= 0:
		return badUsage(fs, "-ready-timeout must be positive, got %v", *readyTimeout)
	case *tick <= 0:
		return badUsage(fs, "-tick must be positive, got %v", *tick)
	}
	if *parent == "" {
		*parent = *orch
	}
	// Children write $HERD_RESULT from their own cwd, so the dir must be absolute.
	resultDir, err := filepath.Abs(*results)
	if err != nil {
		return fmt.Errorf("resolve -results: %w", err)
	}
	if err := os.MkdirAll(resultDir, 0o755); err != nil {
		return fmt.Errorf("create results dir: %w", err)
	}
	st, err := openStore(*db, true)
	if err != nil {
		return err
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Restore default handling once cancelled, so a second signal kills a stuck shutdown.
	context.AfterFunc(ctx, stop)

	conn, err := herdr.Dial(ctx, *socket)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = engine.New(engine.Config{
		Slots:        *slots,
		Orchestrator: *orch,
		ParentPane:   *parent,
		Source:       *source,
		ResultDir:    resultDir,
		ReadyTimeout: *readyTimeout,
		StepTimeout:  *stepTimeout,
		SafetyTick:   *tick,
		MaxAttempts:  *maxAttempts,
	}, conn, st).Run(ctx)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}

func cmdEnqueue(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	in, err := openInput(pos[0])
	if err != nil {
		return err
	}
	jobs, err := engine.ParseJobs(in)
	in.Close()
	if err != nil {
		return fmt.Errorf("parse jobs: %w", err)
	}
	st, err := openStore(*db, true)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Enqueue(ctx, jobs); err != nil {
		return err
	}
	return printJSON(struct {
		Enqueued int `json:"enqueued"`
	}{len(jobs)})
}

func cmdDecide(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	pos, err := parseArgs(fs, args, 2)
	if err != nil {
		return err
	}
	id, err := strconv.ParseInt(pos[0], 10, 64)
	if err != nil {
		return badUsage(fs, "decision id %q is not an integer", pos[0])
	}
	in, err := openInput(pos[1])
	if err != nil {
		return err
	}
	data, err := io.ReadAll(in)
	in.Close()
	if err != nil {
		return fmt.Errorf("read recipe: %w", err)
	}
	if _, err := recipe.Parse(data); err != nil {
		return err
	}
	st, err := openStore(*db, false)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.AnswerDecision(ctx, id, data); err != nil {
		return err
	}
	return printJSON(struct {
		OK       bool  `json:"ok"`
		Decision int64 `json:"decision"`
	}{true, id})
}

func cmdStatus(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	if _, err := parseArgs(fs, args, 0); err != nil {
		return err
	}
	st, err := openStore(*db, false)
	if err != nil {
		return err
	}
	defer st.Close()
	counts, err := st.Counts(ctx)
	if err != nil {
		return err
	}
	pending, err := st.PendingDecisions(ctx)
	if err != nil {
		return err
	}
	if pending == nil {
		pending = []store.Decision{}
	}
	return printJSON(struct {
		Counts  map[store.JobStatus]int `json:"counts"`
		Pending []store.Decision        `json:"pending_decisions"`
	}{counts, pending})
}

func cmdJobs(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	status := fs.String("status", "", "only jobs with this `status`")
	if _, err := parseArgs(fs, args, 0); err != nil {
		return err
	}
	var filter []store.JobStatus
	if *status != "" {
		filter = []store.JobStatus{store.JobStatus(*status)}
	}
	st, err := openStore(*db, false)
	if err != nil {
		return err
	}
	defer st.Close()
	jobs, err := st.List(ctx, filter...)
	if err != nil {
		return err
	}
	return printJSON(jobs...)
}

func cmdEvents(ctx context.Context, fs *flag.FlagSet, args []string) error {
	db := dbFlag(fs)
	pos, err := parseArgs(fs, args, 1)
	if err != nil {
		return err
	}
	st, err := openStore(*db, false)
	if err != nil {
		return err
	}
	defer st.Close()
	events, err := st.Events(ctx, pos[0])
	if err != nil {
		return err
	}
	return printJSON(events...)
}

func cmdDoctor(ctx context.Context, fs *flag.FlagSet, args []string) error {
	socket := socketFlag(fs)
	if _, err := parseArgs(fs, args, 0); err != nil {
		return err
	}
	if os.Getenv("HERDR_ENV") != "1" {
		fmt.Fprintln(os.Stderr, "hint: HERDR_ENV is not 1, so this is not a herdr pane; run drover and the orchestrator inside herdr")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := herdr.Dial(ctx, *socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	pane, err := conn.CurrentPane(ctx)
	if err != nil {
		return fmt.Errorf("pane.current: %w", err)
	}
	return printJSON(pane)
}

// flag stops at the first positional; re-parsing the rest lets flags follow positionals.
func parseArgs(fs *flag.FlagSet, args []string, want int) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, err
			}
			return nil, usageError{} // flag already printed the error and usage
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
	if len(pos) != want {
		return nil, badUsage(fs, "expected %d argument(s), got %d", want, len(pos))
	}
	return pos, nil
}

func badUsage(fs *flag.FlagSet, format string, a ...any) error {
	fmt.Fprintf(fs.Output(), "drover %s: %s\n", fs.Name(), fmt.Sprintf(format, a...))
	fs.Usage()
	return usageError{}
}

func dbFlag(fs *flag.FlagSet) *string {
	return fs.String("db", ".drover/drover.db", "SQLite database `path`")
}

func socketFlag(fs *flag.FlagSet) *string {
	return fs.String("socket", "", "herdr socket `path` (default $HERDR_SOCKET or ~/.config/herdr/herdr.sock)")
}

// Only run and enqueue may create the db; readers run from a wrong cwd must fail,
// not leave a stray empty database behind.
func openStore(path string, create bool) (store.Store, error) {
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
	} else if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	return store.Open(path)
}

// "-" is stdin; an argument starting with "{" is inline JSON (the form the
// engine's decision prompt uses); anything else is a file path.
func openInput(arg string) (io.ReadCloser, error) {
	switch {
	case arg == "-":
		return io.NopCloser(os.Stdin), nil
	case strings.HasPrefix(strings.TrimSpace(arg), "{"):
		return io.NopCloser(strings.NewReader(arg)), nil
	}
	f, err := os.Open(arg)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func printJSON[T any](vs ...T) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	for i := range vs {
		if err := enc.Encode(&vs[i]); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	return nil
}
