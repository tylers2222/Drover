// Package engine: the workflow loop. Holds the slot semaphore, subscribes to
// herdr events, routes each event to its pane's step FSM, never polls.
package engine

import (
	"context"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/store"
)

// Orchestrator is the pane asked for recipes. Source is exec'd (sh -c) when
// the queue is empty and prints one job JSON per line.
type Config struct {
	Slots          int
	Orchestrator   string
	ParentPane     string
	Source         string
	ResultDir      string
	DecisionPrompt string
	ReadyTimeout   time.Duration
	StepTimeout    time.Duration
	SafetyTick     time.Duration
	MaxAttempts    int
}

func New(cfg Config, conn herdr.Conn, st store.Store) *Engine { return newEngine(cfg, conn, st) }

// Run blocks until ctx is cancelled or the socket dies. Safe to restart:
// it reconciles the store against live panes before filling slots.
func (e *Engine) Run(ctx context.Context) error { return e.run(ctx) }
