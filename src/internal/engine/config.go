// Package engine: the workflow loop. Holds the slot semaphore, subscribes to
// herdr events, routes each event to its pane's step FSM, never polls.
package engine

import "time"

// Callers' defaults for Slots and SafetyTick; the engine applies none itself.
const (
	DefaultSlots      = 2
	DefaultSafetyTick = 30 * time.Second
)

// Orchestrator is the pane asked for recipes. Source is exec'd (sh -c) when
// the queue is empty and prints one job JSON per line. Every field is taken as
// given: Slots at least 1, durations positive, ResultDir existing and absolute.
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
