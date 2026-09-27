// Package recipe: the program the engine runs for one job. Steps run in order;
// only the last step's terminal state is the job's terminal state.
package recipe

import (
	"time"

	"github.com/tylers2222/drover/internal/herdr"
)

// Engine always injects HERD_JOB_ID and HERD_RESULT into Spawn.Env.
type Recipe struct {
	Class string `json:"class,omitempty"`
	Spawn Spawn  `json:"spawn"`
	Steps []Step `json:"steps"`
}

// Workspace=false splits beside the engine's parent pane. Ready nil = wait for idle.
type Spawn struct {
	Command   string            `json:"command"`
	CWD       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Workspace bool              `json:"workspace,omitempty"`
	Ready     *Wait             `json:"ready,omitempty"`
}

// ask=agent.prompt (starts a turn), text=pane.send_text (no Enter),
// keys=pane.send_keys, steer=keys with one key (parity with write_to_pane.sh).
type SendKind string

const (
	SendAsk   SendKind = "ask"
	SendText  SendKind = "text"
	SendKeys  SendKind = "keys"
	SendSteer SendKind = "steer"
)

type Step struct {
	Send SendKind `json:"send"`
	Text string   `json:"text,omitempty"`
	Keys []string `json:"keys,omitempty"`
	Wait Wait     `json:"wait"`
}

// none=return at once, settle=sleep Settle, status=agent status via events,
// output=pane output matches Regex (anchor it; scrollback echoes the command).
type WaitKind string

const (
	WaitNone   WaitKind = "none"
	WaitSettle WaitKind = "settle"
	WaitStatus WaitKind = "status"
	WaitOutput WaitKind = "output"
)

type Wait struct {
	Kind    WaitKind          `json:"kind"`
	Status  herdr.AgentStatus `json:"status,omitempty"`
	Regex   string            `json:"regex,omitempty"`
	Settle  Duration          `json:"settle,omitempty"`
	Timeout Duration          `json:"timeout,omitempty"`
}

// JSON form is a Go duration string: "30s", "5m".
type Duration time.Duration

const (
	DefaultReadyTimeout = 2 * time.Minute
	DefaultStepTimeout  = 30 * time.Minute
	DefaultSettle       = 500 * time.Millisecond
)

// Parse decodes and validates; rules documented on parse in recipe.go.
func Parse(data []byte) (*Recipe, error) { return parse(data) }
