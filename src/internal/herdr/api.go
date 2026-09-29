// Package herdr: newline-delimited JSON over the herdr unix socket.
// Requests/responses matched by id; events pushed as {"event":..., "data":...}.
package herdr

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type AgentStatus string

const (
	StatusIdle    AgentStatus = "idle"
	StatusWorking AgentStatus = "working"
	StatusBlocked AgentStatus = "blocked"
	StatusDone    AgentStatus = "done"
	StatusUnknown AgentStatus = "unknown"
)

type PaneInfo struct {
	PaneID         string      `json:"pane_id"`
	WorkspaceID    string      `json:"workspace_id"`
	TabID          string      `json:"tab_id"`
	CWD            string      `json:"cwd"`
	ForegroundCWD  string      `json:"foreground_cwd"`
	Agent          *string     `json:"agent"`
	AgentStatus    AgentStatus `json:"agent_status"`
	StateChangeSeq int64       `json:"state_change_seq"`
	Revision       int64       `json:"revision"`
	TerminalTitle  string      `json:"terminal_title_stripped"`
}

// Codes seen: pane_not_found, agent_not_found, timeout, agent_prompt_stalled, invalid_request.
type RPCError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return e.Code + ": " + e.Message }

func IsCode(err error, code string) bool {
	var re *RPCError
	return errors.As(err, &re) && re.Code == code
}

// PaneID is required for pane.agent_status_changed, ignored otherwise.
type Subscription struct {
	Type   string `json:"type"`
	PaneID string `json:"pane_id,omitempty"`
}

const (
	EventAgentStatusChanged = "pane.agent_status_changed"
	EventPaneClosed         = "pane.closed"
	EventPaneExited         = "pane.exited"
	EventAgentDetected      = "pane.agent_detected"
)

// Kind is one of the Event* names above, or herdr's wire kind for events this
// package does not name. PaneID and AgentStatus are lifted out of Data when
// present.
type Event struct {
	Kind        string          `json:"event"`
	PaneID      string          `json:"-"`
	AgentStatus AgentStatus     `json:"-"`
	Data        json.RawMessage `json:"data"`
	ReceivedAt  time.Time       `json:"-"`
}

// Direction is required by herdr: "right" | "down".
type SplitParams struct {
	TargetPaneID string            `json:"target_pane_id,omitempty"`
	Direction    string            `json:"direction"`
	Ratio        *float64          `json:"ratio,omitempty"`
	CWD          string            `json:"cwd,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	Focus        bool              `json:"focus"`
}

type WorkspaceCreateParams struct {
	CWD   string            `json:"cwd,omitempty"`
	Label string            `json:"label,omitempty"`
	Env   map[string]string `json:"env,omitempty"`
	Focus bool              `json:"focus"`
}

// One Conn owns one socket; every method is safe for concurrent use. The
// Subscribe channel closes when ctx ends or the connection drops; Err then
// holds the cause.
type Conn interface {
	Call(ctx context.Context, method string, params any, result any) error
	Subscribe(ctx context.Context, subs []Subscription) (<-chan Event, error)
	Err() error
	Close() error

	// One herdr request per method; mapping in ops.go.
	SplitPane(ctx context.Context, p SplitParams) (PaneInfo, error)
	CreateWorkspace(ctx context.Context, p WorkspaceCreateParams) (PaneInfo, error)
	RunCommand(ctx context.Context, paneID, command string) error
	SendText(ctx context.Context, paneID, text string) error
	SendKeys(ctx context.Context, paneID string, keys []string) error
	Prompt(ctx context.Context, target, text string) (PaneInfo, error)
	WaitOutput(ctx context.Context, paneID, regex string, timeout time.Duration) (string, error)
	ClosePane(ctx context.Context, paneID string) error
	ListPanes(ctx context.Context, workspaceID string) ([]PaneInfo, error)
	CurrentPane(ctx context.Context) (PaneInfo, error)
}
