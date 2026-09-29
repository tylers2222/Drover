package herdr

import (
	"context"
	"time"
)

// Unwrapped keeps long lines whole instead of cut at the pane width.
const sourceRecentUnwrapped = "recent_unwrapped"

type paneTarget struct {
	PaneID string `json:"pane_id"`
}

type paneResult struct {
	Pane PaneInfo `json:"pane"`
}

// agent.* methods return AgentInfo, a superset of PaneInfo.
type agentResult struct {
	Agent PaneInfo `json:"agent"`
}

type outputMatch struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func (c *conn) SplitPane(ctx context.Context, p SplitParams) (PaneInfo, error) {
	// Empty target is our own pane, as `herdr pane split --current` resolves it;
	// outside herdr the field stays omitted and the server picks.
	if p.TargetPaneID == "" {
		p.TargetPaneID = c.callerPane
	}
	var r paneResult
	if err := c.Call(ctx, "pane.split", p, &r); err != nil {
		return PaneInfo{}, err
	}
	return r.Pane, nil
}

func (c *conn) CreateWorkspace(ctx context.Context, p WorkspaceCreateParams) (PaneInfo, error) {
	var r struct {
		RootPane PaneInfo `json:"root_pane"`
	}
	if err := c.Call(ctx, "workspace.create", p, &r); err != nil {
		return PaneInfo{}, err
	}
	return r.RootPane, nil
}

// The same request `herdr pane run` sends: the text, then Enter.
func (c *conn) RunCommand(ctx context.Context, paneID, command string) error {
	return c.Call(ctx, "pane.send_input", struct {
		PaneID string   `json:"pane_id"`
		Text   string   `json:"text"`
		Keys   []string `json:"keys"`
	}{paneID, command, []string{"Enter"}}, nil)
}

func (c *conn) SendText(ctx context.Context, paneID, text string) error {
	return c.Call(ctx, "pane.send_text", struct {
		PaneID string `json:"pane_id"`
		Text   string `json:"text"`
	}{paneID, text}, nil)
}

func (c *conn) SendKeys(ctx context.Context, paneID string, keys []string) error {
	return c.Call(ctx, "pane.send_keys", struct {
		PaneID string   `json:"pane_id"`
		Keys   []string `json:"keys"`
	}{paneID, keys}, nil)
}

func (c *conn) Prompt(ctx context.Context, target, text string) (PaneInfo, error) {
	var r agentResult
	err := c.Call(ctx, "agent.prompt", struct {
		Target string `json:"target"`
		Text   string `json:"text"`
	}{target, text}, &r)
	if err != nil {
		return PaneInfo{}, err
	}
	return r.Agent, nil
}

func (c *conn) WaitOutput(ctx context.Context, paneID, regex string, timeout time.Duration) (string, error) {
	var r struct {
		MatchedLine string `json:"matched_line"`
	}
	err := c.Call(ctx, "pane.wait_for_output", struct {
		PaneID    string      `json:"pane_id"`
		Source    string      `json:"source"`
		Match     outputMatch `json:"match"`
		TimeoutMS int64       `json:"timeout_ms,omitempty"`
	}{paneID, sourceRecentUnwrapped, outputMatch{"regex", regex}, millis(timeout)}, &r)
	if err != nil {
		return "", err
	}
	return r.MatchedLine, nil
}

func (c *conn) ClosePane(ctx context.Context, paneID string) error {
	return c.Call(ctx, "pane.close", paneTarget{paneID}, nil)
}

func (c *conn) ListPanes(ctx context.Context, workspaceID string) ([]PaneInfo, error) {
	var r struct {
		Panes []PaneInfo `json:"panes"`
	}
	err := c.Call(ctx, "pane.list", struct {
		WorkspaceID string `json:"workspace_id,omitempty"`
	}{workspaceID}, &r)
	if err != nil {
		return nil, err
	}
	return r.Panes, nil
}

// Like `herdr pane current`: the pane we run in when known, else herdr's choice.
func (c *conn) CurrentPane(ctx context.Context) (PaneInfo, error) {
	var r paneResult
	err := c.Call(ctx, "pane.current", struct {
		CallerPaneID string `json:"caller_pane_id,omitempty"`
	}{c.callerPane}, &r)
	if err != nil {
		return PaneInfo{}, err
	}
	return r.Pane, nil
}

// Rounds up: a sub-millisecond timeout must not become 0, which is omitted and
// so means no timeout at all.
func millis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}
