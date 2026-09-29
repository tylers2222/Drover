package recipe

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/tylers2222/drover/internal/herdr"
	"github.com/tylers2222/drover/internal/jsonx"
)

// Parse strictly decodes (unknown fields, trailing data rejected), defaults a nil
// Spawn.Ready to wait for idle, then validates: a spawn command, no EnvJobID or
// EnvResult in spawn.env, ≥1 step, valid send and wait per step, last step waits on
// a status.
func Parse(data []byte) (*Recipe, error) {
	var r Recipe
	if err := jsonx.DecodeStrict(data, &r); err != nil {
		return nil, fmt.Errorf("decode recipe: %w", err)
	}
	if r.Spawn.Ready == nil {
		r.Spawn.Ready = &Wait{Kind: WaitStatus, Status: herdr.StatusIdle}
	}
	if err := r.Validate(); err != nil {
		return nil, fmt.Errorf("invalid recipe: %w", err)
	}
	return &r, nil
}

func (r *Recipe) Validate() error {
	if r.Spawn.Command == "" {
		return errors.New("spawn.command is required")
	}
	for _, k := range [...]string{EnvJobID, EnvResult} {
		if _, ok := r.Spawn.Env[k]; ok {
			return fmt.Errorf("spawn.env.%s is reserved: the engine injects it", k)
		}
	}
	if r.Spawn.Ready != nil {
		if err := r.Spawn.Ready.validate(); err != nil {
			return fmt.Errorf("spawn.ready: %w", err)
		}
	}
	if len(r.Steps) == 0 {
		return errors.New("steps: at least one step is required")
	}
	for i := range r.Steps {
		if err := r.Steps[i].validate(); err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
	}
	// The last step's terminal state is the job's, so it must wait on a status.
	if last := len(r.Steps) - 1; r.Steps[last].Wait.Kind != WaitStatus {
		return fmt.Errorf("steps[%d]: the last step must wait for kind %q, got %q", last, WaitStatus, r.Steps[last].Wait.Kind)
	}
	return nil
}

func (s *Step) validate() error {
	switch s.Send {
	case SendAsk, SendText:
		if s.Text == "" {
			return fmt.Errorf("send %s requires text", s.Send)
		}
	case SendKeys:
		if len(s.Keys) == 0 {
			return errors.New("send keys requires at least one key")
		}
	case SendSteer:
		if len(s.Keys) != 1 {
			return fmt.Errorf("send steer takes exactly one key, got %d", len(s.Keys))
		}
	default:
		return fmt.Errorf("send %q: want ask, text, keys or steer", s.Send)
	}
	if err := s.Wait.validate(); err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	return nil
}

func (w *Wait) validate() error {
	switch w.Kind {
	case WaitNone:
	case WaitSettle:
		if w.Settle <= 0 {
			return errors.New("kind settle requires settle > 0")
		}
	case WaitStatus:
		switch w.Status {
		case herdr.StatusIdle, herdr.StatusWorking, herdr.StatusBlocked, herdr.StatusDone:
		default:
			return fmt.Errorf("kind status: status %q: want idle, working, blocked or done", w.Status)
		}
	case WaitOutput:
		if _, err := regexp.Compile(w.Regex); err != nil {
			return fmt.Errorf("kind output: %w", err)
		}
	default:
		return fmt.Errorf("kind %q: want none, settle, status or output", w.Kind)
	}
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Accepts a Go duration string or a JSON integer of nanoseconds; null is a no-op.
func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration %s: want a string like \"30s\" or integer nanoseconds: %w", b, err)
	}
	*d = Duration(n)
	return nil
}
