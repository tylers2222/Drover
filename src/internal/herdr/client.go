package herdr

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const eventBuffer = 256

// herdr answers one request per connection and a subscribed connection accepts
// nothing further, so every Call and every Subscribe dials its own socket.
type conn struct {
	path       string
	callerPane string
	seq        atomic.Uint64
	life       context.Context
	stop       context.CancelFunc

	mu        sync.Mutex
	streamErr error
	wg        sync.WaitGroup
}

type request struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

// Result holds the caller's pointer so the payload decodes in place.
type response struct {
	Result any       `json:"result"`
	Error  *RPCError `json:"error"`
}

func Dial(ctx context.Context, socketPath string) (Conn, error) {
	path, err := resolveSocket(socketPath)
	if err != nil {
		return nil, fmt.Errorf("herdr: resolve socket: %w", err)
	}
	life, stop := context.WithCancel(context.Background())
	c := &conn{path: path, callerPane: os.Getenv("HERDR_PANE_ID"), life: life, stop: stop}
	// Nothing stays connected, so ping once to fail fast when herdr is down.
	if err := c.Call(ctx, "ping", nil, nil); err != nil {
		stop()
		return nil, err
	}
	return c, nil
}

// HERDR_SOCKET_PATH is what herdr itself exports into its panes.
func resolveSocket(path string) (string, error) {
	if p := cmp.Or(path, os.Getenv("HERDR_SOCKET"), os.Getenv("HERDR_SOCKET_PATH")); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "herdr", "herdr.sock"), nil
}

func (c *conn) Call(ctx context.Context, method string, params any, result any) error {
	ctx, done := c.bind(ctx)
	defer done()
	if _, err := c.exchange(ctx, method, params, result); err != nil {
		return wrap(ctx, method, err)
	}
	return nil
}

func (c *conn) Subscribe(ctx context.Context, subs []Subscription) (<-chan Event, error) {
	const method = "events.subscribe"
	c.mu.Lock()
	if c.life.Err() != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("herdr %s: %w", method, net.ErrClosed)
	}
	c.wg.Add(1)
	c.mu.Unlock()

	ctx, done := c.bind(ctx)
	params := struct {
		Subscriptions []Subscription `json:"subscriptions"`
	}{subs}
	var started struct {
		Type string `json:"type"`
	}
	dec, err := c.exchange(ctx, method, params, &started)
	if err == nil && started.Type != "subscription_started" {
		err = fmt.Errorf("unexpected result type %q", started.Type)
	}
	if err != nil {
		err = wrap(ctx, method, err)
		done()
		c.wg.Done()
		return nil, err
	}
	ch := make(chan Event, eventBuffer)
	go c.stream(ctx, done, dec, ch)
	return ch, nil
}

// Sends never block: a full subscriber loses the event so the socket is always
// drained and herdr never waits on us. Losses are logged at powers of two, so a
// stalled subscriber shows without flooding the log.
func (c *conn) stream(ctx context.Context, done func(), dec *json.Decoder, ch chan<- Event) {
	defer c.wg.Done()
	defer close(ch)
	defer done()
	var dropped int
	for {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			if ctx.Err() == nil {
				c.mu.Lock()
				c.streamErr = fmt.Errorf("herdr: event stream: %w", err)
				c.mu.Unlock()
			}
			return
		}
		ev.ReceivedAt = time.Now()
		if ev.Kind == "" {
			continue
		}
		lift(&ev)
		select {
		case ch <- ev:
		default:
			dropped++
			if dropped&(dropped-1) == 0 {
				slog.Warn("herdr: subscriber full, dropping events", "event", ev.Kind, "pane", ev.PaneID, "dropped", dropped)
			}
		}
	}
}

// Lifecycle events arrive as "pane_closed" (event.$defs.EventKind in
// docs/herdr-api-schema.json); subscriptions name them "pane.closed"
// (request.$defs.Subscription). Subscription events already arrive dotted
// (subscription_event.$defs.SubscriptionEventKind). Unlisted kinds keep their
// wire spelling.
var subscriptionKinds = map[string]string{
	"pane_closed":               EventPaneClosed,
	"pane_exited":               EventPaneExited,
	"pane_agent_detected":       EventAgentDetected,
	"pane_agent_status_changed": EventAgentStatusChanged,
}

// pane_created/updated/moved nest the pane under data.pane.
func lift(ev *Event) {
	if k, ok := subscriptionKinds[ev.Kind]; ok {
		ev.Kind = k
	}
	var d struct {
		PaneID      string      `json:"pane_id"`
		AgentStatus AgentStatus `json:"agent_status"`
		Pane        struct {
			PaneID      string      `json:"pane_id"`
			AgentStatus AgentStatus `json:"agent_status"`
		} `json:"pane"`
	}
	if json.Unmarshal(ev.Data, &d) != nil {
		return
	}
	ev.PaneID = cmp.Or(d.PaneID, d.Pane.PaneID)
	ev.AgentStatus = cmp.Or(d.AgentStatus, d.Pane.AgentStatus)
}

// Err reports the last event-stream failure; a stream ended by its own ctx is
// not a failure.
func (c *conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.life.Err() != nil {
		return net.ErrClosed
	}
	return c.streamErr
}

// Returns once every in-flight Subscribe has finished and every channel is closed.
func (c *conn) Close() error {
	c.mu.Lock()
	c.stop()
	c.mu.Unlock()
	c.wg.Wait()
	return nil
}

// The returned ctx also ends on Close, with cause net.ErrClosed. The eager check
// matters: AfterFunc fires asynchronously, and nothing may be sent after Close.
func (c *conn) bind(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(c.life, func() { cancel(net.ErrClosed) })
	if c.life.Err() != nil {
		cancel(net.ErrClosed)
	}
	return ctx, func() { stop(); cancel(nil) }
}

// exchange dials, sends one request and decodes its response into result. The
// socket closes when ctx ends; the decoder is left positioned for events.
func (c *conn) exchange(ctx context.Context, method string, params, result any) (*json.Decoder, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "unix", c.path)
	if err != nil {
		return nil, err
	}
	context.AfterFunc(ctx, func() { nc.Close() })
	// herdr requires a params object even for methods that take none.
	if params == nil {
		params = struct{}{}
	}
	req := request{ID: strconv.FormatUint(c.seq.Add(1), 10), Method: method, Params: params}
	if err := json.NewEncoder(nc).Encode(req); err != nil {
		return nil, err
	}
	if result == nil {
		result = &struct{}{}
	}
	resp := response{Result: result}
	dec := json.NewDecoder(nc)
	if err := dec.Decode(&resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return dec, nil
}

// A cancelled op fails with whatever closing its socket caused; report why it
// was cancelled instead.
func wrap(ctx context.Context, method string, err error) error {
	if ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	return fmt.Errorf("herdr %s: %w", method, err)
}
