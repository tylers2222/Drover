package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/tylers2222/drover/internal/store"
)

const (
	sourceTimeout      = 60 * time.Second
	defaultMaxAttempts = 2
)

type jobLine struct {
	ID          string          `json:"id"`
	Class       string          `json:"class"`
	Title       string          `json:"title"`
	Spec        json.RawMessage `json:"spec"`
	MaxAttempts int             `json:"max_attempts"`
}

func (e *Engine) pullSource(ctx context.Context) ([]store.Job, error) {
	ctx, cancel := context.WithTimeout(ctx, sourceTimeout)
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "sh", "-c", e.cfg.Source)
	cmd.Stderr = &stderr
	// After a timeout kill, stop waiting on grandchildren that still hold stdout.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err != nil {
		if msg := bytes.TrimSpace(stderr.Bytes()); len(msg) > 0 {
			err = fmt.Errorf("%w: %s", err, msg)
		}
		return nil, fmt.Errorf("source: %w", err)
	}
	jobs, err := ParseJobs(bytes.NewReader(out))
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	return jobs, nil
}

// ParseJobs reads `drover enqueue` JSONL, one job per line, blank lines skipped.
// A missing id is left empty for store.Enqueue to generate.
func ParseJobs(r io.Reader) ([]store.Job, error) {
	var jobs []store.Job
	br := bufio.NewReader(r)
	for n := 1; ; n++ {
		line, err := br.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			job, perr := parseJob(line)
			if perr != nil {
				return nil, fmt.Errorf("line %d: %w", n, perr)
			}
			jobs = append(jobs, job)
		}
		if errors.Is(err, io.EOF) {
			return jobs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
	}
}

func parseJob(line []byte) (store.Job, error) {
	var l jobLine
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&l); err != nil {
		return store.Job{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return store.Job{}, errors.New("unexpected data after the JSON value")
	}
	if l.MaxAttempts < 0 {
		return store.Job{}, fmt.Errorf("max_attempts %d is negative", l.MaxAttempts)
	}
	if l.MaxAttempts == 0 {
		l.MaxAttempts = defaultMaxAttempts
	}
	return store.Job{
		ID:          l.ID,
		Class:       l.Class,
		Title:       l.Title,
		Spec:        l.Spec,
		MaxAttempts: l.MaxAttempts,
		Status:      store.JobQueued,
	}, nil
}
