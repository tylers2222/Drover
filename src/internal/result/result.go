package result

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

func load(path, want string) (*Report, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("job %s wrote no report at %s: %w", want, path, os.ErrNotExist)
	}
	if err != nil {
		return nil, fmt.Errorf("read report: %w", err)
	}
	var r Report
	if err := decodeStrict(data, &r); err != nil {
		return nil, fmt.Errorf("report %s: %w", path, err)
	}
	if err := r.validate(want); err != nil {
		return nil, fmt.Errorf("report %s: %w", path, err)
	}
	return &r, nil
}

func (r *Report) validate(want string) error {
	if r.JobID != want {
		return fmt.Errorf("job_id %q, want %q", r.JobID, want)
	}
	switch r.Status {
	case Success:
	case Failure:
		if r.Error == "" {
			return errors.New("status failure requires error")
		}
	case Blocked:
		if r.Question == "" {
			return errors.New("status blocked requires question")
		}
	default:
		return fmt.Errorf("status %q: want success, failure or blocked", r.Status)
	}
	return nil
}

// json.Unmarshal semantics plus DisallowUnknownFields: trailing data is an error.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the JSON value")
	}
	return nil
}
