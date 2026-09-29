package result

import (
	"errors"
	"fmt"
	"os"

	"github.com/tylers2222/drover/internal/jsonx"
)

// Load strictly decodes and validates the report at path; JobID must equal want.
func Load(path, want string) (*Report, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("job %s wrote no report at %s: %w", want, path, os.ErrNotExist)
	}
	if err != nil {
		return nil, fmt.Errorf("read report: %w", err)
	}
	var r Report
	if err := jsonx.DecodeStrict(data, &r); err != nil {
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
