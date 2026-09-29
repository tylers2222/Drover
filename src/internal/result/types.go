// Package result: the report a child writes to $HERD_RESULT before ending its
// final turn. Missing or invalid report = job failed, never silently accepted.
package result

type Status string

const (
	Success Status = "success"
	Failure Status = "failure"
	Blocked Status = "blocked"
)

type Report struct {
	JobID     string   `json:"job_id"`
	Status    Status   `json:"status"`
	Summary   string   `json:"summary"`
	Artifacts []string `json:"artifacts,omitempty"`
	Error     string   `json:"error,omitempty"`
	Question  string   `json:"question,omitempty"`
}
