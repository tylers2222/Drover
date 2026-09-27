// Package store: SQLite (WAL) is the source of truth. Every lifecycle
// transition is a row write before it is a side effect.
package store

import (
	"context"
	"encoding/json"
	"time"
)

type JobStatus string

const (
	JobQueued   JobStatus = "queued"
	JobDeciding JobStatus = "deciding"
	JobRunning  JobStatus = "running"
	JobDone     JobStatus = "done"
	JobBlocked  JobStatus = "blocked"
	JobFailed   JobStatus = "failed"
	JobAccepted JobStatus = "accepted"
	JobRejected JobStatus = "rejected"
)

// Spec is the caller's ticket payload, opaque to the engine. Recipe is nil
// until decided. Step is the index of the step currently executing.
type Job struct {
	ID          string          `json:"id"`
	Class       string          `json:"class,omitempty"`
	Title       string          `json:"title,omitempty"`
	Spec        json.RawMessage `json:"spec,omitempty"`
	Recipe      json.RawMessage `json:"recipe,omitempty"`
	Status      JobStatus       `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	PaneID      string          `json:"pane_id,omitempty"`
	WorkspaceID string          `json:"workspace_id,omitempty"`
	Step        int             `json:"step"`
	ResultPath  string          `json:"result_path,omitempty"`
	Error       string          `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
}

// Response is nil until the orchestrator answers via `drover decide`.
type Decision struct {
	ID         int64           `json:"id"`
	JobID      string          `json:"job_id"`
	Question   string          `json:"question"`
	Response   json.RawMessage `json:"response,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	AskedAt    *time.Time      `json:"asked_at,omitempty"`
	AnsweredAt *time.Time      `json:"answered_at,omitempty"`
}

type EventRecord struct {
	ID    int64           `json:"id"`
	JobID string          `json:"job_id"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data,omitempty"`
	At    time.Time       `json:"at"`
}

// Claim is atomic queued→deciding, oldest first; nil,nil when empty.
// Implemented in sqlite.go over the embedded schema.sql.
type Store interface {
	Enqueue(ctx context.Context, jobs []Job) error
	Claim(ctx context.Context) (*Job, error)
	Update(ctx context.Context, job *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	List(ctx context.Context, statuses ...JobStatus) ([]Job, error)
	Counts(ctx context.Context) (map[JobStatus]int, error)

	CreateDecision(ctx context.Context, jobID, question string) (*Decision, error)
	MarkAsked(ctx context.Context, id int64) error
	AnswerDecision(ctx context.Context, id int64, response json.RawMessage) error
	GetDecision(ctx context.Context, id int64) (*Decision, error)
	PendingDecisions(ctx context.Context) ([]Decision, error)

	ClassRecipe(ctx context.Context, class string) (json.RawMessage, error)
	SaveClassRecipe(ctx context.Context, class string, recipe json.RawMessage) error

	AppendEvent(ctx context.Context, jobID, kind string, data json.RawMessage) error
	Events(ctx context.Context, jobID string) ([]EventRecord, error)

	Close() error
}

func Open(path string) (Store, error) { return open(path) }
