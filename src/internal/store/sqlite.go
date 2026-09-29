package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var ErrNotFound = errors.New("not found")

// Fixed-width UTC so TEXT order is chronological; RFC3339Nano trims zeros and breaks that.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

const (
	jobCols      = "id, class, title, spec, recipe, status, attempts, max_attempts, pane_id, workspace_id, step, result_path, error, created_at, updated_at, started_at, finished_at"
	decisionCols = "id, job_id, question, response, created_at, asked_at, answered_at"
	eventCols    = "id, job_id, kind, data, at"
)

type sqliteStore struct {
	db *sql.DB
}

func Open(path string) (Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open store %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init store %s: %w", path, err)
	}
	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) Close() error { return s.db.Close() }

// Sole owner of job defaults (ID, status, max attempts, timestamps); they are written back into jobs.
func (s *sqliteStore) Enqueue(ctx context.Context, jobs []Job) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	defer tx.Rollback()
	ins, err := tx.PrepareContext(ctx, "INSERT INTO jobs ("+jobCols+") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	defer ins.Close()

	now := time.Now().UTC()
	nextID := now.UnixNano()
	for i := range jobs {
		j := &jobs[i]
		if j.ID == "" {
			j.ID = "j" + strconv.FormatInt(nextID, 36)
			nextID++
		}
		if j.Status == "" {
			j.Status = JobQueued
		}
		if j.MaxAttempts == 0 {
			j.MaxAttempts = DefaultMaxAttempts
		}
		if j.CreatedAt.IsZero() {
			j.CreatedAt = now
		}
		if j.UpdatedAt.IsZero() {
			j.UpdatedAt = now
		}
		if _, err := ins.ExecContext(ctx, j.ID, j.Class, j.Title, nullJSON(j.Spec), nullJSON(j.Recipe), j.Status,
			j.Attempts, j.MaxAttempts, j.PaneID, j.WorkspaceID, j.Step, j.ResultPath, j.Error,
			fmtTime(j.CreatedAt), fmtTime(j.UpdatedAt), fmtNullTime(j.StartedAt), fmtNullTime(j.FinishedAt)); err != nil {
			return fmt.Errorf("enqueue job %s: %w", j.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// One UPDATE picks and flips the row under SQLite's single write lock, and status=queued in
// its WHERE makes it a compare-and-set, so concurrent claimers can never win the same job.
func (s *sqliteStore) Claim(ctx context.Context) (*Job, error) {
	var j Job
	err := scanJob(s.db.QueryRowContext(ctx, `UPDATE jobs SET status = ?, updated_at = ?
		WHERE status = ? AND id = (SELECT id FROM jobs WHERE status = ? ORDER BY created_at, rowid LIMIT 1)
		RETURNING `+jobCols,
		JobDeciding, fmtTime(time.Now()), JobQueued, JobQueued), &j)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return &j, nil
}

// Full-row write of every column except id and created_at; stamps j.UpdatedAt.
func (s *sqliteStore) Update(ctx context.Context, j *Job) error {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET class = ?, title = ?, spec = ?, recipe = ?, status = ?,
		attempts = ?, max_attempts = ?, pane_id = ?, workspace_id = ?, step = ?, result_path = ?, error = ?,
		updated_at = ?, started_at = ?, finished_at = ?
		WHERE id = ?`,
		j.Class, j.Title, nullJSON(j.Spec), nullJSON(j.Recipe), j.Status,
		j.Attempts, j.MaxAttempts, j.PaneID, j.WorkspaceID, j.Step, j.ResultPath, j.Error,
		fmtTime(now), fmtNullTime(j.StartedAt), fmtNullTime(j.FinishedAt),
		j.ID)
	if err == nil {
		err = affected(res)
	}
	if err != nil {
		return fmt.Errorf("update job %s: %w", j.ID, err)
	}
	j.UpdatedAt = now
	return nil
}

func (s *sqliteStore) Get(ctx context.Context, id string) (*Job, error) {
	var j Job
	if err := scanJob(s.db.QueryRowContext(ctx, "SELECT "+jobCols+" FROM jobs WHERE id = ?", id), &j); err != nil {
		return nil, fmt.Errorf("get job %s: %w", id, notFound(err))
	}
	return &j, nil
}

func (s *sqliteStore) List(ctx context.Context, statuses ...JobStatus) ([]Job, error) {
	q := "SELECT " + jobCols + " FROM jobs"
	args := make([]any, len(statuses))
	for i, st := range statuses {
		args[i] = st
	}
	if len(statuses) > 0 {
		q += " WHERE status IN (?" + strings.Repeat(", ?", len(statuses)-1) + ")"
	}
	jobs, err := queryAll(ctx, s.db, scanJob, q+" ORDER BY created_at, rowid", args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

func (s *sqliteStore) Counts(ctx context.Context) (map[JobStatus]int, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT status, count(*) FROM jobs GROUP BY status")
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()
	counts := make(map[JobStatus]int)
	for rows.Next() {
		var st JobStatus
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, fmt.Errorf("count jobs: %w", err)
		}
		counts[st] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	return counts, nil
}

func (s *sqliteStore) CreateDecision(ctx context.Context, jobID, question string) (*Decision, error) {
	d := &Decision{JobID: jobID, Question: question, CreatedAt: time.Now().UTC()}
	res, err := s.db.ExecContext(ctx, "INSERT INTO decisions (job_id, question, created_at) VALUES (?, ?, ?)",
		jobID, question, fmtTime(d.CreatedAt))
	if err == nil {
		d.ID, err = res.LastInsertId()
	}
	if err != nil {
		return nil, fmt.Errorf("create decision for job %s: %w", jobID, err)
	}
	return d, nil
}

func (s *sqliteStore) MarkAsked(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, "UPDATE decisions SET asked_at = ? WHERE id = ?", fmtTime(time.Now()), id)
	if err == nil {
		err = affected(res)
	}
	if err != nil {
		return fmt.Errorf("mark decision %d asked: %w", id, err)
	}
	return nil
}

// Write-once: a second answer errors rather than replacing one the engine may already have acted on.
func (s *sqliteStore) AnswerDecision(ctx context.Context, id int64, response json.RawMessage) error {
	if len(response) == 0 {
		return fmt.Errorf("answer decision %d: empty response", id)
	}
	res, err := s.db.ExecContext(ctx, "UPDATE decisions SET response = ?, answered_at = ? WHERE id = ? AND response IS NULL",
		string(response), fmtTime(time.Now()), id)
	if err == nil {
		err = affected(res)
	}
	if errors.Is(err, ErrNotFound) {
		err = notFound(s.db.QueryRowContext(ctx, "SELECT 1 FROM decisions WHERE id = ?", id).Scan(new(int)))
		if err == nil {
			err = errors.New("already answered")
		}
	}
	if err != nil {
		return fmt.Errorf("answer decision %d: %w", id, err)
	}
	return nil
}

// The answered subset of ids, by id: unknown and still-pending ids are omitted.
func (s *sqliteStore) AnsweredDecisions(ctx context.Context, ids []int64) ([]Decision, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	q := "SELECT " + decisionCols + " FROM decisions WHERE id IN (?" + strings.Repeat(", ?", len(ids)-1) + ") AND response IS NOT NULL ORDER BY id"
	ds, err := queryAll(ctx, s.db, scanDecision, q, args...)
	if err != nil {
		return nil, fmt.Errorf("answered decisions: %w", err)
	}
	return ds, nil
}

// Unanswered only (response IS NULL), oldest first.
func (s *sqliteStore) PendingDecisions(ctx context.Context) ([]Decision, error) {
	ds, err := queryAll(ctx, s.db, scanDecision, "SELECT "+decisionCols+" FROM decisions WHERE response IS NULL ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("pending decisions: %w", err)
	}
	return ds, nil
}

// A class with no saved recipe yields nil, nil.
func (s *sqliteStore) ClassRecipe(ctx context.Context, class string) (json.RawMessage, error) {
	var recipe []byte
	err := s.db.QueryRowContext(ctx, "SELECT recipe FROM class_recipes WHERE class = ?", class).Scan(&recipe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("class recipe %s: %w", class, err)
	}
	return recipe, nil
}

func (s *sqliteStore) SaveClassRecipe(ctx context.Context, class string, recipe json.RawMessage) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO class_recipes (class, recipe, updated_at) VALUES (?, ?, ?)
		ON CONFLICT (class) DO UPDATE SET recipe = excluded.recipe, updated_at = excluded.updated_at`,
		class, nullJSON(recipe), fmtTime(time.Now()))
	if err != nil {
		return fmt.Errorf("save class recipe %s: %w", class, err)
	}
	return nil
}

func (s *sqliteStore) AppendEvent(ctx context.Context, jobID, kind string, data json.RawMessage) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO events (job_id, kind, data, at) VALUES (?, ?, ?, ?)",
		jobID, kind, nullJSON(data), fmtTime(time.Now()))
	if err != nil {
		return fmt.Errorf("append event %s for job %s: %w", kind, jobID, err)
	}
	return nil
}

func (s *sqliteStore) Events(ctx context.Context, jobID string) ([]EventRecord, error) {
	evs, err := queryAll(ctx, s.db, scanEvent, "SELECT "+eventCols+" FROM events WHERE job_id = ? ORDER BY id", jobID)
	if err != nil {
		return nil, fmt.Errorf("events for job %s: %w", jobID, err)
	}
	return evs, nil
}

type rowScanner interface{ Scan(dest ...any) error }

// JSON columns scan through *[]byte: json.RawMessage has no Scan method.
func scanJob(sc rowScanner, j *Job) error {
	return sc.Scan(&j.ID, &j.Class, &j.Title, (*[]byte)(&j.Spec), (*[]byte)(&j.Recipe), &j.Status,
		&j.Attempts, &j.MaxAttempts, &j.PaneID, &j.WorkspaceID, &j.Step, &j.ResultPath, &j.Error,
		textTime{&j.CreatedAt}, textTime{&j.UpdatedAt}, nullTextTime{&j.StartedAt}, nullTextTime{&j.FinishedAt})
}

func scanDecision(sc rowScanner, d *Decision) error {
	return sc.Scan(&d.ID, &d.JobID, &d.Question, (*[]byte)(&d.Response),
		textTime{&d.CreatedAt}, nullTextTime{&d.AskedAt}, nullTextTime{&d.AnsweredAt})
}

func scanEvent(sc rowScanner, e *EventRecord) error {
	return sc.Scan(&e.ID, &e.JobID, &e.Kind, (*[]byte)(&e.Data), textTime{&e.At})
}

func queryAll[T any](ctx context.Context, db *sql.DB, scan func(rowScanner, *T) error, query string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var v T
		if err := scan(rows, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func affected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// Bound as TEXT because the driver stores []byte as a BLOB; empty means NULL.
func nullJSON(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return string(m)
}

func fmtTime(t time.Time) string { return t.UTC().Format(timeLayout) }

func fmtNullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return fmtTime(*t)
}

type textTime struct{ t *time.Time }

func (tt textTime) Scan(src any) error {
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("time column holds %T, want TEXT", src)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*tt.t = t
	return nil
}

type nullTextTime struct{ t **time.Time }

func (nt nullTextTime) Scan(src any) error {
	if src == nil {
		*nt.t = nil
		return nil
	}
	t := new(time.Time)
	if err := (textTime{t}).Scan(src); err != nil {
		return err
	}
	*nt.t = t
	return nil
}
