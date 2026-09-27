-- Times are fixed-width UTC RFC3339 TEXT, so string order is chronological.
CREATE TABLE IF NOT EXISTS jobs (
	id           TEXT PRIMARY KEY,
	class        TEXT NOT NULL,
	title        TEXT NOT NULL,
	spec         TEXT,
	recipe       TEXT,
	status       TEXT NOT NULL,
	attempts     INTEGER NOT NULL,
	max_attempts INTEGER NOT NULL,
	pane_id      TEXT NOT NULL,
	workspace_id TEXT NOT NULL,
	step         INTEGER NOT NULL,
	result_path  TEXT NOT NULL,
	error        TEXT NOT NULL,
	created_at   TEXT NOT NULL,
	updated_at   TEXT NOT NULL,
	started_at   TEXT,
	finished_at  TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, created_at);

CREATE TABLE IF NOT EXISTS decisions (
	id          INTEGER PRIMARY KEY,
	job_id      TEXT NOT NULL REFERENCES jobs (id),
	question    TEXT NOT NULL,
	response    TEXT,
	created_at  TEXT NOT NULL,
	asked_at    TEXT,
	answered_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS decisions_pending ON decisions (id) WHERE response IS NULL;

CREATE TABLE IF NOT EXISTS class_recipes (
	class      TEXT PRIMARY KEY,
	recipe     TEXT NOT NULL,
	updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS events (
	id     INTEGER PRIMARY KEY,
	job_id TEXT NOT NULL,
	kind   TEXT NOT NULL,
	data   TEXT,
	at     TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS events_job ON events (job_id);
