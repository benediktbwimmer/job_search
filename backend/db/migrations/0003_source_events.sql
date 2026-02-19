CREATE TABLE IF NOT EXISTS source_fetch_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_type TEXT,
  source_url TEXT,
  attempts INTEGER NOT NULL,
  success INTEGER NOT NULL,
  jobs_fetched INTEGER NOT NULL,
  duration_ms INTEGER NOT NULL,
  error_message TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_source_fetch_events_run
  ON source_fetch_events(run_id);

CREATE INDEX IF NOT EXISTS idx_source_fetch_events_name
  ON source_fetch_events(source_name);
